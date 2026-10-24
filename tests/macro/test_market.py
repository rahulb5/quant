"""Tests for src/macro/market.py and the market loaders on synthetic data."""

import numpy as np
import pandas as pd
import pytest

from src.db.client import Database
from src.macro import market
from src.macro.inputs import load_market_inputs, load_prices

IDX = pd.bdate_range("2000-01-03", "2012-12-31")


def _walk(seed, n=len(IDX), scale=0.05, start=5.0, idx=IDX):
    return pd.Series(start + np.random.default_rng(seed).normal(size=n).cumsum() * scale, index=idx)


def _weekly(seed, start="2006-06-16", n=500):
    idx = pd.date_range(start, periods=n, freq="7D")
    return pd.Series(np.random.default_rng(seed).normal(size=n) * 10, index=idx)


def _inputs():
    d = {k: _walk(i) for i, k in enumerate(
        ["BAMLH0A0HYM2", "BAMLC0A0CM", "BAMLC0A4CBBB", "DTWEXBGS", "VIXCLS", "T10Y2Y", "T10YIE", "DGS2", "DFF"])}
    for i, k in enumerate(["spx", "rut", "copper", "gold"]):
        d[k] = np.exp(_walk(50 + i, scale=0.01, start=5))
    for i, k in enumerate(market.POSITIONING):
        d[k] = _weekly(100 + i)
    return d


def test_credit_and_dollar_definitions():
    inp = _inputs()
    c = market.credit(inp, IDX)
    assert c["bbb_minus_ig"].iloc[50] == pytest.approx(inp["BAMLC0A4CBBB"].iloc[50] - inp["BAMLC0A0CM"].iloc[50])
    assert c["d_hy_oas"].iloc[100] == pytest.approx(inp["BAMLH0A0HYM2"].iloc[100] - inp["BAMLH0A0HYM2"].iloc[37])
    d = market.dollar(inp, IDX)
    assert d["usd_gap"].iloc[:199].isna().all() and d["usd_gap"].iloc[199:].notna().all()
    usd = inp["DTWEXBGS"]
    assert d["usd_gap"].iloc[300] == pytest.approx(usd.iloc[300] / usd.iloc[101:301].mean() - 1)
    assert ((d["usd_gap"] > 0) == (d["usd_state"] == "strong_usd")).all()


def test_dollar_zero_gap_is_weak():
    flat = pd.Series(100.0, index=IDX)
    assert market.dollar({"DTWEXBGS": flat}, IDX)["usd_state"].iloc[-1] == "weak_usd"


def test_volatility_burn_in_and_labels():
    v = market.volatility(_inputs(), IDX)
    assert v["vol_regime"].first_valid_index() == IDX[market.VOL_BURN_IN - 1]
    assert set(v["vol_regime"].dropna()) <= set(market.VOL_LABELS)


def test_equity_trend_labels():
    up = pd.Series(np.linspace(100, 200, len(IDX)), index=IDX)
    t = market.equity_trend({"spx": up}, IDX)
    assert t.iloc[:199].isna().all() and (t.iloc[199:] == "risk_on").all()
    down = up[::-1].set_axis(IDX)
    assert (market.equity_trend({"spx": down}, IDX).iloc[199:] == "risk_off").all()


def test_growth_pricing_needs_two_components_and_signs():
    inp = _inputs()
    g = market.growth_pricing(inp, IDX)
    assert g["growth_pricing"].notna().any() and g["growth_pricing_n"].max() == 4
    assert g.loc[g["growth_pricing"].notna(), "growth_pricing_n"].min() >= 2
    one = market.growth_pricing({"T10Y2Y": inp["T10Y2Y"]}, IDX)
    assert one["growth_pricing"].isna().all()
    # wider HY spread (worse credit) lowers growth pricing
    rising = {"T10Y2Y": pd.Series(np.random.default_rng(2).normal(size=len(IDX)) * 0.05, index=IDX), "BAMLH0A0HYM2": pd.Series(np.r_[np.zeros(len(IDX) - 30), np.linspace(0, 5, 30)] + np.random.default_rng(1).normal(size=len(IDX)) * 0.05, index=IDX)}
    assert market.growth_pricing(rising, IDX)["growth_pricing"].dropna().iloc[-1] < 0


def test_pricing_z_scores_and_missing_inputs():
    inp = _inputs()
    assert market.inflation_pricing(inp, IDX).notna().any()
    assert market.policy_pricing(inp, IDX).abs().max() <= 3.0
    assert market.inflation_pricing({}, IDX).isna().all()
    assert market.policy_pricing({"DGS2": inp["DGS2"]}, IDX).isna().all()
    assert market.market_state({}, IDX).drop(columns=["growth_pricing_n", "usd_approx"]).isna().all().all()


def test_positioning_burn_in_and_columns():
    p = market.positioning(_inputs(), IDX)
    assert list(p.columns) == list(market.POSITIONING)
    first = _inputs()["pos_es"].index[155]
    assert p["pos_es"].first_valid_index() == first      # 156th weekly observation
    assert p.abs().max().max() <= 3.0


def test_truncation_invariance():
    inp = _inputs()
    cut = IDX[2000]
    full = market.market_state(inp, IDX).loc[:cut]
    trunc_in = {k: v[v.index <= cut] for k, v in inp.items()}
    trunc = market.market_state(trunc_in, IDX[IDX <= cut])
    pd.testing.assert_frame_equal(full.drop(columns=["growth_pricing_n"]), trunc.drop(columns=["growth_pricing_n"]), check_dtype=False)


def test_loaders(tmp_path):
    path = str(tmp_path / "q.db")
    db = Database(path); db.open()
    db.run("INSERT INTO macro_series (id, code, name, source, frequency, units, description) VALUES "
           "(1,'CFTC.TFF_FUT.13874A','ES','CFTC','weekly','contracts',NULL)")
    for i, d in enumerate(pd.date_range("2020-01-07", periods=5, freq="7D")):
        db.run("""INSERT INTO cot_positions (series_id, report_date, release_date, report_type, market_name,
                  open_interest, tff_lf_long, tff_lf_short) VALUES (1, ?, ?, 'tff_fut', 'ES', 1000, ?, 100)""",
               [d.date(), (d + pd.Timedelta(days=3)).date(), 200 + i * 10])
    inp = load_market_inputs(db)
    db.close()
    s = inp["pos_es"]
    assert s.index[0] == pd.Timestamp("2020-01-10")            # report Tuesday + 3
    assert s.iloc[0] == pytest.approx(10.0)                    # (200-100)/1000*100
    assert "VIXCLS" not in inp and "spx" not in inp            # missing series are omitted


def test_dollar_flags_approximate_period_before_first_vintage():
    usd = _walk(7, start=100, scale=0.1)
    usd.attrs["first_vintage"] = pd.Timestamp("2005-06-01")
    d = market.dollar({"DTWEXBGS": usd}, IDX)
    assert d.loc[d.index < "2005-06-01", "usd_approx"].eq(d["usd_gap"].notna()[d.index < "2005-06-01"]).all()
    assert not d.loc[d.index >= "2005-06-01", "usd_approx"].any()
    assert d["usd_approx"].dtype == bool
    assert not market.dollar({}, IDX)["usd_approx"].any()
