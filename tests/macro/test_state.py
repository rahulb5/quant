"""Tests for src/macro/state.py and src/macro/inputs.py on synthetic data."""

import numpy as np
import pandas as pd
import pytest

from src.db.client import Database
from src.macro.inputs import (
    avg_yoy,
    avg_yoy_then_change,
    gap_to_expanding_median,
    load_state_inputs,
    yoy,
    yoy_then_change,
)
from src.macro.state import BURN_IN, expanding_z, macro_state


def _monthly_release_series(n=420, start="1990-01-15", seed=0, drift=0.0, scale=1.0):
    rng = np.random.default_rng(seed)
    idx = pd.date_range(start, periods=n, freq="MS") + pd.Timedelta(days=14)
    return pd.Series(drift + scale * rng.normal(size=n).cumsum() * 0.1 + rng.normal(size=n), index=idx)


def _inputs(seed=0):
    """A complete synthetic input dict (release-dated), 35 years."""
    d = {}
    for i, k in enumerate(["ip_yoy", "ip_yoy_d6", "pay_yoy", "pay_yoy_d6", "unr_gap", "unr_d6", "unr_d3",
                           "temp_6m", "hours_d6", "core_yoy", "core_yoy_d6", "sloos"]):
        d[k] = _monthly_release_series(seed=seed + i)
    for i, k in enumerate(["claims_yoy13_d26", "claims_yoy13", "cont_yoy13", "bank_credit_yoy"]):
        idx = pd.date_range("1990-01-12", periods=1800, freq="7D")
        d[k] = pd.Series(np.random.default_rng(100 + seed + i).normal(size=1800), index=idx)
    for i, k in enumerate(["dff", "hy_oas", "vix", "t10y3m"]):
        idx = pd.bdate_range("1990-01-02", periods=9000)
        d[k] = pd.Series(np.random.default_rng(200 + seed + i).normal(size=9000).cumsum() * 0.05 + 5, index=idx)
    d["core_yoy"] = d["core_yoy"].abs() + 1.0
    return d


IDX = pd.bdate_range("1990-01-02", "2024-06-28")


# ── expanding_z ───────────────────────────────────────────────────────────────

def test_expanding_z_burn_in_clip_and_values():
    s = pd.Series(np.arange(100.0), index=pd.date_range("2000-01-01", periods=100, freq="MS"))
    z = expanding_z(s, burn_in=10)
    assert z.index[0] == s.index[9]
    m, sd = s.iloc[:30].mean(), s.iloc[:30].std()
    assert z.loc[s.index[29]] == pytest.approx((s.iloc[29] - m) / sd)
    spike = s.copy(); spike.iloc[-1] = 1e6
    assert expanding_z(spike, 10).iloc[-1] == 3.0


def test_expanding_z_truncation_invariance_and_constant_series():
    s = _monthly_release_series(n=200)
    full = expanding_z(s, 60)
    cut = expanding_z(s.iloc[:120], 60)
    pd.testing.assert_series_equal(cut, full.iloc[: len(cut)])
    assert expanding_z(pd.Series(1.0, index=s.index), 60).empty


# ── macro_state ───────────────────────────────────────────────────────────────

def test_macro_state_columns_and_composition():
    st = macro_state(_inputs(), IDX)
    for c in ["G_level", "G_impulse", "pi_level", "pi_impulse", "L_level", "L_impulse",
              "P_stance", "P_impulse", "FC", "FC_impulse", "CS", "CS_impulse"]:
        assert c in st.columns and st[c].notna().any(), c
    assert st["G_level_n"].max() == 3 and st["G_impulse_n"].max() == 4
    assert st["L_impulse_n"].max() == 5 and st["FC_n"].max() == 3 and st["CS_n"].max() == 2
    assert set(st.attrs["valid_from"]) >= {"G_level", "FC", "P_stance"}


def test_growth_level_is_mean_of_signed_z_scores():
    inp = _inputs()
    st = macro_state(inp, IDX)
    t = pd.Timestamp("2015-06-30")
    from src.macro.pit import to_daily
    parts = [
        to_daily(expanding_z(inp["ip_yoy"], BURN_IN["monthly"]), IDX).loc[t],
        to_daily(expanding_z(inp["pay_yoy"], BURN_IN["monthly"]), IDX).loc[t],
        -to_daily(expanding_z(inp["unr_gap"], BURN_IN["monthly"]), IDX).loc[t],   # slack lowers growth
    ]
    assert st.loc[t, "G_level"] == pytest.approx(np.mean(parts))


def test_financial_conditions_needs_all_three_components():
    inp = _inputs()
    del inp["t10y3m"]
    st = macro_state(inp, IDX)
    assert st["FC"].isna().all() and (st["FC_n"] == 0).all()
    assert st["G_level"].notna().any()      # other dimensions unaffected


def test_credit_supply_needs_both_components():
    inp = _inputs()
    del inp["sloos"]
    assert macro_state(inp, IDX)["CS"].isna().all()


def test_policy_stance_and_impulse():
    inp = _inputs()
    st = macro_state(inp, IDX)
    t = pd.Timestamp("2010-03-31")
    from src.macro.pit import to_daily
    dff = to_daily(inp["dff"], IDX)
    core = to_daily(inp["core_yoy"], IDX)
    assert st.loc[t, "P_stance_raw"] == pytest.approx(dff.loc[t] - core.loc[t])
    assert st.loc[t, "P_impulse"] == pytest.approx(dff.loc[t] - dff.shift(126).loc[t])
    assert st.loc[t, "pi_level"] == pytest.approx(core.loc[t] - 2.0)
    # stance subtracts the expanding median of the real rate taken at CPI releases
    rel = inp["core_yoy"].index
    raw_m = (dff.reindex(dff.index.union(rel)).ffill().reindex(rel) - inp["core_yoy"]).dropna()
    med = raw_m.expanding(min_periods=60).median()
    expected = st.loc[t, "P_stance_raw"] - med[med.index <= t].iloc[-1]
    assert st.loc[t, "P_stance"] == pytest.approx(expected)


def test_macro_state_truncation_invariance():
    inp = _inputs()
    full = macro_state(inp, IDX)
    k = pd.Timestamp("2008-09-30")
    cut_inp = {n: s.loc[:k] for n, s in inp.items()}
    cut = macro_state(cut_inp, IDX[IDX <= k])
    value_cols = [c for c in full.columns if not c.endswith("_staleness")]
    pd.testing.assert_frame_equal(cut[value_cols], full.loc[:k, value_cols], check_exact=False, atol=1e-9)


def test_missing_and_empty_inputs_do_not_raise():
    st = macro_state({}, IDX)
    assert st.shape[0] == len(IDX) and st.drop(columns=[c for c in st if c.endswith("_n")]).isna().all().all()
    st = macro_state({"core_yoy": _inputs()["core_yoy"]}, IDX)
    assert st["pi_level"].notna().any() and st["P_stance"].isna().all()


def test_staleness_is_days_since_newest_input_release():
    st = macro_state(_inputs(), IDX)
    s = st["G_staleness"].dropna()
    assert (s >= 0).all() and s.max() < 80


# ── inputs: causal transforms and DB loading ──────────────────────────────────

def test_transforms_are_causal():
    s = pd.Series(np.random.default_rng(1).normal(size=400).cumsum() + 100,
                  index=pd.date_range("2000-01-01", periods=400, freq="MS"))
    w = pd.Series(np.random.default_rng(2).normal(size=700).cumsum() + 500,
                  index=pd.date_range("2000-01-01", periods=700, freq="7D"))
    for fn, x in [(yoy, s), (yoy_then_change(6), s), (gap_to_expanding_median, s),
                  (avg_yoy(13), w), (avg_yoy_then_change(13, 26), w)]:
        full = fn(x)
        cut = fn(x.iloc[: len(x) // 2])
        pd.testing.assert_series_equal(cut, full.loc[: cut.index[-1]], check_freq=False)


def test_load_state_inputs_builds_release_dated_series(tmp_path):
    path = str(tmp_path / "q.db")
    db = Database(path); db.open()
    db.run("INSERT INTO macro_series (id, code, name, source, frequency, units) VALUES "
           "(1,'PAYEMS','x','FRED','monthly','n'),(2,'DFF','x','FRED','daily','n')")
    months = pd.date_range("2018-01-01", periods=48, freq="MS")
    for i, d in enumerate(months):
        db.run("INSERT INTO macro_observations VALUES (1, ?, ?, ?, true)",
               [d.date(), (d + pd.Timedelta(days=35)).date(), 1000.0 + 10 * i])
    for i, d in enumerate(pd.bdate_range("2021-01-01", periods=60)):
        db.run("INSERT INTO macro_observations VALUES (2, ?, ?, ?, true)", [d.date(), d.date(), 0.1 + 0.01 * i])
    db.close()
    ro = Database(path, read_only=True)

    inp = load_state_inputs(ro)
    assert set(inp) == {"pay_yoy", "pay_yoy_d6", "dff"}          # series without data are omitted
    assert inp["pay_yoy"].index[0] == months[12] + pd.Timedelta(days=35)
    assert inp["pay_yoy"].iloc[0] == pytest.approx(120 / 1000 * 100)
    assert inp["dff"].iloc[0] == pytest.approx(0.1)


def test_state_reports_inputs_that_are_approximate_before_their_first_vintage():
    inp = _inputs()
    inp["cont_yoy13"].attrs.update(pre_vintage="latest_lagged", first_vintage=pd.Timestamp("2009-09-10"))
    st = macro_state(inp, IDX)
    assert st.attrs["approximate_before"] == {"cont_yoy13": pd.Timestamp("2009-09-10")}
    assert macro_state(_inputs(), IDX).attrs["approximate_before"] == {}
