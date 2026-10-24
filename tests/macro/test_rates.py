"""Tests for src/macro/rates.py on synthetic data."""

import numpy as np
import pandas as pd
import pytest

from src.macro import rates

IDX = pd.bdate_range("2003-01-02", "2010-12-31")


def _s(values, idx=IDX):
    return pd.Series(values, index=idx, dtype=float)


def _inputs(seed=0):
    rng = np.random.default_rng(seed)
    n = len(IDX)
    real = 1.0 + rng.normal(size=n).cumsum() * 0.02
    be = 2.0 + rng.normal(size=n).cumsum() * 0.01
    d = {"DFII10": _s(real), "T10YIE": _s(be), "DGS10": _s(real + be)}
    d["DFII5"], d["T5YIE"], d["DGS5"] = d["DFII10"] * 0.9, d["T10YIE"] * 0.9, (d["DFII10"] + d["T10YIE"]) * 0.9
    d["T5YIFR"] = _s(2.0 + rng.normal(size=n) * 0.01)
    d["DGS2"], d["DGS1"], d["DFF"] = (_s(3 + rng.normal(size=n).cumsum() * 0.01) for _ in range(3))
    d["T10Y2Y"] = d["DGS10"] - d["DGS2"]
    d["T10Y3M"] = _s(rng.normal(size=n))
    d["THREEFYTP10"] = _s(0.5 + rng.normal(size=n) * 0.05)
    return d


def test_decomposition_attributes_change_to_parts():
    d = rates.nominal_decomposition(_inputs(), IDX)
    assert d["resid"].abs().max() < 1e-9
    both = d.dropna(subset=["d_nominal"])
    assert (both["d_nominal"] - both["d_real"] - both["d_breakeven"]).abs().max() < 1e-9
    shares = both.dropna(subset=["share_real"])
    assert ((shares["share_real"] + shares["share_breakeven"]) - 1).abs().max() < 1e-9


def test_shares_only_above_10bp():
    d = rates.nominal_decomposition(_inputs(), IDX)
    small = d[d["d_nominal"].abs() <= 0.10]
    assert small["share_real"].isna().all() and small["share_breakeven"].isna().all()
    assert d["share_real"].notna().any()


def test_decomposition_5y_uses_5y_series():
    inp = _inputs()
    d5 = rates.nominal_decomposition(inp, IDX, tenor=5)
    assert np.allclose(d5["nominal"].dropna(), inp["DGS5"].reindex(d5["nominal"].dropna().index))


def test_missing_series_give_nan_not_error():
    d = rates.nominal_decomposition({}, IDX)
    assert d["nominal"].isna().all() and d["share_real"].isna().all()
    assert rates.curve({}, IDX)["curve_state"].isna().all()
    assert rates.treasury_proxy_returns({}, IDX).isna().all()


def test_changes_are_causal_3_month_differences():
    n = rates.CHANGE_WINDOW
    assert n == 63
    inp = _inputs()
    d = rates.nominal_decomposition(inp, IDX)
    assert d["d_real"].iloc[200] == pytest.approx(inp["DFII10"].iloc[200] - inp["DFII10"].iloc[200 - n])
    assert d["d_real"].iloc[:n].isna().all() and d["d_real"].iloc[n:].notna().all()


def test_truncation_invariance():
    inp = _inputs()
    cut = IDX[1000]
    full = rates.curve(inp, IDX)
    trunc = rates.curve({k: v[v.index <= cut] for k, v in inp.items()}, IDX[IDX <= cut])
    pd.testing.assert_frame_equal(full.loc[:cut], trunc)


def test_curve_state_labels():
    y10 = _s(np.r_[np.linspace(4, 3, 200), np.linspace(3, 5, len(IDX) - 200)])
    spread = _s(np.r_[np.linspace(1, 2, 200), np.linspace(2, 0, len(IDX) - 200)])
    c = rates.curve({"DGS10": y10, "T10Y2Y": spread}, IDX)
    assert c["curve_state"].iloc[:rates.CHANGE_WINDOW].isna().all()
    assert c["curve_state"].iloc[150] == "bull_steepener"
    assert c["curve_state"].iloc[-1] == "bear_flattener"


def test_curve_state_boundary_zero_change_is_bear_steepener():
    flat = _s(np.ones(len(IDX)))
    c = rates.curve({"DGS10": flat, "T10Y2Y": flat}, IDX)
    assert c["curve_state"].iloc[-1] == "bear_steepener"


def test_policy_expectations_and_term_premium():
    inp = _inputs()
    p = rates.policy_expectations(inp, IDX)
    assert p["policy_exp_2y"].iloc[10] == pytest.approx(inp["DGS2"].iloc[10] - inp["DFF"].iloc[10])
    assert p["policy_exp_1y"].iloc[10] == pytest.approx(inp["DGS1"].iloc[10] - inp["DFF"].iloc[10])
    tp = rates.term_premium(inp, IDX)
    assert tp["term_premium"].iloc[5] == pytest.approx(inp["THREEFYTP10"].iloc[5])


def test_treasury_proxy_formula():
    y = _s(np.r_[5.0, 5.0, 4.0, 4.0, 4.5, np.full(len(IDX) - 5, 4.5)])
    r = rates.treasury_proxy_returns({"DGS10": y}, IDX)
    assert np.isnan(r.iloc[0])
    assert r.iloc[1] == pytest.approx(5 / 100 / 252)
    assert r.iloc[2] == pytest.approx(5 / 100 / 252 + 8.5 * 0.01)       # yield fell 100 bp
    assert r.iloc[4] == pytest.approx(4 / 100 / 252 - 8.5 * 0.005)      # yield rose 50 bp


@pytest.mark.parametrize("fn", [
    rates.nominal_decomposition,
    lambda i, ix: rates.nominal_decomposition(i, ix, tenor=5),
    rates.term_premium, rates.policy_expectations, rates.treasury_proxy_returns,
])
def test_every_rates_function_is_truncation_invariant(fn):
    inp, cut = _inputs(), IDX[1500]
    full = fn(inp, IDX)
    trunc = fn({k: v[v.index <= cut] for k, v in inp.items()}, IDX[IDX <= cut])
    check = pd.testing.assert_series_equal if isinstance(full, pd.Series) else pd.testing.assert_frame_equal
    check(full.loc[:cut], trunc, check_names=False) if isinstance(full, pd.Series) else check(full.loc[:cut], trunc)
