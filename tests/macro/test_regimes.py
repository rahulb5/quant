"""Tests for src/macro/regimes.py and src/macro/flags.py on synthetic data."""

import numpy as np
import pandas as pd
import pytest

from src.backtest.regimes import Regime, VolRegime
from src.macro import flags as fl
from src.macro import regimes as rg

IDX = pd.bdate_range("1990-01-02", "2012-12-31")


def _s(values, idx=IDX):
    return pd.Series(values, index=idx, dtype=float)


def _panel(seed=0):
    rng = np.random.default_rng(seed)
    n = len(IDX)
    f = pd.DataFrame({
        "G_level": rng.normal(size=n).cumsum() * 0.05,
        "G_impulse": rng.normal(size=n).cumsum() * 0.05,
        "pi_impulse": rng.normal(size=n).cumsum() * 0.05,
        "P_stance": rng.normal(size=n).cumsum() * 0.05,
        "P_impulse": rng.normal(size=n).cumsum() * 0.05,
        "FC": rng.normal(size=n).cumsum() * 0.05,
        "L_impulse": rng.normal(size=n),
        "curve_state": np.where(rng.normal(size=n) > 0, "bull_steepener", "bear_flattener"),
        "usd_state": np.where(rng.normal(size=n) > 0, "strong_usd", "weak_usd"),
    }, index=IDX)
    return rg.StatePanel.from_frame(f)


# ── sign_hold ─────────────────────────────────────────────────────────────────

def test_sign_hold_zero_keeps_previous_and_nan_stays():
    x = pd.Series([np.nan, 0.0, 1.0, 0.0, 0.0, -2.0, 0.0, np.nan, 3.0])
    out = rg.sign_hold(x)
    assert np.isnan(out.iloc[0]) and np.isnan(out.iloc[1])     # no earlier sign to keep
    assert out.iloc[2:7].tolist() == [1, 1, 1, -1, -1]
    assert np.isnan(out.iloc[7]) and out.iloc[8] == 1


# ── min_duration ──────────────────────────────────────────────────────────────

def _lab(seq):
    return pd.Series(seq, index=pd.bdate_range("2000-01-03", periods=len(seq)), dtype=object)


def test_min_duration_switches_on_day_n_not_retroactively():
    raw = _lab(["a"] * 10 + ["b"] * 30)
    out = rg.min_duration_labels(raw, 21)
    assert (out.iloc[:30] == "a").all()          # raw switched at 10; confirmed on the 21st b = index 30
    assert (out.iloc[30:] == "b").all()


def test_min_duration_ignores_short_blips_and_resets_run():
    raw = _lab(["a"] * 30 + ["b"] * 20 + ["a"] * 5 + ["b"] * 20 + ["a"] * 30)
    out = rg.min_duration_labels(raw, 21)
    assert (out.iloc[:75] == "a").all()          # 20 b's, then a's, then 20 b's: never 21 in a row
    assert (out == "a").all()


def test_min_duration_nan_resets_and_passthrough():
    raw = _lab(["a"] * 5 + [np.nan] * 3 + ["b"] * 5)
    out = rg.min_duration_labels(raw, 21)
    assert out.iloc[:5].eq("a").all() and out.iloc[5:8].isna().all() and out.iloc[8:].eq("b").all()
    pd.testing.assert_series_equal(rg.min_duration_labels(raw, 1), raw)


def test_min_duration_is_truncation_invariant():
    rng = np.random.default_rng(3)
    raw = _lab(rng.choice(["a", "b", "c"], size=400, p=[0.45, 0.45, 0.1]).tolist())
    full = rg.min_duration_labels(raw, 21)
    for k in (50, 123, 300):
        pd.testing.assert_series_equal(rg.min_duration_labels(raw.iloc[:k], 21), full.iloc[:k])


# ── label functions ───────────────────────────────────────────────────────────

def test_growth_inflation_quadrants():
    g = _s(np.r_[np.full(30, 1.0), np.full(30, 1.0), np.full(30, -1.0), np.full(30, -1.0)], IDX[:120])
    p = _s(np.r_[np.full(30, -1.0), np.full(30, 1.0), np.full(30, 1.0), np.full(30, -1.0)], IDX[:120])
    out = rg.growth_inflation_labels(g, p, min_duration=1)
    assert [out.iloc[i] for i in (10, 40, 70, 100)] == ["goldilocks", "reflation", "stagflation", "deflation"]


def test_growth_inflation_default_min_duration_is_21():
    g = _s(np.ones(100), IDX[:100])
    p = _s(np.r_[np.full(50, -1.0), np.full(50, 1.0)], IDX[:100])
    out = rg.growth_inflation_labels(g, p)
    assert out.iloc[49] == "goldilocks" and out.iloc[69] == "goldilocks" and out.iloc[70] == "reflation"


def test_growth_level_labels():
    out = rg.growth_level_labels(_s([np.nan, 0.5, -0.5, 0.0, 0.3], IDX[:5]))
    assert np.isnan(out.iloc[0]) and out.iloc[1:].tolist() == ["above_trend", "below_trend", "below_trend", "above_trend"]


def test_policy_direction_held_inside_deadband_and_undefined_before_first_cross():
    imp = _s([0.1, 0.0, 0.3, 0.1, -0.1, -0.3, -0.1, 0.2, 0.25], IDX[:9])
    stance = _s([0.5, 0.5, 0.5, 0.5, -0.5, -0.5, -0.5, 0.5, 0.5], IDX[:9])
    out = rg.policy_labels(stance, imp)
    assert out.iloc[:2].isna().all()                                  # no direction yet
    assert out.iloc[2] == "restrictive_tightening" and out.iloc[3] == "restrictive_tightening"
    assert out.iloc[4] == "accommodative_tightening"                  # stance flips, direction held
    assert out.iloc[5] == "accommodative_easing" and out.iloc[6] == "accommodative_easing"
    assert out.iloc[7] == "restrictive_easing"                        # 0.2 < 0.25, still easing
    assert out.iloc[8] == "restrictive_tightening"                    # exactly +25 bp counts


def test_financial_conditions_terciles_and_burn_in():
    fc = _s(np.linspace(0, 10, len(IDX)))
    out = rg.financial_conditions_labels(fc, burn_in=100)
    assert out.iloc[:99].isna().all() and out.iloc[99:].notna().all()
    assert out.iloc[-1] == "tight"
    low = rg.financial_conditions_labels(fc[::-1].set_axis(IDX), burn_in=100)
    assert low.iloc[-1] == "loose"
    assert rg.financial_conditions_labels(_s([np.nan] * 10, IDX[:10])).empty


# ── classes ───────────────────────────────────────────────────────────────────

def test_registry_layers_and_names():
    assert set(rg.MACRO_REGIME_NAMES) <= set(rg.REGIME_REGISTRY)
    assert {"volatility", "rates_curve", "equity_trend"} <= set(rg.REGIME_REGISTRY)
    for name in rg.MACRO_REGIME_NAMES:
        cls = rg.REGIME_REGISTRY[name]
        assert issubclass(cls, Regime) and cls.name == name
    assert [rg.REGIME_REGISTRY[n].layer for n in rg.MACRO_REGIME_NAMES] == \
        ["macro", "macro", "macro", "macro", "market", "market"]
    assert VolRegime.layer == "market"


def test_macro_regimes_share_one_panel_and_label():
    panel = _panel()
    regs = rg.macro_regimes(panel=panel)
    assert [r.name for r in regs] == rg.MACRO_REGIME_NAMES
    assert all(r._panel is panel for r in regs)
    for r in regs:
        lab = r.labels()
        assert isinstance(lab, pd.Series) and lab.dropna().map(type).eq(str).all()
        assert lab.index.equals(IDX)
    assert {r.name for r in rg.macro_regimes(panel=panel, names=["policy"])} == {"policy"}
    with pytest.raises(KeyError):
        rg.macro_regimes(panel=panel, names=["nope"])


def test_start_end_slice_and_missing_column():
    panel = _panel()
    lab = rg.GrowthLevelRegime(panel=panel, start="2000-01-01", end="2005-12-31").labels()
    assert lab.index[0] >= pd.Timestamp("2000-01-01") and lab.index[-1] <= pd.Timestamp("2005-12-31")
    bad = rg.StatePanel.from_frame(panel.frame.drop(columns=["G_level"]))
    with pytest.raises(KeyError, match="G_level"):
        rg.GrowthLevelRegime(panel=bad).labels()


def test_regime_labels_truncation_invariant():
    panel = _panel()
    cut = IDX[3000]
    trunc = rg.StatePanel.from_frame(panel.frame.loc[:cut])
    for name in rg.MACRO_REGIME_NAMES:
        full = rg.REGIME_REGISTRY[name](panel=panel).labels().loc[:cut]
        short = rg.REGIME_REGISTRY[name](panel=trunc).labels()
        pd.testing.assert_series_equal(full, short, check_names=False)


# ── flags ─────────────────────────────────────────────────────────────────────

def test_sahm_indicator_known_values():
    u = pd.Series([4.0] * 15 + [4.2, 4.5, 4.9, 5.4], index=pd.date_range("2000-01-01", periods=19, freq="MS"))
    s = fl.sahm_indicator(u)
    assert s.iloc[:14].isna().all()
    assert s.iloc[14] == pytest.approx(0.0)
    # ma3 at index 18 = (4.5+4.9+5.4)/3 = 4.9333; min of prior 12 ma3 values = 4.0
    assert s.iloc[18] == pytest.approx((4.5 + 4.9 + 5.4) / 3 - 4.0)


def test_sahm_triggered_threshold_and_na():
    rel = pd.Series([0.1, 0.49, 0.5, 0.2], index=pd.to_datetime(["2000-02-04", "2000-03-03", "2000-04-07", "2000-05-05"]))
    idx = pd.bdate_range("2000-01-03", "2000-06-30")
    out = fl.sahm_triggered(rel, idx)
    assert out.dtype == "boolean" and out[:"2000-02-03"].isna().all()
    assert not out["2000-03-10"] and out["2000-04-10"] and not out["2000-05-10"]
    assert fl.sahm_triggered(None, idx).isna().all()


def test_labor_deteriorating_burn_in_and_threshold():
    x = _s(np.random.default_rng(1).normal(size=len(IDX)))
    out = fl.labor_deteriorating(x)
    assert out.iloc[:fl.LABOR_BURN_IN - 1].isna().all() and out.iloc[fl.LABOR_BURN_IN - 1:].notna().all()
    assert 0.1 < out.dropna().astype(bool).mean() < 0.3          # ~ top 20%
    spike = x.copy(); spike.iloc[-1] = 100
    assert fl.labor_deteriorating(spike).iloc[-1]


def test_curve_inverted_and_event_flags_frame():
    t = _s(np.r_[np.full(10, 1.0), np.full(10, -0.5), np.full(len(IDX) - 20, np.nan)])
    ci = fl.curve_inverted(t)
    assert not ci.iloc[0] and ci.iloc[15] and ci.iloc[30] is pd.NA
    panel = _panel()
    out = fl.event_flags(panel.frame, {"t10y3m": t.dropna()}, IDX)
    assert list(out.columns) == ["sahm_triggered", "labor_deteriorating", "curve_inverted"]
    assert all(out[c].dtype == "boolean" for c in out.columns)


def test_sahm_crosscheck_reports_differences():
    a = pd.Series([0.4, 0.6], index=pd.to_datetime(["2000-02-04", "2000-03-03"]))
    b = pd.Series([0.4, 0.45], index=pd.to_datetime(["2000-02-04", "2000-03-03"]))
    idx = pd.bdate_range("2000-02-01", "2000-04-28")
    x = fl.sahm_crosscheck(a, b, idx)
    assert x["diff"]["2000-02-10"] == pytest.approx(0.0) and x["diff"]["2000-03-10"] == pytest.approx(0.15)
    assert not x["flag_disagree"]["2000-02-10"] and x["flag_disagree"]["2000-03-10"]
    assert fl.sahm_crosscheck(None, b, idx)["diff"].isna().all()


# ── D1: min_duration on every regime except growth_level ──────────────────────

def _raw_labels(name, f):
    if name == "policy":
        return rg.policy_labels(f["P_stance"], f["P_impulse"])
    if name == "financial_conditions":
        return rg.financial_conditions_labels(f["FC"])
    return f["curve_state" if name == "curve_dynamics" else "usd_state"].astype(object)


@pytest.mark.parametrize("name", ["policy", "financial_conditions", "curve_dynamics", "dollar_trend"])
def test_min_duration_21_is_the_default_and_min_duration_1_gives_raw(name):
    from src.macro.validation import episodes
    panel = _panel(seed=4)
    cls = rg.REGIME_REGISTRY[name]
    raw = _raw_labels(name, panel.frame)
    assert rg.MIN_DURATION == 21
    pd.testing.assert_series_equal(cls(panel=panel, min_duration=1).labels(), raw, check_names=False)
    ruled = cls(panel=panel).labels()
    pd.testing.assert_series_equal(ruled, rg.min_duration_labels(raw, 21), check_names=False)
    assert len(episodes(ruled)) < len(episodes(raw))
    # after the first run, every episode is at least 21 business days long
    ep = episodes(ruled)
    assert (ep["n_days"].iloc[1:-1] >= 21).all()


def test_growth_level_has_no_rule_by_default_and_growth_inflation_keeps_21():
    panel = _panel(seed=4)
    gi = rg.GrowthInflationRegime(panel=panel)
    assert gi._min_duration == 21
    raw = rg.growth_level_labels(panel.frame["G_level"])
    pd.testing.assert_series_equal(rg.GrowthLevelRegime(panel=panel).labels(), raw, check_names=False)


def test_rule_takes_effect_on_the_21st_consecutive_day_i_e_20_days_after_the_raw_change():
    idx = pd.bdate_range("2000-01-03", periods=200)
    f = pd.DataFrame({"usd_state": ["strong_usd"] * 100 + ["weak_usd"] * 100}, index=idx)
    out = rg.DollarTrendRegime(panel=rg.StatePanel.from_frame(f)).labels()
    assert out.iloc[119] == "strong_usd" and out.iloc[120] == "weak_usd"      # raw switched at 100 (day 1 of the run); the run's 21st day is index 120


# ── reviewer-requested tests (C6 amendment review) ────────────────────────────

def test_growth_level_accepts_min_duration_and_everything_accepts_1_for_raw():
    panel = _panel(seed=4)
    raw = rg.growth_level_labels(panel.frame["G_level"])
    pd.testing.assert_series_equal(rg.GrowthLevelRegime(panel=panel, min_duration=1).labels(), raw, check_names=False)
    from src.macro.validation import episodes
    assert len(episodes(rg.GrowthLevelRegime(panel=panel, min_duration=21).labels())) <= len(episodes(raw))
    for name in rg.MACRO_REGIME_NAMES:
        rg.REGIME_REGISTRY[name](panel=panel, min_duration=1).labels()


def test_min_duration_is_truncation_invariant_with_switches():
    """Random runs of 5-45 days so labels really switch under the rule, and every cut is tried."""
    rng = np.random.default_rng(7)
    runs = np.repeat(rng.choice(["a", "b", "c"], size=100), rng.integers(5, 45, size=100))[:1500]
    raw = pd.Series(runs, index=pd.bdate_range("2000-01-03", periods=len(runs)), dtype=object)
    raw.iloc[700:705] = np.nan
    full = rg.min_duration_labels(raw, 21)
    v = full.dropna()
    assert (v != v.shift()).iloc[1:].sum() >= 10
    for k in range(1, len(raw) + 1, 7):
        pd.testing.assert_series_equal(rg.min_duration_labels(raw.iloc[:k], 21), full.iloc[:k])


def _persistent_panel(seed=0):
    f = _panel(seed).frame.copy()
    rng = np.random.default_rng(seed + 100)

    def runs(labels):
        return np.repeat(rng.choice(labels, size=len(f)), rng.integers(5, 60, size=len(f)))[:len(f)]
    f["curve_state"] = runs(["bull_steepener", "bear_steepener", "bull_flattener", "bear_flattener"])
    f["usd_state"] = runs(["strong_usd", "weak_usd"])
    return rg.StatePanel.from_frame(f)


def test_regime_labels_truncation_invariant_at_every_switch():
    panel = _persistent_panel()
    for name in rg.MACRO_REGIME_NAMES:
        full = rg.REGIME_REGISTRY[name](panel=panel).labels()
        v = full.dropna()
        sw = v.index[(v != v.shift()).to_numpy()][1:]
        assert len(sw) >= 5, name
        cuts = sorted({d for s in sw for d in full.index[max(0, full.index.get_loc(s) - 25): full.index.get_loc(s) + 1]})
        for cut in cuts[::3]:
            short = rg.REGIME_REGISTRY[name](panel=rg.StatePanel.from_frame(panel.frame.loc[:cut])).labels()
            pd.testing.assert_series_equal(full.loc[:cut], short, check_names=False)


def test_labels_do_not_depend_on_the_requested_start_date():
    panel = _persistent_panel(seed=2)
    for name in rg.MACRO_REGIME_NAMES:
        full = rg.REGIME_REGISTRY[name](panel=panel).labels()
        cut = rg.REGIME_REGISTRY[name](panel=panel, start="2004-01-01", end="2010-12-31").labels()
        pd.testing.assert_series_equal(cut, full.loc["2004-01-01":"2010-12-31"], check_names=False)
    names = [r.name for r in rg.macro_regimes(panel=panel, start="2004-01-01", end="2010-12-31")]
    assert names == rg.MACRO_REGIME_NAMES
    assert rg.macro_regimes(panel=panel, start="2004-01-01")[0].labels().index[0] >= pd.Timestamp("2004-01-01")


def test_event_flags_are_truncation_invariant():
    panel = _panel(seed=3)
    f = panel.frame
    sahm = pd.Series(np.random.default_rng(5).normal(0.3, 0.3, 270), index=pd.date_range("1990-02-05", periods=270, freq="MS") + pd.Timedelta(days=4))
    t10 = pd.Series(np.random.default_rng(6).normal(0, 1, len(IDX)), index=IDX)
    cut = IDX[3000]
    full = fl.event_flags(f, {"sahm": sahm, "t10y3m": t10}, IDX)
    trunc = fl.event_flags(f.loc[:cut], {"sahm": sahm[sahm.index <= cut], "t10y3m": t10[:cut]}, IDX[IDX <= cut])
    pd.testing.assert_frame_equal(full.loc[:cut], trunc)


def test_build_regimes_keeps_order_and_shares_one_panel():
    from src.backtest.regimes import VolRegime
    from src.macro.regimes import (
        CurveDynamicsRegime, PolicyRegime, REGIME_REGISTRY, StatePanel, build_regimes,
    )

    out = build_regimes(["volatility", "policy", "curve_dynamics"])
    assert [r.name for r in out] == ["volatility", "policy", "curve_dynamics"]
    assert isinstance(out[0], VolRegime) and isinstance(out[1], PolicyRegime) and isinstance(out[2], CurveDynamicsRegime)
    assert out[1]._panel is out[2]._panel and isinstance(out[1]._panel, StatePanel)
    assert [r.name for r in build_regimes(list(REGIME_REGISTRY))] == list(REGIME_REGISTRY)
    assert build_regimes([]) == []
    with pytest.raises(KeyError, match="nope"):
        build_regimes(["policy", "nope"])


def test_build_regimes_uses_a_supplied_panel():
    from src.macro.regimes import StatePanel, build_regimes

    panel = StatePanel.from_frame(pd.DataFrame())
    assert all(r._panel is panel for r in build_regimes(["policy", "dollar_trend"], panel=panel))
