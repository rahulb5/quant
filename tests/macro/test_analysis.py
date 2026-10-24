"""Tests for src/macro/analysis.py (C7): synthetic data with known answers."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from src.backtest.conditioning import REGIME_PERF_COLS, Conditioner, regime_performance
from src.macro.analysis import (
    GAP_LABELS,
    current_regime,
    divergence,
    divergence_forward_returns,
    episode_bootstrap_ci,
    episodes,
    forward_regression,
    forward_returns,
    forward_returns_by_regime,
    performance_by_regime,
    pricing_divergence,
    regime_durations,
    transition_matrix,
)


def _idx(n: int, start: str = "2000-01-03") -> pd.DatetimeIndex:
    return pd.bdate_range(start, periods=n)


def _blocks(spec: list[tuple[str, int]], start: str = "2000-01-03") -> pd.Series:
    vals = [lbl for lbl, n in spec for _ in range(n)]
    return pd.Series(vals, index=_idx(len(vals), start), dtype=object)


# ── forward returns and episodes ──────────────────────────────────────────────

def test_forward_returns_start_the_day_after():
    r = pd.Series([0.0, 0.10, 0.20, -0.5], index=_idx(4))
    fwd = forward_returns(r, 2)
    assert fwd.iloc[0] == pytest.approx(1.1 * 1.2 - 1)      # r_1, r_2: excludes r_0
    assert fwd.iloc[1] == pytest.approx(1.2 * 0.5 - 1)
    assert fwd.iloc[2:].isna().all()


def test_episodes_nan_breaks_runs():
    s = pd.Series(["a", "a", np.nan, "a", "b", "b"], index=_idx(6), dtype=object)
    ep = episodes(s)
    assert ep["label"].tolist() == ["a", "a", "b"]
    assert ep["length"].tolist() == [2, 1, 2]
    assert ep["pos"].tolist() == [0, 3, 4]


def test_episodes_empty():
    assert episodes(pd.Series([], dtype=object)).empty
    assert episodes(pd.Series([np.nan] * 3, index=_idx(3), dtype=object)).empty


# ── 1. forward regression ─────────────────────────────────────────────────────

def test_forward_regression_recovers_slope_and_enforces_lag():
    rng = np.random.default_rng(1)
    n = 3000
    idx = _idx(n)
    x = pd.Series(rng.standard_normal(n), index=idx, name="x")
    r = pd.Series(0.002 * x.shift(1).fillna(0) + 0.001 * rng.standard_normal(n), index=idx)
    out = forward_regression(x, r, horizons=(1, 21))
    assert out.loc[(1, "x"), "coef"] == pytest.approx(0.002, rel=0.05)
    assert out.loc[(1, "x"), "t_stat_hac"] > 10
    assert out.loc[(21, "x"), "hac_lag"] == 21
    assert out.loc[(21, "x"), "n_nonoverlap"] == pytest.approx(np.ceil(out.loc[(21, "x"), "n_obs"] / 21))
    assert np.isfinite(out.loc[(21, "x"), "coef_nonoverlap"])
    with pytest.raises(ValueError, match="hac_lag"):
        forward_regression(x, r, horizons=(21, 63), hac_lag=21)
    assert forward_regression(x, r, horizons=(21,), hac_lag=40).loc[(21, "x"), "hac_lag"] == 40


def test_forward_regression_hac_wider_than_ols_on_overlap():
    """Overlapping 63-day returns on a persistent regressor: HAC t-stat must be smaller than the
    naive OLS one (it would not be if the lag were the ceil(n^0.25) default)."""
    import statsmodels.api as sm

    rng = np.random.default_rng(2)
    n = 4000
    idx = _idx(n)
    x = pd.Series(np.cumsum(rng.standard_normal(n)) / 30, index=idx, name="x")
    r = pd.Series(0.01 * rng.standard_normal(n), index=idx)
    out = forward_regression(x, r, horizons=(63,))
    data = pd.concat([forward_returns(r, 63), x], axis=1).dropna()
    naive = sm.OLS(data.iloc[:, 0].to_numpy(), sm.add_constant(data["x"].to_numpy())).fit()
    assert abs(out.loc[(63, "x"), "t_stat_hac"]) < abs(naive.tvalues[1])


def test_forward_regression_short_history_gives_nan():
    idx = _idx(20)
    out = forward_regression(pd.Series(range(20), index=idx, name="x", dtype=float),
                             pd.Series(0.01, index=idx), horizons=(5,))
    assert out["coef"].isna().all()


# ── 2. regime tables ──────────────────────────────────────────────────────────

def test_shared_helper_matches_conditioner_by_regime():
    rng = np.random.default_rng(3)
    lbl = _blocks([("a", 50), ("b", 30), ("a", 40), ("b", 80)])
    r = pd.Series(0.01 * rng.standard_normal(len(lbl)), index=lbl.index)

    class _Fixed:
        name = "fixed"

        def labels(self):
            return lbl

    cond = Conditioner(r, regimes=[_Fixed()], reference=object())
    pd.testing.assert_frame_equal(cond.by_regime()["fixed"], regime_performance(lbl, r))


def test_performance_by_regime_counts_episodes():
    lbl = _blocks([("a", 50), ("b", 30), ("a", 40), ("b", 80), ("c", 10)])
    r = pd.Series(0.001, index=lbl.index)
    t = performance_by_regime(lbl, r)
    assert list(t.columns) == REGIME_PERF_COLS
    assert t.loc["a", "n_episodes"] == 2 and t.loc["a", "mean_episode_length"] == 45
    assert t.loc["b", "n_episodes"] == 2 and t.loc["b", "n_obs"] == 110
    assert t.loc["c", "n_episodes"] == 1
    assert t["pct_time"].sum() == pytest.approx(1.0)


def test_performance_by_regime_empty():
    r = pd.Series(0.001, index=_idx(10))
    assert performance_by_regime(pd.Series([np.nan] * 10, index=r.index, dtype=object), r).empty


# ── 3. episode bootstrap ──────────────────────────────────────────────────────

def test_bootstrap_two_episodes_gives_wide_ci():
    """Two 'a' episodes with very different means: the episode CI spans both episode means,
    while a day-level bootstrap would give a tight CI around the pooled mean."""
    rng = np.random.default_rng(4)
    lbl = _blocks([("a", 200), ("b", 100), ("a", 200)])
    r = pd.Series(0.0005 * rng.standard_normal(len(lbl)), index=lbl.index)
    r.iloc[:200] += 0.004
    r.iloc[300:] -= 0.002
    out = episode_bootstrap_ci(lbl, r, n_boot=500)
    a = out.loc["a"]
    assert a["n_episodes"] == 2 and bool(a["few_episodes"])
    assert a["mean_lo"] < -0.002 * 252 * 0.9 and a["mean_hi"] > 0.004 * 252 * 0.9
    day_level = np.std([rng.choice(r.iloc[np.r_[0:200, 300:500]].to_numpy(), 400).mean() * 252
                        for _ in range(300)])
    assert (a["mean_hi"] - a["mean_lo"]) > 10 * day_level
    assert {"sharpe", "sharpe_lo", "sharpe_hi"} <= set(out.columns)


def test_bootstrap_many_episodes_brackets_truth_and_is_seeded():
    rng = np.random.default_rng(5)
    lbl = _blocks([("a", 20), ("b", 20)] * 30)
    r = pd.Series(0.001 + 0.01 * rng.standard_normal(len(lbl)), index=lbl.index)
    out = episode_bootstrap_ci(lbl, r, stat=("mean",), n_boot=400, seed=7)
    assert not out.loc["a", "few_episodes"]
    assert out.loc["a", "mean_lo"] < out.loc["a", "mean"] < out.loc["a", "mean_hi"]
    pd.testing.assert_frame_equal(out, episode_bootstrap_ci(lbl, r, stat=("mean",), n_boot=400, seed=7))
    with pytest.raises(ValueError):
        episode_bootstrap_ci(lbl, r, stat=("median",))


# ── 4. forward returns by regime entry ────────────────────────────────────────

def test_forward_returns_by_regime_entry():
    lbl = _blocks([("a", 30), ("b", 30)] * 10)
    r = pd.Series(0.0, index=lbl.index)
    # every 'a' entry is followed by +1% on each of the next 5 days
    for p in episodes(lbl).query("label == 'a'")["pos"]:
        r.iloc[p + 1:p + 6] = 0.01
    out = forward_returns_by_regime(lbl, r, horizons=(5,))
    # the first 'a' episode starts the sample, so it is not an entry: 9 switches into 'a'
    assert out.loc[(5, "a"), "mean_fwd"] == pytest.approx(1.01 ** 5 - 1)
    assert out.loc[(5, "a"), "n_entries"] == 9 and out.loc[(5, "a"), "n_episodes"] == 10
    assert out.loc[(5, "b"), "mean_fwd"] == pytest.approx(0.0)
    assert out.loc[(5, "a"), "excess"] > 0
    assert out.loc[(5, "a"), "overlap_days"] == 5


def test_entries_are_switches_only():
    s = pd.Series(["a"] * 5 + [np.nan] * 3 + ["a"] * 5 + ["b"] * 5 + ["a"] * 30, index=_idx(48), dtype=object)
    r = pd.Series(0.001, index=s.index)
    out = forward_returns_by_regime(s, r, horizons=(5,))
    assert out.loc[(5, "a"), "n_entries"] == 1          # sample start and post-NaN resumption excluded
    assert out.loc[(5, "a"), "n_episodes"] == 3
    assert out.loc[(5, "b"), "n_entries"] == 1


def test_forward_regression_does_not_fill_sparse_state():
    idx = _idx(3000)
    rng = np.random.default_rng(12)
    r = pd.Series(0.01 * rng.standard_normal(3000), index=idx)
    x = pd.Series(rng.standard_normal(3000), index=idx, name="x").iloc[::21]   # monthly-like
    out = forward_regression(x, r, horizons=(5,))
    assert out.loc[(5, "x"), "n_obs"] == forward_returns(r, 5).reindex(x.index).notna().sum()


def test_entry_se_counts_close_entries_as_overlapping():
    """Same daily variance: entries 1 day apart share almost every day of their 21-day windows,
    so their SE must be ~sqrt(n) times larger than for entries 100 days apart, never smaller."""
    from src.macro.analysis import _overlap_se

    far = _overlap_se(np.arange(6) * 100.0, 21, 1e-4)
    close = _overlap_se(np.arange(6) * 1.0, 21, 1e-4)
    assert far == pytest.approx(np.sqrt(1e-4 * 21 / 6))
    assert close > 2 * far
    assert close <= np.sqrt(1e-4 * 21)              # never above "one entry"


def test_entry_tstat_nonzero_on_noise_entries():
    rng = np.random.default_rng(11)
    lbl = _blocks([("a", 3), ("b", 3)] * 200)       # entries every 6 days: heavy overlap at h=21
    r = pd.Series(0.01 * rng.standard_normal(len(lbl)), index=lbl.index)
    out = forward_returns_by_regime(lbl, r, horizons=(21,))
    assert abs(out.loc[(21, "a"), "t_stat"]) < 4                     # no spurious significance


# ── 5. pricing divergence ─────────────────────────────────────────────────────

def test_divergence_sign_and_columns():
    idx = _idx(2000)
    rng = np.random.default_rng(6)
    m = pd.Series(rng.standard_normal(2000), index=idx)
    k = pd.Series(rng.standard_normal(2000), index=idx)
    out = divergence(m, k, burn_in=100)
    assert list(out.columns) == ["macro_z", "market_z", "gap", "gap_z", "gap_tercile"]
    np.testing.assert_allclose(out["gap"], m - k)
    assert out["gap_z"].iloc[:99].isna().all() and out["gap_z"].iloc[99:].notna().all()
    assert set(out["gap_tercile"].dropna()) == set(GAP_LABELS)
    hi = out["gap_tercile"] == "data_ahead"
    assert (out.loc[hi, "gap_z"].mean() > out.loc[out["gap_tercile"] == "market_ahead", "gap_z"].mean())


def test_divergence_truncation_invariance():
    idx = _idx(1500)
    rng = np.random.default_rng(8)
    m = pd.Series(np.cumsum(rng.standard_normal(1500)) / 10, index=idx)
    k = pd.Series(rng.standard_normal(1500), index=idx)
    full = divergence(m, k, burn_in=200)
    for cut in (400, 900):
        part = divergence(m.iloc[:cut], k.iloc[:cut], burn_in=200)
        pd.testing.assert_frame_equal(part, full.iloc[:cut])


def test_pricing_divergence_maps_dimensions_and_truncates_cleanly():
    idx = _idx(1200)
    rng = np.random.default_rng(9)
    state = pd.DataFrame({c: np.cumsum(rng.standard_normal(1200)) for c in ("G_impulse", "pi_impulse")},
                         index=idx)
    market = pd.DataFrame({c: rng.standard_normal(1200) for c in ("growth_pricing", "inflation_pricing",
                                                                 "policy_pricing")}, index=idx)
    out = pricing_divergence(state, market, burn_in=100)
    assert set(out) == {"growth", "inflation"}          # policy skipped: no P_impulse
    part = pricing_divergence(state.iloc[:700], market.iloc[:700], burn_in=100)
    for dim in out:
        pd.testing.assert_frame_equal(part[dim], out[dim].iloc[:700])


def test_pricing_divergence_truncation_mixed_grids():
    """Monthly step macro series (late start, interior NaN) vs a market series on another grid."""
    rng = np.random.default_rng(0)
    idx = pd.bdate_range("2000-01-03", periods=2500)
    m = pd.Series(np.nan, index=idx)
    mi = idx[200::21]
    m[mi] = np.cumsum(rng.standard_normal(len(mi)))
    m = m.ffill()
    m.iloc[1200:1210] = np.nan
    tidx = idx[idx.dayofweek != 0]
    market = pd.DataFrame({"inflation_pricing": pd.Series(rng.standard_normal(len(tidx)), index=tidx)})
    market.iloc[:300] = np.nan
    state = pd.DataFrame({"pi_impulse": m})
    full = pricing_divergence(state, market, burn_in=100)["inflation"]
    for cut in (900, 1205, 1800):
        d = idx[cut]
        part = pricing_divergence(state[:d], market[:d], burn_in=100)["inflation"]
        pd.testing.assert_frame_equal(part, full.loc[:d], check_freq=False)


def test_pricing_divergence_growth_used_as_is():
    idx = _idx(600)
    rng = np.random.default_rng(13)
    state = pd.DataFrame({"G_impulse": rng.standard_normal(600)}, index=idx)
    market = pd.DataFrame({"growth_pricing": rng.standard_normal(600)}, index=idx)
    out = pricing_divergence(state, market, burn_in=100)["growth"]
    pd.testing.assert_series_equal(out["macro_z"], state["G_impulse"], check_names=False)
    assert out["gap_tercile"].iloc[:99].isna().all() and out["gap_tercile"].iloc[99:].notna().all()


def test_divergence_forward_returns_by_tercile():
    idx = _idx(3000)
    rng = np.random.default_rng(10)
    gap = divergence(pd.Series(rng.standard_normal(3000), index=idx),
                     pd.Series(0.0, index=idx), burn_in=100)
    hi = (gap["gap_tercile"] == "data_ahead").shift(1, fill_value=False)
    r = pd.Series(0.0005 * rng.standard_normal(3000), index=idx) + 0.002 * hi
    out = divergence_forward_returns(gap, r, horizons=(1, 5))
    assert out.loc[(1, "data_ahead"), "mean_fwd"] == pytest.approx(0.002, abs=2e-4)
    assert out.loc[(1, "market_ahead"), "mean_fwd"] == pytest.approx(0.0, abs=2e-4)
    assert out.loc[(1, "spread"), "t_stat_hac"] > 10
    assert out.loc[(5, "data_ahead"), "hac_lag"] == 5
    assert set(out.index.get_level_values("tercile")) == set(GAP_LABELS) | {"spread"}


# ── 6. transitions, durations, current ────────────────────────────────────────

def test_transition_matrix_ignores_gaps():
    s = pd.Series(["a", "a", "b", "a", np.nan, "b", "c", "c"], index=_idx(8), dtype=object)
    counts = transition_matrix(s, normalize=False)
    assert counts.loc["a", "b"] == 1 and counts.loc["b", "a"] == 1 and counts.loc["b", "c"] == 1
    assert counts.loc["a", "a"] == 0 and counts.values.sum() == 3      # a -> NaN -> b not counted
    probs = transition_matrix(s)
    assert probs.loc["b"].sum() == pytest.approx(1.0)
    assert probs.loc["c"].isna().all()                                 # c never left


def test_regime_durations_and_current():
    s = _blocks([("a", 10), ("b", 5), ("a", 20), ("b", 7)])
    d = regime_durations(s)
    assert d.loc["a", "n_episodes"] == 2 and d.loc["a", "mean"] == 15 and d.loc["a", "max"] == 20
    assert d["pct_time"].sum() == pytest.approx(1.0)
    assert bool(d.loc["b", "last_censored"]) and not bool(d.loc["a", "last_censored"])
    cur = current_regime(s)
    assert cur == {"label": "b", "since": s.index[35], "days_in_regime": 7}
    assert current_regime(pd.Series([], dtype=object))["label"] is None
    tail_nan = pd.concat([s, pd.Series([np.nan], index=_idx(1, "2010-01-04"), dtype=object)])
    assert current_regime(tail_nan)["label"] is None
    assert regime_durations(pd.Series([], dtype=object)).empty
