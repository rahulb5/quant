"""tests/backtest/test_stats.py"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from src.backtest.metrics import sharpe_ratio
from src.backtest.stats import (
    block_bootstrap_sharpe_ci,
    deflated_sharpe_ratio,
    gaussian_top_trim_shift,
    sharpe_excluding_best_year,
    sharpe_excluding_tails,
    sharpe_excluding_top,
)


def _returns(mean=0.001, std=0.01, n=1000, seed=0) -> pd.Series:
    rng = np.random.default_rng(seed)
    idx = pd.bdate_range("2018-01-01", periods=n)
    return pd.Series(rng.normal(mean, std, n), index=idx)


def test_bootstrap_ci_brackets_point_estimate():
    r = _returns()
    low, high = block_bootstrap_sharpe_ci(r, periods_per_year=252, n_boot=500)
    assert low < sharpe_ratio(r.to_numpy()) < high


def test_bootstrap_ci_is_reproducible_for_a_seed():
    r = _returns()
    assert block_bootstrap_sharpe_ci(r, n_boot=300, seed=3) == block_bootstrap_sharpe_ci(r, n_boot=300, seed=3)


def test_bootstrap_ci_nan_when_too_short():
    low, high = block_bootstrap_sharpe_ci(pd.Series([0.01, 0.02, -0.01]))
    assert np.isnan(low) and np.isnan(high)


def test_deflated_sharpe_decreases_with_more_trials():
    r = _returns(mean=0.0004)
    d1, d10, d100 = (deflated_sharpe_ratio(r, n_trials=n) for n in (1, 10, 100))
    assert d1 > d10 > d100


def test_deflated_sharpe_extremes():
    assert deflated_sharpe_ratio(_returns(mean=0.003), n_trials=10) > 0.99
    assert deflated_sharpe_ratio(_returns(mean=-0.003), n_trials=10) < 0.01


def test_deflated_sharpe_single_trial_is_probabilistic_sharpe_vs_zero():
    r = _returns(mean=0.0, seed=5)
    assert 0.0 <= deflated_sharpe_ratio(r, n_trials=1) <= 1.0


def test_deflated_sharpe_nan_for_constant_returns():
    assert np.isnan(deflated_sharpe_ratio(pd.Series([0.01] * 20)))


def test_sharpe_excluding_top_drops_the_outlier():
    r = _returns(mean=0.0, n=500)
    r.iloc[10] = 0.5
    with_outlier = sharpe_ratio(r.to_numpy())
    without = sharpe_excluding_top(r, frac=0.002)  # 1 of 500 periods
    assert without < with_outlier
    assert without == pytest.approx(sharpe_ratio(r.drop(r.index[10]).to_numpy()))


def test_sharpe_excluding_best_year_removes_that_year():
    r = _returns(mean=0.0, n=756)
    best_year = 2019
    r[r.index.year == best_year] += 0.01
    sharpe, year = sharpe_excluding_best_year(r)
    assert year == best_year
    assert sharpe < sharpe_ratio(r.to_numpy())


def test_sharpe_excluding_best_year_needs_two_years():
    sharpe, year = sharpe_excluding_best_year(_returns(n=100))
    assert np.isnan(sharpe) and year is None


def test_sharpe_excluding_tails_keeps_symmetric_noise_but_one_sided_trim_does_not():
    r = _returns(mean=0.0004, std=0.01, n=6000, seed=2)
    base = sharpe_ratio(r.to_numpy())
    assert sharpe_excluding_tails(r, 0.01) == pytest.approx(base, abs=0.2)
    assert sharpe_excluding_top(r, 0.01) < base - 0.2


def test_gaussian_top_trim_shift_matches_simulation():
    rng = np.random.default_rng(11)
    r = pd.Series(rng.normal(0.0004, 0.01, 400_000))
    simulated = sharpe_ratio(r.to_numpy()) - sharpe_excluding_top(r, 0.01)
    assert gaussian_top_trim_shift(0.01) == pytest.approx(simulated, abs=0.1)
