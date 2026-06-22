"""
tests/backtest/test_metrics.py

Unit tests for src/backtest/metrics.py — asserts against hand-computed values.

Run with: pytest tests/backtest/test_metrics.py -v
"""

from __future__ import annotations

import math

import numpy as np
import pandas as pd
import pytest

import src.backtest.metrics as m


# ── Helpers ───────────────────────────────────────────────────────────────────

def is_nan(v: float) -> bool:
    return isinstance(v, float) and math.isnan(v)


# ── total_return ──────────────────────────────────────────────────────────────

def test_total_return_basic():
    # equity: 1.1, 0.55, 0.66  →  total = 0.66 - 1 = -0.34
    assert pytest.approx(m.total_return([0.1, -0.5, 0.2]), rel=1e-9) == 0.66 - 1.0


def test_total_return_positive_only():
    assert pytest.approx(m.total_return([0.1, 0.1]), rel=1e-9) == 1.1 * 1.1 - 1.0


def test_total_return_empty():
    assert is_nan(m.total_return([]))


def test_total_return_single():
    assert is_nan(m.total_return([0.1]))


# ── cagr ─────────────────────────────────────────────────────────────────────

def test_cagr_basic():
    r = [0.0] * 252  # zero returns → CAGR 0
    assert pytest.approx(m.cagr(r), abs=1e-12) == 0.0


def test_cagr_empty():
    assert is_nan(m.cagr([]))


def test_cagr_single():
    assert is_nan(m.cagr([0.01]))


# ── annualized_return ─────────────────────────────────────────────────────────

def test_annualized_return_constant():
    r = [0.001] * 252
    assert pytest.approx(m.annualized_return(r), rel=1e-9) == 0.252


def test_annualized_return_empty():
    assert is_nan(m.annualized_return([]))


# ── annualized_volatility ─────────────────────────────────────────────────────

def test_annualized_volatility_constant_is_zero():
    r = [0.001] * 252
    assert pytest.approx(m.annualized_volatility(r), abs=1e-12) == 0.0


def test_annualized_volatility_empty():
    assert is_nan(m.annualized_volatility([]))


def test_annualized_volatility_formula():
    rng = np.random.default_rng(1)
    r = rng.normal(0, 0.01, 300)
    expected = float(np.std(r, ddof=1) * np.sqrt(252))
    assert pytest.approx(m.annualized_volatility(r), rel=1e-9) == expected


# ── sharpe_ratio ──────────────────────────────────────────────────────────────

def test_sharpe_ratio_constant_is_nan():
    # zero volatility → nan
    assert is_nan(m.sharpe_ratio([0.001] * 252))


def test_sharpe_ratio_matches_formula():
    rng = np.random.default_rng(42)
    r = rng.normal(0.001, 0.01, 500)
    expected = float(np.mean(r) / np.std(r, ddof=1) * np.sqrt(252))
    assert pytest.approx(m.sharpe_ratio(r), rel=1e-9) == expected


def test_sharpe_ratio_with_rf():
    rng = np.random.default_rng(7)
    r = rng.normal(0.001, 0.01, 300)
    rf = 0.0001
    excess = r - rf
    expected = float(np.mean(excess) / np.std(excess, ddof=1) * np.sqrt(252))
    assert pytest.approx(m.sharpe_ratio(r, rf=rf), rel=1e-9) == expected


def test_sharpe_ratio_empty():
    assert is_nan(m.sharpe_ratio([]))


# ── sortino_ratio ─────────────────────────────────────────────────────────────

def test_sortino_ratio_positive_only_is_nan():
    # no downside → downside_dev = 0 → nan
    assert is_nan(m.sortino_ratio([0.01, 0.02, 0.03]))


def test_sortino_ratio_basic():
    rng = np.random.default_rng(10)
    r = rng.normal(0.001, 0.015, 500)
    result = m.sortino_ratio(r)
    # verify it is a finite number (not nan or inf)
    assert math.isfinite(result)


# ── max_drawdown ─────────────────────────────────────────────────────────────

def test_max_drawdown_basic():
    r = [0.1, -0.5, 0.2]
    # equity: 1.1, 0.55, 0.66
    # peak:   1.1, 1.1,  1.1
    # dd:     0,  0.55/1.1 - 1 = -0.5,  0.66/1.1 - 1 = -0.4
    expected = 0.55 / 1.1 - 1.0  # ≈ -0.5
    assert pytest.approx(m.max_drawdown(r), abs=1e-10) == expected


def test_max_drawdown_no_drawdown():
    r = [0.01, 0.02, 0.03]
    assert m.max_drawdown(r) == pytest.approx(0.0, abs=1e-12)


def test_max_drawdown_empty():
    assert is_nan(m.max_drawdown([]))


def test_max_drawdown_single():
    assert is_nan(m.max_drawdown([0.05]))


# ── max_drawdown_duration ─────────────────────────────────────────────────────

def test_max_drawdown_duration_basic():
    # r = [0.1, -0.1, -0.1, 0.5]
    # equity:       1.1, 0.99, 0.891, 1.3365
    # running_max:  1.1, 1.1,  1.1,   1.3365
    # underwater:   F,   T,    T,     F
    # max run = 2
    r = [0.1, -0.1, -0.1, 0.5]
    assert m.max_drawdown_duration(r) == 2


def test_max_drawdown_duration_never_underwater():
    r = [0.01, 0.02, 0.03]
    assert m.max_drawdown_duration(r) == 0


def test_max_drawdown_duration_empty():
    assert m.max_drawdown_duration([]) == 0


# ── calmar_ratio ─────────────────────────────────────────────────────────────

def test_calmar_ratio_no_dd_is_nan():
    assert is_nan(m.calmar_ratio([0.01, 0.02, 0.03]))


def test_calmar_ratio_basic():
    rng = np.random.default_rng(5)
    r = rng.normal(0.001, 0.015, 500)
    c = m.cagr(r)
    mdd = m.max_drawdown(r)
    expected = c / abs(mdd)
    assert pytest.approx(m.calmar_ratio(r), rel=1e-9) == expected


# ── skewness / kurtosis ───────────────────────────────────────────────────────

def test_skewness_symmetric():
    rng = np.random.default_rng(99)
    r = rng.normal(0, 1, 10_000)
    assert abs(m.skewness(r)) < 0.1


def test_kurtosis_normal():
    rng = np.random.default_rng(99)
    r = rng.normal(0, 1, 10_000)
    assert abs(m.kurtosis(r)) < 0.3  # excess kurtosis ≈ 0 for normal


def test_skewness_empty():
    assert is_nan(m.skewness([]))


# ── hit_rate ─────────────────────────────────────────────────────────────────

def test_hit_rate_basic():
    r = [0.1, -0.1, 0.1, 0.0]
    # strictly positive: r[0], r[2] → 2 out of 4
    assert m.hit_rate(r) == 0.5


def test_hit_rate_empty():
    assert is_nan(m.hit_rate([]))


def test_hit_rate_all_positive():
    assert m.hit_rate([0.01, 0.02, 0.03]) == pytest.approx(1.0, abs=1e-12)


# ── win_loss_ratio ────────────────────────────────────────────────────────────

def test_win_loss_ratio_no_losses_is_nan():
    assert is_nan(m.win_loss_ratio([0.1, 0.2, 0.3]))


def test_win_loss_ratio_no_wins_is_nan():
    assert is_nan(m.win_loss_ratio([-0.1, -0.2]))


def test_win_loss_ratio_basic():
    r = [0.1, -0.05]
    expected = 0.1 / 0.05
    assert pytest.approx(m.win_loss_ratio(r), rel=1e-9) == expected


def test_win_loss_ratio_multiple():
    r = [0.10, 0.20, -0.05, -0.15]
    expected = np.mean([0.10, 0.20]) / abs(np.mean([-0.05, -0.15]))
    assert pytest.approx(m.win_loss_ratio(r), rel=1e-9) == expected


# ── VaR / CVaR ───────────────────────────────────────────────────────────────

def test_var_known_array():
    r = [-0.05, -0.02, 0.01, 0.03, 0.04]
    expected = float(np.quantile(r, 0.05))  # 1 - 0.95
    assert pytest.approx(m.value_at_risk(r, level=0.95), rel=1e-9) == expected


def test_cvar_known_array():
    r = np.array([-0.05, -0.02, 0.01, 0.03, 0.04])
    var = float(np.quantile(r, 0.05))
    expected = float(np.mean(r[r <= var]))
    assert pytest.approx(m.conditional_value_at_risk(r, level=0.95), rel=1e-9) == expected


def test_var_empty():
    assert is_nan(m.value_at_risk([]))


def test_cvar_empty():
    assert is_nan(m.conditional_value_at_risk([]))


# ── best_period / worst_period ────────────────────────────────────────────────

def test_best_worst_period():
    r = [0.1, -0.2, 0.05, -0.01]
    assert pytest.approx(m.best_period(r), rel=1e-9) == 0.1
    assert pytest.approx(m.worst_period(r), rel=1e-9) == -0.2


def test_best_period_empty():
    assert is_nan(m.best_period([]))


def test_worst_period_empty():
    assert is_nan(m.worst_period([]))


# ── sharpe_tstat ──────────────────────────────────────────────────────────────

def test_sharpe_tstat_constant_is_nan():
    assert is_nan(m.sharpe_tstat([0.001] * 100))


def test_sharpe_tstat_equals_sharpe_times_sqrt_n():
    """t-stat = periodic Sharpe × √n; so if ppy == n, t-stat == annualized Sharpe."""
    rng = np.random.default_rng(0)
    n = 300
    r = rng.normal(0.001, 0.01, n)
    tstat = m.sharpe_tstat(r)
    sr_with_n_as_ppy = m.sharpe_ratio(r, periods_per_year=n)
    assert pytest.approx(tstat, rel=1e-9) == sr_with_n_as_ppy


def test_sharpe_tstat_formula():
    rng = np.random.default_rng(3)
    r = rng.normal(0.001, 0.01, 400)
    expected = float(np.mean(r) / (np.std(r, ddof=1) / np.sqrt(len(r))))
    assert pytest.approx(m.sharpe_tstat(r), rel=1e-9) == expected


# ── drawdown_series ───────────────────────────────────────────────────────────

def test_drawdown_series_all_non_positive():
    r = [0.1, -0.5, 0.2, -0.1, 0.3]
    dd = m.drawdown_series(r)
    assert all(v <= 1e-12 for v in dd)


def test_drawdown_series_preserves_series_index():
    idx = pd.date_range("2024-01-01", periods=5, freq="D")
    s = pd.Series([0.01, -0.02, 0.01, -0.01, 0.005], index=idx)
    dd = m.drawdown_series(s)
    assert isinstance(dd, pd.Series)
    assert list(dd.index) == list(idx)
    assert all(dd <= 1e-12)


def test_drawdown_series_returns_ndarray_for_list():
    dd = m.drawdown_series([0.05, -0.02, 0.03])
    assert isinstance(dd, np.ndarray)


# ── Edge cases: general ───────────────────────────────────────────────────────

@pytest.mark.parametrize("fn", [
    m.total_return, m.cagr, m.annualized_return, m.annualized_volatility,
    m.sharpe_ratio, m.max_drawdown, m.hit_rate, m.win_loss_ratio,
    m.value_at_risk, m.conditional_value_at_risk, m.best_period, m.worst_period,
])
def test_empty_array_returns_nan_not_raise(fn):
    result = fn([])
    assert is_nan(result), f"{fn.__name__}([]) returned {result!r}, expected nan"


@pytest.mark.parametrize("fn", [
    m.total_return, m.cagr, m.sharpe_ratio, m.max_drawdown,
])
def test_single_element_returns_nan_not_raise(fn):
    result = fn([0.01])
    assert is_nan(result), f"{fn.__name__}([0.01]) returned {result!r}, expected nan"
