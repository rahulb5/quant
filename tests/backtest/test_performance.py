"""
tests/backtest/test_performance.py

Tests for src/backtest/performance.PerformanceReport.

Run with: pytest tests/backtest/test_performance.py -v
"""

from __future__ import annotations

import logging
import math

import numpy as np
import pandas as pd
import pytest

from src.backtest.performance import PerformanceReport


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def sample_returns() -> pd.Series:
    rng = np.random.default_rng(7)
    r = rng.normal(0.001, 0.012, 252)
    idx = pd.date_range("2023-01-02", periods=252, freq="B")
    return pd.Series(r, index=idx, name="test")


@pytest.fixture
def report(sample_returns: pd.Series) -> PerformanceReport:
    return PerformanceReport(sample_returns, name="test_strategy")


# ── Constructor errors ────────────────────────────────────────────────────────

def test_non_series_ndarray_raises():
    with pytest.raises(TypeError, match="pd.Series"):
        PerformanceReport(np.array([0.01, -0.01, 0.02]))


def test_non_series_list_raises():
    with pytest.raises(TypeError, match="pd.Series"):
        PerformanceReport([0.01, -0.01, 0.02])


def test_non_series_dict_raises():
    with pytest.raises(TypeError):
        PerformanceReport({"a": 0.01})


# ── Short series warns but does not raise ─────────────────────────────────────

def test_short_series_logs_warning(caplog: pytest.LogCaptureFixture):
    rng = np.random.default_rng(42)
    r = pd.Series(rng.normal(0, 0.01, 10), index=pd.date_range("2024-01-01", periods=10))
    with caplog.at_level(logging.WARNING, logger="quant"):
        rep = PerformanceReport(r, name="short")
    warning_msgs = [m for m in caplog.messages if "WARNING" in caplog.text or True]
    assert any(
        "10" in msg or "observation" in msg.lower() or "fewer" in msg.lower()
        for msg in caplog.messages
    )
    # still computes metrics without raising
    mets = rep.metrics()
    assert isinstance(mets, dict)


def test_short_series_metrics_still_work():
    rng = np.random.default_rng(1)
    r = pd.Series(rng.normal(0, 0.01, 5), index=pd.date_range("2024-01-01", periods=5))
    rep = PerformanceReport(r)
    mets = rep.metrics()
    assert "sharpe" in mets


# ── metrics() keys ────────────────────────────────────────────────────────────

_EXPECTED_KEYS = {
    "total_return", "cagr", "ann_return", "ann_vol",
    "sharpe", "sortino", "calmar",
    "max_drawdown", "max_dd_duration",
    "skew", "excess_kurtosis",
    "hit_rate", "win_loss_ratio",
    "var_95", "cvar_95",
    "best_day", "worst_day",
    "sharpe_tstat",
    "n_periods",
    "start", "end",
}


def test_metrics_has_all_keys(report: PerformanceReport):
    mets = report.metrics()
    assert set(mets.keys()) == _EXPECTED_KEYS


def test_metrics_start_end_populated(report: PerformanceReport):
    mets = report.metrics()
    assert mets["start"] is not None
    assert mets["end"] is not None
    assert isinstance(mets["start"], str)
    assert isinstance(mets["end"], str)
    assert mets["start"] < mets["end"]


def test_metrics_n_periods(report: PerformanceReport, sample_returns: pd.Series):
    mets = report.metrics()
    assert mets["n_periods"] == float(len(sample_returns.dropna()))


def test_metrics_numeric_values_are_finite_or_nan(report: PerformanceReport):
    mets = report.metrics()
    numeric_keys = _EXPECTED_KEYS - {"start", "end"}
    for k in numeric_keys:
        v = mets[k]
        assert isinstance(v, float), f"metric {k!r} is not a float: {v!r}"
        assert not math.isinf(v), f"metric {k!r} is infinite"


# ── equity_curve() ────────────────────────────────────────────────────────────

def test_equity_curve_first_value(report: PerformanceReport, sample_returns: pd.Series):
    eq = report.equity_curve()
    expected = 1.0 + sample_returns.dropna().iloc[0]
    assert pytest.approx(eq.iloc[0], rel=1e-12) == expected


def test_equity_curve_same_index(report: PerformanceReport, sample_returns: pd.Series):
    eq = report.equity_curve()
    assert list(eq.index) == list(sample_returns.dropna().index)


def test_equity_curve_is_series(report: PerformanceReport):
    assert isinstance(report.equity_curve(), pd.Series)


# ── drawdown_curve() ──────────────────────────────────────────────────────────

def test_drawdown_curve_all_non_positive(report: PerformanceReport):
    dd = report.drawdown_curve()
    assert (dd <= 1e-12).all(), "drawdown curve has positive values"


def test_drawdown_curve_same_index(report: PerformanceReport, sample_returns: pd.Series):
    dd = report.drawdown_curve()
    assert list(dd.index) == list(sample_returns.dropna().index)


def test_drawdown_curve_is_series(report: PerformanceReport):
    assert isinstance(report.drawdown_curve(), pd.Series)


# ── summary() ────────────────────────────────────────────────────────────────

def test_summary_is_series(report: PerformanceReport):
    s = report.summary()
    assert isinstance(s, pd.Series)


def test_summary_has_strategy_name(report: PerformanceReport):
    s = report.summary()
    assert s.name == "test_strategy"


def test_summary_contains_key_metrics(report: PerformanceReport):
    s = report.summary()
    for key in ("sharpe", "ann_vol", "max_drawdown", "total_return"):
        assert key in s.index, f"summary missing key {key!r}"


# ── rf as Series ──────────────────────────────────────────────────────────────

def test_rf_series_reindexed(sample_returns: pd.Series):
    rf = pd.Series(
        0.0001,
        index=pd.date_range("2023-01-02", periods=252, freq="B"),
    )
    rep = PerformanceReport(sample_returns, rf=rf)
    mets = rep.metrics()
    # sharpe should differ from rf=0 case
    rep0 = PerformanceReport(sample_returns, rf=0.0)
    assert mets["sharpe"] != rep0.metrics()["sharpe"]


# ── repr ─────────────────────────────────────────────────────────────────────

def test_repr_contains_name(report: PerformanceReport):
    assert "test_strategy" in repr(report)


def test_repr_is_string(report: PerformanceReport):
    assert isinstance(repr(report), str)


def test_repr_contains_key_labels(report: PerformanceReport):
    r = repr(report)
    for label in ("Sharpe", "CAGR", "Sortino"):
        assert label in r, f"repr missing label {label!r}"
