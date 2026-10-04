"""
tests/backtest/test_backtest.py

Tests for src/backtest/backtest — run_backtest() and BacktestReport.

Uses the in-memory DuckDB fixture pattern from test_reference.py / test_conditioning.py.
The in-memory DB is seeded with no factor/regime data, so the reference/regime layer
degrades gracefully (empty results) — this test suite validates orchestration wiring,
not factor analytics (already covered by test_conditioning.py).

run_backtest() has no `database` parameter, so the in-memory DB is installed as the
module-level singleton that ReferenceData (and, through it, Regime) falls back to.

Run with: pytest tests/backtest/test_backtest.py -v
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

import src.backtest.reference as reference_module
from src.backtest.backtest import BacktestReport, run_backtest
from src.backtest.execution import apply_costs
from src.backtest.metrics import sharpe_ratio
from src.db.client import Database


# ═══════════════════════════════════════════════════════════════════════════════
# Fixtures
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.fixture(scope="module")
def mem_db() -> Database:
    """Empty in-memory DuckDB (migrations applied, no rows) used as the singleton."""
    database = Database(":memory:")
    database.open()
    yield database
    database.close()


@pytest.fixture(autouse=True)
def _patch_singleton_db(monkeypatch: pytest.MonkeyPatch, mem_db: Database) -> None:
    """Redirect ReferenceData's default singleton to the in-memory DB for every test."""
    monkeypatch.setattr(reference_module, "_singleton_db", mem_db)


@pytest.fixture(scope="module")
def strategy_inputs() -> tuple[pd.Series, pd.Series]:
    """Synthetic long/short weights + asset returns, 400 obs (well above the n<30 warning)."""
    rng = np.random.default_rng(0)
    n = 400
    idx = pd.bdate_range("2020-01-02", periods=n)
    weights = pd.Series(
        np.where(rng.normal(size=n) > 0, 1.0, -1.0), index=idx, name="w"
    )
    asset_returns = pd.Series(rng.normal(0.0005, 0.01, n), index=idx, name="r")
    return weights, asset_returns


# ═══════════════════════════════════════════════════════════════════════════════
# run_backtest() — return type and attribute population
# ═══════════════════════════════════════════════════════════════════════════════

class TestRunBacktestReturnsPopulatedReport:
    def test_returns_backtest_report(self, strategy_inputs):
        weights, asset_returns = strategy_inputs
        report = run_backtest(weights=weights, asset_returns=asset_returns, verbose=False)
        assert isinstance(report, BacktestReport)

    def test_all_attributes_non_none(self, strategy_inputs):
        weights, asset_returns = strategy_inputs
        report = run_backtest(weights=weights, asset_returns=asset_returns, verbose=False)
        for attr in (
            "name", "execution", "performance", "conditioning",
            "yearly", "by_regime", "correlations", "betas",
            "factor_regression", "rolling_corr", "conditional_perf",
        ):
            assert getattr(report, attr) is not None, f"{attr} is None"

    def test_name_stored(self, strategy_inputs):
        weights, asset_returns = strategy_inputs
        report = run_backtest(
            weights=weights, asset_returns=asset_returns, name="eq_trend_ls", verbose=False
        )
        assert report.name == "eq_trend_ls"

    def test_yearly_has_data(self, strategy_inputs):
        """yearly() only depends on net_returns, not the DB, so it must be populated."""
        weights, asset_returns = strategy_inputs
        report = run_backtest(weights=weights, asset_returns=asset_returns, verbose=False)
        assert not report.yearly.empty


# ═══════════════════════════════════════════════════════════════════════════════
# execution.net_returns — regression check against apply_costs() directly
# ═══════════════════════════════════════════════════════════════════════════════

class TestExecutionRegression:
    def test_net_returns_matches_apply_costs_directly(self, strategy_inputs):
        weights, asset_returns = strategy_inputs
        report = run_backtest(
            weights=weights, asset_returns=asset_returns,
            cost_bps=2.5, lag=1, verbose=False,
        )
        direct = apply_costs(weights, asset_returns, cost_bps=2.5, lag=1)
        pd.testing.assert_series_equal(report.execution.net_returns, direct.net_returns)

    def test_gross_returns_matches_apply_costs_directly(self, strategy_inputs):
        weights, asset_returns = strategy_inputs
        report = run_backtest(
            weights=weights, asset_returns=asset_returns,
            cost_bps=0.0, lag=2, verbose=False,
        )
        direct = apply_costs(weights, asset_returns, cost_bps=0.0, lag=2)
        pd.testing.assert_series_equal(report.execution.gross_returns, direct.gross_returns)

    def test_performance_wraps_execution_net_returns(self, strategy_inputs):
        weights, asset_returns = strategy_inputs
        report = run_backtest(weights=weights, asset_returns=asset_returns, verbose=False)
        direct = apply_costs(weights, asset_returns, cost_bps=1.0, lag=1)
        expected_sharpe = sharpe_ratio(direct.net_returns, 0.0, 252)
        assert report.performance.metrics()["sharpe"] == pytest.approx(expected_sharpe)


# ═══════════════════════════════════════════════════════════════════════════════
# verbose flag
# ═══════════════════════════════════════════════════════════════════════════════

class TestVerbose:
    def test_verbose_false_suppresses_print(self, strategy_inputs, capsys):
        weights, asset_returns = strategy_inputs
        run_backtest(weights=weights, asset_returns=asset_returns, verbose=False)
        captured = capsys.readouterr()
        assert captured.out == ""

    def test_verbose_true_prints_strategy_name(self, strategy_inputs, capsys):
        weights, asset_returns = strategy_inputs
        run_backtest(
            weights=weights, asset_returns=asset_returns, name="my_strat", verbose=True
        )
        captured = capsys.readouterr()
        assert "my_strat" in captured.out

    def test_verbose_defaults_to_true(self, strategy_inputs, capsys):
        weights, asset_returns = strategy_inputs
        run_backtest(weights=weights, asset_returns=asset_returns, name="default_strat")
        captured = capsys.readouterr()
        assert "default_strat" in captured.out


# ═══════════════════════════════════════════════════════════════════════════════
# BacktestReport.__repr__
# ═══════════════════════════════════════════════════════════════════════════════

class TestRepr:
    def test_is_string(self, strategy_inputs):
        weights, asset_returns = strategy_inputs
        report = run_backtest(weights=weights, asset_returns=asset_returns, verbose=False)
        assert isinstance(repr(report), str)

    def test_contains_all_section_headers(self, strategy_inputs):
        weights, asset_returns = strategy_inputs
        report = run_backtest(weights=weights, asset_returns=asset_returns, verbose=False)
        text = repr(report)
        for header in (
            "Execution Summary",
            "Yearly Performance",
            "By Regime",
            "Correlations",
            "Betas",
            "Factor Regression",
            "Rolling Correlation",
            "Conditional Performance",
        ):
            assert header in text, f"missing section: {header}"

    def test_contains_performance_report_repr(self, strategy_inputs):
        weights, asset_returns = strategy_inputs
        report = run_backtest(
            weights=weights, asset_returns=asset_returns, name="repr_check", verbose=False
        )
        assert repr(report.performance) in repr(report)
