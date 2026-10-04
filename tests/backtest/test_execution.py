"""
tests/backtest/test_execution.py

Tests for src/backtest/execution — ExecutionResult and apply_costs.

Pure in-memory; no database access required.

Run with: pytest tests/backtest/test_execution.py -v
"""

from __future__ import annotations

import logging
import math

import numpy as np
import pandas as pd
import pytest

from src.backtest.execution import ExecutionResult, apply_costs
from src.backtest.performance import PerformanceReport


# ── Shared helpers ────────────────────────────────────────────────────────────

def _dates(n: int, start: str = "2024-01-01") -> pd.DatetimeIndex:
    return pd.date_range(start, periods=n, freq="D")


def _s(values, start: str = "2024-01-01", name: str | None = None) -> pd.Series:
    idx = _dates(len(values), start)
    return pd.Series(values, index=idx, dtype=float, name=name)


def _df(data: dict, start: str = "2024-01-01") -> pd.DataFrame:
    n = len(next(iter(data.values())))
    return pd.DataFrame(data, index=_dates(n, start), dtype=float)


# ═══════════════════════════════════════════════════════════════════════════════
# ExecutionResult dataclass + summary()
# ═══════════════════════════════════════════════════════════════════════════════

class TestExecutionResult:
    def _make(self, n: int = 10) -> ExecutionResult:
        idx = _dates(n)
        gr = pd.Series(np.random.default_rng(0).normal(0.001, 0.01, n), index=idx, name="gross_returns")
        costs = pd.Series([0.0005] * n, index=idx, name="costs")
        nr = (gr - costs).rename("net_returns")
        to = pd.Series([0.1] * n, index=idx, name="turnover")
        return ExecutionResult(gross_returns=gr, net_returns=nr, turnover=to, costs=costs, cost_bps=5.0, lag=1)

    def test_is_frozen(self):
        er = self._make()
        with pytest.raises((AttributeError, TypeError)):
            er.cost_bps = 99.0  # type: ignore[misc]

    def test_summary_has_all_keys(self):
        keys = {"total_gross", "total_net", "total_cost", "avg_turnover", "ann_turnover"}
        assert set(self._make().summary().keys()) == keys

    def test_summary_total_cost_is_sum_of_costs(self):
        er = self._make()
        assert pytest.approx(er.summary()["total_cost"]) == float(er.costs.sum())

    def test_summary_avg_turnover_is_mean(self):
        er = self._make()
        assert pytest.approx(er.summary()["avg_turnover"]) == float(er.turnover.mean())

    def test_summary_ann_turnover_scales_by_ppy(self):
        er = self._make()
        s252 = er.summary(periods_per_year=252)
        s12 = er.summary(periods_per_year=12)
        assert pytest.approx(s252["ann_turnover"]) == er.turnover.mean() * 252
        assert pytest.approx(s12["ann_turnover"]) == er.turnover.mean() * 12

    def test_summary_total_gross_is_compound(self):
        er = self._make()
        expected = float((1 + er.gross_returns).prod() - 1)
        assert pytest.approx(er.summary()["total_gross"]) == expected

    def test_summary_empty_result_all_nan(self):
        empty = ExecutionResult(
            gross_returns=pd.Series(dtype=float, name="gross_returns"),
            net_returns=pd.Series(dtype=float, name="net_returns"),
            turnover=pd.Series(dtype=float, name="turnover"),
            costs=pd.Series(dtype=float, name="costs"),
            cost_bps=0.0,
            lag=1,
        )
        s = empty.summary()
        for v in s.values():
            assert math.isnan(v)


# ═══════════════════════════════════════════════════════════════════════════════
# apply_costs — single-asset, lag correctness
# ═══════════════════════════════════════════════════════════════════════════════

class TestSingleAssetLag:
    """Constant weight=1.0: gross should equal the asset return on the same date."""

    def test_lag1_gross_equals_asset_returns_on_same_date(self):
        """With weight=1 and lag=1, gross_returns[d] == asset_returns[d]."""
        r = _s([0.01, -0.02, 0.03, 0.005, 0.008])
        w = _s([1.0, 1.0, 1.0, 1.0, 1.0])
        result = apply_costs(w, r, cost_bps=0.0, lag=1)
        # Output drops the first date; for remaining dates effective_w=1.0
        expected = r.iloc[1:].values
        assert pytest.approx(result.gross_returns.values, abs=1e-12) == expected

    def test_lag1_output_starts_at_second_date(self):
        r = _s([0.01, -0.02, 0.03])
        w = _s([1.0, 1.0, 1.0])
        result = apply_costs(w, r, lag=1)
        assert result.gross_returns.index[0] == r.index[1]

    def test_lag1_output_length_is_n_minus_lag(self):
        n = 6
        r = _s(list(range(n)))
        w = _s([1.0] * n)
        result = apply_costs(w, r, lag=1)
        assert len(result.gross_returns) == n - 1

    def test_zero_cost_net_equals_gross(self):
        r = _s([0.01, -0.02, 0.03])
        w = _s([1.0, 1.0, 1.0])
        result = apply_costs(w, r, cost_bps=0.0, lag=1)
        pd.testing.assert_series_equal(
            result.net_returns,
            result.gross_returns.rename("net_returns"),
        )

    def test_all_output_series_share_same_index(self):
        r = _s([0.01, -0.02, 0.03, 0.005])
        w = _s([1.0, 1.0, 1.0, 1.0])
        result = apply_costs(w, r, cost_bps=10.0, lag=1)
        for s in (result.net_returns, result.turnover, result.costs):
            assert list(s.index) == list(result.gross_returns.index)

    def test_output_series_are_named(self):
        r = _s([0.01, 0.02, 0.03])
        w = _s([1.0, 1.0, 1.0])
        result = apply_costs(w, r, lag=1)
        assert result.gross_returns.name == "gross_returns"
        assert result.net_returns.name == "net_returns"
        assert result.turnover.name == "turnover"
        assert result.costs.name == "costs"

    def test_lag0_contemporaneous_starts_at_first_date(self):
        """lag=0 → effective_w = weights, no rows dropped, output from date 0."""
        r = _s([0.01, -0.02, 0.03])
        w = _s([2.0, 2.0, 2.0])
        result = apply_costs(w, r, cost_bps=0.0, lag=0)
        assert len(result.gross_returns) == 3
        assert result.gross_returns.index[0] == r.index[0]
        assert pytest.approx(result.gross_returns.iloc[0]) == 2.0 * 0.01

    def test_lag2_drops_first_two_dates(self):
        n = 5
        r = _s([0.01] * n)
        w = _s([1.0] * n)
        result = apply_costs(w, r, lag=2)
        assert len(result.gross_returns) == n - 2
        assert result.gross_returns.index[0] == r.index[2]


# ═══════════════════════════════════════════════════════════════════════════════
# apply_costs — turnover arithmetic
# ═══════════════════════════════════════════════════════════════════════════════

class TestTurnover:
    """Verify one-sided turnover = 0.5 * |Δ effective_weight|."""

    def test_initial_period_turnover_is_zero(self):
        """First output period has no prior position → turnover=0."""
        w = _s([1.0, 1.0, 1.0])
        r = _s([0.01, 0.02, 0.03])
        result = apply_costs(w, r, lag=1)
        assert result.turnover.iloc[0] == 0.0

    def test_unchanged_weight_gives_zero_turnover(self):
        """Constant weight produces zero turnover after the first period."""
        w = _s([0.5, 0.5, 0.5, 0.5])
        r = _s([0.01, 0.02, -0.01, 0.005])
        result = apply_costs(w, r, lag=1)
        # All periods except the first should have turnover 0
        assert (result.turnover.iloc[1:] == 0.0).all()

    def test_entry_and_exit_turnover_pattern(self):
        """0 → 1 → 1 → 0 weight pattern covers entry, hold, and exit."""
        # weights at 5 dates: [0, 1, 1, 0, 0]
        # With lag=1, effective_w at [d1, d2, d3, d4] = [0, 1, 1, 0]
        # Expected turnover:
        #   d1: 0.0 (initial, no prior)
        #   d2: 0.5 * |1 - 0| = 0.5 (entry)
        #   d3: 0.5 * |1 - 1| = 0.0 (hold)
        #   d4: 0.5 * |0 - 1| = 0.5 (exit)
        w = _s([0.0, 1.0, 1.0, 0.0, 0.0])
        r = _s([0.01, 0.02, -0.01, 0.005, 0.0])
        result = apply_costs(w, r, lag=1)
        assert len(result.turnover) == 4
        assert result.turnover.iloc[0] == pytest.approx(0.0)   # initial
        assert result.turnover.iloc[1] == pytest.approx(0.5)   # entry
        assert result.turnover.iloc[2] == pytest.approx(0.0)   # hold
        assert result.turnover.iloc[3] == pytest.approx(0.5)   # exit

    def test_step_change_turnover_formula(self):
        """Arbitrary step: 0.3 → 0.7, expected turnover = 0.5 * |0.7 - 0.3| = 0.2."""
        w = _s([0.3, 0.7, 0.7])
        r = _s([0.01, 0.02, 0.03])
        result = apply_costs(w, r, lag=1)
        # effective_w at [d1, d2] = [0.3, 0.7]
        # turnover at d1: 0.0 (initial)
        # turnover at d2: 0.5 * |0.7 - 0.3| = 0.2
        assert result.turnover.iloc[1] == pytest.approx(0.2)

    def test_lag0_no_turnover_on_constant_weight(self):
        """lag=0, constant weight: turnover should be zero after the first period."""
        w = _s([0.5, 0.5, 0.5, 0.5])
        r = _s([0.01, 0.02, -0.01, 0.005])
        result = apply_costs(w, r, lag=0)
        # First period gets 0.0 (no prior); rest should be 0.0
        assert (result.turnover == 0.0).all()


# ═══════════════════════════════════════════════════════════════════════════════
# apply_costs — cost subtraction
# ═══════════════════════════════════════════════════════════════════════════════

class TestCostSubtraction:
    def test_costs_equal_cost_rate_times_turnover(self):
        w = _s([0.0, 1.0, 1.0, 0.0, 0.0])
        r = _s([0.01, 0.02, -0.01, 0.005, 0.0])
        cost_bps = 10.0
        result = apply_costs(w, r, cost_bps=cost_bps, lag=1)
        expected_costs = (cost_bps / 1e4) * result.turnover
        pd.testing.assert_series_equal(
            result.costs,
            expected_costs.rename("costs"),
            check_names=True,
        )

    def test_net_equals_gross_minus_costs_exactly(self):
        w = _s([0.0, 1.0, 0.5, 0.5])
        r = _s([0.01, 0.02, -0.01, 0.005])
        result = apply_costs(w, r, cost_bps=20.0, lag=1)
        pd.testing.assert_series_equal(
            result.net_returns,
            (result.gross_returns - result.costs).rename("net_returns"),
            check_names=True,
        )

    def test_higher_bps_lowers_net_return(self):
        w = _s([0.0, 1.0, 1.0, 0.0])
        r = _s([0.01, 0.02, -0.01, 0.005])
        res0 = apply_costs(w, r, cost_bps=0.0, lag=1)
        res10 = apply_costs(w, r, cost_bps=10.0, lag=1)
        # net_returns with costs should be ≤ gross everywhere (costs ≥ 0)
        assert (res10.net_returns <= res0.net_returns + 1e-12).all()

    def test_cost_bps_stored_on_result(self):
        r = _s([0.01, 0.02, 0.03])
        w = _s([1.0, 1.0, 1.0])
        result = apply_costs(w, r, cost_bps=7.5, lag=1)
        assert result.cost_bps == pytest.approx(7.5)

    def test_lag_stored_on_result(self):
        r = _s([0.01, 0.02, 0.03])
        w = _s([1.0, 1.0, 1.0])
        result = apply_costs(w, r, lag=2)
        assert result.lag == 2


# ═══════════════════════════════════════════════════════════════════════════════
# apply_costs — multi-asset
# ═══════════════════════════════════════════════════════════════════════════════

class TestMultiAsset:
    def test_gross_is_weighted_sum_of_lagged_weights_times_returns(self):
        """Row-wise: gross[t] = Σ_i effective_w_i[t] * r_i[t]."""
        dates = _dates(4)
        weights = pd.DataFrame(
            {"A": [0.6, 0.4, 0.4, 0.4], "B": [0.4, 0.6, 0.6, 0.6]},
            index=dates, dtype=float,
        )
        returns = pd.DataFrame(
            {"A": [0.01, 0.02, -0.01, 0.015], "B": [-0.005, 0.01, 0.02, -0.01]},
            index=dates, dtype=float,
        )
        result = apply_costs(weights, returns, lag=1)
        # effective_w at d1: [0.6, 0.4], asset_returns at d1: [0.02, 0.01]
        assert pytest.approx(result.gross_returns.iloc[0]) == 0.6 * 0.02 + 0.4 * 0.01
        # effective_w at d2: [0.4, 0.6], asset_returns at d2: [-0.01, 0.02]
        assert pytest.approx(result.gross_returns.iloc[1]) == 0.4 * (-0.01) + 0.6 * 0.02
        # effective_w at d3: [0.4, 0.6], asset_returns at d3: [0.015, -0.01]
        assert pytest.approx(result.gross_returns.iloc[2]) == 0.4 * 0.015 + 0.6 * (-0.01)

    def test_turnover_aggregates_across_assets(self):
        """Turnover = 0.5 * Σ_assets |Δ effective_w|."""
        # Weight shift at d2: A goes 0.6→0.4 (Δ=0.2), B goes 0.4→0.6 (Δ=0.2)
        # Expected turnover at d2: 0.5*(0.2+0.2) = 0.2
        dates = _dates(4)
        weights = pd.DataFrame(
            {"A": [0.6, 0.4, 0.4, 0.4], "B": [0.4, 0.6, 0.6, 0.6]},
            index=dates, dtype=float,
        )
        returns = pd.DataFrame(
            {"A": [0.01, 0.02, -0.01, 0.015], "B": [-0.005, 0.01, 0.02, -0.01]},
            index=dates, dtype=float,
        )
        result = apply_costs(weights, returns, lag=1)
        assert result.turnover.iloc[0] == pytest.approx(0.0)   # initial
        assert result.turnover.iloc[1] == pytest.approx(0.2)   # rebalance at d2
        assert result.turnover.iloc[2] == pytest.approx(0.0)   # hold

    def test_multi_asset_output_is_series_not_dataframe(self):
        w = _df({"A": [0.5, 0.5, 0.5], "B": [0.5, 0.5, 0.5]})
        r = _df({"A": [0.01, 0.02, -0.01], "B": [-0.005, 0.01, 0.02]})
        result = apply_costs(w, r, lag=1)
        assert isinstance(result.gross_returns, pd.Series)
        assert isinstance(result.turnover, pd.Series)

    def test_column_mismatch_warns_uses_intersection(
        self, caplog: pytest.LogCaptureFixture
    ):
        """Extra columns in weights/returns → warn and use intersection."""
        w = _df({"A": [1.0, 1.0, 1.0], "C": [0.0, 0.0, 0.0]})  # C not in returns
        r = _df({"A": [0.01, 0.02, -0.01], "D": [0.001, 0.002, -0.001]})  # D not in weights
        with caplog.at_level(logging.WARNING, logger="quant"):
            result = apply_costs(w, r, lag=1)
        assert any("mismatch" in m.lower() or "intersection" in m.lower()
                   for m in caplog.messages)
        # Result should only reflect column A
        assert not result.gross_returns.empty

    def test_column_mismatch_result_matches_intersection_directly(self):
        """Result with mismatch equals a direct call using only common columns."""
        w_full = _df({"A": [0.6, 0.4, 0.4], "B": [0.4, 0.6, 0.6], "X": [1.0, 1.0, 1.0]})
        r_full = _df({"A": [0.01, 0.02, -0.01], "B": [-0.005, 0.01, 0.02]})
        w_intersect = _df({"A": [0.6, 0.4, 0.4], "B": [0.4, 0.6, 0.6]})

        res_full = apply_costs(w_full, r_full, lag=1)
        res_int = apply_costs(w_intersect, r_full, lag=1)
        pd.testing.assert_series_equal(res_full.gross_returns, res_int.gross_returns)


# ═══════════════════════════════════════════════════════════════════════════════
# apply_costs — type validation
# ═══════════════════════════════════════════════════════════════════════════════

class TestTypeValidation:
    def test_series_vs_dataframe_raises_type_error(self):
        w_s = _s([1.0, 1.0, 1.0])
        r_df = _df({"A": [0.01, 0.02, 0.03]})
        with pytest.raises(TypeError, match="pd.Series"):
            apply_costs(w_s, r_df)

    def test_dataframe_vs_series_raises_type_error(self):
        w_df = _df({"A": [1.0, 1.0, 1.0]})
        r_s = _s([0.01, 0.02, 0.03])
        with pytest.raises(TypeError, match="pd.DataFrame"):
            apply_costs(w_df, r_s)


# ═══════════════════════════════════════════════════════════════════════════════
# apply_costs — graceful degradation
# ═══════════════════════════════════════════════════════════════════════════════

class TestGracefulDegradation:
    def test_disjoint_dates_returns_empty_result(
        self, caplog: pytest.LogCaptureFixture
    ):
        w = _s([1.0, 1.0], start="2024-01-01")
        r = _s([0.01, 0.02], start="2025-01-01")  # different year
        with caplog.at_level(logging.WARNING, logger="quant"):
            result = apply_costs(w, r, lag=1)
        assert result.gross_returns.empty
        assert result.net_returns.empty
        assert result.turnover.empty
        assert result.costs.empty
        assert any("overlap" in m.lower() or "overlapping" in m.lower()
                   for m in caplog.messages)

    def test_no_common_columns_returns_empty_result(
        self, caplog: pytest.LogCaptureFixture
    ):
        w = _df({"A": [1.0, 1.0, 1.0]})
        r = _df({"B": [0.01, 0.02, 0.03]})
        with caplog.at_level(logging.WARNING, logger="quant"):
            result = apply_costs(w, r, lag=1)
        assert result.gross_returns.empty

    def test_lag_exceeds_data_returns_empty_result(
        self, caplog: pytest.LogCaptureFixture
    ):
        w = _s([1.0, 1.0])  # only 2 dates
        r = _s([0.01, 0.02])
        with caplog.at_level(logging.WARNING, logger="quant"):
            result = apply_costs(w, r, lag=5)  # lag larger than data
        assert result.gross_returns.empty

    def test_empty_result_summary_is_all_nan(
        self, caplog: pytest.LogCaptureFixture
    ):
        w = _s([1.0, 1.0], start="2024-01-01")
        r = _s([0.01, 0.02], start="2025-01-01")
        with caplog.at_level(logging.WARNING, logger="quant"):
            result = apply_costs(w, r, lag=1)
        s = result.summary()
        for v in s.values():
            assert math.isnan(v)


# ═══════════════════════════════════════════════════════════════════════════════
# End-to-end wiring with PerformanceReport
# ═══════════════════════════════════════════════════════════════════════════════

class TestEndToEndWithPerformanceReport:
    """net_returns from apply_costs must drop straight into PerformanceReport."""

    def _make_result(self) -> ExecutionResult:
        rng = np.random.default_rng(42)
        n = 300
        idx = pd.bdate_range("2020-01-02", periods=n)
        weights = pd.Series(
            [1.0] * n,
            index=idx,
        )
        returns = pd.Series(
            rng.normal(0.0005, 0.01, n),
            index=idx,
        )
        return apply_costs(weights, returns, cost_bps=5.0, lag=1)

    def test_net_returns_feeds_performance_report(self):
        result = self._make_result()
        rpt = PerformanceReport(result.net_returns, name="test_net")
        metrics = rpt.metrics()
        assert "sharpe" in metrics
        assert isinstance(metrics["sharpe"], float)

    def test_net_returns_report_has_start_end(self):
        result = self._make_result()
        rpt = PerformanceReport(result.net_returns)
        mets = rpt.metrics()
        assert mets["start"] is not None
        assert mets["end"] is not None

    def test_gross_net_sharpe_ordering(self):
        """With costs > 0, net Sharpe should be ≤ gross Sharpe."""
        result = self._make_result()
        gross_sharpe = PerformanceReport(result.gross_returns).metrics()["sharpe"]
        net_sharpe = PerformanceReport(result.net_returns).metrics()["sharpe"]
        # Allow floating-point tolerance
        assert net_sharpe <= gross_sharpe + 1e-10

    def test_net_returns_repr_works(self):
        result = self._make_result()
        rpt = PerformanceReport(result.net_returns, name="net_strategy")
        assert isinstance(repr(rpt), str)
        assert "net_strategy" in repr(rpt)

    def test_two_asset_net_returns_feeds_performance_report(self):
        """Multi-asset gross → net → PerformanceReport, no errors."""
        rng = np.random.default_rng(7)
        n = 200
        idx = pd.bdate_range("2021-01-04", periods=n)
        weights = pd.DataFrame(
            {"A": [0.6] * n, "B": [0.4] * n},
            index=idx, dtype=float,
        )
        returns = pd.DataFrame(
            {
                "A": rng.normal(0.0003, 0.008, n),
                "B": rng.normal(0.0005, 0.015, n),
            },
            index=idx, dtype=float,
        )
        result = apply_costs(weights, returns, cost_bps=10.0, lag=1)
        rpt = PerformanceReport(result.net_returns, name="two_asset")
        metrics = rpt.metrics()
        assert isinstance(metrics["max_drawdown"], float)
        assert not math.isnan(metrics["sharpe"])
