"""
src/backtest/backtest.py

run_backtest(): orchestrates the full validation pipeline — execution costs,
performance reporting, and conditioning — into a single call, returning a
BacktestReport that consolidates every layer's output into one text report.

This module adds no new analytics; it wires together apply_costs()
(execution.py), PerformanceReport (performance.py), and Conditioner
(conditioning.py), reusing PerformanceReport.__repr__ for the performance
block. See notebooks/signals/backtest_stack_validation.ipynb for the original
per-cell walkthrough this replaces with one call.
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from src.backtest.conditioning import Conditioner
from src.backtest.execution import ExecutionResult, apply_costs
from src.backtest.performance import PerformanceReport
from src.backtest.regimes import Regime


class BacktestReport:
    """Bundles execution, performance, and conditioning results for one strategy run.

    Plain attribute container populated by run_backtest(). __repr__ concatenates
    each layer's summary into one consolidated text report, in the same order as
    the original validation notebook.

    Args:
        name: Strategy label.
        execution: ExecutionResult from apply_costs().
        performance: PerformanceReport wrapping execution.net_returns.
        conditioning: Conditioner wrapping execution.net_returns.
        yearly: cond.yearly() output.
        by_regime: cond.by_regime() output.
        correlations: cond.correlations() output.
        betas: cond.betas() output.
        factor_regression: cond.factor_regression() output.
        rolling_corr: cond.rolling_correlation() output.
        conditional_perf: cond.conditional_performance() output.
    """

    def __init__(
        self,
        name: str,
        execution: ExecutionResult,
        performance: PerformanceReport,
        conditioning: Conditioner,
        yearly: pd.DataFrame,
        by_regime: dict[str, pd.DataFrame],
        correlations: pd.Series,
        betas: pd.Series,
        factor_regression: dict[str, Any],
        rolling_corr: pd.Series,
        conditional_perf: pd.DataFrame,
    ) -> None:
        self.name = name
        self.execution = execution
        self.performance = performance
        self.conditioning = conditioning
        self.yearly = yearly
        self.by_regime = by_regime
        self.correlations = correlations
        self.betas = betas
        self.factor_regression = factor_regression
        self.rolling_corr = rolling_corr
        self.conditional_perf = conditional_perf

    def __repr__(self) -> str:
        """Consolidated text report — same content and order as the validation notebook."""
        sections = [repr(self.performance)]

        exec_lines = ["=== Execution Summary ==="]
        for k, v in self.execution.summary().items():
            exec_lines.append(f"  {k:<20s}  {v:.6f}")
        sections.append("\n".join(exec_lines))

        yearly_lines = ["=== Yearly Performance ==="]
        yearly_lines.append(
            self.yearly.to_string() if not self.yearly.empty else "(no data)"
        )
        sections.append("\n".join(yearly_lines))

        regime_lines: list[str] = []
        if self.by_regime:
            for regime_name, df in self.by_regime.items():
                regime_lines.append(f"=== Regime: {regime_name} ===")
                regime_lines.append(df.to_string())
                regime_lines.append("")
            sections.append("\n".join(regime_lines).rstrip())
        else:
            sections.append("=== By Regime ===\n(no regimes)")

        corr_lines = ["=== Correlations ==="]
        corr_lines.append(
            self.correlations.to_string() if not self.correlations.empty else "(no factors)"
        )
        corr_lines.append("")
        corr_lines.append("=== Betas ===")
        corr_lines.append(
            self.betas.to_string() if not self.betas.empty else "(no factors)"
        )
        sections.append("\n".join(corr_lines))

        reg_lines = ["=== Factor Regression ==="]
        reg = self.factor_regression
        if reg:
            reg_lines.append(reg["table"].to_string())
            reg_lines.append(f"\nann_alpha    : {reg['ann_alpha']:.4f}")
            reg_lines.append(f"r_squared    : {reg['r_squared']:.4f}")
            reg_lines.append(f"n_obs        : {reg['n_obs']}")
            reg_lines.append(f"hac_maxlags  : {reg['hac_maxlags']}")
        else:
            reg_lines.append("factor_regression() returned empty dict — check warnings above")
        sections.append("\n".join(reg_lines))

        rc_lines = ["=== Rolling Correlation ==="]
        rc_valid = self.rolling_corr.dropna()
        if not rc_valid.empty:
            rc_lines.append(f"Current value: {rc_valid.iloc[-1]:.4f}")
        else:
            rc_lines.append("(no data)")
        sections.append("\n".join(rc_lines))

        cp_lines = ["=== Conditional Performance ==="]
        cp_lines.append(
            self.conditional_perf.to_string() if not self.conditional_perf.empty else "(no factors)"
        )
        sections.append("\n".join(cp_lines))

        header = f"BacktestReport — {self.name}"
        return header + "\n\n" + "\n\n".join(sections)


def run_backtest(
    weights: pd.Series,
    asset_returns: pd.Series,
    *,
    cost_bps: float = 1.0,
    lag: int = 1,
    name: str = "strategy",
    periods_per_year: int = 252,
    rf: float | pd.Series = 0.0,
    rolling_window: int = 252,
    rolling_factor: str = "equities",
    regimes: list[Regime] | None = None,
    verbose: bool = True,
) -> BacktestReport:
    """Run the full validation pipeline: execution → performance → conditioning.

    Args:
        weights: Target weight per date (see apply_costs).
        asset_returns: Per-period simple returns aligned to weights.
        cost_bps: One-sided transaction cost in basis points.
        lag: Execution lag in periods (see apply_costs).
        name: Strategy label used for PerformanceReport and the report title.
        periods_per_year: Trading periods per year used for annualisation.
        rf: Per-period risk-free rate, scalar or pd.Series.
        rolling_window: Window length for the rolling correlation.
        rolling_factor: Factor name for the rolling correlation.
        regimes: Regime list for by_regime(); None uses Conditioner's default
            (default_regimes()) — the caller must have called db.open() first
            unless regimes is explicitly supplied.
        verbose: If True (default), print(report) before returning.

    Returns:
        BacktestReport bundling every layer's output.
    """
    execution = apply_costs(weights, asset_returns, cost_bps, lag)

    performance = PerformanceReport(
        execution.net_returns, rf=rf, periods_per_year=periods_per_year, name=name
    )

    cond = Conditioner(
        execution.net_returns,
        regimes=regimes,
        rf=rf,
        periods_per_year=periods_per_year,
    )

    report = BacktestReport(
        name=name,
        execution=execution,
        performance=performance,
        conditioning=cond,
        yearly=cond.yearly(),
        by_regime=cond.by_regime(),
        correlations=cond.correlations(),
        betas=cond.betas(),
        factor_regression=cond.factor_regression(),
        rolling_corr=cond.rolling_correlation(window=rolling_window, factor=rolling_factor),
        conditional_perf=cond.conditional_performance(),
    )

    if verbose:
        print(report)

    return report
