"""
src/backtest/performance.py

PerformanceReport: wraps a return series and exposes metrics, equity/drawdown curves, and a summary.
"""

from __future__ import annotations

import math

import pandas as pd

import src.backtest.metrics as m
from src.shared.utils import logger

_MIN_OBS = 30


class PerformanceReport:
    """Wraps a daily return series and exposes performance analytics.

    Pure/return-series-only: no database access, no reference data.

    Args:
        returns: Daily return series (must be pd.Series).
        rf: Per-period risk-free rate — scalar or pd.Series aligned to the
            returns index.  Missing values are filled with 0.0.
        periods_per_year: Trading periods per year used for annualization.
        name: Label for the strategy (used in summary and repr).

    Raises:
        TypeError: If returns is not a pd.Series.
    """

    def __init__(
        self,
        returns: pd.Series,
        rf: float | pd.Series = 0.0,
        periods_per_year: int = 252,
        name: str = "strategy",
    ) -> None:
        if not isinstance(returns, pd.Series):
            raise TypeError(
                f"returns must be a pd.Series, got {type(returns).__name__}"
            )

        self._returns: pd.Series = returns.dropna()
        self._periods_per_year = periods_per_year
        self.name = name

        n = len(self._returns)
        if n < _MIN_OBS:
            logger.warning(
                f"[PerformanceReport:{name}] Only {n} observation(s) after dropna "
                f"— fewer than {_MIN_OBS}, results may be unreliable."
            )

        if isinstance(rf, pd.Series):
            self._rf: float | pd.Series = (
                rf.reindex(self._returns.index).fillna(0.0)
            )
        else:
            self._rf = float(rf)

    # ── Public interface ──────────────────────────────────────────────────────

    def metrics(self) -> dict[str, object]:
        """Compute all performance metrics.

        Returns:
            Dict with snake_case keys. Numeric values are floats.
            start and end are ISO date strings (or None if index is not DatetimeIndex).
        """
        r = self._returns
        rf = self._rf
        ppy = self._periods_per_year

        idx = r.index
        if len(idx) > 0 and isinstance(idx, pd.DatetimeIndex):
            start: str | None = idx[0].isoformat()
            end: str | None = idx[-1].isoformat()
        else:
            start = None
            end = None

        return {
            "total_return": m.total_return(r),
            "cagr": m.cagr(r, ppy),
            "ann_return": m.annualized_return(r, ppy),
            "ann_vol": m.annualized_volatility(r, ppy),
            "sharpe": m.sharpe_ratio(r, rf, ppy),
            "sortino": m.sortino_ratio(r, rf, periods_per_year=ppy),
            "calmar": m.calmar_ratio(r, ppy),
            "max_drawdown": m.max_drawdown(r),
            "max_dd_duration": float(m.max_drawdown_duration(r)),
            "skew": m.skewness(r),
            "excess_kurtosis": m.kurtosis(r),
            "hit_rate": m.hit_rate(r),
            "win_loss_ratio": m.win_loss_ratio(r),
            "var_95": m.value_at_risk(r),
            "cvar_95": m.conditional_value_at_risk(r),
            "best_day": m.best_period(r),
            "worst_day": m.worst_period(r),
            "sharpe_tstat": m.sharpe_tstat(r, rf),
            "n_periods": float(len(r)),
            "start": start,
            "end": end,
        }

    def equity_curve(self) -> pd.Series:
        """Cumulative equity index (starts at 1 + first return).

        Returns:
            pd.Series with the same index as the cleaned returns.
        """
        return (1 + self._returns).cumprod()

    def drawdown_curve(self) -> pd.Series:
        """Per-period drawdown path (values ≤ 0).

        Returns:
            pd.Series with the same index as the cleaned returns.
        """
        dd = m.drawdown_series(self._returns)
        if not isinstance(dd, pd.Series):
            dd = pd.Series(dd, index=self._returns.index)
        return dd

    def summary(self) -> pd.Series:
        """Key metrics in a sensible display order.

        Returns:
            pd.Series of numeric metrics, name=self.name.
        """
        mets = self.metrics()
        order = [
            "total_return",
            "cagr",
            "ann_return",
            "ann_vol",
            "sharpe",
            "sortino",
            "calmar",
            "max_drawdown",
            "max_dd_duration",
            "skew",
            "excess_kurtosis",
            "hit_rate",
            "win_loss_ratio",
            "var_95",
            "cvar_95",
            "best_day",
            "worst_day",
            "sharpe_tstat",
            "n_periods",
        ]
        return pd.Series(
            {k: mets[k] for k in order if k in mets},
            name=self.name,
        )

    def __repr__(self) -> str:
        """Compact text table of key metrics."""
        mets = self.metrics()

        def _pct(v: object) -> str:
            if not isinstance(v, float) or math.isnan(v):
                return "     nan"
            return f"{v * 100:7.2f}%"

        def _num(v: object, fmt: str = "7.3f") -> str:
            if not isinstance(v, float) or math.isnan(v):
                return "     nan"
            return f"{v:{fmt}}"

        rows = [
            ("Return (total)", _pct(mets["total_return"])),
            ("CAGR", _pct(mets["cagr"])),
            ("Ann. Vol", _pct(mets["ann_vol"])),
            ("Sharpe", _num(mets["sharpe"])),
            ("Sortino", _num(mets["sortino"])),
            ("Calmar", _num(mets["calmar"])),
            ("Max Drawdown", _pct(mets["max_drawdown"])),
            ("Sharpe t-stat", _num(mets["sharpe_tstat"])),
        ]

        header = f"PerformanceReport — {self.name}"
        sep = "─" * (len(header) + 2)
        lines = [header, sep]
        for label, value in rows:
            lines.append(f"  {label:<20s}  {value}")
        return "\n".join(lines)
