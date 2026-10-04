"""
src/backtest/execution.py

Execution layer: apply execution lag and transaction costs to a portfolio weight
series, producing gross and net-of-cost return series plus turnover.

Conventions
-----------
Lag:
    Weights are decided at the close of date t and earn the asset return of
    t+1 (t+lag in general).  Default lag=1.  The caller has already baked any
    publication or signal delay into the weights' index; this lag is purely
    the decide-at-close / execute-next-period mechanic.

Turnover:
    One-sided: 0.5 × Σ_assets |w_t − w_{t−1}| per period, computed on the
    *effective* (lagged) weight panel so it aligns with when the trade actually
    executes.  The first output period receives turnover=0.0 because there is
    no prior recorded position.

Costs:
    A single flat cost_bps applied to one-sided turnover each period:
        cost_t = (cost_bps / 1e4) × turnover_t
    subtracted from the gross return of that period.

Weight drift:
    The input weights are taken as the target/held weight on each date in their
    index.  The caller is responsible for forward-filling or re-gridding to the
    desired rebalance frequency before calling apply_costs — this layer
    executes weights exactly as given without any re-gridding.  The turnover
    semantics are therefore determined entirely by the signal layer, not here.
"""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from src.shared.utils import logger


# ── ExecutionResult ───────────────────────────────────────────────────────────

@dataclass(frozen=True)
class ExecutionResult:
    """Result of applying execution lag and transaction costs to a weight series.

    Attributes:
        gross_returns: Per-period portfolio return before costs.
        net_returns:   Per-period portfolio return after costs.
        turnover:      One-sided turnover per period.
        costs:         Transaction cost per period (in return units).
        cost_bps:      Cost rate used (basis points, one-sided).
        lag:           Execution lag in periods.
    """

    gross_returns: pd.Series
    net_returns: pd.Series
    turnover: pd.Series
    costs: pd.Series
    cost_bps: float
    lag: int

    def summary(self, periods_per_year: int = 252) -> dict:
        """Compact execution summary.

        Args:
            periods_per_year: Trading periods per year for annualisation.

        Returns:
            Dict with keys: total_gross (compound), total_net (compound),
            total_cost (sum), avg_turnover (mean), ann_turnover (mean ×
            periods_per_year).  Values are nan when the result is empty.
        """
        if self.gross_returns.empty:
            nan = float("nan")
            return {
                "total_gross": nan,
                "total_net": nan,
                "total_cost": nan,
                "avg_turnover": nan,
                "ann_turnover": nan,
            }
        return {
            "total_gross": float((1 + self.gross_returns).prod() - 1),
            "total_net": float((1 + self.net_returns).prod() - 1),
            "total_cost": float(self.costs.sum()),
            "avg_turnover": float(self.turnover.mean()),
            "ann_turnover": float(self.turnover.mean() * periods_per_year),
        }


# ── Private helper ────────────────────────────────────────────────────────────

def _empty_result(cost_bps: float, lag: int) -> ExecutionResult:
    """Return an ExecutionResult with all-empty Series."""
    def _e(name: str) -> pd.Series:
        return pd.Series(dtype=float, name=name)
    return ExecutionResult(
        gross_returns=_e("gross_returns"),
        net_returns=_e("net_returns"),
        turnover=_e("turnover"),
        costs=_e("costs"),
        cost_bps=float(cost_bps),
        lag=int(lag),
    )


# ── Core function ─────────────────────────────────────────────────────────────

def apply_costs(
    weights: pd.Series | pd.DataFrame,
    asset_returns: pd.Series | pd.DataFrame,
    cost_bps: float = 0.0,
    lag: int = 1,
) -> ExecutionResult:
    """Apply execution lag and transaction costs to portfolio weights.

    Both Series (single-asset) and DataFrame (multi-asset, columns = asset ids)
    inputs are accepted.  One code path handles both cases internally by
    normalising Series to single-column DataFrames and squeezing back to
    per-date Series for all outputs.

    Args:
        weights: Target weight on each date.  pd.Series for a single asset;
            pd.DataFrame (columns = asset ids) for multiple assets.  The caller
            is responsible for forward-filling to the desired rebalance
            frequency; this function does not re-grid.
        asset_returns: Per-period simple returns aligned to the same index as
            weights.  Must match the type of weights (both Series or both
            DataFrame).
        cost_bps: One-sided transaction cost in basis points applied to
            turnover each period.  Default 0.0 (no costs).
        lag: Execution lag in periods.  A lag of 1 (default) means the weight
            decided at the close of date t earns the return of date t+1.

    Returns:
        ExecutionResult with gross_returns, net_returns, turnover, costs,
        cost_bps, and lag.  All Series share the same DatetimeIndex (starting
        at the first date with a valid effective weight, i.e. lag dates after
        the start of the aligned window).  An empty ExecutionResult is returned
        (with warning logged) when there are no overlapping dates/columns.

    Raises:
        TypeError: If one of weights/asset_returns is a pd.Series and the
            other is a pd.DataFrame.
    """
    # ── 1. Type validation and normalisation ──────────────────────────────────

    w_is_s = isinstance(weights, pd.Series)
    r_is_s = isinstance(asset_returns, pd.Series)

    if w_is_s != r_is_s:
        raise TypeError(
            f"weights and asset_returns must both be pd.Series or both be "
            f"pd.DataFrame; got {type(weights).__name__} and "
            f"{type(asset_returns).__name__}."
        )

    if w_is_s:
        # Single-asset: normalise to single-column DataFrames
        w_df = weights.to_frame(name="_asset")
        r_df = asset_returns.to_frame(name="_asset")
    else:
        w_df = weights.copy()
        r_df = asset_returns.copy()

        # Column alignment — warn and restrict to intersection
        w_cols = set(w_df.columns)
        r_cols = set(r_df.columns)
        if w_cols != r_cols:
            dropped_w = sorted(str(c) for c in w_cols - r_cols)
            dropped_r = sorted(str(c) for c in r_cols - w_cols)
            logger.warning(
                f"[apply_costs] Column mismatch — weights-only: {dropped_w}, "
                f"returns-only: {dropped_r}. Using intersection only."
            )
        common = sorted(w_cols & r_cols)
        if not common:
            logger.warning(
                "[apply_costs] No common columns between weights and returns "
                "— returning empty ExecutionResult."
            )
            return _empty_result(cost_bps, lag)
        w_df = w_df[common]
        r_df = r_df[common]

    # ── 2. Date-index alignment (inner join, sorted) ──────────────────────────

    common_dates = w_df.index.intersection(r_df.index).sort_values()
    if common_dates.empty:
        logger.warning(
            "[apply_costs] No overlapping dates between weights and returns "
            "— returning empty ExecutionResult."
        )
        return _empty_result(cost_bps, lag)

    w = w_df.loc[common_dates]
    r = r_df.loc[common_dates]

    # ── 3. Execution lag ──────────────────────────────────────────────────────

    # Weight decided at t earns the return of t+lag.
    effective_w = w.shift(lag)

    # ── 4. Gross portfolio return (row-wise weighted sum) ─────────────────────

    gross = (effective_w * r).sum(axis=1)

    # ── 5. One-sided turnover ─────────────────────────────────────────────────

    # Computed on the effective weight panel so it lines up with execution.
    # diff() on the first lag+1 rows produces NaN (shift creates NaN rows,
    # and the first diff of a valid row against a NaN row is also NaN).
    # sum(axis=1, skipna=True) converts those to 0.0; fillna(0.0) is an
    # explicit guard for any remaining NaN.  This encodes the convention that
    # the first execution period has zero turnover (no prior position).
    turnover = 0.5 * effective_w.diff().abs().sum(axis=1)
    turnover = turnover.fillna(0.0)

    # ── 6. Costs and net returns ──────────────────────────────────────────────

    costs = (cost_bps / 1e4) * turnover
    net = gross - costs

    # ── 7. Drop leading rows where effective_w is entirely NaN ────────────────

    # These are the first `lag` dates where no prior weight is available.
    valid_mask = ~effective_w.isna().all(axis=1)
    if not valid_mask.any():
        logger.warning(
            "[apply_costs] After applying lag, no valid rows remain "
            "(lag may exceed the number of aligned dates) "
            "— returning empty ExecutionResult."
        )
        return _empty_result(cost_bps, lag)

    valid_idx = effective_w.index[valid_mask]
    gross = gross.loc[valid_idx]
    net = net.loc[valid_idx]
    turnover = turnover.loc[valid_idx]
    costs = costs.loc[valid_idx]

    # ── 8. Name the output Series ─────────────────────────────────────────────

    gross.name = "gross_returns"
    net.name = "net_returns"
    turnover.name = "turnover"
    costs.name = "costs"

    return ExecutionResult(
        gross_returns=gross,
        net_returns=net,
        turnover=turnover,
        costs=costs,
        cost_bps=float(cost_bps),
        lag=int(lag),
    )
