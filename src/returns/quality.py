"""
src/returns/quality.py

Per-instrument quality report for a daily returns panel (R6 of
docs/plans/cross_asset_strategies.md). Pure; no DB access.

Flags:
- "abs>25%":  a daily return above 25% in absolute value (bad print or a
               non-positive price).
- "gap>5bd":  more than 5 business days between two observations.
- "nonfinite": an infinite value.

Usage:
    from src.returns.quality import quality_report
    report = quality_report(returns_panel)
"""

from __future__ import annotations

import numpy as np
import pandas as pd

MAX_ABS_RETURN = 0.25
MAX_GAP_BDAYS = 5
N_WORST = 5


def remove_return_spikes(r: pd.Series, threshold: float) -> tuple[pd.Series, pd.DatetimeIndex]:
    """Repair one-day spike-and-revert bad prints in a simple-return series.

    A spike at t: |log return| above `threshold` at t and t+1, opposite signs,
    and a net two-day move under threshold / 2. The bad price at t is replaced
    by the geometric midpoint of its neighbours, so r_t and r_{t+1} each get
    half the two-day log move: each half is earned by the weight held over its
    own day (NaN-plus-compounding would credit day t's move to day t's weight).

    Looks one day ahead, and the last observation is never checked: a repair
    of stored history, not a live rule, so values on repaired dates differ from
    what a truncated history would show. NaNs are ignored (each series' own
    observations are compared).

    Returns (repaired series on r's index, dates of removed prints).
    """
    obs = r.dropna()
    lr = np.log1p(obs)
    nxt = lr.shift(-1)
    spike = ((lr.abs() > threshold) & (nxt.abs() > threshold) & (np.sign(lr) != np.sign(nxt))
             & ((lr + nxt).abs() < threshold / 2)).fillna(False)
    fixed = obs.copy()
    for t in obs.index[spike.to_numpy()]:
        nxt_date = obs.index[obs.index.get_loc(t) + 1]
        half = np.sqrt((1 + obs[t]) * (1 + obs[nxt_date])) - 1
        fixed[t] = fixed[nxt_date] = half
    return fixed.reindex(r.index), obs.index[spike.to_numpy()]


def _quality_row(r: pd.Series) -> dict:
    raw = r.dropna()
    if raw.empty:
        return {"first": None, "last": None, "n_obs": 0, "flags": "no data"}
    finite = raw[np.isfinite(raw)]
    dates = raw.index
    bdays = pd.bdate_range(dates[0], dates[-1])
    gaps = np.busday_count(dates[:-1].values.astype("datetime64[D]"),
                           dates[1:].values.astype("datetime64[D]")) if len(dates) > 1 else np.array([0])
    worst = finite.reindex(finite.abs().sort_values(ascending=False).index[:N_WORST])

    flags = []
    if (finite.abs() > MAX_ABS_RETURN).any():
        flags.append("abs>25%")
    if gaps.max() > MAX_GAP_BDAYS:
        flags.append("gap>5bd")
    if len(finite) < len(raw):
        flags.append("nonfinite")

    return {
        "first": dates[0].date(),
        "last": dates[-1].date(),
        "n_obs": len(raw),
        "pct_missing_bdays": round(100 * (1 - len(raw) / len(bdays)), 2),
        "ann_mean": finite.mean() * 252,
        "ann_vol": finite.std() * np.sqrt(252),
        "max_abs": finite.abs().max(),
        "excess_kurtosis": finite.kurt(),
        "n_abs_gt_10pct": int((finite.abs() > 0.10).sum()),
        "max_gap_bdays": int(gaps.max()),
        "worst_days": ", ".join(f"{d.date()} {v:+.1%}" for d, v in worst.items()),
        "flags": ", ".join(flags),
    }


def quality_report(returns: pd.DataFrame) -> pd.DataFrame:
    """One row per column of `returns` (daily simple returns, NaN = no observation)."""
    return pd.DataFrame({col: _quality_row(returns[col]) for col in returns.columns}).T
