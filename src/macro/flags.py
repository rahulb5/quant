"""
src/macro/flags.py

Event flags (C6): plain boolean series, not regimes, and not part of
Conditioner.by_regime(). Pure pandas. Definitions are frozen in
docs/research/macro_state_definitions_v1.md section 4.

    sahm_triggered      real-time 3-month average UNRATE minus its minimum over the
                        prior 12 months >= 0.5 pp (cross-checked against SAHMREALTIME)
    labor_deteriorating L_impulse above its expanding 80th percentile
    curve_inverted      T10Y3M < 0

Flags are pandas nullable booleans: <NA> where the underlying input is not yet
available, so "False" always means "measured and not triggered".

Usage:
    flags = event_flags(state, inputs, index)
    flags["sahm_triggered"].loc["2008-01":"2009-12"]
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from src.macro.pit import to_daily

SAHM_THRESHOLD = 0.5            # percentage points
SAHM_MONTHS = 3
SAHM_LOOKBACK = 12
LABOR_PERCENTILE = 0.80
LABOR_BURN_IN = 756             # business days (same as the daily z-score burn-in)


def sahm_indicator(unrate: pd.Series) -> pd.Series:
    """Causal Sahm-rule value: 3-month average of `unrate` minus the minimum of that
    average over the 12 months before the current one. `unrate` is a monthly,
    period-indexed Series (one snapshot)."""
    ma = unrate.rolling(SAHM_MONTHS).mean()
    return ma - ma.shift(1).rolling(SAHM_LOOKBACK).min()


def _bool(cond: pd.Series, known: pd.Series) -> pd.Series:
    return cond.where(known).astype("boolean")


def sahm_triggered(sahm: pd.Series | None, index: pd.DatetimeIndex) -> pd.Series:
    """True when the real-time Sahm value (release-dated, from sahm_indicator applied
    to each UNRATE vintage) is >= 0.5 pp."""
    if sahm is None or sahm.empty:
        return pd.Series(pd.NA, index=index, dtype="boolean", name="sahm_triggered")
    v = to_daily(sahm, index)
    return _bool(v >= SAHM_THRESHOLD, v.notna()).rename("sahm_triggered")


def sahm_crosscheck(own: pd.Series | None, fred: pd.Series | None, index: pd.DatetimeIndex) -> pd.DataFrame:
    """Own real-time Sahm value beside FRED's SAHMREALTIME (both release-dated), and the
    days on which the two triggered flags disagree. FRED's series is the reference."""
    a = to_daily(own, index) if own is not None and not own.empty else pd.Series(np.nan, index=index)
    b = to_daily(fred, index) if fred is not None and not fred.empty else pd.Series(np.nan, index=index)
    both = a.notna() & b.notna()
    return pd.DataFrame({
        "own": a, "fred": b, "diff": (a - b).where(both),
        "flag_disagree": ((a >= SAHM_THRESHOLD) != (b >= SAHM_THRESHOLD)).where(both),
    })


def labor_deteriorating(l_impulse: pd.Series) -> pd.Series:
    """L_impulse above its expanding 80th percentile (business-day values, burn-in 756).
    The current value is part of its own distribution (contemporaneous, as for the
    terciles in src.backtest.regimes)."""
    thr = l_impulse.expanding(min_periods=LABOR_BURN_IN).quantile(LABOR_PERCENTILE)
    return _bool(l_impulse > thr, thr.notna() & l_impulse.notna()).rename("labor_deteriorating")


def curve_inverted(t10y3m: pd.Series) -> pd.Series:
    """T10Y3M < 0 (daily)."""
    return _bool(t10y3m < 0, t10y3m.notna()).rename("curve_inverted")


def event_flags(state: pd.DataFrame, inputs: dict[str, pd.Series], index: pd.DatetimeIndex) -> pd.DataFrame:
    """All three flags on `index`. `state` is macro_state() output (L_impulse);
    `inputs` holds release-dated "sahm" and "t10y3m" (see inputs.load_flag_inputs)."""
    t = inputs.get("t10y3m")
    return pd.DataFrame({
        "sahm_triggered": sahm_triggered(inputs.get("sahm"), index),
        "labor_deteriorating": labor_deteriorating(state["L_impulse"].reindex(index)),
        "curve_inverted": curve_inverted(to_daily(t, index) if t is not None and not t.empty
                                         else pd.Series(np.nan, index=index)),
    })
