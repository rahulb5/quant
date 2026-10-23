"""
src/macro/transforms.py

Pure transforms for macro / positioning series: sampling-rate inference,
level/change transforms, and a trailing rolling z-score. No DB, no Streamlit.

Stored `macro_series.frequency` labels are not trusted (several weekly FRED
series are tagged 'daily'), so periods-per-year is inferred from the dates.

Usage:
    from src.macro.transforms import periods_per_year, transform, rolling_zscore

    ppy = periods_per_year(s.index)
    yoy = transform(s, "yoy_pct")
    z = rolling_zscore(yoy, window=round(5 * ppy))
"""

from __future__ import annotations

import pandas as pd

TRANSFORMS: dict[str, str] = {
    "level":      "Level",
    "diff":       "Change (1 period)",
    "pct":        "% change (1 period)",
    "yoy_diff":   "YoY change",
    "yoy_pct":    "YoY % change",
}

# Transforms whose output is a flow (period-over-period); drawn as bars by default.
CHANGE_TRANSFORMS = frozenset({"diff", "pct", "yoy_diff", "yoy_pct"})

# Median gap (days) → nominal periods per year. Business-day series land on 252.
_PPY_BUCKETS: list[tuple[float, int, str]] = [
    (4, 252, "daily"),
    (10, 52, "weekly"),
    (45, 12, "monthly"),
    (120, 4, "quarterly"),
    (float("inf"), 1, "annual"),
]


def _bucket(index: pd.DatetimeIndex) -> tuple[int, str]:
    if len(index) < 2:
        return 1, "annual"
    gap = pd.Series(index).diff().dt.days.median()
    for limit, ppy, label in _PPY_BUCKETS:
        if gap <= limit:
            return ppy, label
    raise AssertionError("unreachable")


def periods_per_year(index: pd.DatetimeIndex) -> int:
    """Nominal observations per year, inferred from the median date gap."""
    return _bucket(index)[0]


def frequency_label(index: pd.DatetimeIndex) -> str:
    """'daily' / 'weekly' / 'monthly' / 'quarterly' / 'annual', inferred from dates."""
    return _bucket(index)[1]


def history_years(index: pd.DatetimeIndex) -> float:
    if len(index) < 2:
        return 0.0
    return (index[-1] - index[0]).days / 365.25


def _year_ago(s: pd.Series) -> pd.Series:
    """Value as of one calendar year before each date (last obs on or before)."""
    prior = s.asof(s.index - pd.DateOffset(years=1))
    prior.index = s.index
    return prior


def transform(s: pd.Series, kind: str) -> pd.Series:
    """Apply a TRANSFORMS key to a date-indexed series. NaNs are dropped.

    YoY transforms compare against the last observation at or before the same
    date a calendar year earlier, so they are robust to missing business days.
    """
    if kind not in TRANSFORMS:
        raise ValueError(f"Unknown transform {kind!r}; expected one of {list(TRANSFORMS)}")
    s = s.dropna().sort_index()

    if kind == "level":
        out = s
    elif kind == "diff":
        out = s.diff()
    elif kind == "pct":
        out = s.pct_change(fill_method=None) * 100
    elif kind == "yoy_diff":
        out = s - _year_ago(s)
    else:  # yoy_pct
        out = (s / _year_ago(s) - 1) * 100

    return out.replace([float("inf"), float("-inf")], float("nan")).dropna()


def rolling_zscore(s: pd.Series, window: int, min_periods: int | None = None) -> pd.Series:
    """Trailing z-score: (x_t - mean) / std over the last `window` obs, inclusive of t.

    Uses only data up to t, so it is lookahead-clean. `min_periods` defaults
    to the full window; periods with zero dispersion yield NaN.
    """
    if window < 2:
        raise ValueError("window must be >= 2")
    min_periods = window if min_periods is None else min_periods
    roll = s.rolling(window, min_periods=min_periods)
    std = roll.std()
    z = (s - roll.mean()) / std.where(std > 0)
    return z.dropna()
