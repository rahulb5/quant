"""
src/backtest/regimes.py

Market-regime classifiers that produce lookahead-clean categorical labels.

Design — lookahead discipline
-----------------------------
A regime label for date t must use only information available through t.
Volatility-tercile thresholds are therefore computed on an expanding window
(or a trailing rolling window) — never on the full sample.  Sign- and
trailing-MA-based regimes are point-in-time by construction (only historical
prices are needed).

Contemporaneous note: including the current observation in its own expanding
quantile is contemporaneous, not lookahead — the quantile is over the
distribution of past-and-current values, not future ones.  This is standard
practice for regime construction and mirrors how a practitioner would have
classified conditions in real time.

Structure: pure label functions (no DB, fully unit-testable) plus Regime
subclasses that load a level series via ReferenceData and call those functions.
This mirrors the metrics / PerformanceReport split in Pieces 1–2.
"""

from __future__ import annotations

from abc import ABC, abstractmethod

import pandas as pd

from src.backtest.reference import Factor, ReferenceData
from src.db.client import db as _singleton_db
from src.shared.utils import logger


# ── Pure label functions ──────────────────────────────────────────────────────

def expanding_bucket_labels(
    series: pd.Series,
    n_buckets: int = 3,
    burn_in: int = 252,
    window: int | None = None,
    labels: list[str] | None = None,
) -> pd.Series:
    """Classify each observation into a quantile bucket using only past data.

    Quantile thresholds are re-estimated at every date using an expanding
    window (``window=None``) or a trailing rolling window of length ``window``,
    with ``min_periods=burn_in``.  Dates before enough data has accumulated
    receive a NaN label.

    Note: the current observation is included when computing its own threshold
    (contemporaneous), which is correct for a real-time practitioner standpoint.
    The key lookahead-clean property is that truncating the series at any date k
    produces identical labels for the prefix — confirmed by test.

    Args:
        series:    Input level/value series (must have a DatetimeIndex).
        n_buckets: Number of quantile buckets (default 3 → terciles).
        burn_in:   Minimum observations required before emitting labels.
        window:    Rolling window length; ``None`` uses an expanding window.
        labels:    Bucket names in ascending order.  Defaults to
                   ``["low", "mid", "high"]`` for ``n_buckets=3``, or
                   ``["bucket_0", ..., "bucket_{n-1}"]`` otherwise.
                   Must satisfy ``len(labels) == n_buckets``.

    Returns:
        pd.Series (object dtype) with the same index as ``series``.
        NaN where the burn-in period has not yet been satisfied.

    Raises:
        ValueError: If ``len(labels) != n_buckets``.
    """
    if labels is None:
        if n_buckets == 3:
            labels = ["low", "mid", "high"]
        else:
            labels = [f"bucket_{i}" for i in range(n_buckets)]

    if len(labels) != n_buckets:
        raise ValueError(
            f"len(labels)={len(labels)} must equal n_buckets={n_buckets}"
        )

    # Build threshold series for each cut point q = i/n_buckets
    thresholds: list[pd.Series] = []
    for i in range(1, n_buckets):
        q = i / n_buckets
        if window is None:
            thr = series.expanding(min_periods=burn_in).quantile(q)
        else:
            thr = series.rolling(window, min_periods=burn_in).quantile(q)
        thresholds.append(thr)

    result = pd.Series(index=series.index, dtype=object)

    # Dates where we have valid thresholds and a non-NaN observation
    if thresholds:
        valid_mask = series.notna() & thresholds[0].notna()
    else:
        valid_mask = series.notna()

    # Assign top bucket for all valid dates, then override downward
    result.loc[valid_mask] = labels[-1]
    for b in range(n_buckets - 2, -1, -1):
        condition = valid_mask & (series <= thresholds[b])
        result.loc[condition] = labels[b]

    return result


def threshold_labels(
    series: pd.Series,
    thresholds: list[float],
    labels: list[str],
) -> pd.Series:
    """Classify each observation against fixed cut points.

    Args:
        series:     Input level series.
        thresholds: Fixed cut points in ascending order.
                    Must satisfy ``len(labels) == len(thresholds) + 1``.
        labels:     Bucket names in ascending order.  The bucket at index ``b``
                    covers values strictly less than ``thresholds[b]``;
                    the final bucket covers values >= the last threshold.

    Returns:
        pd.Series (object dtype) with the same index as ``series``.
        NaN where ``series`` is NaN.
    """
    result = pd.Series(index=series.index, dtype=object)
    valid = series.notna()
    result.loc[valid] = labels[-1]
    for b in range(len(thresholds) - 1, -1, -1):
        condition = valid & (series < thresholds[b])
        result.loc[condition] = labels[b]
    return result


def ma_trend_labels(
    series: pd.Series,
    ma_window: int = 200,
    labels: tuple[str, str] = ("below", "above"),
) -> pd.Series:
    """Label each date as above or below a trailing moving average.

    Point-in-time by construction: only uses data through each date.

    Args:
        series:    Input price/level series.
        ma_window: Number of periods in the trailing simple moving average.
        labels:    ``(below_label, above_label)``.  Value strictly greater
                   than the MA gets ``labels[1]``; otherwise ``labels[0]``.

    Returns:
        pd.Series (object dtype) with the same index as ``series``.
        NaN for the first ``ma_window - 1`` observations.
    """
    ma = series.rolling(ma_window).mean()
    result = pd.Series(index=series.index, dtype=object)
    valid = series.notna() & ma.notna()
    result.loc[valid & (series > ma)] = labels[1]
    result.loc[valid & (series <= ma)] = labels[0]
    return result


# ── Regime base class ─────────────────────────────────────────────────────────

class Regime(ABC):
    """Abstract base for lookahead-clean regime classifiers.

    Each concrete subclass exposes a ``labels()`` method that returns a
    pd.Series of categorical strings indexed by date.

    Regimes load the *full* available history by default (``start``/``end``
    optional) so that the expanding burn-in is consumed by pre-strategy data
    where possible.  The Conditioner (Piece 3b) aligns labels to the strategy
    window after loading.

    Args:
        database: DB instance to forward to ReferenceData.  Defaults to the
                  module-level singleton (caller must have called db.open()).
        start:    Optional ISO date string lower bound for the level query.
        end:      Optional ISO date string upper bound for the level query.
    """

    name: str  # set by each concrete subclass

    def __init__(
        self,
        database=None,
        start: str | None = None,
        end: str | None = None,
    ) -> None:
        self._db = database
        self._start = start
        self._end = end

    @abstractmethod
    def labels(self) -> pd.Series:
        """Return categorical regime labels indexed by date.

        Returns:
            pd.Series (object dtype).  Empty if underlying data is missing.
        """
        ...

    # ── Protected helper ──────────────────────────────────────────────────────

    def _load_level(self, factor: Factor) -> pd.Series:
        """Load the level series for a single factor via ReferenceData.

        Args:
            factor: Factor descriptor (source, key, name).

        Returns:
            pd.Series of level values, or an empty pd.Series (float dtype)
            if the factor produced no data (warning logged).
        """
        rd = ReferenceData(
            factors=[factor],
            start=self._start,
            end=self._end,
            database=self._db,
        ).load()

        try:
            return rd.levels(factor.name)
        except KeyError:
            logger.warning(
                f"[{self.__class__.__name__}:{self.name}] No level data for "
                f"factor '{factor.name}' — labels() will return an empty Series."
            )
            return pd.Series(dtype=float)


# ── Concrete regimes ──────────────────────────────────────────────────────────

class VolRegime(Regime):
    """Volatility tercile regime based on VIX (VIXCLS).

    Uses an expanding-window quantile so thresholds are always computed on
    past-and-current data only.  The long burn-in (252 days by default)
    ensures the initial tercile boundaries are stable before being used.

    Labels (ascending VIX level): ``low_vol``, ``mid_vol``, ``high_vol``.
    """

    name = "volatility"

    def __init__(
        self,
        series_code: str = "VIXCLS",
        n_buckets: int = 3,
        burn_in: int = 252,
        window: int | None = None,
        labels: list[str] | None = None,
        database=None,
        start: str | None = None,
        end: str | None = None,
    ) -> None:
        super().__init__(database=database, start=start, end=end)
        self._series_code = series_code
        self._n_buckets = n_buckets
        self._burn_in = burn_in
        self._window = window
        self._labels = labels or ["low_vol", "mid_vol", "high_vol"]

    def labels(self) -> pd.Series:
        """Return VIX-tercile regime labels by date."""
        factor = Factor(self.name, "macro", self._series_code)
        level = self._load_level(factor)
        if level.empty:
            return pd.Series(dtype=object)
        return expanding_bucket_labels(
            level,
            n_buckets=self._n_buckets,
            burn_in=self._burn_in,
            window=self._window,
            labels=self._labels,
        )


class RatesRegime(Regime):
    """Yield-curve regime based on the 10Y–2Y Treasury spread (T10Y2Y).

    Classifies the curve as inverted (spread < threshold) or normal
    (spread >= threshold).  Point-in-time by construction — only the
    current spread value is needed.

    Labels: ``inverted``, ``normal``.
    """

    name = "rates_curve"

    def __init__(
        self,
        series_code: str = "T10Y2Y",
        threshold: float = 0.0,
        labels: list[str] | None = None,
        database=None,
        start: str | None = None,
        end: str | None = None,
    ) -> None:
        super().__init__(database=database, start=start, end=end)
        self._series_code = series_code
        self._threshold = threshold
        self._labels = labels or ["inverted", "normal"]

    def labels(self) -> pd.Series:
        """Return yield-curve regime labels by date."""
        factor = Factor(self.name, "macro", self._series_code)
        level = self._load_level(factor)
        if level.empty:
            return pd.Series(dtype=object)
        return threshold_labels(level, [self._threshold], self._labels)


class TrendRegime(Regime):
    """Equity trend regime based on S&P 500 price vs. its trailing MA.

    Classifies each date as risk-on (price > 200-day MA) or risk-off
    (price <= 200-day MA).  Trailing MA → point-in-time by construction.

    Labels: ``risk_off``, ``risk_on``.
    """

    name = "equity_trend"

    def __init__(
        self,
        asset_id: int = 100001,
        ma_window: int = 200,
        labels: tuple[str, str] | None = None,
        database=None,
        start: str | None = None,
        end: str | None = None,
    ) -> None:
        super().__init__(database=database, start=start, end=end)
        self._asset_id = asset_id
        self._ma_window = ma_window
        self._labels = labels or ("risk_off", "risk_on")

    def labels(self) -> pd.Series:
        """Return equity-trend regime labels by date."""
        factor = Factor(self.name, "price", self._asset_id)
        level = self._load_level(factor)
        if level.empty:
            return pd.Series(dtype=object)
        return ma_trend_labels(level, ma_window=self._ma_window, labels=self._labels)


# ── Default regime set ────────────────────────────────────────────────────────

def default_regimes(
    database=None,
    start: str | None = None,
    end: str | None = None,
) -> list[Regime]:
    """Return the standard set of regime classifiers.

    The three default regimes cover volatility, rates curve shape, and equity
    trend.  All share the same ``database``, ``start``, and ``end`` parameters.
    The Conditioner (Piece 3b) iterates this list and aligns each regime's
    labels to the strategy window.

    Args:
        database: DB instance to inject; defaults to the module-level singleton.
        start:    Optional ISO date string lower bound for history loading.
        end:      Optional ISO date string upper bound for history loading.

    Returns:
        List of [VolRegime, RatesRegime, TrendRegime].
    """
    kwargs: dict = dict(database=database, start=start, end=end)
    return [
        VolRegime(**kwargs),
        RatesRegime(**kwargs),
        TrendRegime(**kwargs),
    ]
