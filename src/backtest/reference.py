"""
src/backtest/reference.py

ReferenceData: loads standard descriptive market-factor series from the DB
and exposes them as aligned daily simple-return and level pd.Series.

Design note — descriptive vs. point-in-time
--------------------------------------------
These factors are used *after the fact* to describe what the market was doing
during a strategy's history.  Because we are conditioning, not predicting, we
use final/latest-revision values rather than point-in-time vintages.  Point-in-
time discipline belongs in signal construction, not in performance attribution.

Dollar-sign convention
----------------------
DTWEXBGS is a broad *trade-weighted* USD index.  A positive daily return means
the dollar *appreciated* against the basket.  Strategies that are long risk
assets will therefore tend to show a negative correlation with this series.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd

from src.db.client import db as _singleton_db
from src.shared.utils import logger

# ── Factor descriptor ─────────────────────────────────────────────────────────

_MIN_OBS = 50


@dataclass(frozen=True)
class Factor:
    """Descriptor for a single reference market factor.

    Args:
        name:   Short identifier used as dict key and column name.
        source: ``'price'`` (prices table) or ``'macro'`` (macro_observations table).
        key:    ``asset_id`` (int) for ``'price'``; FRED/series ``code`` (str) for ``'macro'``.
        note:   Optional human-readable note, e.g. sign convention.
    """

    name: str
    source: str
    key: int | str
    note: str = ""


# ── Default factor set ────────────────────────────────────────────────────────
# Bonds intentionally omitted — TBD.  To add later: Factor("bonds", ...) only.

DEFAULT_FACTORS: list[Factor] = [
    Factor("equities", "price",  100001, "S&P 500 index"),
    Factor("dollar",   "macro",  "DTWEXBGS", "broad USD; +ve = USD up"),
    Factor("gold",     "price",  400003, "S&P GSCI Gold ER"),
    Factor("crude",    "price",  400001, "S&P GSCI Crude Oil ER"),
]


# ── ReferenceData ─────────────────────────────────────────────────────────────

class ReferenceData:
    """Load and expose standard descriptive market-factor return series.

    Caller is responsible for opening and closing the DB connection
    (``db.open()`` / ``db.close()``), consistent with the collector pattern.

    Args:
        factors:  Factor descriptors to load; defaults to ``DEFAULT_FACTORS``.
        start:    Optional ISO date string lower bound (inclusive) for the query.
        end:      Optional ISO date string upper bound (inclusive) for the query.
        database: Database instance to use; defaults to the module-level singleton.
                  Pass an in-memory ``Database(":memory:")`` in tests to stay
                  filesystem-free.

    Example::

        db.open()
        rd = ReferenceData().load()
        print(rd.available())
        print(rd.panel().tail())
        db.close()
    """

    def __init__(
        self,
        factors: list[Factor] | None = None,
        start: str | None = None,
        end: str | None = None,
        database=None,
    ) -> None:
        self._factors: list[Factor] = factors if factors is not None else DEFAULT_FACTORS
        self._start = start
        self._end = end
        self._db = database if database is not None else _singleton_db

        self._lvl: dict[str, pd.Series] = {}
        self._ret: dict[str, pd.Series] = {}

    # ── Public interface ──────────────────────────────────────────────────────

    def load(self) -> "ReferenceData":
        """Query the DB for every configured factor and build return series.

        Factors with zero rows are warned about and excluded; they never raise.
        Returns self for chaining.

        Returns:
            self
        """
        for factor in self._factors:
            try:
                self._load_factor(factor)
            except Exception as exc:
                logger.warning(
                    f"[ReferenceData] Failed to load factor '{factor.name}': {exc}"
                )
        return self

    def levels(self, name: str) -> pd.Series:
        """Return the raw level series for a loaded factor.

        Piece 3's regime layer uses levels (e.g. price above/below MA).

        Args:
            name: Factor name, e.g. ``'equities'``.

        Returns:
            pd.Series indexed by date, name=factor name.

        Raises:
            KeyError: If the factor is not available.
        """
        if name not in self._lvl:
            _avail = ", ".join(self._lvl) or "none"
            raise KeyError(
                f"Factor '{name}' is not available. Loaded factors: {_avail}"
            )
        return self._lvl[name]

    def returns(self, name: str) -> pd.Series:
        """Return the simple daily return series for a loaded factor.

        Returns are ``level.pct_change().dropna()`` so the series is NaN-free
        and one observation shorter than the level series.

        Args:
            name: Factor name, e.g. ``'equities'``.

        Returns:
            pd.Series indexed by date, name=factor name.

        Raises:
            KeyError: If the factor is not available.
        """
        if name not in self._ret:
            _avail = ", ".join(self._ret) or "none"
            raise KeyError(
                f"Factor '{name}' is not available. Loaded factors: {_avail}"
            )
        return self._ret[name]

    def available(self) -> list[str]:
        """Names of factors that loaded successfully.

        Returns:
            List of factor names in load order.
        """
        return list(self._ret.keys())

    def panel(
        self,
        index: pd.DatetimeIndex | None = None,
        how: str = "inner",
    ) -> pd.DataFrame:
        """Factor returns as a DataFrame with one column per factor.

        Args:
            index: Optional DatetimeIndex to reindex to before joining.
            how:   ``'inner'`` (default) drops rows with any NaN;
                   ``'outer'`` keeps all dates and leaves NaNs.

        Returns:
            DataFrame of factor returns.  Empty DataFrame if no factors loaded.
        """
        if not self._ret:
            return pd.DataFrame()

        df = pd.concat(list(self._ret.values()), axis=1, sort=True)

        if index is not None:
            df = df.reindex(index)

        if how == "inner":
            n_before = len(df)
            df = df.dropna(how="any")
            n_dropped = n_before - len(df)
            if n_dropped > 0:
                longest = max(len(s) for s in self._ret.values())
                if longest > 0 and n_dropped / longest > 0.20:
                    logger.warning(
                        f"[ReferenceData] Inner join dropped {n_dropped}/{n_before} rows "
                        f"({n_dropped / n_before:.1%}) — possible calendar mismatch "
                        f"between factor return series."
                    )

        return df

    # ── Private helpers ─────────────────��─────────────────────────────────────

    def _load_factor(self, factor: Factor) -> None:
        """Query one factor, build level + return series, store internally."""
        if factor.source == "price":
            sql, params = self._build_price_query(factor)
            val_col = "close"
        elif factor.source == "macro":
            sql, params = self._build_macro_query(factor)
            val_col = "value"
        else:
            logger.warning(
                f"[ReferenceData] Unknown source '{factor.source}' for factor "
                f"'{factor.name}' — skipping."
            )
            return

        rows = self._db.query(sql, params)

        if not rows:
            logger.warning(
                f"[ReferenceData] No data found for factor '{factor.name}' — skipping."
            )
            return

        dates = pd.to_datetime([row["d"] for row in rows])
        values = [float(row[val_col]) for row in rows]

        lvl = pd.Series(values, index=dates, name=factor.name, dtype=float).sort_index()
        ret = lvl.pct_change().dropna()
        ret.name = factor.name

        n = len(ret)
        if n == 0:
            logger.warning(
                f"[ReferenceData] Factor '{factor.name}' produced zero return "
                f"observations (only {len(lvl)} level row(s)) — skipping."
            )
            return
        if n < _MIN_OBS:
            logger.warning(
                f"[ReferenceData] Factor '{factor.name}' has only {n} return "
                f"observation(s) — fewer than {_MIN_OBS}, results may be unreliable."
            )

        self._lvl[factor.name] = lvl
        self._ret[factor.name] = ret

    def _build_price_query(self, factor: Factor) -> tuple[str, list]:
        """Build SQL + params for a price-source factor.

        Deduplicates by calendar date, keeping the row with the latest intraday
        timestamp (handles any sub-daily duplicates defensively).
        """
        inner_clauses = ["asset_id = ?", "interval = '1d'"]
        params: list[object] = [factor.key]

        # Use timezone('UTC', timestamp)::DATE so the calendar date is always
        # extracted in UTC, regardless of the DuckDB session timezone.
        if self._start is not None:
            inner_clauses.append("timezone('UTC', timestamp)::DATE >= ?")
            params.append(self._start)
        if self._end is not None:
            inner_clauses.append("timezone('UTC', timestamp)::DATE <= ?")
            params.append(self._end)

        where = " AND ".join(inner_clauses)
        sql = f"""
            SELECT d, close FROM (
                SELECT timezone('UTC', timestamp)::DATE AS d, close,
                       ROW_NUMBER() OVER (
                           PARTITION BY timezone('UTC', timestamp)::DATE
                           ORDER BY timestamp DESC
                       ) AS rn
                FROM prices
                WHERE {where}
            ) AS sub
            WHERE sub.rn = 1
            ORDER BY d
        """
        return sql, list(params)

    def _build_macro_query(self, factor: Factor) -> tuple[str, list]:
        """Build SQL + params for a macro-source factor.

        Uses the latest-revision value per period_date, robust whether or not
        ``is_final`` is set.
        """
        inner_clauses = ["ms.code = ?"]
        params: list[object] = [factor.key]

        if self._start is not None:
            inner_clauses.append("mo.period_date >= ?")
            params.append(self._start)
        if self._end is not None:
            inner_clauses.append("mo.period_date <= ?")
            params.append(self._end)

        where = " AND ".join(inner_clauses)
        sql = f"""
            SELECT period_date AS d, value FROM (
                SELECT mo.period_date, mo.value,
                       ROW_NUMBER() OVER (
                           PARTITION BY mo.period_date
                           ORDER BY mo.release_date DESC
                       ) AS rn
                FROM macro_observations mo
                JOIN macro_series ms ON ms.id = mo.series_id
                WHERE {where}
            ) AS sub
            WHERE sub.rn = 1
            ORDER BY d
        """
        return sql, list(params)
