"""
data/repository.py

Typed insert helpers for each table in quant.db.
Uses the shared `db` singleton — call `db.open()` before using any function.

Usage:
    from src.db.client import db
    from data.repository import insert_asset, insert_prices, log_fetch, Asset, Price

    db.open()
    insert_asset(Asset(id=1, ticker="AAPL", name="Apple Inc.", asset_class="equity"))
    db.close()
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from src.db.client import db

# ── Types ────────────────────────────────────────────────────────────────────

AssetClass     = Literal["equity", "crypto", "forex", "commodity", "futures", "index"]
PriceInterval  = Literal["1m", "5m", "15m", "30m", "1h", "4h", "1d", "1w", "1M"]
MacroFrequency = Literal["daily", "weekly", "monthly", "quarterly", "annual"]
FetchStatus    = Literal["success", "partial", "failed"]


# ── Assets ────────────────────────────────────────────────────────────────────

@dataclass
class Asset:
    id: int
    ticker: str
    name: str
    asset_class: AssetClass
    exchange: str | None = None
    currency: str = "USD"
    is_active: bool = True


def insert_asset(asset: Asset) -> int:
    """Insert one asset. Skips on duplicate (ticker, exchange). Returns rows changed."""
    return db.run(
        """
        INSERT INTO assets (id, ticker, name, asset_class, exchange, currency, is_active)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT DO NOTHING
        """,
        [asset.id, asset.ticker, asset.name, asset.asset_class,
         asset.exchange, asset.currency, asset.is_active],
    )


def insert_assets(assets: list[Asset]) -> None:
    """Bulk-insert assets in a single transaction."""
    def steps(q):
        for asset in assets:
            q(
                """
                INSERT INTO assets (id, ticker, name, asset_class, exchange, currency, is_active)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT DO NOTHING
                """,
                [asset.id, asset.ticker, asset.name, asset.asset_class,
                 asset.exchange, asset.currency, asset.is_active],
            )

    db.transaction(steps)


# ── Prices ────────────────────────────────────────────────────────────────────

@dataclass
class Price:
    asset_id: int
    interval: PriceInterval
    timestamp: str        # ISO 8601, e.g. "2024-01-01T00:00:00Z"
    open: float
    high: float
    low: float
    close: float
    volume: float
    source: str
    adj_close: float | None = None
    vwap: float | None = None


def insert_price(price: Price) -> int:
    """Insert one OHLCV row. Skips on duplicate primary key. Returns rows changed."""
    return db.run(
        """
        INSERT INTO prices
          (asset_id, interval, timestamp, open, high, low, close, volume, source, adj_close, vwap)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT DO NOTHING
        """,
        [price.asset_id, price.interval, price.timestamp,
         price.open, price.high, price.low, price.close,
         price.volume, price.source, price.adj_close, price.vwap],
    )


def insert_prices(prices: list[Price]) -> None:
    """Bulk-insert OHLCV rows in a single transaction."""
    def steps(q):
        for price in prices:
            q(
                """
                INSERT INTO prices
                  (asset_id, interval, timestamp, open, high, low, close, volume, source, adj_close, vwap)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT DO NOTHING
                """,
                [price.asset_id, price.interval, price.timestamp,
                 price.open, price.high, price.low, price.close,
                 price.volume, price.source, price.adj_close, price.vwap],
            )

    db.transaction(steps)


# ── Macro series ──────────────────────────────────────────────────────────────

@dataclass
class MacroSeries:
    id: int
    code: str
    name: str
    source: str
    frequency: MacroFrequency
    units: str
    seasonal_adj: bool = False
    description: str | None = None


def insert_macro_series(series: MacroSeries) -> int:
    """Insert a macro series definition. Skips on duplicate code. Returns rows changed."""
    return db.run(
        """
        INSERT INTO macro_series
          (id, code, name, source, frequency, units, seasonal_adj, description)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT DO NOTHING
        """,
        [series.id, series.code, series.name, series.source,
         series.frequency, series.units, series.seasonal_adj, series.description],
    )


# ── Macro observations ────────────────────────────────────────────────────────

@dataclass
class MacroObservation:
    series_id: int
    period_date: str      # DATE string, e.g. "2024-01-01"
    release_date: str     # DATE string
    value: float
    is_final: bool = False


def insert_macro_observation(obs: MacroObservation) -> int:
    """Insert one macro observation. Skips on duplicate primary key. Returns rows changed."""
    return db.run(
        """
        INSERT INTO macro_observations
          (series_id, period_date, release_date, value, is_final)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT DO NOTHING
        """,
        [obs.series_id, obs.period_date, obs.release_date, obs.value, obs.is_final],
    )


def insert_macro_observations(observations: list[MacroObservation]) -> None:
    """Bulk-insert macro observations in a single transaction."""
    def steps(q):
        for obs in observations:
            q(
                """
                INSERT INTO macro_observations
                  (series_id, period_date, release_date, value, is_final)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT DO NOTHING
                """,
                [obs.series_id, obs.period_date, obs.release_date, obs.value, obs.is_final],
            )

    db.transaction(steps)


# ── Data fetch log ────────────────────────────────────────────────────────────

@dataclass
class DataFetchLog:
    fetched_from: str     # DATE string
    fetched_to: str       # DATE string
    source: str
    rows_inserted: int
    status: FetchStatus
    asset_id: int | None = None
    series_id: int | None = None
    interval: str | None = None
    error_msg: str | None = None


def log_fetch(entry: DataFetchLog) -> int:
    """Append a row to the data_fetch_log audit table. Returns rows changed."""
    return db.run(
        """
        INSERT INTO data_fetch_log
          (asset_id, series_id, interval, fetched_from, fetched_to, source, rows_inserted, status, error_msg)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [entry.asset_id, entry.series_id, entry.interval,
         entry.fetched_from, entry.fetched_to,
         entry.source, entry.rows_inserted, entry.status, entry.error_msg],
    )
