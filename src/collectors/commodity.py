"""
src/collectors/commodity.py

Commodity futures price collector using Nasdaq Data Link (CHRIS dataset).

Fetches daily OHLCV for continuous contract series (front through 4th month)
and inserts into the prices table. Uses the 300xxx asset ID block.

CHRIS ticker format: CHRIS/{EXCHANGE}_{ROOT}{N}
  e.g. CHRIS/CME_CL1 = front-month WTI crude

Usage:
    from src.db.client import db
    from src.collectors.commodity import CommodityCollector

    db.open()
    collector = CommodityCollector()
    collector.fetch_all()
    db.close()
"""

from __future__ import annotations

import os
from datetime import date, timedelta

import nasdaqdatalink
import pandas as pd

from src.collectors.base import BaseCollector
from src.db.client import db

_ID_BLOCK_START = 300_001

# ── Ticker catalogue ──────────────────────────────────────────────────────────

COMMODITY_CATALOGUE: list[dict[str, str]] = [
    # Energy
    {"root": "CL", "exchange": "CME", "name": "Crude Oil WTI"},
    {"root": "NG", "exchange": "CME", "name": "Natural Gas"},
    # Metals
    {"root": "GC", "exchange": "CME", "name": "Gold"},
    {"root": "SI", "exchange": "CME", "name": "Silver"},
    {"root": "HG", "exchange": "CME", "name": "Copper"},
    # Agriculture
    {"root": "C",  "exchange": "CME", "name": "Corn"},
    {"root": "W",  "exchange": "CME", "name": "Wheat"},
    {"root": "S",  "exchange": "CME", "name": "Soybeans"},
    # Softs
    {"root": "KC", "exchange": "ICE", "name": "Coffee"},
    {"root": "SB", "exchange": "ICE", "name": "Sugar"},
    {"root": "CC", "exchange": "ICE", "name": "Cocoa"},
]

CONTRACT_MONTHS = [1, 2, 3, 4]


class CommodityCollector(BaseCollector):
    """Collects daily OHLCV commodity futures prices from Nasdaq Data Link."""

    def __init__(self) -> None:
        super().__init__(source="nasdaq")
        api_key = os.environ.get("NASDAQ_API_KEY")
        if not api_key:
            raise RuntimeError("NASDAQ_API_KEY is not set. Add it to your .env file.")
        nasdaqdatalink.ApiConfig.api_key = api_key

    # ── Public interface ──────────────────────────────────────────────────────

    def get_all_tickers(self) -> list[dict[str, str]]:
        """Return the full list of commodity-month combinations.

        Returns:
            List of dicts with keys:
              - ticker:      e.g. 'CL1', 'CL2'
              - chris_code:  e.g. 'CHRIS/CME_CL1'
              - name:        e.g. 'Crude Oil WTI - Front Month'
        """
        tickers = []
        month_labels = {1: "Front Month", 2: "2nd Month", 3: "3rd Month", 4: "4th Month"}
        for commodity in COMMODITY_CATALOGUE:
            for month in CONTRACT_MONTHS:
                ticker = f"{commodity['root']}{month}"
                chris_code = f"CHRIS/{commodity['exchange']}_{ticker}"
                name = f"{commodity['name']} - {month_labels[month]}"
                tickers.append({
                    "ticker": ticker,
                    "chris_code": chris_code,
                    "name": name,
                })
        return tickers

    def ensure_asset(self, ticker: str, name: str) -> int:
        """Return the asset id for ticker, inserting as 'futures' if needed.

        Uses the 300xxx ID block.
        """
        rows = db.query("SELECT id FROM assets WHERE ticker = ?", [ticker])
        if rows:
            return int(rows[0]["id"])

        next_id = self._next_commodity_id()
        db.run(
            """
            INSERT INTO assets (id, ticker, name, asset_class, currency)
            VALUES (?, ?, ?, 'futures', 'USD')
            """,
            [next_id, ticker, name],
        )
        self.logger.debug(f"[{self.source}] Registered futures {ticker} (id={next_id})")
        return next_id

    def fetch_single(
        self,
        chris_code: str,
        asset_id: int,
        ticker: str,
        from_date: str,
        to_date: str,
    ) -> int:
        """Fetch OHLCV for one CHRIS series and insert into prices.

        Args:
            chris_code: e.g. 'CHRIS/CME_CL1'
            asset_id:   Row id in assets table.
            ticker:     e.g. 'CL1' (for logging).
            from_date:  Start date inclusive.
            to_date:    End date inclusive.

        Returns:
            Number of rows inserted.
        """
        try:
            df = nasdaqdatalink.get(
                chris_code,
                start_date=from_date,
                end_date=to_date,
            )
        except Exception as e:
            self.logger.warning(f"[{self.source}] {ticker}: API error: {e}")
            return 0

        if df is None or df.empty:
            self.logger.warning(f"[{self.source}] {ticker}: no data returned")
            return 0

        return self._insert_prices(asset_id, ticker, df)

    # ── BaseCollector implementation (not used directly, but required) ────────

    def collect(
        self,
        from_date: str,
        to_date: str,
        asset_id: int | None = None,
        series_id: int | None = None,
        interval: str | None = None,
    ) -> int:
        """Not used directly — use fetch_single() instead."""
        raise NotImplementedError("Use fetch_single() for commodity futures.")

    # ── Private helpers ───────────────────────────────────────────────────────

    def _insert_prices(self, asset_id: int, ticker: str, df: pd.DataFrame) -> int:
        """Insert OHLCV rows from a CHRIS DataFrame into the prices table.

        CHRIS DataFrames have columns like 'Open', 'High', 'Low', 'Close'
        (or 'Last', 'Settle' depending on the contract). Volume may be
        'Volume' or 'Prev. Day Open Interest' or missing entirely.

        Handles column name variations across different CHRIS series.
        Uses ON CONFLICT DO NOTHING for idempotent inserts.
        """
        # Normalise column names: CHRIS datasets vary
        col_map = {}
        cols_lower = {c.lower(): c for c in df.columns}

        # Open
        for candidate in ["open", "opening price"]:
            if candidate in cols_lower:
                col_map["open"] = cols_lower[candidate]
                break

        # High
        for candidate in ["high", "high price"]:
            if candidate in cols_lower:
                col_map["high"] = cols_lower[candidate]
                break

        # Low
        for candidate in ["low", "low price"]:
            if candidate in cols_lower:
                col_map["low"] = cols_lower[candidate]
                break

        # Close — prefer 'settle' or 'last' over 'close' for futures
        for candidate in ["settle", "last", "close", "closing price"]:
            if candidate in cols_lower:
                col_map["close"] = cols_lower[candidate]
                break

        # Volume — optional, default to 0
        for candidate in ["volume", "total volume"]:
            if candidate in cols_lower:
                col_map["volume"] = cols_lower[candidate]
                break

        # Must have at least OHLC
        for required in ["open", "high", "low", "close"]:
            if required not in col_map:
                self.logger.warning(
                    f"[{self.source}] {ticker}: missing '{required}' column. "
                    f"Available: {list(df.columns)}"
                )
                return 0

        has_volume = "volume" in col_map
        inserted = 0

        def steps(q) -> None:
            nonlocal inserted
            for ts, row in df.iterrows():
                o = row[col_map["open"]]
                h = row[col_map["high"]]
                l = row[col_map["low"]]
                c = row[col_map["close"]]

                # Skip rows with NaN in OHLC
                if pd.isna(o) or pd.isna(h) or pd.isna(l) or pd.isna(c):
                    continue

                v = float(row[col_map["volume"]]) if has_volume and not pd.isna(row[col_map["volume"]]) else 0.0

                timestamp = ts.isoformat() if hasattr(ts, "isoformat") else str(ts)[:10]

                q(
                    """
                    INSERT INTO prices
                      (asset_id, interval, timestamp, open, high, low, close, volume, source)
                    VALUES (?, '1d', ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT DO NOTHING
                    """,
                    [asset_id, timestamp, float(o), float(h), float(l), float(c), v, self.source],
                )
                inserted += 1

        db.transaction(steps)
        return inserted

    def _next_commodity_id(self) -> int:
        """Return the next available id in the 300xxx block."""
        rows = db.query(
            "SELECT COALESCE(MAX(id), ?) AS max_id FROM assets WHERE id >= ? AND id < ?",
            [_ID_BLOCK_START - 1, _ID_BLOCK_START, 400_000],
        )
        return int(rows[0]["max_id"]) + 1
