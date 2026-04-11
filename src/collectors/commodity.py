"""
src/collectors/commodity.py

Commodity futures price collector using Stooq (front-month continuous contracts).

Fetches daily OHLC data via Stooq's CSV endpoint. Volume is not available
from Stooq, so it defaults to 0.0.

Uses the 300xxx asset ID block. Asset class is 'futures'.

Stooq URL format:
    https://stooq.com/q/d/l/?s={stooq_ticker}&i=d
    e.g. https://stooq.com/q/d/l/?s=cl.f&i=d

Usage:
    from src.db.client import db
    from src.collectors.commodity import CommodityCollector

    db.open()
    collector = CommodityCollector()
    collector.fetch_all()
    db.close()
"""

from __future__ import annotations

from io import StringIO

import pandas as pd
import requests

from src.collectors.base import BaseCollector
from src.db.client import db

_ID_BLOCK_START = 300_001

# ── Ticker catalogue ──────────────────────────────────────────────────────────

COMMODITY_CATALOGUE: list[dict[str, str]] = [
    # Energy
    {"ticker": "CL1", "stooq": "cl.f", "name": "Crude Oil WTI - Front Month"},
    {"ticker": "NG1", "stooq": "ng.f", "name": "Natural Gas - Front Month"},
    # Metals
    {"ticker": "GC1", "stooq": "gc.f", "name": "Gold - Front Month"},
    {"ticker": "SI1", "stooq": "si.f", "name": "Silver - Front Month"},
    {"ticker": "HG1", "stooq": "hg.f", "name": "Copper - Front Month"},
    # Agriculture
    {"ticker": "ZC1", "stooq": "zc.f", "name": "Corn - Front Month"},
    {"ticker": "ZW1", "stooq": "zw.f", "name": "Wheat - Front Month"},
    {"ticker": "ZS1", "stooq": "zs.f", "name": "Soybeans - Front Month"},
    # Softs
    {"ticker": "KC1", "stooq": "kc.f", "name": "Coffee - Front Month"},
    {"ticker": "SB1", "stooq": "sb.f", "name": "Sugar - Front Month"},
    {"ticker": "CC1", "stooq": "cc.f", "name": "Cocoa - Front Month"},
]


class CommodityCollector(BaseCollector):
    """Collects daily OHLC commodity futures prices from Stooq."""

    def __init__(self) -> None:
        super().__init__(source="stooq")

    # ── Public interface ──────────────────────────────────────────────────────

    @staticmethod
    def get_all_tickers() -> list[dict[str, str]]:
        """Return the full commodity catalogue.

        Returns:
            List of dicts with keys: ticker, stooq, name.
        """
        return list(COMMODITY_CATALOGUE)

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
        stooq_ticker: str,
        asset_id: int,
        ticker: str,
        from_date: str | None = None,
        to_date: str | None = None,
    ) -> int:
        """Fetch OHLC for one Stooq series and insert into prices.

        Args:
            stooq_ticker: Stooq ticker, e.g. 'cl.f'.
            asset_id:     Row id in assets table.
            ticker:       Our ticker, e.g. 'CL1' (for logging).
            from_date:    Start date inclusive (optional, Stooq returns all if omitted).
            to_date:      End date inclusive (optional).

        Returns:
            Number of rows inserted.
        """
        df = self._fetch_stooq(stooq_ticker)

        if df is None or df.empty:
            self.logger.warning(f"[{self.source}] {ticker}: no data returned")
            return 0

        # Filter by date range if specified
        if from_date:
            df = df[df.index >= from_date]
        if to_date:
            df = df[df.index <= to_date]

        if df.empty:
            self.logger.info(f"[{self.source}] {ticker}: no data in range")
            return 0

        return self._insert_prices(asset_id, ticker, df)

    # ── BaseCollector implementation (required but not used directly) ─────────

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

    def _fetch_stooq(self, stooq_ticker: str) -> pd.DataFrame | None:
        """Download daily OHLC from Stooq's CSV endpoint.

        Returns a DataFrame indexed by date with columns: Open, High, Low, Close.
        Returns None on error.
        """
        url = f"https://stooq.com/q/d/l/?s={stooq_ticker}&i=d"
        self.logger.debug(f"[{self.source}] Fetching {url}")

        try:
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
        except Exception as e:
            self.logger.error(f"[{self.source}] Request failed for {stooq_ticker}: {e}")
            return None

        text = resp.text.strip()

        # Stooq returns "No data" or a very short response if ticker is invalid
        if len(text) < 50 or "No data" in text:
            return None

        try:
            df = pd.read_csv(StringIO(text), parse_dates=["Date"], index_col="Date")
        except Exception as e:
            self.logger.error(f"[{self.source}] CSV parse failed for {stooq_ticker}: {e}")
            return None

        # Stooq returns columns: Date, Open, High, Low, Close (no Volume)
        required = {"Open", "High", "Low", "Close"}
        if not required.issubset(set(df.columns)):
            self.logger.warning(
                f"[{self.source}] {stooq_ticker}: unexpected columns {list(df.columns)}"
            )
            return None

        df = df.sort_index()
        return df

    def _insert_prices(self, asset_id: int, ticker: str, df: pd.DataFrame) -> int:
        """Insert OHLC rows into the prices table.

        Volume is set to 0.0 since Stooq doesn't provide it for commodities.
        Uses ON CONFLICT DO NOTHING for idempotent inserts.
        """
        inserted = 0

        def steps(q) -> None:
            nonlocal inserted
            for ts, row in df.iterrows():
                o = row["Open"]
                h = row["High"]
                l = row["Low"]
                c = row["Close"]

                if pd.isna(o) or pd.isna(h) or pd.isna(l) or pd.isna(c):
                    continue

                timestamp = ts.isoformat() if hasattr(ts, "isoformat") else str(ts)[:10]

                q(
                    """
                    INSERT INTO prices
                      (asset_id, interval, timestamp, open, high, low, close, volume, source)
                    VALUES (?, '1d', ?, ?, ?, ?, ?, 0.0, ?)
                    ON CONFLICT DO NOTHING
                    """,
                    [asset_id, timestamp, float(o), float(h), float(l), float(c), self.source],
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