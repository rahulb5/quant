"""
src/collectors/commodity.py

Commodity futures price collector using Yahoo Finance (front-month continuous contracts).

Fetches daily OHLCV data via yfinance. Safe to re-run — existing rows are
skipped via ON CONFLICT DO NOTHING.

Uses the 300xxx asset ID block. Asset class is 'futures'.

Yahoo Finance ticker format: <root>=F  (e.g. CL=F for WTI Crude Oil)

Usage:
    from src.db.client import db
    from src.collectors.commodity import CommodityCollector

    db.open()
    collector = CommodityCollector()
    collector.fetch_all()
    db.close()
"""

from __future__ import annotations

import pandas as pd
import yfinance as yf

from src.collectors.base import BaseCollector
from src.db.client import db

_ID_BLOCK_START = 300_001

# ── Ticker catalogue ──────────────────────────────────────────────────────────

COMMODITY_CATALOGUE: list[dict[str, str]] = [
    # Energy
    {"ticker": "CL1", "yahoo": "CL=F",  "name": "Crude Oil WTI - Front Month"},
    {"ticker": "NG1", "yahoo": "NG=F",  "name": "Natural Gas - Front Month"},
    # Metals
    {"ticker": "GC1", "yahoo": "GC=F",  "name": "Gold - Front Month"},
    {"ticker": "SI1", "yahoo": "SI=F",  "name": "Silver - Front Month"},
    {"ticker": "HG1", "yahoo": "HG=F",  "name": "Copper - Front Month"},
    # Agriculture
    {"ticker": "ZC1", "yahoo": "ZC=F",  "name": "Corn - Front Month"},
    {"ticker": "ZW1", "yahoo": "ZW=F",  "name": "Wheat - Front Month"},
    {"ticker": "ZS1", "yahoo": "ZS=F",  "name": "Soybeans - Front Month"},
    # Softs
    {"ticker": "KC1", "yahoo": "KC=F",  "name": "Coffee - Front Month"},
    {"ticker": "SB1", "yahoo": "SB=F",  "name": "Sugar - Front Month"},
    {"ticker": "CC1", "yahoo": "CC=F",  "name": "Cocoa - Front Month"},
]


class CommodityCollector(BaseCollector):
    """Collects daily OHLCV commodity futures prices from Yahoo Finance."""

    def __init__(self) -> None:
        super().__init__(source="yahoo")

    # ── Public interface ──────────────────────────────────────────────────────

    @staticmethod
    def get_all_tickers() -> list[dict[str, str]]:
        return list(COMMODITY_CATALOGUE)

    def ensure_asset(self, ticker: str, name: str) -> int:
        """Return the asset id for ticker, inserting as 'futures' if needed."""
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
        yahoo_ticker: str,
        asset_id: int,
        ticker: str,
        from_date: str | None = None,
        to_date: str | None = None,
    ) -> int:
        """Fetch OHLCV for one Yahoo Finance futures ticker and insert into prices.

        Args:
            yahoo_ticker: Yahoo Finance ticker, e.g. 'CL=F'.
            asset_id:     Row id in assets table.
            ticker:       Our internal ticker, e.g. 'CL1' (for logging).
            from_date:    Start date inclusive (YYYY-MM-DD). Fetches max history if omitted.
            to_date:      End date inclusive (YYYY-MM-DD). Defaults to today if omitted.

        Returns:
            Number of rows inserted.
        """
        df = self._fetch_yahoo(yahoo_ticker, from_date=from_date, to_date=to_date)

        if df is None or df.empty:
            self.logger.warning(f"[{self.source}] {ticker}: no data returned")
            return 0

        return self._insert_prices(asset_id, ticker, df)

    # ── BaseCollector implementation ──────────────────────────────────────────

    def collect(  # type: ignore[override]
        self,
        from_date: str,
        to_date: str,
        asset_id: int | None = None,
        series_id: int | None = None,
        interval: str | None = None,
    ) -> int:
        """Not used directly — use fetch_single() instead."""
        del from_date, to_date, asset_id, series_id, interval
        raise NotImplementedError("Use fetch_single() for commodity futures.")

    # ── Private helpers ───────────────────────────────────────────────────────

    def _fetch_yahoo(
        self,
        yahoo_ticker: str,
        from_date: str | None = None,
        to_date: str | None = None,
    ) -> pd.DataFrame | None:
        """Download daily OHLCV from Yahoo Finance via yfinance.

        Returns a DataFrame indexed by date with columns: Open, High, Low, Close, Volume.
        Returns None on error.
        """
        try:
            kwargs: dict = {"interval": "1d", "auto_adjust": True, "progress": False}
            if from_date:
                kwargs["start"] = from_date
            else:
                kwargs["period"] = "max"
            if to_date:
                kwargs["end"] = to_date

            raw = yf.download(yahoo_ticker, **kwargs)
        except Exception as e:
            self.logger.error(f"[{self.source}] Download failed for {yahoo_ticker}: {e}")
            return None

        if raw is None or raw.empty:
            return None

        # yfinance may return MultiIndex columns when auto_adjust=True — flatten
        if isinstance(raw.columns, pd.MultiIndex):
            raw.columns = raw.columns.get_level_values(0)

        required = {"Open", "High", "Low", "Close"}
        if not required.issubset(set(raw.columns)):
            self.logger.warning(
                f"[{self.source}] {yahoo_ticker}: unexpected columns {list(raw.columns)}"
            )
            return None

        raw.index = pd.to_datetime(raw.index).tz_localize(None)
        return raw.sort_index()

    def _insert_prices(self, asset_id: int, ticker: str, df: pd.DataFrame) -> int:
        """Insert OHLCV rows into the prices table.

        Uses ON CONFLICT DO NOTHING for idempotent inserts.
        """
        self.logger.debug(f"[{self.source}] Inserting {len(df)} rows for {ticker}")
        inserted = 0

        def steps(q) -> None:
            nonlocal inserted
            for ts, row in df.iterrows():
                o = row["Open"]
                h = row["High"]
                l = row["Low"]
                c = row["Close"]
                v = row.get("Volume", 0.0)

                if pd.isna(o) or pd.isna(h) or pd.isna(l) or pd.isna(c):
                    continue

                timestamp = ts.isoformat() if hasattr(ts, "isoformat") else str(ts)[:10]

                q(
                    """
                    INSERT INTO prices
                      (asset_id, interval, timestamp, open, high, low, close, volume, source)
                    VALUES (?, '1d', ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT DO NOTHING
                    """,
                    [asset_id, timestamp, float(o), float(h), float(l), float(c),
                     float(v) if not pd.isna(v) else 0.0, self.source],
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
