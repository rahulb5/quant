"""
src/collectors/gsci.py

S&P GSCI single-commodity excess return index collector using Yahoo Finance.

Fetches daily OHLCV data via yfinance. Safe to re-run — existing rows are
skipped via ON CONFLICT DO NOTHING.

Uses the 400xxx asset ID block. Asset class is 'index'.

Yahoo Finance ticker format: ^SPGSCLP, ^SPGSNGP, etc.

Usage:
    from src.db.client import db
    from src.collectors.gsci import GsciCollector

    db.open()
    collector = GsciCollector()
    collector.fetch_all()
    db.close()
"""

from __future__ import annotations

import datetime

import pandas as pd
import yfinance as yf

from src.collectors.base import BaseCollector
from src.db.client import db

_ID_BLOCK_START = 400_001

# ── Ticker catalogue ──────────────────────────────────────────────────────────

GSCI_CATALOGUE: list[dict] = [
    {"id": 400001, "ticker": "SPGSCLP", "yf_ticker": "^SPGSCL", "name": "S&P GSCI Crude Oil ER"},
    {"id": 400002, "ticker": "SPGSNGP", "yf_ticker": "^SPGSNG", "name": "S&P GSCI Natural Gas ER"},
    {"id": 400003, "ticker": "SPGSGCP", "yf_ticker": "^SPGSGC", "name": "S&P GSCI Gold ER"},
    {"id": 400004, "ticker": "SPGSSIP", "yf_ticker": "^SPGSSI", "name": "S&P GSCI Silver ER"},
    {"id": 400005, "ticker": "SPGSCPP", "yf_ticker": "^SPGSHG", "name": "S&P GSCI Copper ER"},
    {"id": 400006, "ticker": "SPGSCNP", "yf_ticker": "^SPGSCN", "name": "S&P GSCI Corn ER"},
    {"id": 400007, "ticker": "SPGSWHP", "yf_ticker": "^SPGSWH", "name": "S&P GSCI Wheat ER"},
    {"id": 400008, "ticker": "SPGSSOP", "yf_ticker": "^SPGSSO", "name": "S&P GSCI Soybeans ER"},
    {"id": 400009, "ticker": "SPGSKCP", "yf_ticker": "^SPGSKC", "name": "S&P GSCI Coffee ER"},
    {"id": 400010, "ticker": "SPGSSBP", "yf_ticker": "^SPGSSB", "name": "S&P GSCI Sugar ER"},
    {"id": 400011, "ticker": "SPGSCCP", "yf_ticker": "^SPGSCC", "name": "S&P GSCI Cocoa ER"},
]


class GsciCollector(BaseCollector):
    """Collects daily OHLCV S&P GSCI excess return index prices from Yahoo Finance."""

    def __init__(self) -> None:
        super().__init__(source="yfinance")

    # ── Public interface ──────────────────────────────────────────────────────

    @staticmethod
    def get_all_tickers() -> list[dict]:
        return list(GSCI_CATALOGUE)

    def ensure_asset(self, asset_id: int, ticker: str, name: str) -> int:
        """Return the asset id for ticker, inserting as 'index' if needed."""
        rows = db.query("SELECT id FROM assets WHERE ticker = ?", [ticker])
        if rows:
            return int(rows[0]["id"])

        db.run(
            """
            INSERT INTO assets (id, ticker, name, asset_class, currency)
            VALUES (?, ?, ?, 'index', 'USD')
            """,
            [asset_id, ticker, name],
        )
        self.logger.debug(f"[{self.source}] Registered index {ticker} (id={asset_id})")
        return asset_id

    def last_price_date(self, asset_id: int) -> str:
        """Return the most recent price date for this asset, or '2000-01-01'."""
        rows = db.query(
            "SELECT MAX(timestamp) AS last_date FROM prices WHERE asset_id = ?",
            [asset_id],
        )
        last = rows[0]["last_date"] if rows else None
        if last is None:
            return "2000-01-01"
        # timestamp may be a date string or datetime; normalise to YYYY-MM-DD
        return str(last)[:10]

    def fetch_single(
        self,
        entry: dict,
        asset_id: int,
    ) -> int:
        """Fetch OHLCV for one GSCI index ticker and insert into prices.

        Args:
            entry:    Catalogue entry with yf_ticker, ticker, etc.
            asset_id: Row id in assets table.

        Returns:
            Number of rows inserted.
        """
        ticker    = entry["ticker"]
        yf_ticker = entry["yf_ticker"]
        today     = datetime.date.today().isoformat()
        from_date = self.last_price_date(asset_id)

        df = self._fetch_yahoo(yf_ticker, from_date=from_date, to_date=today)

        if df is None or df.empty:
            self.logger.warning(f"[{self.source}] {ticker}: no data returned from Yahoo Finance")
            return 0

        rows = self._insert_prices(asset_id, ticker, df)
        self._log_fetch(
            from_date=from_date,
            to_date=today,
            asset_id=asset_id,
            interval="1d",
            rows_inserted=rows,
            status="success" if rows >= 0 else "failed",
        )
        return rows

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
        raise NotImplementedError("Use fetch_single() for GSCI indices.")

    # ── Private helpers ───────────────────────────────────────────────────────

    def _fetch_yahoo(
        self,
        yf_ticker: str,
        from_date: str,
        to_date: str,
    ) -> pd.DataFrame | None:
        """Download daily OHLCV from Yahoo Finance via yf.Ticker.history().

        Returns a DataFrame indexed by date with columns: Open, High, Low, Close, Volume.
        Returns None on error or empty result.
        """
        try:
            t = yf.Ticker(yf_ticker)
            raw = t.history(
                start=from_date,
                end=to_date,
                interval="1d",
                auto_adjust=True,
                actions=False,
            )
        except Exception as e:
            self.logger.error(f"[{self.source}] Download failed for {yf_ticker}: {e}")
            return None

        if raw is None or raw.empty:
            return None

        # Flatten MultiIndex columns if present
        if isinstance(raw.columns, pd.MultiIndex):
            raw.columns = raw.columns.get_level_values(0)

        required = {"Open", "High", "Low", "Close"}
        if not required.issubset(set(raw.columns)):
            self.logger.warning(
                f"[{self.source}] {yf_ticker}: unexpected columns {list(raw.columns)}"
            )
            return None

        raw.index = pd.to_datetime(raw.index).tz_localize(None)
        return raw.sort_index()

    def _insert_prices(self, asset_id: int, ticker: str, df: pd.DataFrame) -> int:
        """Insert OHLCV rows into the prices table using ON CONFLICT DO NOTHING."""
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
                    [
                        asset_id,
                        timestamp,
                        float(o),
                        float(h),
                        float(l),
                        float(c),
                        float(v) if not pd.isna(v) else 0.0,
                        self.source,
                    ],
                )
                inserted += 1

        db.transaction(steps)
        return inserted
