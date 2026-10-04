"""
src/collectors/currency.py

FX/crypto price collector using the Twelve Data API.

Fetches daily OHLCV data via Twelve Data's time_series endpoint. Safe to
re-run — existing rows are skipped via ON CONFLICT DO NOTHING.

Twelve Data's free tier is rate-limited to 8 requests/minute; fetch_all()
sleeps between requests accordingly.

Usage:
    from src.db.client import db
    from src.collectors.currency import CurrencyCollector

    db.open()
    collector = CurrencyCollector()
    collector.fetch_all()          # incremental update, all pairs
    collector.fetch_all(mode="full")
    db.close()
"""

from __future__ import annotations

import os
import time
from datetime import date, timedelta

import requests

from src.collectors.base import BaseCollector
from src.db.client import db

BASE_URL = "https://api.twelvedata.com"
REQUEST_DELAY_SECONDS = 8  # Twelve Data free tier: 8 req/min

# ── Pair catalogue ────────────────────────────────────────────────────────────

CURRENCY_PAIRS: list[dict] = [
    {"id": 200001, "ticker": "EURUSD",  "name": "Euro / US Dollar",                      "asset_class": "forex"},
    {"id": 200002, "ticker": "USDJPY",  "name": "US Dollar / Japanese Yen",              "asset_class": "forex"},
    {"id": 200003, "ticker": "GBPUSD",  "name": "British Pound / US Dollar",             "asset_class": "forex"},
    {"id": 200004, "ticker": "AUDUSD",  "name": "Australian Dollar / US Dollar",         "asset_class": "forex"},
    {"id": 200005, "ticker": "USDCAD",  "name": "US Dollar / Canadian Dollar",           "asset_class": "forex"},
    {"id": 200006, "ticker": "USDCHF",  "name": "US Dollar / Swiss Franc",               "asset_class": "forex"},
    {"id": 200007, "ticker": "NZDUSD",  "name": "New Zealand Dollar / US Dollar",        "asset_class": "forex"},
    {"id": 200008, "ticker": "EURGBP",  "name": "Euro / British Pound",                  "asset_class": "forex"},
    {"id": 200009, "ticker": "EURJPY",  "name": "Euro / Japanese Yen",                   "asset_class": "forex"},
    {"id": 200010, "ticker": "GBPJPY",  "name": "British Pound / Japanese Yen",          "asset_class": "forex"},
    {"id": 200011, "ticker": "AUDJPY",  "name": "Australian Dollar / Japanese Yen",      "asset_class": "forex"},
    {"id": 200012, "ticker": "EURAUD",  "name": "Euro / Australian Dollar",              "asset_class": "forex"},
    {"id": 200013, "ticker": "EURCHF",  "name": "Euro / Swiss Franc",                    "asset_class": "forex"},
    {"id": 200014, "ticker": "GBPAUD",  "name": "British Pound / Australian Dollar",     "asset_class": "forex"},
    {"id": 200015, "ticker": "GBPCAD",  "name": "British Pound / Canadian Dollar",       "asset_class": "forex"},
    {"id": 200016, "ticker": "USDMXN",  "name": "US Dollar / Mexican Peso",              "asset_class": "forex"},
    {"id": 200017, "ticker": "USDZAR",  "name": "US Dollar / South African Rand",        "asset_class": "forex"},
    {"id": 200018, "ticker": "USDTRY",  "name": "US Dollar / Turkish Lira",              "asset_class": "forex"},
    {"id": 200019, "ticker": "USDSGD",  "name": "US Dollar / Singapore Dollar",          "asset_class": "forex"},
    {"id": 200020, "ticker": "USDHKD",  "name": "US Dollar / Hong Kong Dollar",          "asset_class": "forex"},
    {"id": 200021, "ticker": "USDBRL",  "name": "US Dollar / Brazilian Real",            "asset_class": "forex"},
    {"id": 200022, "ticker": "USDINR",  "name": "US Dollar / Indian Rupee",              "asset_class": "forex"},
    {"id": 200023, "ticker": "USDKRW",  "name": "US Dollar / South Korean Won",          "asset_class": "forex"},
    {"id": 200024, "ticker": "USDCNY",  "name": "US Dollar / Chinese Yuan",              "asset_class": "forex"},
    {"id": 200025, "ticker": "USDMYR",  "name": "US Dollar / Malaysian Ringgit",         "asset_class": "forex"},
    {"id": 200026, "ticker": "USDTHB",  "name": "US Dollar / Thai Baht",                 "asset_class": "forex"},
    {"id": 200027, "ticker": "USDSEK",  "name": "US Dollar / Swedish Krona",             "asset_class": "forex"},
    {"id": 200028, "ticker": "USDNOK",  "name": "US Dollar / Norwegian Krone",           "asset_class": "forex"},
    {"id": 200029, "ticker": "USDDKK",  "name": "US Dollar / Danish Krone",              "asset_class": "forex"},
    {"id": 200030, "ticker": "BTCUSD",  "name": "Bitcoin / US Dollar",                   "asset_class": "forex"},
    {"id": 200031, "ticker": "ETHUSD",  "name": "Ethereum / US Dollar",                  "asset_class": "forex"},
]


class CurrencyCollector(BaseCollector):
    """Collects daily OHLCV FX/crypto prices from Twelve Data."""

    def __init__(self) -> None:
        super().__init__(source="twelvedata")

    # ── Public interface ──────────────────────────────────────────────────────

    @staticmethod
    def get_all_pairs() -> list[dict]:
        return list(CURRENCY_PAIRS)

    def ensure_asset(self, pair: dict) -> int:
        """Return the asset id for a catalogue entry, inserting it if needed."""
        rows = db.query("SELECT id FROM assets WHERE id = ?", [pair["id"]])
        if rows:
            return int(rows[0]["id"])

        db.run(
            """
            INSERT INTO assets (id, ticker, name, asset_class, currency, is_active)
            VALUES (?, ?, ?, ?, 'USD', true)
            """,
            [pair["id"], pair["ticker"], pair["name"], pair["asset_class"]],
        )
        self.logger.debug(f"[{self.source}] Registered {pair['ticker']} (id={pair['id']})")
        return int(pair["id"])

    def last_price_date(self, asset_id: int) -> str:
        """Return the most recent price date for this asset, or '1980-01-01'."""
        rows = db.query(
            "SELECT MAX(timestamp)::DATE AS last_date FROM prices "
            "WHERE asset_id = ? AND interval = '1d'",
            [asset_id],
        )
        last = rows[0]["last_date"] if rows else None
        if last is None:
            return "1980-01-01"
        return last.isoformat() if hasattr(last, "isoformat") else str(last)[:10]

    def fetch_all(
        self,
        mode: str = "update",
        pairs: list[dict] | None = None,
    ) -> dict[str, int]:
        """Fetch every catalogued pair via run(), tolerating per-pair failures.

        Args:
            mode:  'update' — incremental from each pair's last stored date (default).
                   'full'   — full history from 1980-01-01.
            pairs: Catalogue subset to fetch. Defaults to all of CURRENCY_PAIRS.

        Returns:
            Dict mapping ticker -> rows_inserted for every pair that succeeded.
        """
        pairs = pairs if pairs is not None else CURRENCY_PAIRS
        today = date.today().isoformat()
        results: dict[str, int] = {}

        for pair in pairs:
            ticker = pair["ticker"]
            asset_id = self.ensure_asset(pair)

            if mode == "full":
                from_date = "1980-01-01"
            else:
                last_date = self.last_price_date(asset_id)
                from_date = (date.fromisoformat(last_date) + timedelta(days=1)).isoformat()
                if from_date >= today:
                    self.logger.info(f"[{self.source}] {ticker}: already up to date")
                    continue

            time.sleep(REQUEST_DELAY_SECONDS)
            try:
                rows = self.run(asset_id=asset_id, from_date=from_date, to_date=today, interval="1d")
                results[ticker] = rows
                self.logger.info(f"[{self.source}] {ticker}: inserted {rows} row(s)")
            except Exception:
                continue  # run() already logged + audited the failure

        return results

    # ── BaseCollector implementation ──────────────────────────────────────────

    def collect(
        self,
        from_date: str,
        to_date: str,
        asset_id: int | None = None,
        series_id: int | None = None,
        interval: str | None = None,
    ) -> int:
        """Fetch daily OHLCV for asset_id and insert into the prices table."""
        if asset_id is None:
            raise ValueError("asset_id is required for CurrencyCollector.collect()")

        ticker = self._get_ticker(asset_id)
        rows = self._fetch_ohlcv(ticker, from_date, to_date)

        if not rows:
            return 0

        return self._insert_prices(asset_id, rows)

    # ── Private helpers ───────────────────────────────────────────────────────

    def _get_ticker(self, asset_id: int) -> str:
        rows = db.query("SELECT ticker FROM assets WHERE id = ?", [asset_id])
        if not rows:
            raise ValueError(f"No asset found with id={asset_id}")
        return str(rows[0]["ticker"])

    def _to_td_symbol(self, ticker: str) -> str:
        """Insert a slash after the first 3 characters: 'EURUSD' → 'EUR/USD'."""
        return ticker[:3] + "/" + ticker[3:]

    def _fetch_ohlcv(self, ticker: str, from_date: str, to_date: str) -> list[dict]:
        """Fetch daily OHLCV from Twelve Data. Returns the 'values' list or []."""
        api_key = os.environ.get("TWELVE_DATA_API_KEY")
        if not api_key:
            raise RuntimeError("TWELVE_DATA_API_KEY is not set. Add it to your .env file.")

        resp = requests.get(
            f"{BASE_URL}/time_series",
            params={
                "symbol": self._to_td_symbol(ticker),
                "interval": "1day",
                "start_date": from_date,
                "end_date": to_date,
                "order": "ASC",
                "outputsize": 5000,
                "apikey": api_key,
            },
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()

        if "code" in data:
            self.logger.warning(f"[{self.source}] {ticker}: {data.get('message', data)}")
            return []

        return data.get("values", [])

    def _insert_prices(self, asset_id: int, rows: list[dict]) -> int:
        """Insert OHLCV rows into prices with ON CONFLICT DO NOTHING."""

        def steps(q) -> None:
            for row in rows:
                q(
                    """
                    INSERT INTO prices
                      (asset_id, timestamp, open, high, low, close, adj_close, volume, interval, source)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, '1d', ?)
                    ON CONFLICT DO NOTHING
                    """,
                    [
                        asset_id,
                        row["datetime"],
                        float(row["open"]),
                        float(row["high"]),
                        float(row["low"]),
                        float(row["close"]),
                        float(row["close"]),  # adj_close = close for FX/crypto
                        float(row["volume"]) if row.get("volume") else 0.0,
                        self.source,
                    ],
                )

        db.transaction(steps)
        return len(rows)
