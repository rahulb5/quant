"""
scripts/fetch_currencies.py

Fetches daily OHLCV data for FX pairs and crypto from the Twelve Data API
and stores results in the prices table.

Usage:
    python scripts/fetch_currencies.py                   # incremental update (default)
    python scripts/fetch_currencies.py --mode full       # full history from 1980
    python scripts/fetch_currencies.py --pair EURUSD     # single pair, incremental
    python scripts/fetch_currencies.py --pair EURUSD --mode full
"""

from __future__ import annotations

import os
import time
import argparse
from datetime import date, timedelta

import requests
from dotenv import load_dotenv

from src.db.client import db
from src.shared.utils import logger

load_dotenv(dotenv_path=".env")

# ── Constants ─────────────────────────────────────────────────────────────────

BASE_URL = "https://api.twelvedata.com"
REQUEST_DELAY_SECONDS = 8  # Twelve Data free tier: 8 req/min

# ── Pair catalogue ────────────────────────────────────────────────────────────

CURRENCY_PAIRS = [
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

# ── Helpers ───────────────────────────────────────────────────────────────────

def _to_td_symbol(ticker: str) -> str:
    """Insert a slash after the first 3 characters: 'EURUSD' → 'EUR/USD'."""
    return ticker[:3] + "/" + ticker[3:]


def _ensure_assets() -> None:
    """Insert any missing pairs into the assets table."""
    existing = {int(r["id"]) for r in db.query("SELECT id FROM assets")}
    missing = [p for p in CURRENCY_PAIRS if p["id"] not in existing]
    if not missing:
        return

    def inserts(q) -> None:
        for pair in missing:
            q(
                """
                INSERT INTO assets (id, ticker, name, asset_class, currency, is_active)
                VALUES (?, ?, ?, ?, 'USD', true)
                """,
                [pair["id"], pair["ticker"], pair["name"], pair["asset_class"]],
            )

    db.transaction(inserts)
    logger.info(f"Inserted {len(missing)} new asset(s) into assets table")


def _fetch_ohlcv(ticker: str, start_date: str, end_date: str) -> list[dict]:
    """Fetch daily OHLCV from Twelve Data. Returns the 'values' list or []."""
    api_key = os.environ.get("TWELVE_DATA_API_KEY")
    if not api_key:
        raise RuntimeError("TWELVE_DATA_API_KEY is not set. Add it to your .env file.")

    resp = requests.get(
        f"{BASE_URL}/time_series",
        params={
            "symbol":     _to_td_symbol(ticker),
            "interval":   "1day",
            "start_date": start_date,
            "end_date":   end_date,
            "order":      "ASC",
            "outputsize": 5000,
            "apikey":     api_key,
        },
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()

    if "code" in data:
        logger.warning(f"{ticker}: Twelve Data error — {data.get('message', data)}")
        return []

    return data.get("values", [])


def _insert_prices(asset_id: int, rows: list[dict]) -> int:
    """Insert OHLCV rows into prices with ON CONFLICT DO NOTHING."""
    def inserts(q) -> None:
        for row in rows:
            q(
                """
                INSERT INTO prices
                  (asset_id, timestamp, open, high, low, close, adj_close, volume, interval, source)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, '1d', 'twelvedata')
                ON CONFLICT DO NOTHING
                """,
                [
                    asset_id,
                    row["datetime"],
                    float(row["open"]),
                    float(row["high"]),
                    float(row["low"]),
                    float(row["close"]),
                    float(row["close"]),   # adj_close = close for FX/crypto
                    float(row["volume"]) if row.get("volume") else 0.0,
                ],
            )

    db.transaction(inserts)
    return len(rows)


def _get_last_date(asset_id: int) -> str:
    """Return the most recent price date for this asset, or '1980-01-01'."""
    rows = db.query(
        "SELECT MAX(timestamp)::DATE AS last_date FROM prices WHERE asset_id = ? AND interval = '1d'",
        [asset_id],
    )
    last = rows[0]["last_date"] if rows else None
    if last is None:
        return "1980-01-01"
    return last.isoformat() if hasattr(last, "isoformat") else str(last)[:10]


def _log_fetch(
    asset_id: int,
    from_date: str,
    to_date: str,
    rows_inserted: int,
    status: str,
    error_msg: str | None = None,
) -> None:
    """Write an audit row to data_fetch_log."""
    try:
        next_id_rows = db.query(
            "SELECT COALESCE(MAX(id), 0) + 1 AS next_id FROM data_fetch_log"
        )
        next_id = int(next_id_rows[0]["next_id"])
        db.run(
            """
            INSERT INTO data_fetch_log
              (id, asset_id, interval, fetched_from, fetched_to,
               source, rows_inserted, status, error_msg)
            VALUES (?, ?, '1d', ?, ?, 'twelvedata', ?, ?, ?)
            """,
            [next_id, asset_id, from_date, to_date, rows_inserted, status, error_msg],
        )
    except Exception as e:
        print(f"  [WARN] Failed to write fetch log: {e}")


# ── Main functions ────────────────────────────────────────────────────────────

def fetch_full_history() -> None:
    """Fetch all available history for each pair from 1980-01-01."""
    _ensure_assets()
    today = date.today().isoformat()
    total_rows = 0

    print(f"Fetching full history for {len(CURRENCY_PAIRS)} pairs (from 1980-01-01)\n")

    for pair in CURRENCY_PAIRS:
        ticker   = pair["ticker"]
        asset_id = pair["id"]

        print(f"  {ticker:>8}", end="", flush=True)
        time.sleep(REQUEST_DELAY_SECONDS)

        try:
            rows = _fetch_ohlcv(ticker, "1980-01-01", today)
        except Exception as e:
            print(f"  FAILED: {e}")
            _log_fetch(asset_id, "1980-01-01", today, 0, "failed", str(e))
            continue

        if not rows:
            print("  no data")
            _log_fetch(asset_id, "1980-01-01", today, 0, "partial")
            continue

        count = _insert_prices(asset_id, rows)
        _log_fetch(asset_id, "1980-01-01", today, count, "success")
        total_rows += count
        print(f"  {count:>7,} rows  ✓")

    print(f"\nTotal rows inserted: {total_rows:,}")


def update() -> None:
    """Incrementally update each pair from its last stored date."""
    _ensure_assets()
    today = date.today().isoformat()
    total_rows = 0
    skipped = 0

    print(f"Updating {len(CURRENCY_PAIRS)} pairs\n")

    for pair in CURRENCY_PAIRS:
        ticker   = pair["ticker"]
        asset_id = pair["id"]

        last_date = _get_last_date(asset_id)
        from_date = (date.fromisoformat(last_date) + timedelta(days=1)).isoformat()

        if from_date >= today:
            print(f"  {ticker:>8}  already up to date")
            skipped += 1
            continue

        print(f"  {ticker:>8}", end="", flush=True)
        time.sleep(REQUEST_DELAY_SECONDS)

        try:
            rows = _fetch_ohlcv(ticker, from_date, today)
        except Exception as e:
            print(f"  FAILED: {e}")
            _log_fetch(asset_id, from_date, today, 0, "failed", str(e))
            continue

        if not rows:
            print("  no data")
            _log_fetch(asset_id, from_date, today, 0, "partial")
            continue

        count = _insert_prices(asset_id, rows)
        _log_fetch(asset_id, from_date, today, count, "success")
        total_rows += count
        print(f"  {count:>7,} rows  ✓")

    print(f"\nPairs skipped (up to date): {skipped}")
    print(f"Total rows inserted:        {total_rows:,}")


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch FX and crypto OHLCV from Twelve Data")
    parser.add_argument(
        "--mode", choices=["full", "update"], default="update",
        help="full: fetch all history from 1980. update: incremental (default).",
    )
    parser.add_argument(
        "--pair", type=str, default=None,
        help="Run a single pair only, e.g. --pair EURUSD",
    )
    args = parser.parse_args()

    if args.pair:
        ticker_upper = args.pair.upper()
        matched = [p for p in CURRENCY_PAIRS if p["ticker"] == ticker_upper]
        if not matched:
            valid = ", ".join(p["ticker"] for p in CURRENCY_PAIRS)
            raise ValueError(f"Unknown pair '{ticker_upper}'. Valid options: {valid}")
        CURRENCY_PAIRS.clear()
        CURRENCY_PAIRS.extend(matched)

    db.open()
    if args.mode == "full":
        fetch_full_history()
    else:
        update()
    db.close()
