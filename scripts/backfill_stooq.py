"""
scripts/backfill_stooq.py

Backfills historical FX/crypto OHLCV data from Stooq for dates before the
earliest Twelve Data row already in the database.

Only fills gaps — if a pair has no existing data in the DB it is skipped
(run fetch_currencies.py first to establish a baseline).

Usage:
    python scripts/backfill_stooq.py              # all pairs
    python scripts/backfill_stooq.py --pair EURUSD
"""

from __future__ import annotations

import io
import argparse
from datetime import date

import pandas as pd
import requests

from src.db.client import db
from src.shared.utils import logger
from scripts.fetch_currencies import CURRENCY_PAIRS

# ── Helpers ───────────────────────────────────────────────────────────────────

def _to_stooq_symbol(ticker: str) -> str:
    return ticker.lower()


def _fetch_stooq(ticker: str) -> pd.DataFrame:
    """Download the full daily CSV from Stooq. Returns an empty DataFrame on failure."""
    symbol = _to_stooq_symbol(ticker)
    url = f"https://stooq.com/q/d/l/?s={symbol}&i=d"
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        df = pd.read_csv(io.StringIO(resp.text), usecols=["Date", "Open", "High", "Low", "Close"])
        df["Date"] = pd.to_datetime(df["Date"])
        if df.empty or "Open" not in df.columns:
            logger.warning(f"{ticker}: Stooq returned no usable data")
            return pd.DataFrame()
        return df
    except Exception as e:
        logger.warning(f"{ticker}: failed to fetch from Stooq — {e}")
        return pd.DataFrame()


def _get_earliest_date(asset_id: int) -> str | None:
    """Return the earliest price date for this asset, or None if no rows exist."""
    rows = db.query(
        "SELECT MIN(timestamp)::DATE AS earliest FROM prices WHERE asset_id = ? AND interval = '1d'",
        [asset_id],
    )
    earliest = rows[0]["earliest"] if rows else None
    if earliest is None:
        return None
    return earliest.isoformat() if hasattr(earliest, "isoformat") else str(earliest)[:10]


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
            VALUES (?, ?, '1d', ?, ?, 'stooq', ?, ?, ?)
            """,
            [next_id, asset_id, from_date, to_date, rows_inserted, status, error_msg],
        )
    except Exception as e:
        print(f"  [WARN] Failed to write fetch log: {e}")


# ── Main function ─────────────────────────────────────────────────────────────

def backfill() -> None:
    today = date.today().isoformat()
    total_rows = 0

    print(f"Backfilling {len(CURRENCY_PAIRS)} pairs from Stooq\n")

    for pair in CURRENCY_PAIRS:
        ticker   = pair["ticker"]
        asset_id = pair["id"]

        print(f"  {ticker:>8}", end="", flush=True)

        earliest = _get_earliest_date(asset_id)
        if earliest is None:
            print("  no existing data, skipping")
            continue

        df = _fetch_stooq(ticker)
        if df.empty:
            print("  no data from stooq")
            _log_fetch(asset_id, "1900-01-01", today, 0, "partial")
            continue

        # Keep only rows strictly before our earliest Twelve Data date
        df = df[df["Date"] < pd.Timestamp(earliest)].copy()
        if df.empty:
            print("  no gap to fill")
            continue

        # Drop rows with NaN in price columns
        df = df.dropna(subset=["Open", "High", "Low", "Close"])

        from_date = df["Date"].min().date().isoformat()
        to_date   = df["Date"].max().date().isoformat()
        rows_inserted = 0

        def inserts(q, _df=df, _asset_id=asset_id) -> None:
            nonlocal rows_inserted
            for _, row in _df.iterrows():
                q(
                    """
                    INSERT INTO prices
                      (asset_id, timestamp, open, high, low, close, adj_close, volume, interval, source)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, '1d', 'stooq')
                    ON CONFLICT DO NOTHING
                    """,
                    [
                        _asset_id,
                        row["Date"].date().isoformat(),
                        float(row["Open"]),
                        float(row["High"]),
                        float(row["Low"]),
                        float(row["Close"]),
                        float(row["Close"]),
                        0.0,
                    ],
                )
                rows_inserted += 1

        db.transaction(inserts)
        _log_fetch(asset_id, from_date, to_date, rows_inserted, "success")
        total_rows += rows_inserted
        print(f"  {rows_inserted:>7,} rows  ✓  ({from_date} → {to_date})")

    print(f"\nTotal rows inserted: {total_rows:,}")


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill FX/crypto history from Stooq")
    parser.add_argument(
        "--pair", type=str, default=None,
        help="Backfill a single pair only, e.g. --pair EURUSD",
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
    backfill()
    db.close()
