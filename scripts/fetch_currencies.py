"""
scripts/fetch_currencies.py

Fetches daily OHLCV data for FX pairs and crypto from the Twelve Data API
and stores results in the prices table. See src/collectors/currency.py for
the fetch/upsert logic.

Usage:
    python scripts/fetch_currencies.py                   # incremental update (default)
    python scripts/fetch_currencies.py --mode full       # full history from 1980
    python scripts/fetch_currencies.py --pair EURUSD     # single pair, incremental
    python scripts/fetch_currencies.py --pair EURUSD --mode full
"""

import argparse

from src.db.client import db
from src.collectors.currency import CurrencyCollector, CURRENCY_PAIRS

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

    pairs = CURRENCY_PAIRS
    if args.pair:
        ticker_upper = args.pair.upper()
        pairs = [p for p in CURRENCY_PAIRS if p["ticker"] == ticker_upper]
        if not pairs:
            valid = ", ".join(p["ticker"] for p in CURRENCY_PAIRS)
            raise ValueError(f"Unknown pair '{ticker_upper}'. Valid options: {valid}")

    # ── Setup ─────────────────────────────────────────────────────────────────

    db.open()
    collector = CurrencyCollector()

    print(f"Fetching {len(pairs)} pair(s)  mode={args.mode}\n")
    results = collector.fetch_all(mode=args.mode, pairs=pairs)

    # ── Summary ───────────────────────────────────────────────────────────────

    total_rows = sum(results.values())
    with_data = [t for t, r in results.items() if r > 0]
    failed = [p["ticker"] for p in pairs if p["ticker"] not in results]

    print(f"\n── Summary ──────────────────────────────────────────")
    print(f"  Pairs requested      : {len(pairs)}")
    print(f"  With data            : {len(with_data)}")
    print(f"  Failed/skipped       : {len(failed)}")
    print(f"  Total rows inserted  : {total_rows:,}")

    if failed:
        print(f"\n  Failed/skipped:")
        for t in sorted(failed):
            print(f"    {t}")

    # ── Teardown ──────────────────────────────────────────────────────────────

    db.close()
