"""
scripts/fetch_macro.py

Fetches macroeconomic data from the FRED API and stores all vintage
releases in quant.db (macro_series + macro_observations tables). See
src/collectors/macro.py for the fetch/upsert logic.

Usage:
    python scripts/fetch_macro.py                    # all series, 30-year lookback
    python scripts/fetch_macro.py --full-history      # all series, full history
    python scripts/fetch_macro.py --series CPIAUCSL   # single series
    python scripts/fetch_macro.py --series CPIAUCSL --full-history
"""

import argparse

from src.db.client import db
from src.collectors.macro import MacroCollector, SERIES_CODES

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch FRED macro data into quant.db")
    parser.add_argument(
        "--full-history",
        action="store_true",
        help="Fetch all available history (default: 30-year lookback)",
    )
    parser.add_argument(
        "--series",
        type=str,
        default=None,
        metavar="CODE",
        help="Fetch a single series by FRED code, e.g. --series CPIAUCSL",
    )
    args = parser.parse_args()

    codes = [args.series.upper()] if args.series else SERIES_CODES

    print(
        f"Fetching {len(codes)} series  "
        f"(lookback: {'full history' if args.full_history else '30 years'})\n"
    )

    # ── Setup ─────────────────────────────────────────────────────────────────

    db.open()
    collector = MacroCollector()

    results = collector.fetch_all(codes=codes, full_history=args.full_history)

    # ── Summary ───────────────────────────────────────────────────────────────

    failures = [c for c in codes if c not in results]

    print(f"\n── Summary ──────────────────────────────────────────")
    print(f"  Series requested  : {len(codes)}")
    print(f"  Succeeded         : {len(results)}")
    print(f"  Failed            : {len(failures)}")

    if failures:
        print(f"\n  Failed series:")
        for code in failures:
            print(f"    {code}")

    # ── Teardown ──────────────────────────────────────────────────────────────

    db.close()
