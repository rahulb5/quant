"""
scripts/fetch_cot.py

Fetches CFTC Commitments of Traders (COT) data from the Socrata open-data
API and stores it in quant.db (cot_positions table). See
src/collectors/cot.py for the fetch/upsert logic.

Usage:
    python scripts/fetch_cot.py                              # update: from last DB date (default)
    python scripts/fetch_cot.py --mode incremental           # last 2 weeks hardcoded
    python scripts/fetch_cot.py --mode backfill              # full history
    python scripts/fetch_cot.py --report-type legacy_fut     # single report type
    python scripts/fetch_cot.py --mode backfill --report-type disagg_fut
    python scripts/fetch_cot.py --targeted                   # 11 target physical commodities only
"""

import argparse

from src.db.client import db
from src.collectors.cot import CotCollector

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch CFTC COT data into quant.db")
    parser.add_argument(
        "--mode",
        choices=["update", "incremental", "backfill"],
        default="update",
        help="update: from last DB date, fallback 2 weeks (default). incremental: last 2 weeks. backfill: full history.",
    )
    parser.add_argument(
        "--report-type",
        choices=["legacy_fut", "legacy_comb", "disagg_fut", "tff_fut"],
        default=None,
        metavar="TYPE",
        help="Fetch a single report type (default: all four).",
    )
    parser.add_argument(
        "--targeted",
        action="store_true",
        help="Fetch only the 11 target physical commodity contracts by CFTC market code.",
    )
    args = parser.parse_args()

    report_types = [args.report_type] if args.report_type else None

    if args.targeted:
        print(f"CFTC COT fetch  mode={args.mode}  targeted=True  contracts=11 physical commodities\n")
    else:
        labels = report_types or CotCollector.get_all_report_types()
        print(f"CFTC COT fetch  mode={args.mode}  reports={labels}\n")

    # ── Setup ─────────────────────────────────────────────────────────────────

    db.open()
    collector = CotCollector()

    results = collector.fetch_all(mode=args.mode, report_types=report_types, targeted=args.targeted)

    # ── Summary ───────────────────────────────────────────────────────────────

    total_rows = sum(results.values())
    requested = len(report_types) if report_types else (1 if args.targeted else len(CotCollector.get_all_report_types()))
    failed = requested - len(results)

    print(f"\n── Summary ──────────────────────────────────────────")
    print(f"  Reports processed   : {len(results)}")
    print(f"  Total rows upserted : {total_rows:,}")
    print(f"  Failed              : {failed}")

    # ── Teardown ──────────────────────────────────────────────────────────────

    db.close()
