"""
scripts/fetch_cot.py

Fetches CFTC Commitments of Traders (COT) data from the Socrata open-data
API and stores it in quant.db (cot_positions table). Only contracts in the
COT_CONTRACTS catalog (src/collectors/cot.py) are fetched. See that module for
the fetch/upsert logic and how to add a contract.

Usage:
    python scripts/fetch_cot.py                              # update: from last DB date (default)
    python scripts/fetch_cot.py --mode incremental           # last 2 weeks hardcoded
    python scripts/fetch_cot.py --mode backfill              # full history, whole catalog
    python scripts/fetch_cot.py --report-type tff_fut        # single report type
    python scripts/fetch_cot.py --mode backfill --contract 13874A --contract 043602
                                                             # backfill specific catalog contracts
"""

import argparse

from src.db.client import db
from src.collectors.cot import COT_CONTRACTS, CotCollector

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
        "--contract",
        action="append",
        default=None,
        metavar="CODE",
        help="CFTC contract code from COT_CONTRACTS; repeatable (default: whole catalog).",
    )
    args = parser.parse_args()

    unknown = [c for c in args.contract or [] if c not in COT_CONTRACTS]
    if unknown:
        parser.error(f"not in COT_CONTRACTS: {unknown}")

    report_types = [args.report_type] if args.report_type else None
    labels = report_types or CotCollector.get_all_report_types()
    n_contracts = len(args.contract) if args.contract else len(COT_CONTRACTS)
    print(f"CFTC COT fetch  mode={args.mode}  reports={labels}  contracts={n_contracts}\n")

    # ── Setup ─────────────────────────────────────────────────────────────────

    db.open()
    collector = CotCollector()

    results = collector.fetch_all(mode=args.mode, report_types=report_types, contracts=args.contract)

    # ── Summary ───────────────────────────────────────────────────────────────

    total_rows = sum(results.values())
    failed = len(labels) - len(results)

    print(f"\n── Summary ──────────────────────────────────────────")
    print(f"  Reports processed   : {len(results)}")
    print(f"  Total rows upserted : {total_rows:,}")
    print(f"  Failed or skipped   : {failed}")

    # ── Teardown ──────────────────────────────────────────────────────────────

    db.close()
