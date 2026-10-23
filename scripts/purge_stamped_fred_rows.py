"""
scripts/purge_stamped_fred_rows.py

Repairs FRED series that were fetched without vintages (see
src/collectors/fred_cleanup.py): fixes their frequency labels, re-fetches them
in vintage mode, deletes the stamped `release_date = period_date` rows that
now have a real release, and recomputes is_final. Copies data/quant.db to a
dated backup first. Needs FRED_API_KEY (the re-fetch is network-bound).

Usage:
    python scripts/purge_stamped_fred_rows.py --dry-run   # counts only; no fetch, no changes
    python scripts/purge_stamped_fred_rows.py             # backup, re-fetch, purge

Refuses to run if today's backup file already exists. Never touches the ICE
credit series (BAML*): FRED now serves only 3 years of them and our copy is
archived at data/archive/ice_credit_spreads.parquet.
"""

import argparse
import shutil
import sys
from datetime import date
from pathlib import Path

from src.collectors.fred_cleanup import AFFECTED, fix_labels, purge_stamped_rows
from src.collectors.macro import MacroCollector
from src.db.client import db

DB_FILE = Path("data/quant.db")


def _print(label: str, result: dict) -> None:
    print(f"\n── {label} ─────────────────────────────")
    print(f"  {'code':<10} {'real rows':>10} {'stamped (has real)':>19} {'stamped (kept)':>15}")
    for code, r in result.items():
        print(f"  {code:<10} {r['real_rows']:>10,} {r['stamped_deleted']:>19,} {r['stamped_kept']:>15,}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Repair FRED series fetched without vintages")
    parser.add_argument("--dry-run", action="store_true", help="Print counts only; no fetch, no changes.")
    args = parser.parse_args()

    backup = DB_FILE.with_name(f"{DB_FILE.name}.bak-{date.today():%Y%m%d}-fred-stamped")
    if not args.dry_run:
        if backup.exists():
            sys.exit(f"Backup already exists, refusing to run: {backup}")
        shutil.copy2(DB_FILE, backup)
        print(f"Backed up {DB_FILE} -> {backup}")

    db.open()
    try:
        _print("Before", purge_stamped_rows(db, dry_run=True))
        print("\n  label fixes:", fix_labels(db, dry_run=True) or "none")
        if args.dry_run:
            print("\n(dry run: 'stamped (kept)' includes periods the re-fetch will cover)")
        else:
            fix_labels(db, dry_run=False)   # before the re-fetch: the label selects the fetch mode
            collector = MacroCollector()
            fetched = collector.fetch_all(codes=list(AFFECTED), full_history=True)
            missing = [c for c in AFFECTED if c not in fetched]
            if missing:
                sys.exit(f"Re-fetch failed for {missing}; nothing deleted. Fix and re-run.")
            print(f"\nRe-fetched in vintage mode: {fetched}")
            purge_stamped_rows(db, dry_run=False)
            _print("After (stamped rows left are pre-vintage history, kept on purpose)",
                   purge_stamped_rows(db, dry_run=True))
    finally:
        db.close()
