"""
scripts/renumber_misplaced_assets.py

Moves equities that were registered outside the equity id block (at 400012+,
see src/collectors/asset_id_cleanup.py) into it, after copying data/quant.db
to a backup.

Usage:
    python scripts/renumber_misplaced_assets.py --dry-run   # plan only
    python scripts/renumber_misplaced_assets.py             # backup, then move

Refuses to run if the backup file already exists.
"""

import argparse
import shutil
import sys
from datetime import date
from pathlib import Path

from src.collectors.asset_id_cleanup import misplaced_equities, renumber_misplaced_equities
from src.db.client import db

DB_FILE = Path("data/quant.db")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Move misplaced equities into the equity id block")
    parser.add_argument("--dry-run", action="store_true", help="Print the plan only; change nothing.")
    args = parser.parse_args()

    backup = DB_FILE.with_name(f"{DB_FILE.name}.bak-{date.today():%Y%m%d}-asset-ids")
    if not args.dry_run:
        if backup.exists():
            sys.exit(f"Backup already exists, refusing to run: {backup}")
        shutil.copy2(DB_FILE, backup)
        print(f"Backed up {DB_FILE} -> {backup}")

    db.open()
    try:
        plan = renumber_misplaced_equities(db, dry_run=args.dry_run)
        for p in plan:
            print(f"  {p['ticker']:<6} {p['old_id']} -> {p['new_id']}  "
                  f"prices={p['prices']:,}  fetch_log={p['data_fetch_log']}")
        print(f"{'Would move' if args.dry_run else 'Moved'} {len(plan)} asset(s)")
        if not args.dry_run:
            print(f"Misplaced equities remaining: {len(misplaced_equities(db))}")
    finally:
        db.close()
