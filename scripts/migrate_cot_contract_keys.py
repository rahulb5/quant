"""
scripts/migrate_cot_contract_keys.py

Deletes the exchange-keyed CFTC series left behind by the old COT collector
(see src/collectors/cot_cleanup.py), after copying data/quant.db to a dated
backup. Re-run `scripts/fetch_cot.py --mode backfill` afterwards.

Usage:
    python scripts/migrate_cot_contract_keys.py --dry-run   # counts only
    python scripts/migrate_cot_contract_keys.py             # backup, then delete

Refuses to run if today's backup file already exists.
"""

import argparse
import shutil
import sys
from datetime import date
from pathlib import Path

from src.collectors.cot_cleanup import purge_exchange_keyed
from src.db.client import db

DB_FILE = Path("data/quant.db")


def _print(label: str, c: dict) -> None:
    print(f"\n── {label} ─────────────────────────────")
    print(f"  exchange-keyed series : {c['n_series']}")
    print(f"  cot_positions rows    : {c['cot_positions']:,}  {c['cot_by_report_type']}")
    print(f"  data_fetch_log rows   : {c['data_fetch_log']:,}")
    print(f"  contract-keyed series kept : {c['kept_series']}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Delete exchange-keyed COT series")
    parser.add_argument("--dry-run", action="store_true", help="Print counts only; change nothing.")
    args = parser.parse_args()

    backup = DB_FILE.with_name(f"{DB_FILE.name}.bak-{date.today():%Y%m%d}")
    if not args.dry_run:
        if backup.exists():
            sys.exit(f"Backup already exists, refusing to run: {backup}")
        shutil.copy2(DB_FILE, backup)
        print(f"Backed up {DB_FILE} -> {backup}")

    db.open()
    try:
        _print("Before", purge_exchange_keyed(db, dry_run=True))
        if not args.dry_run:
            purge_exchange_keyed(db, dry_run=False)
            _print("After (remaining bad rows should be 0)", purge_exchange_keyed(db, dry_run=True))
    finally:
        db.close()
