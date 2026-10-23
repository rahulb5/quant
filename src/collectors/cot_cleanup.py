"""
src/collectors/cot_cleanup.py

One-off cleanup for the COT exchange-keying bug: before the collector was
changed to key series by contract code, non-targeted fetches keyed series by
exchange code (CFTC.LEGACY_FUT.NYME, ...), so every contract on an exchange
overwrote the others. Those series are unrecoverable and are deleted here,
along with the rows that reference them; a catalog backfill replaces them.

A series is "contract-keyed" when the third dotted segment of its code, once
trimmed, is exactly 6 characters (the same rule as src.macro.data.list_series).

Deletes run as separate statements in child -> parent order (cot_positions,
data_fetch_log, macro_series): DuckDB rejects a parent delete that is in the
same transaction as the child delete when a foreign key points at it.
"""

from __future__ import annotations

from typing import Any

from src.db.client import Database

_BAD_SERIES = """
    SELECT id FROM macro_series
    WHERE source = 'CFTC' AND LENGTH(TRIM(SPLIT_PART(code, '.', 3))) <> 6
"""


def purge_exchange_keyed(database: Database, dry_run: bool = True) -> dict[str, Any]:
    """Delete exchange-keyed CFTC series and their dependent rows.

    Args:
        database: An open, writable Database (writable even for dry_run=True,
                  which only issues SELECTs).
        dry_run:  If True (default), count only and change nothing.

    Returns:
        Dict with n_series, cot_positions, data_fetch_log (rows affected or,
        for a dry run, that would be), cot_by_report_type {report_type: rows}
        and kept_series (contract-keyed CFTC series left alone).
    """
    counts = {
        "n_series": database.query(f"SELECT COUNT(*) AS n FROM ({_BAD_SERIES})")[0]["n"],
        "cot_positions": database.query(
            f"SELECT COUNT(*) AS n FROM cot_positions WHERE series_id IN ({_BAD_SERIES})"
        )[0]["n"],
        "data_fetch_log": database.query(
            f"SELECT COUNT(*) AS n FROM data_fetch_log WHERE series_id IN ({_BAD_SERIES})"
        )[0]["n"],
        "cot_by_report_type": {
            r["report_type"]: r["n"]
            for r in database.query(
                f"SELECT report_type, COUNT(*) AS n FROM cot_positions "
                f"WHERE series_id IN ({_BAD_SERIES}) GROUP BY report_type ORDER BY report_type"
            )
        },
        "kept_series": database.query(
            f"SELECT COUNT(*) AS n FROM macro_series "
            f"WHERE source = 'CFTC' AND id NOT IN ({_BAD_SERIES})"
        )[0]["n"],
    }

    if not dry_run and counts["n_series"]:
        database.run(f"DELETE FROM cot_positions WHERE series_id IN ({_BAD_SERIES})")
        database.run(f"DELETE FROM data_fetch_log WHERE series_id IN ({_BAD_SERIES})")
        database.run(f"DELETE FROM macro_series WHERE id IN ({_BAD_SERIES})")

    return counts
