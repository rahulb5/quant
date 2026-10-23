"""
src/collectors/fred_cleanup.py

One-off cleanup for FRED series fetched in the wrong mode. MacroCollector
used to mislabel weekly/monthly series as 'daily' (FRED's "Weekly, Ending
Saturday" fell through FREQ_MAP), so it fetched them without vintages and
wrote `release_date = period_date` on already-revised values. DTWEXBGS is
genuinely daily but is revised, so it got the same rows. For those periods
`load_fred(vintage="first")` returned a revised value dated before it was
published.

Run order matters (scripts/purge_stamped_fred_rows.py does all of it):
  1. fix macro_series.frequency labels FIRST: the collector picks its fetch
     mode from the label, so a re-fetch before this rewrites the stamped rows;
  2. re-fetch the affected series in vintage mode (adds the real releases);
  3. delete a stamped row only where a real release exists for the same
     period. Stamped rows that have no real counterpart (history before
     ALFRED's vintages begin) are kept: they are the only values there are,
     and the point-in-time layer treats `release_date = period_date` on a
     non-daily series as "pre-vintage" data;
  4. recompute is_final for the affected series.
"""

from __future__ import annotations

from typing import Any

from src.db.client import Database

# Series fetched in the wrong mode, and the frequency label they should carry
# (None = already correct).
AFFECTED: dict[str, str | None] = {
    "ICSA": "weekly",
    "WM2NS": "weekly",
    "WALCL": "weekly",
    "JPNASSETS": "monthly",
    "DTWEXBGS": None,
}

# Periods with a release_date later than period_date have a real release.
_COUNTS = """
    SELECT
        COUNT(*) FILTER (WHERE o.release_date = o.period_date AND EXISTS (
            SELECT 1 FROM macro_observations r
            WHERE r.series_id = o.series_id AND r.period_date = o.period_date
              AND r.release_date > r.period_date)) AS stamped_deleted,
        COUNT(*) FILTER (WHERE o.release_date = o.period_date AND NOT EXISTS (
            SELECT 1 FROM macro_observations r
            WHERE r.series_id = o.series_id AND r.period_date = o.period_date
              AND r.release_date > r.period_date)) AS stamped_kept,
        COUNT(*) FILTER (WHERE o.release_date > o.period_date) AS real_rows
    FROM macro_observations o WHERE o.series_id = ?
"""


def fix_labels(database: Database, dry_run: bool = True) -> dict[str, tuple[str, str]]:
    """Correct macro_series.frequency for mislabelled series (see AFFECTED).

    Returns {code: (label_before, label_after)} for every series whose label
    is (dry_run) or was changed. Run this before re-fetching.
    """
    changed: dict[str, tuple[str, str]] = {}
    for code, correct in AFFECTED.items():
        rows = database.query("SELECT frequency FROM macro_series WHERE code = ?", [code])
        if correct is None or not rows or rows[0]["frequency"] == correct:
            continue
        changed[code] = (rows[0]["frequency"], correct)
        if not dry_run:
            database.run("UPDATE macro_series SET frequency = ? WHERE code = ?", [correct, code])
    return changed


def purge_stamped_rows(
    database: Database,
    codes: list[str] | None = None,
    dry_run: bool = True,
) -> dict[str, dict[str, Any]]:
    """Delete stamped rows that have a real release, then recompute is_final.

    Args:
        database: An open, writable Database (writable even for dry_run=True,
                  which only issues SELECTs).
        codes:    FRED codes to clean. Defaults to every key of AFFECTED.
        dry_run:  If True (default), count only and change nothing.

    Returns:
        {code: {stamped_deleted, stamped_kept, real_rows}}
        with counts as they were before any change (what would be / was deleted).
        Codes not present in macro_series are omitted.
    """
    out: dict[str, dict[str, Any]] = {}
    for code in codes or list(AFFECTED):
        series = database.query("SELECT id, frequency FROM macro_series WHERE code = ?", [code])
        if not series:
            continue
        sid = int(series[0]["id"])
        counts = database.query(_COUNTS, [sid])[0]
        out[code] = {
            "stamped_deleted": int(counts["stamped_deleted"]),
            "stamped_kept": int(counts["stamped_kept"]),
            "real_rows": int(counts["real_rows"]),
        }

        if dry_run:
            continue
        database.run(
            """
            DELETE FROM macro_observations
            WHERE series_id = ? AND release_date = period_date AND EXISTS (
                SELECT 1 FROM macro_observations r
                WHERE r.series_id = macro_observations.series_id
                  AND r.period_date = macro_observations.period_date
                  AND r.release_date > r.period_date)
            """,
            [sid],
        )
        database.run(
            """
            UPDATE macro_observations SET is_final = (release_date = (
                SELECT MAX(i.release_date) FROM macro_observations i
                WHERE i.series_id = macro_observations.series_id
                  AND i.period_date = macro_observations.period_date))
            WHERE series_id = ?
            """,
            [sid],
        )
    return out
