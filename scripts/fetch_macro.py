"""
scripts/fetch_macro.py

Fetches macroeconomic data from the FRED API and stores all vintage
releases in quant.db (macro_series + macro_observations tables).

Usage:
    python scripts/fetch_macro.py                    # all series, 30-year lookback
    python scripts/fetch_macro.py --full-history      # all series, full history
    python scripts/fetch_macro.py --series CPIAUCSL   # single series
    python scripts/fetch_macro.py --series CPIAUCSL --full-history
"""

from __future__ import annotations

import argparse
import os
from datetime import date, timedelta

import pandas as pd
from fredapi import Fred

from src.db.client import db

# ── Series catalogue ──────────────────────────────────────────────────────────

SERIES_CODES: list[str] = [
    # Daily — rates
    "DFF", "DTB3", "DGS2", "DGS5", "DGS10", "DGS30",
    # Daily — spreads / credit
    "T10Y2Y", "T10YIE", "BAMLH0A0HYM2", "BAMLC0A0CM", "BAMLC0A4CBBB",
    "BAMLEMCBPIOAS",
    # Daily — other
    "DTWEXBGS",
    # Weekly
    "ICSA", "WM2NS", "WALCL", "ECBASSETSW",
    # Monthly
    "CPIAUCSL", "CPILFESL", "PPIACO", "UNRATE", "PAYEMS", "INDPRO",
    "RETAILSMNSA", "HOUST", "PCE", "M2SL", "JPNASSETS",
    # Monthly — international yields
    "IRLTLT01DEM156N", "IRLTLT01GBM156N", "IRLTLT01FRM156N", "IRLTLT01JPM156N",
    # Quarterly
    "GDP", "GDPCTPI",
]

# Maps FRED's free-text frequency to our schema's allowed values
FREQ_MAP: dict[str, str] = {
    "Daily":       "daily",
    "Weekly":      "weekly",
    "Biweekly":    "weekly",
    "Monthly":     "monthly",
    "Quarterly":   "quarterly",
    "Semiannual":  "annual",
    "Annual":      "annual",
}

# ── CLI args ──────────────────────────────────────────────────────────────────

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

# ── Setup ─────────────────────────────────────────────────────────────────────

db.open()

fred_api_key = os.environ.get("FRED_API_KEY")
if not fred_api_key:
    raise RuntimeError("FRED_API_KEY is not set. Add it to your .env file.")

fred = Fred(api_key=fred_api_key)

today = date.today().isoformat()
lookback_cutoff: str | None = (
    None if args.full_history
    else (date.today() - timedelta(days=365 * 30)).isoformat()
)

series_to_run = [args.series.upper()] if args.series else SERIES_CODES

print(
    f"Fetching {len(series_to_run)} series  "
    f"(lookback: {'full history' if args.full_history else '30 years'})\n"
)

# ── Helpers ───────────────────────────────────────────────────────────────────

def _ensure_series(code: str) -> int:
    """Return the series_id for code, inserting FRED metadata if not present."""
    rows = db.query("SELECT id FROM macro_series WHERE code = ?", [code])
    if rows:
        return int(rows[0]["id"])

    info = fred.get_series_info(code)

    frequency_raw = str(info.get("frequency", "Daily"))
    frequency = FREQ_MAP.get(frequency_raw, "daily")

    seasonal_short = str(info.get("seasonal_adjustment_short", "NSA"))
    seasonal_adj = seasonal_short in ("SA", "SAAR", "SAAQ")

    notes = info.get("notes", None)
    description = str(notes) if pd.notna(notes) else None

    next_id_rows = db.query("SELECT COALESCE(MAX(id), 0) + 1 AS next_id FROM macro_series")
    next_id = int(next_id_rows[0]["next_id"])

    db.run(
        """
        INSERT INTO macro_series
          (id, code, name, source, frequency, units, seasonal_adj, description)
        VALUES (?, ?, ?, 'FRED', ?, ?, ?, ?)
        """,
        [
            next_id,
            code,
            str(info.get("title", code)),
            frequency,
            str(info.get("units_short", "")),
            seasonal_adj,
            description,
        ],
    )
    return next_id


def _log_fetch(
    series_id: int,
    from_date: str,
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
              (id, series_id, interval, fetched_from, fetched_to,
               source, rows_inserted, status, error_msg)
            VALUES (?, ?, NULL, ?, ?, 'FRED', ?, ?, ?)
            """,
            [next_id, series_id, from_date, today, rows_inserted, status, error_msg],
        )
    except Exception as e:
        print(f"  [WARN] Failed to write fetch log: {e}")


def _to_date_str(val) -> str:
    """Convert a Timestamp or date-like to an ISO date string."""
    return val.isoformat()[:10] if hasattr(val, "isoformat") else str(val)[:10]


def _is_daily(series_id: int) -> bool:
    """Return True if the series has daily frequency."""
    rows = db.query("SELECT frequency FROM macro_series WHERE id = ?", [series_id])
    return bool(rows) and rows[0]["frequency"] == "daily"


def _fetch_daily(code: str) -> pd.DataFrame:
    """Fetch a daily series without vintage data.

    Daily series do not have meaningful revisions, so we use get_series()
    instead of get_series_all_releases() (which hits FRED's 2000-vintage limit).
    release_date is set equal to period_date, making is_final True for all rows.
    """
    start = lookback_cutoff if lookback_cutoff else None
    s = fred.get_series(code, observation_start=start)
    s = s.dropna()
    df = s.reset_index()
    df.columns = pd.Index(["period_date", "value"])
    df["release_date"] = df["period_date"]
    return df


def _upsert_observations(series_id: int, df: pd.DataFrame) -> int:
    """Upsert rows into macro_observations and return the row count.

    is_final is set to True only for the row with the MAX release_date
    per period_date; all other vintages are marked False.
    """
    if df.empty:
        return 0

    max_release = df.groupby("period_date")["release_date"].transform("max")
    df = df.copy()
    df["is_final"] = df["release_date"] == max_release

    count = 0

    def steps(q) -> None:
        nonlocal count
        for _, row in df.iterrows():
            q(
                """
                INSERT INTO macro_observations
                  (series_id, period_date, release_date, value, is_final)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT (series_id, period_date, release_date) DO UPDATE SET
                    value    = excluded.value,
                    is_final = excluded.is_final
                """,
                [
                    series_id,
                    _to_date_str(row["period_date"]),
                    _to_date_str(row["release_date"]),
                    float(row["value"]),
                    bool(row["is_final"]),
                ],
            )
            count += 1

    db.transaction(steps)
    return count


# ── Main loop ─────────────────────────────────────────────────────────────────

successes = 0
failures: list[tuple[str, str]] = []

for code in series_to_run:
    print(f"  {code:<20}", end="", flush=True)

    try:
        series_id = _ensure_series(code)

        if _is_daily(series_id):
            # Daily series: no meaningful revisions; get_series_all_releases()
            # hits FRED's 2000-vintage limit for high-frequency series.
            raw_df = _fetch_daily(code)
        else:
            # Non-daily: fetch all vintage releases
            raw_df = fred.get_series_all_releases(code)
            raw_df = raw_df.rename(
                columns={"date": "period_date", "realtime_start": "release_date"}
            )
            raw_df = raw_df.dropna(subset=["value"])

            # Apply lookback window to period_date
            if lookback_cutoff:
                cutoff_ts = pd.Timestamp(lookback_cutoff)
                raw_df = raw_df[raw_df["period_date"] >= cutoff_ts]

        if raw_df.empty:
            print("  no data in window")
            _log_fetch(series_id, lookback_cutoff or "1900-01-01", 0, "success")
            successes += 1
            continue

        from_date_str = _to_date_str(raw_df["period_date"].min())
        rows = _upsert_observations(series_id, raw_df)
        _log_fetch(series_id, from_date_str, rows, "success")

        print(f"  {rows:>8,} rows  ✓")
        successes += 1

    except Exception as e:
        print(f"  FAILED: {e}")
        try:
            sid_rows = db.query("SELECT id FROM macro_series WHERE code = ?", [code])
            if sid_rows:
                _log_fetch(
                    int(sid_rows[0]["id"]),
                    lookback_cutoff or "1900-01-01",
                    0,
                    "failed",
                    str(e),
                )
        except Exception:
            pass
        failures.append((code, str(e)))

# ── Summary ───────────────────────────────────────────────────────────────────

print(f"\n── Summary ──────────────────────────────────────────")
print(f"  Series requested  : {len(series_to_run)}")
print(f"  Succeeded         : {successes}")
print(f"  Failed            : {len(failures)}")

if failures:
    print(f"\n  Failed series:")
    for code, err in failures:
        print(f"    {code}: {err}")

# ── Teardown ──────────────────────────────────────────────────────────────────

db.close()
