"""
scripts/test_macro.py

Smoke test for fetch_macro.py — fetches a single short series (UNRATE)
over a 2-year window and prints the results.

Run from the project root with the venv activated:
    python scripts/test_macro.py
"""

import os
from datetime import date, timedelta

import pandas as pd
from fredapi import Fred

from src.db.client import db

db.open()

fred_api_key = os.environ.get("FRED_API_KEY")
if not fred_api_key:
    raise RuntimeError("FRED_API_KEY not set — check your .env file")

fred = Fred(api_key=fred_api_key)

CODE = "UNRATE"
LOOKBACK_YEARS = 2
cutoff = (date.today() - timedelta(days=365 * LOOKBACK_YEARS)).isoformat()
today = date.today().isoformat()

print(f"── Test: {CODE}  ({cutoff} → {today}) ──\n")

# ── Step 1: series metadata ───────────────────────────────────────────────────

print("1. Fetching series info from FRED...")
info = fred.get_series_info(CODE)
print(f"   Title      : {info.get('title')}")
print(f"   Frequency  : {info.get('frequency')}")
print(f"   Units      : {info.get('units_short')}")
print(f"   Seasonal   : {info.get('seasonal_adjustment_short')}")

# ── Step 2: vintage data ──────────────────────────────────────────────────────

print("\n2. Fetching vintage releases from FRED...")
raw_df = fred.get_series_all_releases(CODE)
raw_df = raw_df.rename(columns={"date": "period_date", "realtime_start": "release_date"})
raw_df = raw_df.dropna(subset=["value"])
raw_df = raw_df[raw_df["period_date"] >= pd.Timestamp(cutoff)]

print(f"   Rows after {LOOKBACK_YEARS}-year filter : {len(raw_df)}")
print(f"   Unique period dates               : {raw_df['period_date'].nunique()}")
print(f"   Unique release dates              : {raw_df['release_date'].nunique()}")
print(f"\n   Sample (first 5 rows):")
print(raw_df.head().to_string(index=False))

# ── Step 3: ensure series row in DB ──────────────────────────────────────────

print("\n3. Ensuring series row in macro_series...")
rows = db.query("SELECT id, name FROM macro_series WHERE code = ?", [CODE])
if rows:
    series_id = int(rows[0]["id"])
    print(f"   Already exists  id={series_id}  name='{rows[0]['name']}'")
else:
    next_id = int(db.query("SELECT COALESCE(MAX(id), 0) + 1 AS n FROM macro_series")[0]["n"])
    seasonal_short = str(info.get("seasonal_adjustment_short", "NSA"))
    seasonal_adj = seasonal_short in ("SA", "SAAR", "SAAQ")
    notes = info.get("notes", None)
    description = str(notes) if pd.notna(notes) else None
    freq_map = {"Daily": "daily", "Weekly": "weekly", "Monthly": "monthly",
                "Quarterly": "quarterly", "Annual": "annual"}
    frequency = freq_map.get(str(info.get("frequency", "Monthly")), "monthly")

    db.run(
        """
        INSERT INTO macro_series (id, code, name, source, frequency, units, seasonal_adj, description)
        VALUES (?, ?, ?, 'FRED', ?, ?, ?, ?)
        """,
        [next_id, CODE, str(info.get("title", CODE)), frequency,
         str(info.get("units_short", "")), seasonal_adj, description],
    )
    series_id = next_id
    print(f"   Inserted  id={series_id}")

# ── Step 4: upsert observations ───────────────────────────────────────────────

print(f"\n4. Upserting {len(raw_df)} observations into macro_observations...")

max_release = raw_df.groupby("period_date")["release_date"].transform("max")
raw_df = raw_df.copy()
raw_df["is_final"] = raw_df["release_date"] == max_release

def _to_date_str(val) -> str:
    return val.isoformat()[:10] if hasattr(val, "isoformat") else str(val)[:10]

inserted = 0

def steps(q) -> None:
    global inserted
    for _, row in raw_df.iterrows():
        q(
            """
            INSERT INTO macro_observations
              (series_id, period_date, release_date, value, is_final)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (series_id, period_date, release_date) DO UPDATE SET
                value    = excluded.value,
                is_final = excluded.is_final
            """,
            [series_id, _to_date_str(row["period_date"]),
             _to_date_str(row["release_date"]), float(row["value"]), bool(row["is_final"])],
        )
        inserted += 1

db.transaction(steps)
print(f"   Upserted {inserted} rows")

# ── Step 5: verify DB contents ────────────────────────────────────────────────

print(f"\n5. Verifying DB contents...")
stored = db.query(
    """
    SELECT period_date, release_date, value, is_final
    FROM macro_observations
    WHERE series_id = ?
    ORDER BY period_date DESC, release_date DESC
    LIMIT 10
    """,
    [series_id],
)
print(f"   Total rows in DB: "
      + str(db.query("SELECT COUNT(*) AS n FROM macro_observations WHERE series_id = ?", [series_id])[0]["n"]))
print(f"\n   Latest 10 observations (period DESC, release DESC):")
for r in stored:
    final_marker = " ← final" if r["is_final"] else ""
    print(f"   period={r['period_date']}  release={r['release_date']}  "
          f"value={r['value']}{final_marker}")

print("\n── Test passed ──")
db.close()
