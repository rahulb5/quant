"""
scripts/diagnose_cot_coverage.py

Diagnostic: finds CFTC market codes for target commodities and checks whether
they are already stored in quant.db.

Steps:
  1. Query the Socrata disagg_fut API for distinct market codes matching our
     target commodity keywords.
  2. Check which of those codes already exist in macro_series.
  3. Print a clear gap report.

Run from the project root:
    python scripts/diagnose_cot_coverage.py
"""

import requests
from src.db.client import db

DISAGG_FUT_URL = "https://publicreporting.cftc.gov/resource/72hh-3qpy.json"
CODE_PREFIX    = "CFTC.DISAGG_FUT"

TARGET_KEYWORDS = [
    "CRUDE", "NATURAL GAS", "GOLD", "SILVER", "COPPER",
    "CORN", "WHEAT", "SOYBEAN", "COFFEE", "SUGAR", "COCOA",
]

# ── 1. Query Socrata for distinct market codes matching target commodities ─────

print("=== Step 1: Socrata API — distinct market codes for target commodities ===\n")

# Build SoQL WHERE clause using upper() LIKE on both commodity and market name fields
like_parts = []
for kw in TARGET_KEYWORDS:
    like_parts.append(f"upper(commodity) like '%{kw}%'")
    like_parts.append(f"upper(market_and_exchange_names) like '%{kw}%'")
where_clause = " OR ".join(like_parts)

params = {
    "$select": "cftc_contract_market_code,market_and_exchange_names,commodity",
    "$where":  where_clause,
    "$group":  "cftc_contract_market_code,market_and_exchange_names,commodity",
    "$order":  "commodity",
    "$limit":  "500",
}

resp = requests.get(DISAGG_FUT_URL, params=params, timeout=30)
resp.raise_for_status()
api_rows = resp.json()

print(f"Found {len(api_rows)} distinct combinations from Socrata\n")
print(f"{'CFTC Market Code':<20} {'Commodity':<25} Market & Exchange Name")
print("-" * 90)
for r in sorted(api_rows, key=lambda x: (x.get("commodity", ""), x.get("cftc_contract_market_code", ""))):
    mkt_code   = r.get("cftc_contract_market_code", "")
    commodity  = r.get("commodity", "")
    mkt_name   = r.get("market_and_exchange_names", "")
    print(f"{mkt_code:<20} {commodity:<25} {mkt_name}")

# ── 2. Check which market codes already exist in macro_series ─────────────────

print(f"\n\n=== Step 2: Cross-check against macro_series in quant.db ===\n")

db.open()

# Pull all disagg_fut series codes currently in the DB
db_rows = db.query(
    "SELECT code, name FROM macro_series WHERE code LIKE ? ORDER BY code",
    [f"{CODE_PREFIX}.%"],
)
db_codes = {r["code"]: r["name"] for r in db_rows}
print(f"Total disagg_fut series in DB: {len(db_codes)}\n")

# Match each API result against DB
print(f"{'CFTC Market Code':<20} {'Series Code':<40} {'In DB?':<8} DB Name")
print("-" * 110)

found_in_db   = []
missing_in_db = []

for r in sorted(api_rows, key=lambda x: (x.get("commodity", ""), x.get("cftc_contract_market_code", ""))):
    mkt_code    = r.get("cftc_contract_market_code", "")
    commodity   = r.get("commodity", "")
    series_code = f"{CODE_PREFIX}.{mkt_code}"
    in_db       = series_code in db_codes

    status = "YES" if in_db else "NO"
    db_name = db_codes.get(series_code, "—")
    print(f"{mkt_code:<20} {series_code:<40} {status:<8} {db_name}")

    if in_db:
        found_in_db.append((mkt_code, commodity, series_code))
    else:
        missing_in_db.append((mkt_code, commodity, series_code))

# ── 3. Gap summary ────────────────────────────────────────────────────────────

print(f"\n\n=== Step 3: Gap Summary ===\n")
print(f"Present in DB ({len(found_in_db)}):")
for mkt_code, commodity, series_code in found_in_db:
    print(f"  ✓  {mkt_code:<20} {commodity}")

print(f"\nMissing from DB ({len(missing_in_db)}):")
for mkt_code, commodity, series_code in missing_in_db:
    print(f"  ✗  {mkt_code:<20} {commodity}  →  {series_code}")

if missing_in_db:
    print("\nConclusion: Run  python scripts/fetch_cot.py --mode backfill --report-type disagg_fut")
    print("            to backfill the missing series.")
else:
    print("\nConclusion: All target commodity series are present in the DB.")

db.close()
