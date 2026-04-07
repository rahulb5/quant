"""
scripts/update_commodities.py

Incrementally updates commodity futures prices from Nasdaq Data Link.
Fetches from last stored date + 1 day to today for each series.

Run from the project root with the venv activated:
    python scripts/update_commodities.py
"""

from datetime import date, timedelta

from src.db.client import db
from src.collectors.commodity import CommodityCollector

# ── Setup ─────────────────────────────────────────────────────────────────────

db.open()
collector = CommodityCollector()
today = date.today().isoformat()

# ── Load registered futures assets ────────────────────────────────────────────

print("Loading registered futures assets...")
asset_rows = db.query(
    "SELECT id AS asset_id, ticker FROM assets WHERE asset_class = 'futures' ORDER BY ticker"
)

if not asset_rows:
    print("No futures assets registered. Run fetch_commodities.py first.")
    db.close()
    exit()

print(f"Found {len(asset_rows)} futures assets\n")

# ── Find last price date per asset ────────────────────────────────────────────

last_date_rows = db.query(
    """
    SELECT asset_id, MAX(timestamp)::DATE AS last_date
    FROM prices
    WHERE interval = '1d' AND asset_id >= 300000 AND asset_id < 400000
    GROUP BY asset_id
    """
)
last_date_by_id: dict[int, str] = {}
for r in last_date_rows:
    aid = int(r["asset_id"])
    last = r["last_date"]
    last_date_by_id[aid] = last.isoformat() if hasattr(last, "isoformat") else str(last)[:10]

# ── Build CHRIS code lookup from catalogue ────────────────────────────────────

chris_lookup: dict[str, str] = {}
for entry in collector.get_all_tickers():
    chris_lookup[entry["ticker"]] = entry["chris_code"]

# ── Update each asset ─────────────────────────────────────────────────────────

updated = 0
already_current = 0
results: dict[str, int] = {}

for row in asset_rows:
    asset_id = int(row["asset_id"])
    ticker = str(row["ticker"])

    if ticker not in chris_lookup:
        continue

    if asset_id in last_date_by_id:
        from_date = (date.fromisoformat(last_date_by_id[asset_id]) + timedelta(days=1)).isoformat()
        if from_date >= today:
            already_current += 1
            continue
    else:
        from_date = "2000-01-01"

    rows = collector.fetch_single(
        chris_code=chris_lookup[ticker],
        asset_id=asset_id,
        ticker=ticker,
        from_date=from_date,
        to_date=today,
    )
    if rows > 0:
        results[ticker] = rows
        updated += 1

# ── Summary ───────────────────────────────────────────────────────────────────

print(f"\n── Summary ──────────────────────────────────────────")
print(f"  Futures assets       : {len(asset_rows)}")
print(f"  Already up to date   : {already_current}")
print(f"  Updated              : {updated}")
print(f"  Total rows inserted  : {sum(results.values()):,}")

db.close()
