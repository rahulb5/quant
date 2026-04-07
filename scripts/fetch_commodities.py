"""
scripts/fetch_commodities.py

Fetches daily OHLCV for commodity futures (front through 4th month)
from Nasdaq Data Link (CHRIS dataset) and stores them in quant.db.

Safe to re-run — existing rows are skipped via ON CONFLICT DO NOTHING.

Uses the 300xxx asset ID block.

Run from the project root with the venv activated:
    python scripts/fetch_commodities.py
"""

from datetime import date

from src.db.client import db
from src.collectors.commodity import CommodityCollector

# ── Setup ─────────────────────────────────────────────────────────────────────

db.open()
collector = CommodityCollector()

from_date = "2000-01-01"
to_date = date.today().isoformat()

# ── Register assets & fetch ───────────────────────────────────────────────────

all_tickers = collector.get_all_tickers()
print(f"Fetching {len(all_tickers)} commodity futures series ({from_date} → {to_date})\n")

results: dict[str, int] = {}
failures: list[str] = []

for entry in all_tickers:
    ticker = entry["ticker"]
    chris_code = entry["chris_code"]
    name = entry["name"]

    print(f"  {ticker:<8} ({chris_code:<20})  ...", end="", flush=True)

    try:
        asset_id = collector.ensure_asset(ticker, name)
        rows = collector.fetch_single(
            chris_code=chris_code,
            asset_id=asset_id,
            ticker=ticker,
            from_date=from_date,
            to_date=to_date,
        )
        results[ticker] = rows
        print(f"  {rows:>7,} rows")
    except Exception as e:
        print(f"  FAILED: {e}")
        failures.append(ticker)

# ── Summary ───────────────────────────────────────────────────────────────────

total_rows = sum(results.values())
with_data = [t for t, r in results.items() if r > 0]
no_data = [t for t, r in results.items() if r == 0]

print(f"\n── Summary ──────────────────────────────────────────")
print(f"  Series requested     : {len(all_tickers)}")
print(f"  With data            : {len(with_data)}")
print(f"  No data returned     : {len(no_data)}")
print(f"  Failed               : {len(failures)}")
print(f"  Total rows inserted  : {total_rows:,}")

if no_data:
    print(f"\n  No data:")
    for t in sorted(no_data):
        print(f"    {t}")

if failures:
    print(f"\n  Failed:")
    for t in sorted(failures):
        print(f"    {t}")

# ── Teardown ──────────────────────────────────────────────────────────────────

db.close()
