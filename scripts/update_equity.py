"""
scripts/update_equity.py

Incrementally updates equity prices for all registered assets, including
equity indices (asset IDs 100001–100015) registered via fetch_equity.py --indices.

Strategy:
  - Tickers with NO data   → collect_batch() from 1970-01-01 to today
  - Tickers WITH data      → collect_batch() from last_date + 1 to today
                             (grouped by last date, batched 100 at a time)

Both phases use yf.download() in chunks of 100, so yfinance is never
called one ticker at a time. Safe to re-run — existing rows are skipped.

Run from the project root with the venv activated:
    python scripts/update_equity.py
"""

import math
from collections import defaultdict
from datetime import date, timedelta

from src.db.client import db
from src.collectors.equity import EquityCollector

# ── Setup ─────────────────────────────────────────────────────────────────────

db.open()
collector = EquityCollector()
today = date.today().isoformat()

# ── Load registered assets ────────────────────────────────────────────────────

print("Loading registered assets from database...")
asset_rows = db.query(
    "SELECT id AS asset_id, ticker FROM assets WHERE asset_class = 'equity' ORDER BY ticker"
)
all_assets = {int(r["asset_id"]): str(r["ticker"]) for r in asset_rows}
print(f"Found {len(all_assets)} registered assets")

# ── Find last price date per asset ────────────────────────────────────────────

last_date_rows = db.query(
    """
    SELECT asset_id, MAX(timestamp)::DATE AS last_date
    FROM prices
    WHERE interval = '1d'
    GROUP BY asset_id
    """
)
last_date_by_id: dict[int, str] = {}
for r in last_date_rows:
    asset_id = int(r["asset_id"])
    last = r["last_date"]
    last_date_by_id[asset_id] = (
        last.isoformat() if hasattr(last, "isoformat") else str(last)[:10]
    )

# ── Split by from_date ────────────────────────────────────────────────────────

# Group assets by the from_date they need so we can batch tickers together.
# Tickers with no data use '1970-01-01'; others use last_date + 1 day.
groups: dict[str, list[dict]] = defaultdict(list)

for asset_id, ticker in all_assets.items():
    if asset_id not in last_date_by_id:
        from_date = "1970-01-01"
    else:
        from_date = (
            date.fromisoformat(last_date_by_id[asset_id]) + timedelta(days=1)
        ).isoformat()

    if from_date >= today:
        continue  # already up to date

    groups[from_date].append({"ticker": ticker, "asset_id": asset_id})

no_data_count = len(groups.get("1970-01-01", []))
needs_update_count = sum(len(v) for k, v in groups.items() if k != "1970-01-01")
already_current = len(all_assets) - no_data_count - needs_update_count

print(f"  {no_data_count} tickers with no data      → full history from 1970-01-01")
print(f"  {needs_update_count} tickers with partial data  → incremental fill")
print(f"  {already_current} tickers already up to date\n")

# ── Collect — all groups, batched ─────────────────────────────────────────────

results: dict[str, int] = {}
chunk_size = 100

# Sort so the full-history group ("1970-01-01") runs first
sorted_from_dates = sorted(groups.keys())

for from_date in sorted_from_dates:
    group_assets = groups[from_date]
    label = (
        "full history (no data)" if from_date == "1970-01-01"
        else f"from {from_date}"
    )
    total_batches = math.ceil(len(group_assets) / chunk_size)

    print(f"── {label}: {len(group_assets)} tickers "
          f"({total_batches} batch{'es' if total_batches != 1 else ''}) ──")

    for batch_num in range(1, total_batches + 1):
        start = (batch_num - 1) * chunk_size
        end   = start + chunk_size
        chunk = group_assets[start:end]
        ticker_range = f"{start + 1}–{min(end, len(group_assets))}"

        print(f"  Batch {batch_num:>{len(str(total_batches))}}/{total_batches}"
              f"  (tickers {ticker_range:>8})  ...", end="", flush=True)

        batch_results = collector.collect_batch(chunk, from_date=from_date, to_date=today)
        results.update(batch_results)

        batch_rows = sum(batch_results.values())
        batch_with_data = len(batch_results)
        print(f"  {batch_with_data:>3} with data  ·  {batch_rows:>8,} rows")

    print()

# ── Summary ───────────────────────────────────────────────────────────────────

all_tickers = set(all_assets.values())
succeeded   = set(results.keys())
failed      = {t for t in all_tickers if t not in succeeded and t not in
               {all_assets[a] for a in all_assets if last_date_by_id.get(a, "") >= today}}

print("── Summary ──────────────────────────────────────────")
print(f"  Assets registered    : {len(all_assets)}")
print(f"  Already up to date   : {already_current}")
print(f"  Tickers updated      : {len(succeeded)}")
print(f"  Failed / no data     : {len(failed)}")
print(f"  Total rows inserted  : {sum(results.values()):,}")

if failed:
    print(f"\n  Tickers with no data or errors:")
    for ticker in sorted(failed):
        print(f"    {ticker}")

# ── Teardown ──────────────────────────────────────────────────────────────────

db.close()
