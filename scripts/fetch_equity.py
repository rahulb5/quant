"""
scripts/fetch_equity.py

Fetches daily OHLCV prices for all S&P 500 constituents from Yahoo Finance
and stores them in quant.db. Safe to re-run — existing rows are skipped.

Run from the project root with the venv activated:
    python scripts/fetch_equity.py
"""

import math
from datetime import date

from src.db.client import db
from src.collectors.equity import EquityCollector

# ── Setup ─────────────────────────────────────────────────────────────────────

db.open()
collector = EquityCollector()

# ── Resolve S&P 500 tickers ───────────────────────────────────────────────────

print("Fetching S&P 500 constituent list from Wikipedia...")
sp500 = collector.get_sp500_tickers()
print(f"Found {len(sp500)} tickers\n")

# ── Register assets ───────────────────────────────────────────────────────────

print("Registering assets...")
assets: list[dict] = []
register_failures: list[str] = []

for entry in sp500:
    ticker = entry["ticker"]
    try:
        asset_id = collector.ensure_asset(ticker, entry["name"])
        assets.append({"ticker": ticker, "asset_id": asset_id})
    except Exception as e:
        print(f"  [WARN] Could not register {ticker}: {e}")
        register_failures.append(ticker)

print(f"Registered {len(assets)} assets ({len(register_failures)} failed)\n")

# ── Collect prices ────────────────────────────────────────────────────────────

from_date = "2000-01-01"
to_date = date.today().isoformat()
chunk_size = 100
total_batches = math.ceil(len(assets) / chunk_size)

print(f"Collecting daily prices {from_date} → {to_date}")
print(f"({len(assets)} tickers · {total_batches} batches of {chunk_size})\n")

results: dict[str, int] = {}

for batch_num in range(1, total_batches + 1):
    start = (batch_num - 1) * chunk_size
    end = start + chunk_size
    chunk = assets[start:end]
    ticker_range = f"{start + 1}–{min(end, len(assets))}"

    print(f"  Batch {batch_num:>{len(str(total_batches))}}/{total_batches}"
          f"  (tickers {ticker_range:>8})  ...", end="", flush=True)

    batch_results = collector.collect_batch(chunk, from_date=from_date, to_date=to_date)
    results.update(batch_results)

    batch_rows = sum(batch_results.values())
    batch_with_data = len(batch_results)
    print(f"  {batch_with_data:>3} with data  ·  {batch_rows:>8,} rows")

# ── Summary ───────────────────────────────────────────────────────────────────

total_rows = sum(results.values())
succeeded = set(results.keys())
expected = {a["ticker"] for a in assets}
failed = expected - succeeded

print("\n── Summary ──────────────────────────────────────────")
print(f"  Tickers requested : {len(sp500)}")
print(f"  Assets registered : {len(assets)}")
print(f"  Tickers with data : {len(succeeded)}")
print(f"  Tickers with no data / failed : {len(failed) + len(register_failures)}")
print(f"  Total rows inserted : {total_rows:,}")

if failed:
    print(f"\n  Tickers skipped (no data or insert error):")
    for ticker in sorted(failed):
        print(f"    {ticker}")

if register_failures:
    print(f"\n  Tickers not registered:")
    for ticker in sorted(register_failures):
        print(f"    {ticker}")

# ── Teardown ──────────────────────────────────────────────────────────────────

db.close()
