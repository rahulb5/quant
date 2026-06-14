"""
scripts/fetch_gsci.py

Fetches daily OHLCV for the 11 S&P GSCI single-commodity excess return indices
from Yahoo Finance and stores them in quant.db.

Safe to re-run — existing rows are skipped via ON CONFLICT DO NOTHING.

Uses the 400xxx asset ID block. Asset class is 'index'.

Run from the project root with the venv activated:
    python scripts/fetch_gsci.py
"""

from src.db.client import db
from src.collectors.gsci import GsciCollector

# ── Setup ─────────────────────────────────────────────────────────────────────

db.open()
collector = GsciCollector()

# ── Register assets & fetch ───────────────────────────────────────────────────

all_tickers = collector.get_all_tickers()
print(f"Fetching {len(all_tickers)} S&P GSCI excess return indices from Yahoo Finance\n")

results: dict[str, int] = {}
failures: list[str] = []

for entry in all_tickers:
    ticker    = entry["ticker"]
    yf_ticker = entry["yf_ticker"]
    name      = entry["name"]
    asset_id  = entry["id"]

    print(f"  {ticker:<10} ({yf_ticker:<12})  ...", end="", flush=True)

    try:
        asset_id = collector.ensure_asset(asset_id, ticker, name)
        rows = collector.fetch_single(entry=entry, asset_id=asset_id)
        results[ticker] = rows
        print(f"  {rows:>7,} rows")
    except Exception as e:
        print(f"  FAILED: {e}")
        failures.append(ticker)

# ── Summary ───────────────────────────────────────────────────────────────────

total_rows = sum(results.values())
with_data  = [t for t, r in results.items() if r > 0]
no_data    = [t for t, r in results.items() if r == 0]

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
