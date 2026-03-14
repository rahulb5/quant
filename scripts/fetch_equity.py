"""
scripts/fetch_equity.py

Fetches daily OHLCV prices from Yahoo Finance and stores them in quant.db.
Safe to re-run — existing rows are skipped.

Usage:
    python scripts/fetch_equity.py                # S&P 500 constituents (default)
    python scripts/fetch_equity.py --indices      # equity indices (full history)
"""

import argparse
import math
from datetime import date

from src.db.client import db
from src.collectors.equity import EquityCollector

# ── Equity index catalogue ────────────────────────────────────────────────────

EQUITY_INDICES = [
    {"id": 100001, "ticker": "^GSPC",    "name": "S&P 500"},
    {"id": 100002, "ticker": "^NDX",     "name": "Nasdaq 100"},
    {"id": 100003, "ticker": "^RUT",     "name": "Russell 2000"},
    {"id": 100004, "ticker": "^STOXX50E","name": "Euro Stoxx 50"},
    {"id": 100005, "ticker": "^FTSE",    "name": "FTSE 100"},
    {"id": 100006, "ticker": "^GDAXI",   "name": "DAX"},
    {"id": 100007, "ticker": "^FCHI",    "name": "CAC 40"},
    {"id": 100008, "ticker": "^IBEX",    "name": "IBEX 35"},
    {"id": 100009, "ticker": "^N225",    "name": "Nikkei 225"},
    {"id": 100010, "ticker": "^HSI",     "name": "Hang Seng"},
    {"id": 100011, "ticker": "000001.SS","name": "Shanghai Composite"},
    {"id": 100012, "ticker": "^KS11",    "name": "Kospi"},
    {"id": 100013, "ticker": "^GSPTSE",  "name": "TSX Composite"},
    {"id": 100014, "ticker": "^AXJO",    "name": "ASX 200"},
    {"id": 100015, "ticker": "^BVSP",    "name": "Bovespa"},
]

# ── Helpers ───────────────────────────────────────────────────────────────────

def _ensure_indices() -> list[dict]:
    """Insert any missing indices into the assets table with fixed IDs."""
    existing = {int(r["id"]) for r in db.query("SELECT id FROM assets")}
    missing = [idx for idx in EQUITY_INDICES if idx["id"] not in existing]

    if missing:
        def inserts(q) -> None:
            for idx in missing:
                q(
                    """
                    INSERT INTO assets (id, ticker, name, asset_class, currency, is_active)
                    VALUES (?, ?, ?, 'equity', 'USD', true)
                    """,
                    [idx["id"], idx["ticker"], idx["name"]],
                )
        db.transaction(inserts)
        print(f"Registered {len(missing)} new index asset(s)")

    return [{"ticker": idx["ticker"], "asset_id": idx["id"]} for idx in EQUITY_INDICES]


# ── CLI ───────────────────────────────────────────────────────────────────────

parser = argparse.ArgumentParser(description="Fetch equity prices from Yahoo Finance")
parser.add_argument(
    "--indices",
    action="store_true",
    help="Fetch equity indices instead of S&P 500 constituents",
)
args = parser.parse_args()

# ── Setup ─────────────────────────────────────────────────────────────────────

db.open()
collector = EquityCollector()
to_date = date.today().isoformat()

# ── Indices mode ──────────────────────────────────────────────────────────────

if args.indices:
    print(f"Fetching {len(EQUITY_INDICES)} equity indices (full history from 1970-01-01)\n")

    assets = _ensure_indices()
    from_date = "1970-01-01"

    results = collector.collect_batch(assets, from_date=from_date, to_date=to_date)

    total_rows = sum(results.values())
    succeeded = set(results.keys())
    failed = {idx["ticker"] for idx in EQUITY_INDICES} - succeeded

    print("\n── Summary ──────────────────────────────────────────")
    print(f"  Indices requested   : {len(EQUITY_INDICES)}")
    print(f"  With data           : {len(succeeded)}")
    print(f"  No data / failed    : {len(failed)}")
    print(f"  Total rows inserted : {total_rows:,}")

    if failed:
        print(f"\n  No data returned for:")
        for ticker in sorted(failed):
            print(f"    {ticker}")

# ── S&P 500 mode (default) ────────────────────────────────────────────────────

else:
    print("Fetching S&P 500 constituent list from Wikipedia...")
    sp500 = collector.get_sp500_tickers()
    print(f"Found {len(sp500)} tickers\n")

    print("Registering assets...")
    assets = []
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

    from_date = "2000-01-01"
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

    total_rows = sum(results.values())
    succeeded = set(results.keys())
    expected = {a["ticker"] for a in assets}
    failed = expected - succeeded

    print("\n── Summary ──────────────────────────────────────────")
    print(f"  Tickers requested             : {len(sp500)}")
    print(f"  Assets registered             : {len(assets)}")
    print(f"  Tickers with data             : {len(succeeded)}")
    print(f"  Tickers with no data / failed : {len(failed) + len(register_failures)}")
    print(f"  Total rows inserted           : {total_rows:,}")

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
