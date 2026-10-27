"""
scripts/fetch_total_return.py

Full-history refresh of the total-return indices and distributing ETFs used
for equity-index dividends. See src/collectors/total_return.py (why it is
always a full refresh) and src/returns/dividends.py (how they're used).

Usage:
    python scripts/fetch_total_return.py
    python scripts/fetch_total_return.py --tickers QQQ,ISF.L
"""

import argparse

from src.collectors.total_return import TOTAL_RETURN_SOURCES, TotalReturnCollector
from src.db.client import db

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch total-return sources into quant.db")
    parser.add_argument("--tickers", type=str, default=None, metavar="TICKER[,TICKER...]")
    args = parser.parse_args()

    tickers = [t.strip() for t in args.tickers.split(",") if t.strip()] if args.tickers else None
    wanted = tickers or [a["ticker"] for a in TOTAL_RETURN_SOURCES]

    db.open()
    results = TotalReturnCollector().fetch_all(tickers=tickers)
    db.close()

    failures = [t for t in wanted if t not in results]
    print("\n── Summary ──────────────────────────────────────────")
    for t in wanted:
        print(f"  {t:10s} {results[t]:>7,} rows" if t in results else f"  {t:10s}  FAILED")
    if failures:
        raise SystemExit(1)
