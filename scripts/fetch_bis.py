"""
scripts/fetch_bis.py

Fetches daily central bank policy rates from the BIS into quant.db
(macro_series + macro_observations, codes BIS.CBPOL.<area>). See
src/collectors/bis.py.

Usage:
    python scripts/fetch_bis.py                  # all areas, last 60 days
    python scripts/fetch_bis.py --full-history   # all areas, full history
    python scripts/fetch_bis.py --areas XM,JP
"""

import argparse

from src.collectors.bis import POLICY_RATE_AREAS, BisPolicyRateCollector
from src.db.client import db

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch BIS central bank policy rates into quant.db")
    parser.add_argument("--full-history", action="store_true", help="Fetch all available history")
    parser.add_argument("--areas", type=str, default=None, metavar="AREA[,AREA...]",
                        help=f"BIS area codes (default: all of {','.join(POLICY_RATE_AREAS)})")
    args = parser.parse_args()

    areas = [a.strip().upper() for a in args.areas.split(",") if a.strip()] if args.areas else list(POLICY_RATE_AREAS)

    db.open()
    results = BisPolicyRateCollector().fetch_all(areas=areas, full_history=args.full_history)
    db.close()

    failures = [a for a in areas if a not in results]
    print("\n── Summary ──────────────────────────────────────────")
    for area in areas:
        print(f"  {area:3s} {results[area]:>7,} rows" if area in results else f"  {area:3s}  FAILED")
    if failures:
        raise SystemExit(1)
