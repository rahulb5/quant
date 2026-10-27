"""
scripts/returns_quality.py

Prints the returns-layer quality report (Checkpoint 0 of
docs/plans/cross_asset_strategies.md): one row per instrument, plus the FX
spikes removed and the carry caveats. Read-only.

Usage:
    python scripts/returns_quality.py
    python scripts/returns_quality.py --csv /path/to/report.csv
"""

import argparse

import pandas as pd

from src.db.client import db
from src.returns.layer import excess_returns
from src.returns.quality import quality_report

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Returns-layer quality report")
    parser.add_argument("--csv", type=str, default=None, help="Also write the report to this CSV path")
    args = parser.parse_args()

    db.open()
    xr = excess_returns(db)
    db.close()

    report = quality_report(xr)
    with pd.option_context("display.width", 250, "display.max_columns", None, "display.max_colwidth", 80):
        cols = ["first", "last", "n_obs", "pct_missing_bdays", "ann_mean", "ann_vol", "max_abs",
                "excess_kurtosis", "max_gap_bdays", "flags"]
        print(report[cols].to_string(float_format=lambda v: f"{v:.3f}"))
        print("\nWorst days:")
        for key, row in report.iterrows():
            print(f"  {key:9s} {row['worst_days']}")

    print("\nSpike-and-revert prints removed:")
    for sleeve in ("equity", "fx"):
        for key, dates in xr.attrs.get(f"{sleeve}.spikes_removed", {}).items():
            if dates:
                print(f"  {key}: {', '.join(d.date().isoformat() for d in dates)}")
    print("\nFX returns nulled (interval skipped more than 3 weekdays of data):")
    for key, dates in xr.attrs.get("fx.gaps_nulled", {}).items():
        if dates:
            print(f"  {key}: {', '.join(d.date().isoformat() for d in dates)}")
    print("\nCarry caveats (policy rate is not a money-market rate):")
    for key, items in xr.attrs.get("fx.carry_approx", {}).items():
        for s, e, why in items:
            print(f"  {key}: {s or 'start'} to {e or 'now'}: {why}")
    print("\nEquity cash currency:", xr.attrs.get("equity.cash_currency"))

    if args.csv:
        report.to_csv(args.csv)
        print(f"\nWrote {args.csv}")
