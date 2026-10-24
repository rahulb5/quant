"""
scripts/trials.py

Read the trial ledger (src/experiments/trials.py).

Usage:
    python scripts/trials.py count <family>     # n_trials as used by the deflated Sharpe
    python scripts/trials.py list <family>      # every row, oldest first
    python scripts/trials.py families           # families with their counts
"""

import argparse

import pandas as pd

from src.db.client import db
from src.experiments import trials

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Trial ledger")
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("count").add_argument("family")
    sub.add_parser("list").add_argument("family")
    sub.add_parser("families")
    args = parser.parse_args()

    db.open()
    try:
        if args.cmd == "count":
            print(trials.n_trials(args.family))
        elif args.cmd == "list":
            with pd.option_context("display.width", 200, "display.max_columns", 20, "display.max_colwidth", 60):
                print(trials.trials_table(args.family).drop(columns=["params"]).to_string(index=False))
        else:
            rows = db.query("SELECT family, COUNT(*) AS rows FROM trials GROUP BY family ORDER BY family")
            for r in rows:
                print(f"{r['family']:30s} counted={trials.n_trials(r['family']):4d} rows={r['rows']}")
    finally:
        db.close()
