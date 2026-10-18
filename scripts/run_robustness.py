"""
scripts/run_robustness.py

Runs the standard robustness suite for one experiment and writes
robustness.json, charts/robustness_*.png, and the research note's Robustness
section next to its config. See src/experiments/robustness.py.

Usage:
    python scripts/run_robustness.py experiments/EXP-0001/config.yaml
    python scripts/run_robustness.py experiments/EXP-0003/config.yaml --unlock-holdout
"""

import argparse

from src.db.client import db
from src.experiments.robustness import run_robustness

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the robustness suite for one experiment")
    parser.add_argument("config_path", help="Path to the experiment's config.yaml")
    parser.add_argument(
        "--unlock-holdout",
        action="store_true",
        help="Score the held-out window. Do this once, after in-sample checks are final; "
        "the result is frozen in holdout.json.",
    )
    args = parser.parse_args()

    db.open()
    try:
        result = run_robustness(args.config_path, unlock_holdout=args.unlock_holdout)
    finally:
        db.close()

    res = result.results
    print(f"\n── {res['experiment_id']} robustness: {res['overall']} ──────────────")
    for name, check in res["checks"].items():
        print(f"  {name:<16} {check['verdict']:<8} {check['reason']}")
    print(f"  Artifacts: {result.experiment_dir}")
