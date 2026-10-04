"""
scripts/run_experiment.py

Runs one experiment end to end from its config.yaml and writes every
artifact (performance.parquet, results.json, diagnostics.json, charts/,
research_note.md) next to that config. See src/experiments/runner.py.

Usage:
    python scripts/run_experiment.py experiments/EXP-0001/config.yaml
"""

import argparse

from src.db.client import db
from src.experiments.runner import run_experiment

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run one experiment from its config.yaml")
    parser.add_argument("config_path", help="Path to the experiment's config.yaml")
    args = parser.parse_args()

    print(f"Running experiment from {args.config_path}\n")

    db.open()
    try:
        result = run_experiment(args.config_path)
    finally:
        db.close()

    metrics = result.report.performance.metrics()
    print(f"\n── {result.config.experiment_id} complete ──────────────────────")
    print(f"  Sharpe       : {metrics['sharpe']:.3f}")
    print(f"  CAGR         : {metrics['cagr']:.4f}")
    print(f"  Max drawdown : {metrics['max_drawdown']:.4f}")
    print(f"  Artifacts    : {result.experiment_dir}")
