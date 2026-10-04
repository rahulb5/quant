"""
scripts/new_experiment.py

Scaffolds a new experiment: allocates the next EXP-000N id, writes a
config.yaml stub into experiments/EXP-000N/, and inserts a 'draft' row into
the experiments registry. Edit the generated config.yaml, then run it with
scripts/run_experiment.py.

Usage:
    python scripts/new_experiment.py \\
        --hypothesis "Extreme commercial positioning predicts forward returns" \\
        --signal-module src.signals.cot_commercial_positioning \\
        --asset-class commodity --resample W-MON \\
        --date-column week_date --asset-column commodity \\
        --signal-column raw_pctile_52w --threshold 90 \\
        --periods-per-year 52 --researcher rhlbh
"""

import argparse
import json
from datetime import date
from pathlib import Path

from src.db.client import db
from src.experiments.config import (
    BacktestConfig,
    DataConfig,
    ExperimentConfig,
    PortfolioConfig,
    SignalConfig,
    ValidationConfig,
    save_config,
)
from src.experiments.registry import ExperimentRecord, insert_experiment, next_experiment_id

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scaffold a new experiment")
    parser.add_argument("--hypothesis", required=True)
    parser.add_argument("--signal-module", required=True)
    parser.add_argument("--asset-class", required=True)
    parser.add_argument("--resample", default=None)
    parser.add_argument("--date-column", required=True)
    parser.add_argument("--asset-column", required=True)
    parser.add_argument("--signal-column", required=True)
    parser.add_argument("--threshold", type=float, required=True)
    parser.add_argument("--trend-filter", default="trend_none")
    parser.add_argument("--portfolio-rule", default="equal_weight_long_threshold")
    parser.add_argument("--cost-bps", type=float, default=1.0)
    parser.add_argument("--lag", type=int, default=1)
    parser.add_argument("--periods-per-year", type=int, default=252)
    parser.add_argument("--parameters", default="{}", help="JSON dict of signal parameters")
    parser.add_argument("--researcher", default=None)
    args = parser.parse_args()

    db.open()
    experiment_id = next_experiment_id()

    config = ExperimentConfig(
        experiment_id=experiment_id,
        hypothesis=args.hypothesis,
        researcher=args.researcher,
        created_at=date.today().isoformat(),
        data=DataConfig(asset_class=args.asset_class, resample=args.resample),
        signal=SignalConfig(module=args.signal_module, parameters=json.loads(args.parameters)),
        portfolio=PortfolioConfig(
            rule=args.portfolio_rule,
            date_column=args.date_column,
            asset_column=args.asset_column,
            signal_column=args.signal_column,
            threshold=args.threshold,
            trend_filter=args.trend_filter,
        ),
        backtest=BacktestConfig(
            cost_bps=args.cost_bps, lag=args.lag, periods_per_year=args.periods_per_year
        ),
        validation=ValidationConfig(),
        status="draft",
    )

    experiment_dir = Path("experiments") / experiment_id
    experiment_dir.mkdir(parents=True, exist_ok=False)
    config_path = experiment_dir / "config.yaml"
    save_config(config, str(config_path))

    insert_experiment(ExperimentRecord(
        id=experiment_id,
        hypothesis=args.hypothesis,
        researcher=args.researcher,
        created_at=config.created_at,
        signal_name=args.signal_module.rsplit(".", 1)[-1],
        asset_class=args.asset_class,
        config_path=str(config_path),
        status="draft",
    ))

    db.close()

    print(f"Created {experiment_id} at {config_path}")
    print("Edit the config, then run:")
    print(f"  python scripts/run_experiment.py {config_path}")
