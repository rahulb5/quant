"""
src/experiments/config.py

ExperimentConfig: typed representation of an experiment's config.yaml,
matching the schema in the build plan's Section 10. Round-trips to/from YAML
so a config file is both the human-editable spec and the runner's input.

Usage:
    from src.experiments.config import load_config, save_config

    config = load_config("experiments/EXP-0001/config.yaml")
    save_config(config, "experiments/EXP-0001/config.yaml")
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

import yaml


@dataclass
class DataConfig:
    asset_class: str
    resample: str | None = None
    start: str | None = None
    end: str | None = None


@dataclass
class SignalConfig:
    module: str
    parameters: dict[str, Any] = field(default_factory=dict)


@dataclass
class PortfolioConfig:
    rule: str
    date_column: str
    asset_column: str
    signal_column: str
    threshold: float
    trend_filter: str = "trend_none"


@dataclass
class BacktestConfig:
    cost_bps: float = 1.0
    lag: int = 1
    periods_per_year: int = 252


@dataclass
class ValidationConfig:
    methodology: str = ""


@dataclass
class ExperimentConfig:
    experiment_id: str
    hypothesis: str
    data: DataConfig
    signal: SignalConfig
    portfolio: PortfolioConfig
    researcher: str | None = None
    created_at: str | None = None
    backtest: BacktestConfig = field(default_factory=BacktestConfig)
    validation: ValidationConfig = field(default_factory=ValidationConfig)
    status: str = "draft"
    conclusion: str | None = None


def load_config(path: str) -> ExperimentConfig:
    """Load and parse a config.yaml into an ExperimentConfig."""
    with open(path) as f:
        raw = yaml.safe_load(f)

    return ExperimentConfig(
        experiment_id=raw["experiment_id"],
        hypothesis=raw["hypothesis"],
        researcher=raw.get("researcher"),
        created_at=raw.get("created_at"),
        data=DataConfig(**raw["data"]),
        signal=SignalConfig(**raw["signal"]),
        portfolio=PortfolioConfig(**raw["portfolio"]),
        backtest=BacktestConfig(**raw.get("backtest", {})),
        validation=ValidationConfig(**raw.get("validation", {})),
        status=raw.get("status", "draft"),
        conclusion=raw.get("conclusion"),
    )


def save_config(config: ExperimentConfig, path: str) -> None:
    """Write an ExperimentConfig back out as config.yaml."""
    with open(path, "w") as f:
        yaml.safe_dump(asdict(config), f, sort_keys=False)
