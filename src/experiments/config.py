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

import re
from dataclasses import asdict, dataclass, field
from typing import Any

import yaml


MARKETS = ("commodities", "equities", "fx", "rates", "macro", "multi")
SIGNAL_FAMILIES = ("carry", "momentum", "reversal", "value", "positioning", "macro", "regime")
FREQUENCIES = ("daily", "weekly", "monthly")
DIRECTIONS = ("long_only", "long_short", "market_neutral")


@dataclass
class ClassificationConfig:
    """Human-facing experiment label; separate from the technical data.asset_class key."""

    asset_class: str
    universe: str
    signal_family: str
    frequency: str
    direction: str

    def __post_init__(self) -> None:
        for name, allowed in (
            ("asset_class", MARKETS),
            ("signal_family", SIGNAL_FAMILIES),
            ("frequency", FREQUENCIES),
            ("direction", DIRECTIONS),
        ):
            if getattr(self, name) not in allowed:
                raise ValueError(
                    f"classification.{name}={getattr(self, name)!r} is invalid; "
                    f"allowed: {', '.join(allowed)}"
                )


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
    threshold: float = 0.0
    trend_filter: str = "trend_none"
    n_deciles: int = 10
    min_names: int = 50


@dataclass
class BacktestConfig:
    cost_bps: float = 1.0
    lag: int = 1
    periods_per_year: int = 252


FAMILY_PATTERN = re.compile(r"^[a-z][a-z0-9_]*$")


@dataclass
class ValidationConfig:
    """`family` names the group of variants this experiment was chosen from (e.g.
    `growth_regime_001`); the trial ledger (src/experiments/trials.py) counts trials per
    family. Required for new experiments (scripts/new_experiment.py); optional here so
    older configs still load, and then nothing is logged. `n_trials` is the researcher's
    declared count; the deflated Sharpe uses max(n_trials, ledger count)."""

    methodology: str = ""
    holdout_start: str | None = None
    n_trials: int = 1
    family: str | None = None
    family_version: str = "v1"

    def __post_init__(self) -> None:
        if self.family is not None and not FAMILY_PATTERN.match(self.family):
            raise ValueError(
                f"validation.family={self.family!r} must be snake_case "
                "(lowercase letters, digits, underscores; starting with a letter)"
            )


def _all_regime_names() -> list[str]:
    from src.macro.regimes import REGIME_REGISTRY   # lazy: keeps config import light

    return list(REGIME_REGISTRY)


@dataclass
class DiagnosticsConfig:
    """What the Diagnostics tab conditions on.

    `regimes` are `REGIME_REGISTRY` keys (src/macro/regimes.py) passed to run_backtest();
    the default is every registered regime (macro and market layers). `state_exposure` adds a
    table regressing the strategy's next-period returns on the macro state vector
    (src.macro.analysis.forward_regression at h = 1); off by default. Neither changes the
    trial identity (src.experiments.trials.experiment_params ignores this section).
    """

    regimes: list[str] = field(default_factory=_all_regime_names)
    state_exposure: bool = False

    def __post_init__(self) -> None:
        known = _all_regime_names()
        unknown = [r for r in self.regimes if r not in known]
        if unknown:
            raise ValueError(f"diagnostics.regimes {unknown} not in REGIME_REGISTRY {known}")


@dataclass
class RobustnessConfig:
    """Sweep settings and verdict thresholds for src/experiments/robustness.py.

    Thresholds are stored with the experiment so a verdict can be traced back
    to the rules that produced it. cost_multipliers scale backtest.cost_bps;
    lag_offsets are added to backtest.lag; param_grid maps dotted config paths
    (e.g. "portfolio.threshold", "signal.parameters.trend_window") to the
    values to try, and should contain modest perturbations of the base setting.
    """

    cost_multipliers: list[float] = field(default_factory=lambda: [0.5, 1.0, 1.5, 2.0, 3.0, 5.0])
    lag_offsets: list[int] = field(default_factory=lambda: [0, 1, 2])
    param_grid: dict[str, list[Any]] = field(default_factory=dict)
    crisis_windows: dict[str, list[str]] = field(default_factory=lambda: {
        "gfc_2008": ["2008-01-01", "2009-06-30"],
        "covid_2020": ["2020-02-01", "2020-06-30"],
        "inflation_2022": ["2022-01-01", "2022-12-31"],
    })
    n_boot: int = 2000
    seed: int = 0
    cost_kill_multiplier: float = 1.5
    cost_flag_multiplier: float = 2.0
    breakeven_flag_bps: float = 5.0
    lag_kill_retention: float = 0.35
    lag_flag_retention: float = 0.5
    outlier_trim_frac: float = 0.01
    positive_year_flag_frac: float = 0.55
    half_sharpe_flag: float = -0.2
    param_kill_frac: float = 0.6
    deflated_sharpe_flag: float = 0.90
    holdout_kill_sharpe: float = -0.25


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
    robustness: RobustnessConfig = field(default_factory=RobustnessConfig)
    diagnostics: DiagnosticsConfig = field(default_factory=DiagnosticsConfig)
    status: str = "draft"
    conclusion: str | None = None
    git_sha: str | None = None
    data_as_of: str | None = None
    classification: ClassificationConfig | None = None


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
        robustness=RobustnessConfig(**raw.get("robustness", {})),
        diagnostics=DiagnosticsConfig(**raw.get("diagnostics", {})),
        status=raw.get("status", "draft"),
        conclusion=raw.get("conclusion"),
        git_sha=raw.get("git_sha"),
        data_as_of=raw.get("data_as_of"),
        classification=(
            ClassificationConfig(**raw["classification"]) if raw.get("classification") else None
        ),
    )


def save_config(config: ExperimentConfig, path: str) -> None:
    """Write an ExperimentConfig back out as config.yaml."""
    with open(path, "w") as f:
        yaml.safe_dump(asdict(config), f, sort_keys=False)
