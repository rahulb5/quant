"""
src/experiments/runner.py

run_experiment(): the orchestration step that closes the hypothesis ->
experiment -> backtest -> report loop. Reads a config.yaml, calls a signal
module, turns the signal into weights, backtests it, and writes every
artifact described in the build plan's Section 10 next to the config.

Signal module contract (the "standard interface" the build plan names):
    load_data() -> tuple[pd.DataFrame, ...]
    calculate_signal(*data, config: SignalConfig) -> pd.DataFrame   # long-format
    class SignalConfig  — constructed from config.signal.parameters

Asset-class contract: config.data.asset_class selects an AssetUniverse from
UNIVERSE_REGISTRY (src/experiments/universe.py); its returns_panel() must
share column labels with the weight panel equal_weight_long_basket()
produces (both keyed by config.portfolio.asset_column's values).
"""

from __future__ import annotations

import importlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

from src.backtest.backtest import BacktestReport, run_backtest
from src.experiments.config import ExperimentConfig, load_config
from src.experiments.portfolio import equal_weight_long_basket
from src.experiments.registry import update_experiment
from src.experiments.report import render_research_note
from src.experiments.universe import UNIVERSE_REGISTRY
from src.shared.utils import logger


@dataclass
class ExperimentResult:
    config: ExperimentConfig
    report: BacktestReport
    experiment_dir: Path


def _to_jsonable(obj: Any) -> Any:
    """Recursively convert pandas/numpy objects into plain JSON-safe types."""
    if isinstance(obj, pd.DataFrame):
        df = obj.copy()
        df.index = df.index.map(str)
        return {str(k): _to_jsonable(v) for k, v in df.to_dict(orient="index").items()}
    if isinstance(obj, pd.Series):
        return {str(k): _to_jsonable(v) for k, v in obj.to_dict().items()}
    if isinstance(obj, dict):
        return {str(k): _to_jsonable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_to_jsonable(v) for v in obj]
    if isinstance(obj, float) and pd.isna(obj):
        return None
    if hasattr(obj, "item"):  # numpy scalar
        return obj.item()
    return obj


def _build_signal(config: ExperimentConfig) -> pd.DataFrame:
    signal_module = importlib.import_module(config.signal.module)
    data = signal_module.load_data()
    if not isinstance(data, tuple):
        data = (data,)
    signal_config = signal_module.SignalConfig(**config.signal.parameters)
    return signal_module.calculate_signal(*data, config=signal_config)


def _build_panels(config: ExperimentConfig, signal_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    weights = equal_weight_long_basket(
        signal_df,
        date_column=config.portfolio.date_column,
        asset_column=config.portfolio.asset_column,
        signal_column=config.portfolio.signal_column,
        threshold=config.portfolio.threshold,
        trend_filter=config.portfolio.trend_filter,
    )

    universe_cls = UNIVERSE_REGISTRY[config.data.asset_class]
    universe = universe_cls()
    asset_returns = universe.returns_panel(
        resample=config.data.resample, start=config.data.start, end=config.data.end
    )

    return weights, asset_returns


def _write_artifacts(config: ExperimentConfig, report: BacktestReport, experiment_dir: Path) -> None:
    metrics = report.performance.metrics()
    exec_summary = report.execution.summary(periods_per_year=config.backtest.periods_per_year)

    performance_df = pd.DataFrame({
        "gross_returns": report.execution.gross_returns,
        "net_returns": report.execution.net_returns,
        "turnover": report.execution.turnover,
        "costs": report.execution.costs,
    })
    performance_df.to_parquet(experiment_dir / "performance.parquet")

    results = {"metrics": _to_jsonable(metrics), "execution_summary": _to_jsonable(exec_summary)}
    (experiment_dir / "results.json").write_text(json.dumps(results, indent=2))

    diagnostics = {
        "yearly": _to_jsonable(report.yearly),
        "by_regime": _to_jsonable(report.by_regime),
        "correlations": _to_jsonable(report.correlations),
        "betas": _to_jsonable(report.betas),
        "factor_regression": _to_jsonable(report.factor_regression),
        "rolling_corr": _to_jsonable(report.rolling_corr),
        "conditional_perf": _to_jsonable(report.conditional_perf),
    }
    (experiment_dir / "diagnostics.json").write_text(json.dumps(diagnostics, indent=2))

    charts_dir = experiment_dir / "charts"
    charts_dir.mkdir(exist_ok=True)

    fig, ax = plt.subplots()
    report.performance.equity_curve().plot(ax=ax, title=f"{config.experiment_id} — Equity Curve")
    fig.savefig(charts_dir / "equity_curve.png")
    plt.close(fig)

    fig, ax = plt.subplots()
    report.performance.drawdown_curve().plot(ax=ax, title=f"{config.experiment_id} — Drawdown")
    fig.savefig(charts_dir / "drawdown.png")
    plt.close(fig)

    note = render_research_note(config, report)
    (experiment_dir / "research_note.md").write_text(note)


def run_experiment(config_path: str) -> ExperimentResult:
    """Run one experiment end to end and write all artifacts next to its config.

    Reads config.yaml -> signal module -> weights/returns panels ->
    run_backtest() -> performance.parquet, results.json, diagnostics.json,
    charts/, research_note.md. Updates the experiments registry row to
    'complete' with headline metrics, or 'failed' with the error on
    exception.

    Caller manages db.open()/db.close(), same convention as the signal and
    backtest modules this wires together.
    """
    config = load_config(config_path)
    experiment_dir = Path(config_path).parent

    try:
        signal_df = _build_signal(config)
        weights, asset_returns = _build_panels(config, signal_df)

        report = run_backtest(
            weights,
            asset_returns,
            cost_bps=config.backtest.cost_bps,
            lag=config.backtest.lag,
            name=config.experiment_id,
            periods_per_year=config.backtest.periods_per_year,
            verbose=False,
        )

        _write_artifacts(config, report, experiment_dir)

        metrics = report.performance.metrics()
        exec_summary = report.execution.summary(periods_per_year=config.backtest.periods_per_year)
        update_experiment(
            config.experiment_id,
            status="complete",
            sharpe=float(metrics["sharpe"]),
            cagr=float(metrics["cagr"]),
            max_drawdown=float(metrics["max_drawdown"]),
            ann_turnover=float(exec_summary["ann_turnover"]),
        )

        return ExperimentResult(config=config, report=report, experiment_dir=experiment_dir)
    except Exception as exc:
        logger.error(f"[run_experiment] {config.experiment_id} failed: {exc}")
        update_experiment(config.experiment_id, status="failed", conclusion=f"FAILED: {exc}")
        raise
