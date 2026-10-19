"""
src/experiments/artifacts.py

Read-only loader for experiment folders under experiments/ (config.yaml,
results.json, robustness.json, ...). No DuckDB and no Streamlit: the
dashboard's Research pages read from disk so they never hold the DB write lock.

Every file except config.yaml is optional; a missing one loads as None.

Usage:
    from src.experiments.artifacts import list_experiments, load_experiment

    for s in list_experiments():
        print(s.experiment_id, s.sharpe, s.robustness_verdict)
    art = load_experiment("EXP-0002")
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

import pandas as pd

from src.experiments.config import ClassificationConfig, ExperimentConfig, load_config

ENV_VAR = "QUANT_EXPERIMENTS_DIR"
_FILES = (
    "config.yaml", "results.json", "diagnostics.json", "robustness.json",
    "holdout.json", "performance.parquet", "research_note.md",
)


@dataclass
class ExperimentSummary:
    experiment_id: str
    hypothesis: str
    signal_name: str
    status: str
    classification: ClassificationConfig | None
    sharpe: float | None
    cagr: float | None
    max_drawdown: float | None
    ann_turnover: float | None
    robustness_verdict: str | None
    sharpe_2x_cost: float | None
    deflated_sharpe: float | None
    sharpe_ci_low: float | None
    breakeven_bps: float | None
    holdout_state: Literal["none", "locked", "scored"]
    created_at: str | None
    data_as_of: str | None
    git_sha: str | None
    path: Path


@dataclass
class ExperimentArtifacts:
    summary: ExperimentSummary
    config: ExperimentConfig
    results: dict | None
    diagnostics: dict | None
    robustness: dict | None
    holdout: dict | None
    performance: pd.DataFrame | None
    note: str | None


def experiments_root(root: str | Path | None = None) -> Path:
    if root is not None:
        return Path(root)
    return Path(os.environ.get(ENV_VAR) or "experiments")


def _read_json(path: Path) -> dict | None:
    return json.loads(path.read_text()) if path.exists() else None


def _dig(obj: Any, *keys: str) -> Any:
    for key in keys:
        if not isinstance(obj, dict):
            return None
        obj = obj.get(key)
    return obj


def _summarize(folder: Path, config: ExperimentConfig) -> ExperimentSummary:
    results = _read_json(folder / "results.json")
    robustness = _read_json(folder / "robustness.json")

    if config.validation.holdout_start is None:
        holdout_state = "none"
    elif (folder / "holdout.json").exists():
        holdout_state = "scored"
    else:
        holdout_state = "locked"

    return ExperimentSummary(
        experiment_id=config.experiment_id,
        hypothesis=config.hypothesis,
        signal_name=config.signal.module.rsplit(".", 1)[-1],
        status=config.status,
        classification=config.classification,
        sharpe=_dig(results, "metrics", "sharpe"),
        cagr=_dig(results, "metrics", "cagr"),
        max_drawdown=_dig(results, "metrics", "max_drawdown"),
        ann_turnover=_dig(results, "execution_summary", "ann_turnover"),
        robustness_verdict=_dig(robustness, "overall"),
        sharpe_2x_cost=_dig(robustness, "checks", "cost_stress", "detail", "sharpe_at_flag_multiplier"),
        deflated_sharpe=_dig(robustness, "checks", "statistical", "detail", "deflated_sharpe"),
        sharpe_ci_low=_dig(robustness, "checks", "statistical", "detail", "ci_low"),
        breakeven_bps=_dig(robustness, "checks", "cost_stress", "detail", "breakeven_bps"),
        holdout_state=holdout_state,
        created_at=config.created_at,
        data_as_of=config.data_as_of,
        git_sha=config.git_sha,
        path=folder,
    )


def _folders(root: Path) -> list[Path]:
    if not root.is_dir():
        return []
    return sorted(p for p in root.iterdir() if (p / "config.yaml").exists())


def list_experiments(root: str | Path | None = None) -> list[ExperimentSummary]:
    """One summary per experiment folder, sorted by id."""
    summaries = [
        _summarize(folder, load_config(str(folder / "config.yaml")))
        for folder in _folders(experiments_root(root))
    ]
    return sorted(summaries, key=lambda s: s.experiment_id)


def load_experiment(experiment_id: str, root: str | Path | None = None) -> ExperimentArtifacts:
    """Everything on disk for one experiment. Raises FileNotFoundError if absent."""
    folder = experiments_root(root) / experiment_id
    if not (folder / "config.yaml").exists():
        raise FileNotFoundError(f"No experiment {experiment_id!r} under {folder.parent}")

    config = load_config(str(folder / "config.yaml"))
    perf_path = folder / "performance.parquet"
    note_path = folder / "research_note.md"
    return ExperimentArtifacts(
        summary=_summarize(folder, config),
        config=config,
        results=_read_json(folder / "results.json"),
        diagnostics=_read_json(folder / "diagnostics.json"),
        robustness=_read_json(folder / "robustness.json"),
        holdout=_read_json(folder / "holdout.json"),
        performance=pd.read_parquet(perf_path) if perf_path.exists() else None,
        note=note_path.read_text() if note_path.exists() else None,
    )


def artifacts_mtime(root: str | Path | None = None) -> float:
    """Latest mtime across experiment files; a cache key for the dashboard."""
    latest = 0.0
    for folder in _folders(experiments_root(root)):
        for name in _FILES:
            path = folder / name
            if path.exists():
                latest = max(latest, path.stat().st_mtime)
    return latest
