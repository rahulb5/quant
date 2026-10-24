"""
src/experiments/trials.py

The trial ledger: every specification tried for a research idea ("family"), so the
number of trials behind a result comes from a table and not from memory. It feeds
the deflated Sharpe ratio (robustness.check_statistical).

Kinds and what counts (decision E4):
    experiment   a run_experiment() / run_robustness() base config             counts
    notebook     a variant logged from a research notebook via log_trial()     counts
    robustness   a param_grid variant from run_robustness()                    recorded, does not count

n_trials(family) is the number of distinct config hashes over experiment + notebook
rows, across all family versions unless one is given (selection among variants is
still selection after a definition is re-versioned).

Limitation: notebook variants are counted only if log_trial() is called for them.

Uses the shared `db` singleton: call db.open() first, same convention as registry.py.

Usage (in a notebook):
    from src.experiments.trials import log_trial, n_trials

    log_trial("growth_regime_001", {"window": 63, "threshold": 0.5}, net_returns)
    n_trials("growth_regime_001")
"""

from __future__ import annotations

import hashlib
import json
import subprocess
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from src.backtest.metrics import sharpe_ratio
from src.db.client import db

KINDS = ("experiment", "notebook", "robustness")
COUNTED_KINDS = ("experiment", "notebook")


def _canonical(obj: Any) -> Any:
    """Plain, order-independent JSON-safe form (numpy scalars and Paths included)."""
    if isinstance(obj, dict):
        return {str(k): _canonical(v) for k, v in sorted(obj.items(), key=lambda kv: str(kv[0]))}
    if isinstance(obj, (list, tuple)):
        return [_canonical(v) for v in obj]
    if isinstance(obj, np.generic):
        return _canonical(obj.item())
    if isinstance(obj, float) and obj == int(obj) and abs(obj) < 1e15:
        return int(obj)           # 90.0 and 90 are the same setting
    if isinstance(obj, (str, int, float, bool)) or obj is None:
        return obj
    return str(obj)


def config_hash(params: dict[str, Any]) -> str:
    """Stable 16-hex-digit hash of a parameter dict (key order and 90 vs 90.0 don't matter)."""
    blob = json.dumps(_canonical(params), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(blob.encode()).hexdigest()[:16]


def experiment_params(config) -> dict[str, Any]:
    """What defines one trial of an ExperimentConfig: data, signal, portfolio and
    backtest settings. Validation, robustness, status and other bookkeeping are excluded,
    so editing them doesn't create a new trial."""
    from dataclasses import asdict

    return _canonical({
        "data": asdict(config.data),
        "signal": asdict(config.signal),
        "portfolio": asdict(config.portfolio),
        "backtest": asdict(config.backtest),
    })


def _git_sha() -> str | None:
    try:
        out = subprocess.run(["git", "rev-parse", "HEAD"], cwd=Path(__file__).resolve().parents[2],
                             capture_output=True, text=True, check=True)
        return out.stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return None


def record_trial(
    family: str,
    params: dict[str, Any],
    sharpe: float | None,
    n_obs: int | None,
    kind: str = "notebook",
    experiment_id: str | None = None,
    family_version: str = "v1",
    git_sha: str | None = None,
) -> int:
    """Insert one ledger row from precomputed numbers. Returns the new row id."""
    if kind not in KINDS:
        raise ValueError(f"kind must be one of {KINDS}, got {kind!r}")
    if not family:
        raise ValueError("family is required")
    canon = _canonical(params)
    if sharpe is not None and not np.isfinite(sharpe):
        sharpe = None
    rows = db.query(
        """
        INSERT INTO trials (family, family_version, kind, experiment_id, config_hash, params, sharpe, n_obs, git_sha)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) RETURNING id
        """,
        [family, family_version, kind, experiment_id, config_hash(canon),
         json.dumps(canon, sort_keys=True), sharpe, n_obs, git_sha if git_sha is not None else _git_sha()],
    )
    return int(rows[0]["id"])


def log_trial(
    family: str,
    params: dict[str, Any],
    returns: pd.Series,
    kind: str = "notebook",
    experiment_id: str | None = None,
    family_version: str = "v1",
    periods_per_year: int = 252,
) -> int:
    """Log one tried specification with the Sharpe of its (net) `returns`.

    Call it once per variant you evaluate, including the ones you discard. `params` is
    whatever distinguishes this variant from the others (window, threshold, universe...);
    the same params logged twice count once.
    """
    r = pd.Series(returns).dropna()
    s = sharpe_ratio(r.to_numpy(dtype=float), periods_per_year=periods_per_year) if len(r) > 1 else None
    return record_trial(family, params, None if s is None else float(s), len(r), kind=kind,
                        experiment_id=experiment_id, family_version=family_version)


def n_trials(family: str, family_version: str | None = None) -> int:
    """Distinct configurations tried in `family` (experiment + notebook rows; E4)."""
    marks = ", ".join("?" for _ in COUNTED_KINDS)
    sql = f"SELECT COUNT(DISTINCT config_hash) AS n FROM trials WHERE family = ? AND kind IN ({marks})"
    args: list[Any] = [family, *COUNTED_KINDS]
    if family_version is not None:
        sql += " AND family_version = ?"
        args.append(family_version)
    return int(db.query(sql, args)[0]["n"])


def effective_n_trials(config) -> tuple[int, int, int]:
    """(used, declared, ledger) for an ExperimentConfig: used = max(declared, ledger), so a
    hand-set value can only raise the count. Ledger is 0 when the config has no family."""
    declared = int(config.validation.n_trials)
    ledger = n_trials(config.validation.family) if config.validation.family else 0
    return max(declared, ledger), declared, ledger


def trials_table(family: str) -> pd.DataFrame:
    """Every ledger row for a family, oldest first."""
    return db.query_df("SELECT * FROM trials WHERE family = ? ORDER BY id", [family])
