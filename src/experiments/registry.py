"""
src/experiments/registry.py

Typed DB CRUD for the `experiments` table (migration version 8) — the
queryable half of the hybrid registry (see src/experiments/runner.py for
the filesystem half: config.yaml, results.json, etc. under experiments/).

Uses the shared `db` singleton — call `db.open()` before use, same
convention as data/repository.py.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

from src.db.client import db

Status = Literal["draft", "running", "complete", "failed"]

_UPDATABLE_FIELDS = {
    "status",
    "sharpe",
    "cagr",
    "max_drawdown",
    "ann_turnover",
    "conclusion",
}


@dataclass
class ExperimentRecord:
    id: str
    hypothesis: str
    signal_name: str
    asset_class: str
    config_path: str
    created_at: str              # DATE string, e.g. "2026-10-04"
    researcher: str | None = None
    status: Status = "draft"
    sharpe: float | None = None
    cagr: float | None = None
    max_drawdown: float | None = None
    ann_turnover: float | None = None
    conclusion: str | None = None


def next_experiment_id() -> str:
    """Allocate the next EXP-000N id based on the highest existing one."""
    rows = db.query("SELECT id FROM experiments")
    nums = [int(r["id"].split("-")[1]) for r in rows]
    return f"EXP-{max(nums, default=0) + 1:04d}"


def insert_experiment(record: ExperimentRecord) -> int:
    """Insert a new experiment row. Returns rows changed."""
    return db.run(
        """
        INSERT INTO experiments
          (id, hypothesis, researcher, created_at, signal_name, asset_class,
           config_path, status, sharpe, cagr, max_drawdown, ann_turnover, conclusion)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            record.id, record.hypothesis, record.researcher, record.created_at,
            record.signal_name, record.asset_class, record.config_path,
            record.status, record.sharpe, record.cagr, record.max_drawdown,
            record.ann_turnover, record.conclusion,
        ],
    )


def update_experiment(experiment_id: str, **fields: Any) -> int:
    """Update one or more fields on an existing experiment row.

    Accepts any of: status, sharpe, cagr, max_drawdown, ann_turnover,
    conclusion. Always bumps updated_at. Returns rows changed.

    Raises:
        ValueError: If an unknown field is passed.
    """
    unknown = set(fields) - _UPDATABLE_FIELDS
    if unknown:
        raise ValueError(f"Unknown experiment field(s): {sorted(unknown)}")
    if not fields:
        return 0

    set_clause = ", ".join(f"{k} = ?" for k in fields)
    return db.run(
        f"UPDATE experiments SET {set_clause}, updated_at = now() WHERE id = ?",
        [*fields.values(), experiment_id],
    )


def get_experiment(experiment_id: str) -> dict[str, Any] | None:
    """Fetch one experiment row, or None if it doesn't exist."""
    rows = db.query("SELECT * FROM experiments WHERE id = ?", [experiment_id])
    return rows[0] if rows else None


def list_experiments(status: Status | None = None) -> list[dict[str, Any]]:
    """List experiments, optionally filtered by status, newest first."""
    if status is not None:
        return db.query(
            "SELECT * FROM experiments WHERE status = ? ORDER BY created_at DESC", [status]
        )
    return db.query("SELECT * FROM experiments ORDER BY created_at DESC")
