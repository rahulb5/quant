"""tests/experiments/test_registry.py"""

from __future__ import annotations

import pytest

import src.experiments.registry as registry_module
from src.db.client import Database
from src.experiments.registry import (
    ExperimentRecord,
    get_experiment,
    insert_experiment,
    list_experiments,
    next_experiment_id,
    update_experiment,
)


@pytest.fixture
def mem_db(monkeypatch) -> Database:
    database = Database(":memory:")
    database.open()
    monkeypatch.setattr(registry_module, "db", database)
    yield database
    database.close()


def _record(id="EXP-0001", status="draft") -> ExperimentRecord:
    return ExperimentRecord(
        id=id,
        hypothesis="test hypothesis",
        signal_name="cot_commercial_positioning",
        asset_class="commodity",
        config_path=f"experiments/{id}/config.yaml",
        created_at="2026-10-04",
        status=status,
    )


def test_next_experiment_id_starts_at_0001(mem_db):
    assert next_experiment_id() == "EXP-0001"


def test_next_experiment_id_increments(mem_db):
    insert_experiment(_record("EXP-0001"))
    insert_experiment(_record("EXP-0003"))
    assert next_experiment_id() == "EXP-0004"


def test_insert_and_get_experiment(mem_db):
    insert_experiment(_record())
    row = get_experiment("EXP-0001")
    assert row["hypothesis"] == "test hypothesis"
    assert row["status"] == "draft"


def test_get_experiment_missing_returns_none(mem_db):
    assert get_experiment("EXP-9999") is None


def test_update_experiment_sets_fields(mem_db):
    insert_experiment(_record())
    update_experiment("EXP-0001", status="complete", sharpe=1.23, max_drawdown=-0.1)

    row = get_experiment("EXP-0001")
    assert row["status"] == "complete"
    assert row["sharpe"] == 1.23
    assert row["max_drawdown"] == -0.1


def test_update_experiment_rejects_unknown_field(mem_db):
    insert_experiment(_record())
    with pytest.raises(ValueError):
        update_experiment("EXP-0001", not_a_real_field=1)


def test_list_experiments_filters_by_status(mem_db):
    insert_experiment(_record("EXP-0001", status="draft"))
    insert_experiment(_record("EXP-0002", status="complete"))

    complete = list_experiments(status="complete")
    assert [r["id"] for r in complete] == ["EXP-0002"]

    all_rows = list_experiments()
    assert len(all_rows) == 2
