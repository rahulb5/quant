"""
tests/experiments/test_runner.py

One happy-path integration test: a monkeypatched signal (so no real COT/price
fetch is needed) run through the real portfolio/universe/backtest/registry
wiring, against an in-memory DuckDB with just enough commodity price data to
build a returns panel. Asserts every artifact gets written and the registry
row ends up 'complete'.
"""

from __future__ import annotations

import pandas as pd
import pytest

import data.repository as repository_module
import src.experiments.registry as registry_module
import src.experiments.runner as runner_module
import src.experiments.universe as universe_module
from data.repository import Asset, Price, insert_asset, insert_price
from src.db.client import Database
from src.experiments.config import (
    BacktestConfig,
    DataConfig,
    ExperimentConfig,
    PortfolioConfig,
    SignalConfig,
    ValidationConfig,
    save_config,
)
from src.experiments.registry import ExperimentRecord, get_experiment, insert_experiment
from src.experiments.runner import run_experiment
from src.experiments.universe import CommodityUniverse


@pytest.fixture
def mem_db(monkeypatch) -> Database:
    database = Database(":memory:")
    database.open()
    monkeypatch.setattr(registry_module, "db", database)
    monkeypatch.setattr(universe_module, "db", database)
    monkeypatch.setattr(repository_module, "db", database)
    yield database
    database.close()


def _seed_commodity_prices(database):
    insert_asset(Asset(id=300001, ticker="CL1", name="Crude Oil", asset_class="futures"))
    insert_asset(Asset(id=300002, ticker="NG1", name="Natural Gas", asset_class="futures"))

    for i in range(90):
        ts = (pd.Timestamp("2024-01-01") + pd.Timedelta(days=i)).isoformat()
        for asset_id, base in [(300001, 10.0), (300002, 5.0)]:
            close = base * (1 + 0.001 * i)
            insert_price(Price(
                asset_id=asset_id, interval="1d", timestamp=ts,
                open=close, high=close, low=close, close=close, volume=0, source="test",
            ))


def test_run_experiment_writes_artifacts_and_completes(tmp_path, mem_db, monkeypatch):
    _seed_commodity_prices(mem_db)

    # Build a signal frame aligned to the same weekly dates the real
    # CommodityUniverse.returns_panel(resample="W-MON") will produce.
    weekly_dates = CommodityUniverse().returns_panel(resample="W-MON").index
    signal_rows = []
    for d in weekly_dates:
        signal_rows.append({"week_date": d, "commodity": "Crude Oil", "sig": 95})
        signal_rows.append({"week_date": d, "commodity": "Natural Gas", "sig": 10})
    signal_df = pd.DataFrame(signal_rows)

    monkeypatch.setattr(runner_module, "_build_signal", lambda config: signal_df)

    config = ExperimentConfig(
        experiment_id="EXP-0001",
        hypothesis="test hypothesis",
        researcher="tester",
        created_at="2026-10-04",
        data=DataConfig(asset_class="commodity", resample="W-MON"),
        signal=SignalConfig(module="unused.module", parameters={}),
        portfolio=PortfolioConfig(
            rule="equal_weight_long_threshold",
            date_column="week_date",
            asset_column="commodity",
            signal_column="sig",
            threshold=90,
        ),
        backtest=BacktestConfig(cost_bps=1.0, lag=1, periods_per_year=52),
        validation=ValidationConfig(),
    )

    experiment_dir = tmp_path / "EXP-0001"
    experiment_dir.mkdir()
    config_path = experiment_dir / "config.yaml"
    save_config(config, str(config_path))

    insert_experiment(ExperimentRecord(
        id="EXP-0001", hypothesis=config.hypothesis, signal_name="fake",
        asset_class="commodity", config_path=str(config_path), created_at=config.created_at,
    ))

    result = run_experiment(str(config_path))

    assert (experiment_dir / "performance.parquet").exists()
    assert (experiment_dir / "results.json").exists()
    assert (experiment_dir / "diagnostics.json").exists()
    assert (experiment_dir / "charts" / "equity_curve.png").exists()
    assert (experiment_dir / "charts" / "drawdown.png").exists()
    assert (experiment_dir / "research_note.md").exists()

    row = get_experiment("EXP-0001")
    assert row["status"] == "complete"
    assert row["sharpe"] is not None

    note = (experiment_dir / "research_note.md").read_text()
    assert "test hypothesis" in note


def test_run_experiment_marks_failed_on_exception(tmp_path, mem_db, monkeypatch):
    def boom(config):
        raise RuntimeError("synthetic failure")

    monkeypatch.setattr(runner_module, "_build_signal", boom)

    config = ExperimentConfig(
        experiment_id="EXP-0002",
        hypothesis="test hypothesis",
        created_at="2026-10-04",
        data=DataConfig(asset_class="commodity", resample="W-MON"),
        signal=SignalConfig(module="unused.module", parameters={}),
        portfolio=PortfolioConfig(
            rule="equal_weight_long_threshold",
            date_column="week_date",
            asset_column="commodity",
            signal_column="sig",
            threshold=90,
        ),
    )

    experiment_dir = tmp_path / "EXP-0002"
    experiment_dir.mkdir()
    config_path = experiment_dir / "config.yaml"
    save_config(config, str(config_path))

    insert_experiment(ExperimentRecord(
        id="EXP-0002", hypothesis=config.hypothesis, signal_name="fake",
        asset_class="commodity", config_path=str(config_path), created_at=config.created_at,
    ))

    with pytest.raises(RuntimeError):
        run_experiment(str(config_path))

    row = get_experiment("EXP-0002")
    assert row["status"] == "failed"
    assert "synthetic failure" in row["conclusion"]
