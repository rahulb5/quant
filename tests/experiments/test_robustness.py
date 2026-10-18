"""
tests/experiments/test_robustness.py

Unit tests for each check's verdict logic on synthetic series, plus one
integration test that runs the whole suite against an in-memory DB with a
monkeypatched signal (same approach as test_runner.py).
"""

from __future__ import annotations

import json

import numpy as np
import pandas as pd
import pytest

import data.repository as repository_module
import src.experiments.registry as registry_module
import src.experiments.universe as universe_module
from data.repository import Asset, Price, insert_asset, insert_price
from src.db.client import Database
from src.experiments import robustness as rb
from src.experiments import runner as runner_module
from src.experiments.config import (
    BacktestConfig,
    DataConfig,
    ExperimentConfig,
    PortfolioConfig,
    RobustnessConfig,
    SignalConfig,
    ValidationConfig,
    load_config,
    save_config,
)
from src.experiments.registry import ExperimentRecord, get_experiment, insert_experiment
from src.experiments.universe import CommodityUniverse

RC = RobustnessConfig()


def _series(mean, std=0.01, n=600, seed=0, start="2018-01-01") -> pd.Series:
    rng = np.random.default_rng(seed)
    return pd.Series(rng.normal(mean, std, n), index=pd.bdate_range(start, periods=n))


def _sharpe_positive(net: pd.Series) -> bool:
    return net.mean() > 0


# ── cost stress ───────────────────────────────────────────────────────────────

def test_cost_stress_passes_when_costs_are_negligible():
    gross = _series(0.002)
    turnover = pd.Series(0.01, index=gross.index)
    result = rb.check_cost_stress(gross, turnover, 1.0, 252, RC)
    assert result.verdict == rb.PASS
    assert result.detail["breakeven_bps"] > RC.breakeven_flag_bps


def test_cost_stress_kills_when_costs_erase_the_edge():
    gross = _series(0.0005)  # ~5 bps/day gross edge
    turnover = pd.Series(1.0, index=gross.index)
    result = rb.check_cost_stress(gross, turnover, 10.0, 252, RC)
    assert result.verdict == rb.KILL


def test_cost_stress_flags_low_breakeven():
    gross = _series(0.00005, std=0.0001)  # 0.5 bp/day edge
    turnover = pd.Series(0.2, index=gross.index)  # breakeven = 0.5bp / 0.2 = 2.5 bps < 5 bps
    result = rb.check_cost_stress(gross, turnover, 0.1, 252, RC)
    assert result.verdict == rb.FLAG
    assert result.detail["breakeven_bps"] == pytest.approx(gross.mean() / 0.2 * 1e4)


def test_cost_stress_skipped_when_base_cost_is_zero():
    gross = _series(0.001)
    result = rb.check_cost_stress(gross, pd.Series(0.1, index=gross.index), 0.0, 252, RC)
    assert result.verdict == rb.SKIPPED


# ── outliers / subperiods / statistical ──────────────────────────────────────

def test_outliers_flags_a_result_carried_by_a_few_big_winners():
    net = _series(-0.0005, std=0.002, n=500)
    net.iloc[[50, 150, 250, 350, 450]] = 0.3
    assert _sharpe_positive(net)
    assert rb.check_outliers(net, 252, RC).verdict == rb.FLAG


def test_outliers_does_not_flag_plain_gaussian_noise_with_a_modest_edge():
    # One-sided trimming alone would push this below zero; symmetric trimming must not.
    net = _series(0.0004, std=0.01, n=6000, seed=7)
    result = rb.check_outliers(net, 252, RC)
    assert result.detail["sharpe_excluding_top"] < result.detail["base_sharpe"] - 0.2
    assert result.verdict == rb.PASS


def test_outliers_passes_for_a_steady_edge():
    assert rb.check_outliers(_series(0.002), 252, RC).verdict == rb.PASS


def test_subperiods_flags_edge_that_exists_in_one_half_only():
    first = _series(0.003, n=400)
    second = _series(-0.003, n=400, seed=1, start="2020-01-01")
    result = rb.check_subperiods(pd.concat([first, second]), 252, RC)
    assert result.verdict == rb.FLAG
    assert result.detail["halves"]["second_half"] < RC.half_sharpe_flag


def test_subperiods_passes_for_a_stable_edge():
    assert rb.check_subperiods(_series(0.002, n=1000), 252, RC).verdict == rb.PASS


def test_subperiods_records_crisis_windows_with_enough_data():
    net = _series(0.001, n=1500, start="2007-01-01")
    result = rb.check_subperiods(net, 252, RC)
    assert "gfc_2008" in result.detail["crisis_windows"]


def test_statistical_flags_a_noise_strategy_with_many_trials():
    result = rb.check_statistical(_series(0.0001, n=400), 252, n_trials=200, rc=RC)
    assert result.verdict == rb.FLAG


def test_statistical_passes_for_a_strong_edge():
    result = rb.check_statistical(_series(0.003, n=1000), 252, n_trials=5, rc=RC)
    assert result.verdict == rb.PASS
    assert result.detail["ci_low"] < result.detail["sharpe"] < result.detail["ci_high"]


# ── overrides ─────────────────────────────────────────────────────────────────

def _config(**overrides) -> ExperimentConfig:
    base = dict(
        experiment_id="EXP-0001",
        hypothesis="test hypothesis",
        created_at="2026-10-04",
        data=DataConfig(asset_class="commodity", resample="W-MON"),
        signal=SignalConfig(module="unused.module", parameters={"window": 5}),
        portfolio=PortfolioConfig(
            rule="equal_weight_long_threshold", date_column="week_date",
            asset_column="commodity", signal_column="sig", threshold=90,
        ),
        backtest=BacktestConfig(cost_bps=1.0, lag=1, periods_per_year=52),
        validation=ValidationConfig(n_trials=4),
    )
    base.update(overrides)
    return ExperimentConfig(**base)


def test_with_overrides_sets_attributes_and_dict_keys_without_mutating_original():
    cfg = _config()
    new = rb._with_overrides(cfg, {"portfolio.threshold": 70, "signal.parameters.window": 9})
    assert new.portfolio.threshold == 70 and new.signal.parameters["window"] == 9
    assert cfg.portfolio.threshold == 90 and cfg.signal.parameters["window"] == 5


def test_with_overrides_rejects_unknown_path():
    with pytest.raises(AttributeError):
        rb._with_overrides(_config(), {"portfolio.thresold": 70})


# ── integration ───────────────────────────────────────────────────────────────

@pytest.fixture
def mem_db(monkeypatch) -> Database:
    database = Database(":memory:")
    database.open()
    monkeypatch.setattr(registry_module, "db", database)
    monkeypatch.setattr(universe_module, "db", database)
    monkeypatch.setattr(repository_module, "db", database)
    yield database
    database.close()


def _seed_prices():
    insert_asset(Asset(id=300001, ticker="CL1", name="Crude Oil", asset_class="futures"))
    insert_asset(Asset(id=300002, ticker="NG1", name="Natural Gas", asset_class="futures"))
    rng = np.random.default_rng(42)
    for asset_id, base in [(300001, 10.0), (300002, 5.0)]:
        closes = base * np.cumprod(1 + rng.normal(0.0006, 0.01, 1000))
        for i, close in enumerate(closes):
            insert_price(Price(
                asset_id=asset_id, interval="1d",
                timestamp=(pd.Timestamp("2021-01-01") + pd.Timedelta(days=i)).isoformat(),
                open=close, high=close, low=close, close=close, volume=0, source="test",
            ))


def _setup_experiment(tmp_path, monkeypatch, **config_overrides):
    _seed_prices()
    weekly = CommodityUniverse().returns_panel(resample="W-MON").index
    rows = []
    for i, d in enumerate(weekly):
        cl = 30 + (i * 7) % 70
        rows.append({"week_date": d, "commodity": "Crude Oil", "sig": cl})
        rows.append({"week_date": d, "commodity": "Natural Gas", "sig": 100 - cl})
    signal_df = pd.DataFrame(rows)

    monkeypatch.setattr(runner_module, "_load_signal_data", lambda config: ())
    monkeypatch.setattr(runner_module, "_compute_signal", lambda config, data: signal_df)

    config = _config(**config_overrides)
    experiment_dir = tmp_path / "EXP-0001"
    experiment_dir.mkdir()
    config_path = experiment_dir / "config.yaml"
    save_config(config, str(config_path))
    (experiment_dir / "research_note.md").write_text("# note\n\n## Results\n\nkeep me\n\n## Robustness\n\n_TODO_\n\n## Failure Modes\n\nkeep me too\n")
    insert_experiment(ExperimentRecord(
        id="EXP-0001", hypothesis="h", signal_name="fake", asset_class="commodity",
        config_path=str(config_path), created_at="2026-10-04",
    ))
    return config_path, experiment_dir


def test_run_robustness_writes_artifacts_registry_and_note(tmp_path, mem_db, monkeypatch):
    rc = RobustnessConfig(param_grid={"portfolio.threshold": [50, 70, 90]}, n_boot=100)
    config_path, experiment_dir = _setup_experiment(tmp_path, monkeypatch, robustness=rc)

    result = rb.run_robustness(str(config_path))

    saved = json.loads((experiment_dir / "robustness.json").read_text())
    assert set(saved["checks"]) == {
        "cost_stress", "lag_sensitivity", "outliers", "subperiods",
        "parameters", "statistical", "holdout",
    }
    assert saved["overall"] in {"KILLED", "NEEDS_REVIEW"}
    assert saved["checks"]["holdout"]["verdict"] == rb.SKIPPED
    assert len(saved["checks"]["parameters"]["detail"]["grid"]) == 3
    assert saved["n_trials"] == 4
    assert result.results["overall"] == saved["overall"]

    assert (experiment_dir / "charts" / "robustness_cost.png").exists()
    assert (experiment_dir / "charts" / "robustness_params.png").exists()

    note = (experiment_dir / "research_note.md").read_text()
    assert saved["overall"] in note and "| cost_stress |" in note
    assert "keep me\n" in note and "keep me too" in note and "_TODO_" not in note

    row = get_experiment("EXP-0001")
    assert row["robustness_verdict"] == saved["overall"]
    assert row["deflated_sharpe"] == saved["checks"]["statistical"]["detail"]["deflated_sharpe"]


def test_holdout_is_hidden_until_unlocked_then_frozen(tmp_path, mem_db, monkeypatch):
    config_path, experiment_dir = _setup_experiment(
        tmp_path, monkeypatch,
        validation=ValidationConfig(n_trials=1, holdout_start="2023-06-01"),
        robustness=RobustnessConfig(n_boot=50),
    )

    locked = rb.run_robustness(str(config_path)).results
    assert locked["checks"]["holdout"]["verdict"] == rb.SKIPPED
    assert not (experiment_dir / "holdout.json").exists()
    assert pd.Timestamp(locked["in_sample_end"]) < pd.Timestamp("2023-06-01")

    unlocked = rb.run_robustness(str(config_path), unlock_holdout=True).results
    assert unlocked["checks"]["holdout"]["verdict"] in {rb.PASS, rb.KILL}
    frozen = json.loads((experiment_dir / "holdout.json").read_text())

    again = rb.run_robustness(str(config_path), unlock_holdout=True).results
    assert json.loads((experiment_dir / "holdout.json").read_text()) == frozen
    assert again["checks"]["holdout"]["detail"]["unlocked_at"] == frozen["unlocked_at"]


def test_runner_apply_holdout_truncates_both_panels():
    idx = pd.date_range("2024-01-01", periods=10)
    w = pd.DataFrame({"a": 1.0}, index=idx)
    r = pd.DataFrame({"a": 0.01}, index=idx)
    w2, r2 = runner_module._apply_holdout(w, r, "2024-01-08")
    assert w2.index.max() == pd.Timestamp("2024-01-07") and r2.index.max() == pd.Timestamp("2024-01-07")
    w3, r3 = runner_module._apply_holdout(w, r, None)
    assert len(w3) == 10 and len(r3) == 10
