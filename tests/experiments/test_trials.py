"""tests/experiments/test_trials.py"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

import src.experiments.trials as trials
from src.db.client import Database
from src.experiments.config import (
    BacktestConfig, DataConfig, ExperimentConfig, PortfolioConfig, SignalConfig, ValidationConfig,
)


@pytest.fixture
def mem_db(monkeypatch) -> Database:
    database = Database(":memory:")
    database.open()
    monkeypatch.setattr(trials, "db", database)
    yield database
    database.close()


def _returns(n=300, seed=0):
    return pd.Series(np.random.default_rng(seed).normal(0.001, 0.01, n))


def _config(**validation) -> ExperimentConfig:
    return ExperimentConfig(
        experiment_id="EXP-0001", hypothesis="h",
        data=DataConfig(asset_class="commodity", resample="W-MON"),
        signal=SignalConfig(module="m", parameters={"window": 5}),
        portfolio=PortfolioConfig(rule="r", date_column="d", asset_column="a", signal_column="s", threshold=90),
        backtest=BacktestConfig(),
        validation=ValidationConfig(**validation),
    )


# ── schema ────────────────────────────────────────────────────────────────────

def test_migration_creates_trials_table_with_expected_columns(mem_db):
    cols = {r["column_name"] for r in mem_db.query("DESCRIBE trials")}
    assert cols == {"id", "family", "family_version", "kind", "experiment_id", "config_hash",
                    "params", "sharpe", "n_obs", "git_sha", "created_at"}


def test_kind_is_constrained(mem_db):
    with pytest.raises(Exception):
        mem_db.run("INSERT INTO trials (family, kind, config_hash, params) VALUES ('f', 'bogus', 'h', '{}')")
    with pytest.raises(ValueError):
        trials.record_trial("f", {"a": 1}, 1.0, 10, kind="bogus")


# ── hashing ───────────────────────────────────────────────────────────────────

def test_config_hash_ignores_key_order_numpy_types_and_int_float():
    a = trials.config_hash({"window": 63, "threshold": 90.0, "nested": {"x": 1, "y": [1, 2]}})
    b = trials.config_hash({"nested": {"y": [1, 2], "x": np.int64(1)}, "threshold": np.float64(90), "window": 63})
    assert a == b and len(a) == 16
    assert trials.config_hash({"window": 64}) != trials.config_hash({"window": 63})
    assert trials.config_hash({"t": 0.5}) != trials.config_hash({"t": 0})


def test_experiment_params_excludes_bookkeeping():
    base = trials.experiment_params(_config(n_trials=1))
    assert trials.config_hash(base) == trials.config_hash(trials.experiment_params(_config(n_trials=500, family="f")))
    cfg = _config()
    cfg.portfolio.threshold = 85
    assert trials.config_hash(trials.experiment_params(cfg)) != trials.config_hash(base)
    cfg2 = _config()
    cfg2.hypothesis = "something else"
    cfg2.status = "complete"
    assert trials.config_hash(trials.experiment_params(cfg2)) == trials.config_hash(base)


# ── logging and counting ──────────────────────────────────────────────────────

def test_log_trial_stores_sharpe_and_n_obs(mem_db):
    row_id = trials.log_trial("fam_a", {"w": 63}, _returns(), periods_per_year=252)
    row = trials.trials_table("fam_a").iloc[0]
    assert row["id"] == row_id and row["kind"] == "notebook" and row["family_version"] == "v1"
    assert row["n_obs"] == 300 and row["sharpe"] == pytest.approx(
        _returns().mean() / _returns().std() * np.sqrt(252), rel=0.01)
    assert row["git_sha"]


def test_log_trial_handles_degenerate_returns(mem_db):
    trials.log_trial("fam_a", {"w": 1}, pd.Series([0.0] * 50))
    trials.log_trial("fam_a", {"w": 2}, pd.Series([0.01]))
    t = trials.trials_table("fam_a")
    assert t["sharpe"].isna().all() and list(t["n_obs"]) == [50, 1]


def test_n_trials_counts_distinct_configs_of_experiment_and_notebook_only(mem_db):
    trials.log_trial("fam_a", {"w": 1}, _returns())
    trials.log_trial("fam_a", {"w": 1}, _returns())                      # same params: counts once
    trials.log_trial("fam_a", {"w": 2}, _returns(seed=1), kind="experiment", experiment_id="EXP-1")
    trials.record_trial("fam_a", {"w": 3}, 0.5, 100, kind="robustness")  # recorded, not counted
    trials.log_trial("fam_b", {"w": 9}, _returns())
    assert trials.n_trials("fam_a") == 2
    assert trials.n_trials("fam_b") == 1 and trials.n_trials("missing") == 0
    assert len(trials.trials_table("fam_a")) == 4


def test_robustness_row_with_same_hash_as_experiment_does_not_double_count(mem_db):
    trials.log_trial("fam_a", {"w": 1}, _returns(), kind="experiment")
    trials.record_trial("fam_a", {"w": 1}, 0.4, 100, kind="robustness")
    assert trials.n_trials("fam_a") == 1


def test_n_trials_family_version_filter_and_default_spans_versions(mem_db):
    trials.log_trial("fam_a", {"w": 1}, _returns(), family_version="v1")
    trials.log_trial("fam_a", {"w": 2}, _returns(), family_version="v2")
    assert trials.n_trials("fam_a") == 2
    assert trials.n_trials("fam_a", "v1") == 1 and trials.n_trials("fam_a", "v2") == 1


def test_effective_n_trials_declared_can_only_raise(mem_db):
    cfg = _config(family="fam_a", n_trials=1)
    for w in range(5):
        trials.log_trial("fam_a", {"w": w}, _returns())
    assert trials.effective_n_trials(cfg) == (5, 1, 5)
    assert trials.effective_n_trials(_config(family="fam_a", n_trials=96)) == (96, 96, 5)
    assert trials.effective_n_trials(_config(n_trials=7)) == (7, 7, 0)       # no family: declared only


# ── config validation ─────────────────────────────────────────────────────────

@pytest.mark.parametrize("bad", ["Growth", "1abc", "a-b", "a b", ""])
def test_family_must_be_snake_case(bad):
    with pytest.raises(ValueError, match="snake_case"):
        ValidationConfig(family=bad)


def test_family_optional_and_round_trips(tmp_path):
    from src.experiments.config import load_config, save_config
    assert ValidationConfig().family is None and ValidationConfig().family_version == "v1"
    cfg = _config(family="growth_regime_001", family_version="v2")
    path = tmp_path / "config.yaml"
    save_config(cfg, str(path))
    assert load_config(str(path)).validation == cfg.validation
