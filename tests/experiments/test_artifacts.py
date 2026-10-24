"""tests/experiments/test_artifacts.py"""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from src.experiments.artifacts import (
    artifacts_mtime,
    list_experiments,
    load_experiment,
)
from src.experiments.config import load_config, save_config

REAL = Path(__file__).resolve().parents[2] / "experiments" / "EXP-0002"


@pytest.fixture
def tree(tmp_path):
    root = tmp_path / "experiments"
    shutil.copytree(REAL, root / "EXP-0002")
    draft = root / "EXP-0009"
    draft.mkdir()
    cfg = load_config(str(REAL / "config.yaml"))
    cfg.experiment_id, cfg.status, cfg.classification = "EXP-0009", "draft", None
    save_config(cfg, str(draft / "config.yaml"))
    (root / "not_an_experiment").mkdir()
    return root


def test_lists_full_and_bare_experiments(tree):
    summaries = list_experiments(tree)

    assert [s.experiment_id for s in summaries] == ["EXP-0002", "EXP-0009"]
    full, bare = summaries
    assert full.sharpe is not None and full.ann_turnover is not None
    assert full.robustness_verdict == "NEEDS_REVIEW"
    assert full.sharpe_2x_cost is not None and full.breakeven_bps is not None
    assert full.deflated_sharpe is not None and full.sharpe_ci_low is not None
    assert full.classification.universe == "sp500_current"
    assert bare.sharpe is None and bare.robustness_verdict is None
    assert bare.classification is None


def test_load_bare_experiment_has_none_artifacts(tree):
    art = load_experiment("EXP-0009", tree)

    assert art.results is None and art.diagnostics is None and art.robustness is None
    assert art.performance is None and art.note is None


def test_load_full_experiment(tree):
    art = load_experiment("EXP-0002", tree)

    assert {"net_returns", "gross_returns"} <= set(art.performance.columns)
    assert art.diagnostics is not None and art.note


def test_missing_experiment_raises(tree):
    with pytest.raises(FileNotFoundError):
        load_experiment("EXP-0404", tree)


def test_holdout_state_cases(tree):
    folder = tree / "EXP-0002"
    assert list_experiments(tree)[0].holdout_state == "none"

    cfg = load_config(str(folder / "config.yaml"))
    cfg.validation.holdout_start = "2025-10-01"
    save_config(cfg, str(folder / "config.yaml"))
    assert list_experiments(tree)[0].holdout_state == "locked"

    (folder / "holdout.json").write_text("{}")
    assert list_experiments(tree)[0].holdout_state == "scored"



def test_env_var_override(tree, monkeypatch):
    monkeypatch.setenv("QUANT_EXPERIMENTS_DIR", str(tree))

    assert len(list_experiments()) == 2
    assert load_experiment("EXP-0002").config.experiment_id == "EXP-0002"


def test_artifacts_mtime_tracks_changes(tree):
    before = artifacts_mtime(tree)
    (tree / "EXP-0002" / "holdout.json").write_text("{}")

    assert artifacts_mtime(tree) >= before > 0
    assert artifacts_mtime(tree / "missing") == 0.0


def test_regime_table_groups_by_layer_and_falls_back_for_old_files():
    from src.experiments.artifacts import regime_table

    row = {"ann_return": 0.1, "sharpe": 1.0, "n_obs": 100.0, "n_episodes": 4.0}
    diag = {
        "by_regime": {"volatility": {"low_vol": row, "high_vol": row}, "policy": {"easing": row}},
        "regime_layers": {"volatility": "market", "policy": "macro"},
    }
    t = regime_table(diag)
    assert list(t.index.names) == ["layer", "regime", "label"]
    assert [i[:2] for i in t.index] == [("macro", "policy"), ("market", "volatility"), ("market", "volatility")]
    assert t.loc[("market", "volatility", "low_vol"), "n_episodes"] == 4.0
    old = regime_table({"by_regime": diag["by_regime"]})                # pre-C9 diagnostics.json
    assert set(old.index.get_level_values("layer")) == {"unassigned"} and len(old) == 3
    assert regime_table({}).empty and regime_table({"by_regime": {}}).empty
