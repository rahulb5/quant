"""tests/dashboard/test_pages.py — AppTest smoke tests against a tmp experiments tree."""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest
from streamlit.testing.v1 import AppTest

from src.experiments.config import load_config, save_config

ROOT = Path(__file__).resolve().parents[2]
REAL = ROOT / "experiments" / "EXP-0002"
DASH = ROOT / "dashboard"


@pytest.fixture
def tree(tmp_path, monkeypatch):
    root = tmp_path / "experiments"
    shutil.copytree(REAL, root / "EXP-0002")
    shutil.copytree(REAL, root / "EXP-0003")
    (root / "EXP-0003" / "robustness.json").unlink()
    cfg = load_config(str(root / "EXP-0003" / "config.yaml"))
    cfg.experiment_id = "EXP-0003"
    save_config(cfg, str(root / "EXP-0003" / "config.yaml"))
    monkeypatch.setenv("QUANT_EXPERIMENTS_DIR", str(root))
    return root


def _run(script: str, **params) -> AppTest:
    at = AppTest.from_file(str(DASH / script), default_timeout=60)
    for k, v in params.items():
        at.query_params[k] = v
    return at.run()


def test_app_shell_renders(tree):
    at = AppTest.from_file(str(DASH / "app.py"), default_timeout=60).run()

    assert not at.exception


def test_experiments_list_renders(tree):
    at = _run("research/experiments.py")

    assert not at.exception
    assert len(at.dataframe) == 1
    assert list(at.dataframe[0].value["ID"]) == ["EXP-0002", "EXP-0003"]


def test_detail_renders_full_experiment(tree):
    at = _run("research/experiment.py", id="EXP-0002")

    assert not at.exception
    assert len(at.tabs) == 5


def test_detail_renders_without_robustness_json(tree):
    at = _run("research/experiment.py", id="EXP-0003")

    assert not at.exception
    assert any("Not run" in i.value for i in at.info)


def test_detail_unknown_or_missing_id_warns(tree):
    for params in ({}, {"id": "EXP-9999"}):
        at = _run("research/experiment.py", **params)
        assert not at.exception
        assert at.warning


# ── Macro explorer ───────────────────────────────────────────────────────────

from tests.macro.conftest import macro_db_path  # noqa: E402,F401  (fixture)


@pytest.fixture
def macro_db(macro_db_path, monkeypatch):
    monkeypatch.setenv("DB_PATH", macro_db_path)
    return macro_db_path


def test_macro_explorer_fred_renders(macro_db):
    at = _run("macro/explorer.py")

    assert not at.exception
    assert at.metric[0].value == "60 obs"  # 10y history → default 5y lookback at 12/yr
    at.sidebar.selectbox[1].select("yoy_pct").run()
    assert not at.exception
    assert len(at.dataframe) == 1


def test_macro_explorer_cot_lists_contract_keyed_by_group(macro_db):
    at = _run("macro/explorer.py")
    at.sidebar.radio[0].set_value("CFTC").run()

    assert not at.exception
    assert at.sidebar.selectbox[1].options == ["all", "energy"]  # asset group
    assert at.sidebar.selectbox[2].options == ["CFTC.DISAGG_FUT.067651 — CRUDE OIL"]  # exchange-keyed hidden
    at.sidebar.selectbox[1].select("energy").run()
    assert not at.exception
    assert len(at.sidebar.selectbox[2].options) == 1


def test_macro_explorer_missing_db_errors_cleanly(tmp_path, monkeypatch):
    monkeypatch.setenv("DB_PATH", str(tmp_path / "missing.db"))
    at = _run("macro/explorer.py")

    assert not at.exception
    assert at.error


def test_detail_renders_regime_table_and_state_exposure_from_new_diagnostics(tree):
    import json

    path = tree / "EXP-0003" / "diagnostics.json"
    d = json.loads(path.read_text())
    row = {"ann_return": 0.1, "ann_vol": 0.2, "sharpe": 0.5, "t_stat": 1.0, "hit_rate": 0.5,
           "pct_time": 1.0, "n_obs": 100.0, "n_episodes": 3.0, "mean_episode_length": 33.0}
    d["by_regime"] = {"policy": {"easing": row}, "volatility": {"low_vol": row}}
    d["regime_layers"] = {"policy": "macro", "volatility": "market"}
    d["state_exposure"] = {"const": {"coef": 0.0, "t_stat_hac": 0.1, "n_obs": 50, "r_squared": 0.0},
                           "G_level": {"coef": 0.1, "t_stat_hac": 2.0, "n_obs": 50, "r_squared": 0.0}}
    path.write_text(json.dumps(d))

    at = _run("research/experiment.py", id="EXP-0003")

    assert not at.exception
    frames = [df.value for df in at.dataframe]
    regime = next(f for f in frames if "n_episodes" in f.columns)
    assert [tuple(i) for i in regime.index] == [("macro", "policy", "easing"), ("market", "volatility", "low_vol")]
    assert any("t (Newey-West)" in f.columns and "G_level" in f.index for f in frames)
