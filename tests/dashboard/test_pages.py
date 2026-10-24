"""tests/dashboard/test_pages.py — AppTest smoke tests against a tmp experiments tree."""

from __future__ import annotations

import shutil
from pathlib import Path

import pandas as pd
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


# ── Macro state page ─────────────────────────────────────────────────────────

def _macro_bundle() -> dict:
    import numpy as np

    from dashboard.components import MACRO_REGIME_LABELS
    from src.macro.analysis import STATE_VECTOR, pricing_divergence
    from src.macro.market import POSITIONING

    rng = np.random.default_rng(0)
    idx = pd.bdate_range("2005-01-03", "2025-12-31")
    n = len(idx)

    def noise(*shape):
        return pd.DataFrame(rng.standard_normal(shape)).rolling(20, min_periods=1).mean().to_numpy()

    state = pd.DataFrame(noise(n, len(STATE_VECTOR)), index=idx, columns=STATE_VECTOR)
    for c in ("G", "pi", "L", "P", "FC", "CS"):
        state[f"{c}_staleness"] = 5.0
    state["sahm_triggered"], state["labor_deteriorating"], state["curve_inverted"] = False, True, False
    market = pd.DataFrame({k: rng.standard_normal(n) for k in
                           ["growth_pricing", "inflation_pricing", "policy_pricing", *POSITIONING]}, index=idx)
    real = pd.Series(noise(n, 1)[:, 0] * 0.1, index=idx)
    breakeven = pd.Series(noise(n, 1)[:, 0] * 0.1, index=idx)
    rates = pd.DataFrame({"nominal": 4.0 + real, "real": 2.0 + real, "breakeven": 2.0 + breakeven,
                          "d_real": real, "d_breakeven": breakeven, "d_nominal": real + breakeven,
                          "share_real": 0.6, "term_premium": 0.5 + real}, index=idx)
    regimes = {}
    for name, labels in MACRO_REGIME_LABELS.items():
        regimes[name] = pd.Series([labels[(i // 150) % len(labels)] for i in range(n)], index=idx, dtype=object)
    assets = pd.DataFrame({"sp500": 0.01 * rng.standard_normal(n), "gold": 0.01 * rng.standard_normal(n)}, index=idx)
    rates["d_term_premium"] = 0.05
    latest = {k: idx[-3] for k in ["DGS10", "THREEFYTP10", *POSITIONING]}
    return {"as_of": idx[-1], "state": state, "market": market, "rates": rates, "regimes": regimes,
            "divergence": pricing_divergence(state, market), "assets": assets, "latest_input": latest}


def test_macro_state_page_renders_every_panel_and_responds_to_controls(monkeypatch):
    bundle = _macro_bundle()
    monkeypatch.setattr("dashboard.components.load_macro_state", lambda: bundle)
    at = _run("macro/state.py")

    assert not at.exception
    assert [s.value for s in at.subheader] == [
        "State vector", "What is driving the 10Y yield?", "Has the market priced what the data shows?",
        "Is positioning crowded?", "Which regime are we in?", "How have assets responded?"]
    assert [m.label for m in at.metric[:6]] == ["Growth", "Inflation", "Labor", "Policy", "Financial conditions", "Credit supply"]
    flags = {m.value.split(":")[0].strip("●○ "): m.value for m in at.markdown if "Sahm rule" in m.value
             or "Labor deteriorating" in m.value or "Curve inverted" in m.value}
    assert ":red-badge[on]" in flags["Labor deteriorating"]                    # set True in the bundle
    assert ":gray-badge[off]" in flags["Sahm rule"] and ":gray-badge[off]" in flags["Curve inverted (10Y-3M)"]
    newest_dgs10 = f"{bundle['latest_input']['DGS10']:%Y-%m-%d}"
    assert any(f"Newest 10Y yield observation: {newest_dgs10}" in c.value for c in at.caption)
    at.sidebar.selectbox[0].select("All history").run()                    # history window
    at.sidebar.selectbox[1].select("gold").run()                            # asset
    at.radio[0].set_value("policy").run()                                   # divergence dimension
    at.radio[1].set_value(252).run()                                        # forward horizon
    next(b for b in at.selectbox if b.label == "Regime").select("policy").run()   # regime for the asset table
    assert not at.exception
    assert next(b for b in at.selectbox if b.label == "Regime").value == "policy"


def test_macro_state_page_without_data_says_so(monkeypatch):
    monkeypatch.setattr("dashboard.components.load_macro_state", lambda: None)
    at = _run("macro/state.py")

    assert not at.exception
    assert any("No macro state data" in i.value for i in at.info)


def test_macro_state_page_reports_a_loader_failure(monkeypatch):
    def boom():
        raise RuntimeError("database is locked")

    monkeypatch.setattr("dashboard.components.load_macro_state", boom)
    at = _run("macro/state.py")

    assert not at.exception
    assert any("database is locked" in e.value for e in at.error)


def test_state_cards_use_latest_valid_values_and_z_the_non_z_levels():
    import numpy as np

    from dashboard.components import state_cards

    idx = pd.bdate_range("2000-01-03", periods=1200)
    panel = pd.DataFrame({"G_level": np.linspace(-1, 1, 1200), "G_impulse": 0.5, "G_staleness": 7.0,
                          "pi_level": np.linspace(0, 3, 1200), "pi_impulse": -0.1, "pi_staleness": 9.0}, index=idx)
    panel.loc[idx[-5:], "G_level"] = np.nan                                   # newest rows missing
    cards = {c["title"]: c for c in state_cards(panel)}

    assert set(cards) == {"Growth", "Inflation"}                              # others have no columns
    assert cards["Growth"]["level"] == pytest.approx(panel["G_level"].dropna().iloc[-1])
    assert cards["Growth"]["z"] == cards["Growth"]["level"]                   # already a z-score
    assert cards["Growth"]["as_of"] == idx[-6] and cards["Growth"]["staleness"] == 7.0
    assert cards["Inflation"]["z"] > 1.0                                      # a rising pp level is high against its history
    assert cards["Inflation"]["impulse"] == pytest.approx(-0.1) and cards["Inflation"]["impulse_unit"] == "pp"


def test_asset_betas_recover_a_planted_slope_with_an_interval():
    import numpy as np

    from dashboard.components import asset_betas
    from src.macro.analysis import STATE_VECTOR

    rng = np.random.default_rng(1)
    idx = pd.bdate_range("2000-01-03", periods=3000)
    state = pd.DataFrame(rng.standard_normal((3000, len(STATE_VECTOR))), index=idx, columns=STATE_VECTOR) * 3
    returns = pd.Series(0.0005 * state["G_impulse"].shift(1).fillna(0) / 3 + 0.002 * rng.standard_normal(3000), index=idx)
    betas = asset_betas(state, returns, 1).set_index("variable")

    sd = state["G_impulse"].std()
    assert betas.loc["G_impulse", "beta_per_sd"] == pytest.approx(0.0005 * sd / 3, rel=0.15)
    assert betas.loc["G_impulse", "ci_low"] < betas.loc["G_impulse", "beta_per_sd"] < betas.loc["G_impulse", "ci_high"]
    assert betas.loc["G_impulse", "t_stat_hac"] > 5 and abs(betas.loc["pi_level", "t_stat_hac"]) < 4
    assert asset_betas(state.iloc[:10], returns.iloc[:10], 1).empty


def test_macro_state_page_runs_on_the_real_loader_against_a_sparse_database(macro_db):
    """The fixture DB has one CPI series and COT rows: no state, rates or asset prices. The real loader
    must still return a bundle (or None) and the page must render without an exception."""
    import dashboard.components as components
    from dashboard.components import macro_state_bundle

    bundle = macro_state_bundle()
    if bundle is not None:
        assert set(bundle) == {"as_of", "state", "market", "rates", "regimes", "divergence", "assets", "latest_input"}
        assert len(bundle["regimes"]) == 6 and bundle["assets"].empty
    components._cached_macro_state.clear()
    at = _run("macro/state.py")

    assert not at.exception
    assert at.info or at.subheader                                            # "no data" notice, or the panels


def test_asset_betas_t_stats_do_not_depend_on_the_state_scaling():
    import numpy as np

    from dashboard.components import asset_betas
    from src.macro.analysis import STATE_VECTOR

    rng = np.random.default_rng(2)
    idx = pd.bdate_range("2000-01-03", periods=2000)
    state = pd.DataFrame(rng.standard_normal((2000, len(STATE_VECTOR))), index=idx, columns=STATE_VECTOR)
    returns = pd.Series(0.002 * rng.standard_normal(2000), index=idx)
    a = asset_betas(state, returns, 21).set_index("variable")
    b = asset_betas(state * 7.0, returns, 21).set_index("variable")

    pd.testing.assert_series_equal(a["t_stat_hac"], b["t_stat_hac"])
    pd.testing.assert_series_equal(a["beta_per_sd"], b["beta_per_sd"])        # per s.d., so scale-free too
    se = (a["ci_high"] - a["ci_low"]) / (2 * 1.96)
    assert (se * a["t_stat_hac"].abs()).round(8).equals(a["beta_per_sd"].abs().round(8))   # CI width is 1.96 se
