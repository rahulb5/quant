"""tests/experiments/test_config.py"""

from __future__ import annotations

import pytest

from src.experiments.config import (
    BacktestConfig,
    ClassificationConfig,
    DataConfig,
    DiagnosticsConfig,
    ExperimentConfig,
    PortfolioConfig,
    SignalConfig,
    ValidationConfig,
    load_config,
    save_config,
)


def _sample_config() -> ExperimentConfig:
    return ExperimentConfig(
        experiment_id="EXP-0001",
        hypothesis="Extreme commercial positioning predicts forward returns.",
        researcher="rhlbh",
        created_at="2026-10-04",
        data=DataConfig(asset_class="commodity", resample="W-MON"),
        signal=SignalConfig(
            module="src.signals.cot_commercial_positioning",
            parameters={"windows": [26, 52], "horizons": [4, 8]},
        ),
        portfolio=PortfolioConfig(
            rule="equal_weight_long_threshold",
            date_column="week_date",
            asset_column="commodity",
            signal_column="raw_pctile_52w",
            threshold=90,
            trend_filter="trend_none",
        ),
        backtest=BacktestConfig(cost_bps=1.0, lag=1, periods_per_year=52),
        validation=ValidationConfig(methodology="Full-sample backtest"),
        status="draft",
    )


def test_round_trip(tmp_path):
    config = _sample_config()
    path = tmp_path / "config.yaml"
    save_config(config, str(path))

    loaded = load_config(str(path))

    assert loaded == config


def test_load_applies_defaults(tmp_path):
    path = tmp_path / "config.yaml"
    path.write_text(
        """
experiment_id: EXP-0002
hypothesis: "test"
data: {asset_class: commodity}
signal: {module: some.module}
portfolio: {rule: r, date_column: d, asset_column: a, signal_column: s, threshold: 1}
"""
    )

    loaded = load_config(str(path))

    assert loaded.backtest.periods_per_year == 252
    assert loaded.status == "draft"
    assert loaded.signal.parameters == {}
    assert loaded.portfolio.trend_filter == "trend_none"


def test_round_trip_preserves_robustness_and_validation_fields(tmp_path):
    config = _sample_config()
    config.validation = ValidationConfig(holdout_start="2025-10-01", n_trials=96)
    config.robustness.param_grid = {"portfolio.threshold": [85, 90, 95]}
    config.robustness.cost_multipliers = [1.0, 2.0]
    path = tmp_path / "config.yaml"
    save_config(config, str(path))

    assert load_config(str(path)) == config


def test_old_config_without_robustness_block_gets_defaults(tmp_path):
    path = tmp_path / "config.yaml"
    path.write_text(
        """
experiment_id: EXP-0003
hypothesis: "test"
data: {asset_class: commodity}
signal: {module: some.module}
portfolio: {rule: r, date_column: d, asset_column: a, signal_column: s}
"""
    )
    loaded = load_config(str(path))

    assert loaded.validation.holdout_start is None
    assert loaded.validation.n_trials == 1
    assert loaded.robustness.cost_kill_multiplier == 1.5
    assert loaded.robustness.param_grid == {}


def _classification(**overrides) -> ClassificationConfig:
    fields = dict(
        asset_class="commodities", universe="cot_11_futures", signal_family="positioning",
        frequency="weekly", direction="long_only",
    )
    return ClassificationConfig(**{**fields, **overrides})


def test_round_trip_with_classification(tmp_path):
    config = _sample_config()
    config.classification = _classification()
    path = tmp_path / "config.yaml"
    save_config(config, str(path))

    assert load_config(str(path)) == config


def test_round_trip_without_classification(tmp_path):
    path = tmp_path / "config.yaml"
    save_config(_sample_config(), str(path))

    assert load_config(str(path)).classification is None


@pytest.mark.parametrize("field", ["asset_class", "signal_family", "frequency", "direction"])
def test_invalid_classification_value_raises(field):
    with pytest.raises(ValueError, match=f"classification.{field}.*allowed"):
        _classification(**{field: "bogus"})


def test_diagnostics_defaults_to_every_registered_regime_and_roundtrips(tmp_path):
    from src.macro.regimes import REGIME_REGISTRY

    cfg = _sample_config()
    assert cfg.diagnostics.regimes == list(REGIME_REGISTRY) and cfg.diagnostics.state_exposure is False
    cfg.diagnostics = DiagnosticsConfig(regimes=["policy", "volatility"], state_exposure=True)
    save_config(cfg, str(tmp_path / "config.yaml"))
    loaded = load_config(str(tmp_path / "config.yaml"))
    assert loaded.diagnostics == cfg.diagnostics


def test_diagnostics_rejects_unknown_regime_and_old_configs_load_with_defaults(tmp_path):
    from src.macro.regimes import REGIME_REGISTRY

    with pytest.raises(ValueError, match="not_a_regime"):
        DiagnosticsConfig(regimes=["policy", "not_a_regime"])
    path = tmp_path / "config.yaml"
    save_config(_sample_config(), str(path))
    text = path.read_text()
    path.write_text(text[:text.index("diagnostics:")] + text[text.index("status:"):])   # a pre-C9 file
    assert "diagnostics" not in path.read_text()
    assert load_config(str(path)).diagnostics.regimes == list(REGIME_REGISTRY)


def test_diagnostics_section_does_not_change_trial_identity():
    from dataclasses import replace

    from src.experiments.trials import config_hash, experiment_params

    a = _sample_config()
    b = replace(a, diagnostics=DiagnosticsConfig(regimes=["policy"], state_exposure=True))
    assert config_hash(experiment_params(a)) == config_hash(experiment_params(b))
