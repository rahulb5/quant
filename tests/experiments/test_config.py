"""tests/experiments/test_config.py"""

from __future__ import annotations

from src.experiments.config import (
    BacktestConfig,
    DataConfig,
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
