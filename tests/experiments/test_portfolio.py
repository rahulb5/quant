"""tests/experiments/test_portfolio.py"""

from __future__ import annotations

import pandas as pd

from src.experiments.portfolio import equal_weight_long_basket


def test_equal_weight_basic_threshold():
    signal_df = pd.DataFrame({
        "date": ["2024-01-01", "2024-01-01", "2024-01-08", "2024-01-08"],
        "asset": ["A", "B", "A", "B"],
        "sig": [95, 50, 20, 10],
    })

    weights = equal_weight_long_basket(
        signal_df, date_column="date", asset_column="asset", signal_column="sig", threshold=90
    )

    assert weights.loc["2024-01-01", "A"] == 1.0
    assert weights.loc["2024-01-01", "B"] == 0.0
    assert weights.loc["2024-01-08"].sum() == 0.0  # no asset crosses threshold


def test_equal_weight_splits_across_multiple_selected():
    signal_df = pd.DataFrame({
        "date": ["2024-01-01"] * 3,
        "asset": ["A", "B", "C"],
        "sig": [95, 92, 10],
    })

    weights = equal_weight_long_basket(
        signal_df, date_column="date", asset_column="asset", signal_column="sig", threshold=90
    )

    assert weights.loc["2024-01-01", "A"] == 0.5
    assert weights.loc["2024-01-01", "B"] == 0.5
    assert weights.loc["2024-01-01", "C"] == 0.0


def test_trend_filter_gates_selection():
    signal_df = pd.DataFrame({
        "date": ["2024-01-01", "2024-01-01"],
        "asset": ["A", "B"],
        "sig": [95, 95],
        "trend_up": [True, False],
    })

    weights = equal_weight_long_basket(
        signal_df,
        date_column="date",
        asset_column="asset",
        signal_column="sig",
        threshold=90,
        trend_filter="trend_up",
    )

    assert weights.loc["2024-01-01", "A"] == 1.0
    assert weights.loc["2024-01-01", "B"] == 0.0


def test_trend_none_skips_gating():
    signal_df = pd.DataFrame({
        "date": ["2024-01-01"],
        "asset": ["A"],
        "sig": [95],
    })

    weights = equal_weight_long_basket(
        signal_df,
        date_column="date",
        asset_column="asset",
        signal_column="sig",
        threshold=90,
        trend_filter="trend_none",
    )

    assert weights.loc["2024-01-01", "A"] == 1.0
