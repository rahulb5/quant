"""tests/experiments/test_portfolio.py"""

from __future__ import annotations

import pandas as pd
import pytest

from src.experiments.portfolio import decile_long_short, equal_weight_long_basket


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


# ── decile_long_short ──────────────────────────────────────────────────────

def _ten_name_signal(date: str = "2024-01-01") -> pd.DataFrame:
    assets = [f"A{i}" for i in range(1, 11)]
    return pd.DataFrame({
        "date": [date] * 10,
        "asset": assets,
        "sig": list(range(1, 11)),  # A1 lowest, A10 highest
    })


def test_decile_long_short_longs_top_shorts_bottom():
    signal_df = _ten_name_signal()

    weights = decile_long_short(
        signal_df, date_column="date", asset_column="asset", signal_column="sig",
        n_deciles=10, min_names=5,
    )

    row = weights.loc["2024-01-01"]
    assert row["A10"] == 1.0   # highest score -> top decile, long
    assert row["A1"] == -1.0   # lowest score -> bottom decile, short
    assert (row.drop(["A1", "A10"]) == 0.0).all()
    assert row.sum() == 0.0          # dollar-neutral
    assert row.abs().sum() == 2.0    # gross long 1.0 / gross short 1.0


def test_decile_long_short_splits_ties_across_multiple_names():
    signal_df = pd.DataFrame({
        "date": ["2024-01-01"] * 4,
        "asset": ["A", "B", "C", "D"],
        "sig": [1, 1, 10, 10],
    })

    weights = decile_long_short(
        signal_df, date_column="date", asset_column="asset", signal_column="sig",
        n_deciles=2, min_names=2,
    )

    row = weights.loc["2024-01-01"]
    assert row["C"] == pytest.approx(0.5)
    assert row["D"] == pytest.approx(0.5)
    assert row["A"] == pytest.approx(-0.5)
    assert row["B"] == pytest.approx(-0.5)


def test_decile_long_short_below_min_names_is_flat():
    signal_df = _ten_name_signal().iloc[:3]  # only 3 names, below min_names

    weights = decile_long_short(
        signal_df, date_column="date", asset_column="asset", signal_column="sig",
        n_deciles=10, min_names=5,
    )

    assert (weights.loc["2024-01-01"] == 0.0).all()


def test_decile_long_short_ignores_nan_scores():
    signal_df = _ten_name_signal()
    signal_df.loc[signal_df["asset"] == "A5", "sig"] = float("nan")

    weights = decile_long_short(
        signal_df, date_column="date", asset_column="asset", signal_column="sig",
        n_deciles=10, min_names=5,
    )

    assert weights.loc["2024-01-01", "A5"] == 0.0
