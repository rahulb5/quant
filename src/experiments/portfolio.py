"""
src/experiments/portfolio.py

Deliberately minimal portfolio-construction rule: equal-weight long basket
of assets whose signal crosses a threshold (optionally gated by a trend
filter). A general portfolio/optimization layer is explicitly out of scope
for now (see build plan Section 31) — this is the "inline simple rule" that
turns a signal frame into weights for run_backtest().

date_column/asset_column are config-driven rather than hardcoded to COT's
week_date/commodity, so a differently-shaped signal frame (e.g. an equities
signal using date/ticker) plugs in without touching this function.
"""

from __future__ import annotations

import numpy as np
import pandas as pd


def equal_weight_long_basket(
    signal_df: pd.DataFrame,
    date_column: str,
    asset_column: str,
    signal_column: str,
    threshold: float,
    trend_filter: str = "trend_none",
) -> pd.DataFrame:
    """Build a wide weight panel from a long-format signal frame.

    On each date, assets with signal_df[signal_column] >= threshold (and,
    if trend_filter != 'trend_none', signal_df[trend_filter] is True) get
    equal weight 1/N; all other assets get 0. Dates with no qualifying
    asset get an all-zero row rather than being dropped.

    Args:
        signal_df: Long-format frame, one row per (date, asset).
        date_column: Column holding the rebalance date.
        asset_column: Column holding the asset key.
        signal_column: Column the threshold is applied to.
        threshold: Inclusive lower bound for selection.
        trend_filter: Column name gating selection, or 'trend_none' to skip
            gating entirely.

    Returns:
        Wide pd.DataFrame: index=date_column values, columns=asset_column
        values, values=weight (sums to 1.0 per row, or 0.0 if no selection).
    """
    mask = signal_df[signal_column] >= threshold
    if trend_filter != "trend_none":
        mask = mask & (signal_df[trend_filter] == True)  # noqa: E712

    selected = signal_df.loc[mask, [date_column, asset_column]].drop_duplicates().copy()
    selected["selected"] = True

    full_grid = signal_df[[date_column, asset_column]].drop_duplicates()
    grid = full_grid.merge(selected, on=[date_column, asset_column], how="left")
    grid["selected"] = grid["selected"].fillna(False)

    wide = grid.pivot_table(
        index=date_column, columns=asset_column, values="selected", aggfunc="max"
    ).fillna(False).astype(float)

    counts = wide.sum(axis=1)
    weights = wide.divide(counts, axis=0).fillna(0.0)
    return weights.sort_index()


def decile_long_short(
    signal_df: pd.DataFrame,
    date_column: str,
    asset_column: str,
    signal_column: str,
    n_deciles: int = 10,
    min_names: int = 50,
) -> pd.DataFrame:
    """Build a wide weight panel implementing a dollar-neutral decile long/short.

    On each date, assets are ranked cross-sectionally by signal_df[signal_column]
    (ties broken by order, via method='first' so decile boundaries stay
    well-defined). The top decile is long, the bottom decile is short, each
    side equal-weighted so gross long = 1.0 and gross short = 1.0. A date
    with fewer than min_names valid (non-NaN) scores holds no position that
    date (flat, all-zero row) rather than trading a thin cross-section.

    Ported from the decile_weights() construction in
    notebooks/signals/formulaic_alphas_batch1.ipynb — see that notebook for
    the full screening process across alpha candidates.

    Args:
        signal_df: Long-format frame, one row per (date, asset).
        date_column: Column holding the rebalance date.
        asset_column: Column holding the asset key.
        signal_column: Column ranked cross-sectionally each date.
        n_deciles: Number of cross-sectional buckets.
        min_names: Minimum valid scores required on a date to trade it.

    Returns:
        Wide pd.DataFrame: index=date_column values, columns=asset_column
        values, values=weight (sums to 0.0, gross 2.0, or all-zero row if
        below min_names).
    """
    wide = signal_df.pivot(index=date_column, columns=asset_column, values=signal_column)

    valid_count = wide.notna().sum(axis=1)
    ranks = wide.rank(axis=1, method="first")

    decile = np.ceil(ranks.div(valid_count.replace(0, np.nan), axis=0) * n_deciles) - 1
    decile = decile.clip(upper=n_deciles - 1)

    top_mask = decile == (n_deciles - 1)
    bottom_mask = decile == 0

    insufficient = valid_count < min_names
    top_mask = top_mask.where(~insufficient, False)
    bottom_mask = bottom_mask.where(~insufficient, False)

    top_n = top_mask.sum(axis=1)
    bottom_n = bottom_mask.sum(axis=1)

    weights = (
        top_mask.astype(float).div(top_n.replace(0, np.nan), axis=0).fillna(0.0)
        - bottom_mask.astype(float).div(bottom_n.replace(0, np.nan), axis=0).fillna(0.0)
    )
    return weights.sort_index()
