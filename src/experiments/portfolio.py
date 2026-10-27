"""
src/experiments/portfolio.py

Deliberately minimal portfolio-construction rules that turn a signal frame
into weights for run_backtest(): an equal-weight long basket of assets whose
signal crosses a threshold (optionally gated by a trend filter), a
cross-sectional decile long/short, and vol-targeted time-series positions
(vol_target_weights, for the trend baseline). A general portfolio/optimization
layer is explicitly out of scope for now (see build plan Section 31).

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


def vol_target_weights(
    signal_df: pd.DataFrame,
    date_column: str,
    asset_column: str,
    signal_column: str,
    vol_column: str = "vol",
    group_column: str = "sleeve",
    return_column: str = "ret",
    trading_column: str = "trading",
    target_vol: float = 0.10,
    port_vol_window: int = 252,
    port_vol_min_obs: int = 126,
    scaler_bounds: tuple[float, float] = (0.5, 5.0),
    max_gross: float = 8.0,
    rebalance: str = "W-FRI",
    hold_limit: int = 10,
    diagnostics: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, pd.DataFrame]:
    """Vol-targeted time-series positions (docs/research/tsmom_definitions_v1.md, section 3).

    On each date t:
    1. raw_i = signal_i x target_vol / vol_i / (G x N_g): equal risk per group
       (sleeve) and per live instrument within it. Signal and vol are carried
       over an instrument's own non-trading days (up to hold_limit rows) so a
       holiday doesn't change the other instruments' share.
    2. lambda = target_vol / realised vol of the raw portfolio's daily returns
       over the past port_vol_window rows on which at least one of its
       instruments traded (returns up to t), clipped to scaler_bounds; 1 before
       port_vol_min_obs such rows.
    3. Gross leverage capped at max_gross by proportional scaling.
    4. Rebalance on a calendar schedule (`rebalance`, default every Friday).
       Each instrument trades on its first own trading day on or after each
       scheduled date (signal_df's trading_column), at that day's target, and
       holds the weight in between. The schedule doesn't depend on which later
       dates exist, so weights up to t are the same whether or not data after
       t is present.
    5. Weight is 0 while an instrument isn't live: before its first signal, and
       once its data has been missing for more than hold_limit rows (a dead or
       stale series is exited rather than held forever).

    Returns a wide (date x asset) weight panel. With diagnostics=True, returns
    (weights, frame with columns scaler and gross): lambda and the gross
    leverage of the held weights per date. (Not stored in .attrs: pandas
    fails to concatenate frames whose attrs hold Series.)
    """
    def wide(col: str) -> pd.DataFrame:
        return signal_df.pivot(index=date_column, columns=asset_column, values=col).sort_index()

    sig = wide(signal_column).ffill(limit=hold_limit)
    vol = wide(vol_column).ffill(limit=hold_limit)
    ret = wide(return_column)
    trading = wide(trading_column).fillna(False).astype(bool)
    groups = signal_df.drop_duplicates(asset_column).set_index(asset_column)[group_column]

    live = sig.notna() & vol.notna() & (vol > 0)
    n_in_group = pd.DataFrame(index=sig.index, columns=sig.columns, dtype=float)
    n_groups = pd.Series(0.0, index=sig.index)
    for g in groups.unique():
        cols = groups.index[groups == g]
        n = live[cols].sum(axis=1)
        n_in_group[cols] = np.repeat(n.to_numpy()[:, None], len(cols), axis=1)
        n_groups += (n > 0).astype(float)

    raw = (sig * target_vol / vol / n_in_group.mul(n_groups, axis=0)).where(live, 0.0)

    # Realised vol only over rows where some instrument traded, so rows from
    # markets outside this portfolio don't dilute the window or annualisation.
    port = (raw.shift(1) * ret).sum(axis=1, min_count=1)
    port = port[trading.any(axis=1)].dropna()
    realised = port.rolling(port_vol_window, min_periods=port_vol_min_obs).std() * np.sqrt(252)
    realised = realised.reindex(raw.index).ffill()
    scaler = (target_vol / realised).clip(*scaler_bounds).fillna(1.0)
    target = raw.mul(scaler, axis=0)

    gross = target.abs().sum(axis=1)
    target = target.mul(np.minimum(1.0, max_gross / gross.replace(0.0, np.nan)).fillna(1.0), axis=0)

    idx = target.index
    scheduled = pd.date_range(idx[0].normalize(), idx[-1], freq=rebalance)
    weights = pd.DataFrame(np.nan, index=idx, columns=target.columns)
    for col in target.columns:
        own = idx[trading[col].to_numpy()]
        if own.empty:
            continue
        pos = own.searchsorted(scheduled)               # first own trading day on/after each date
        trade_days = own[np.unique(pos[pos < len(own)])]
        weights.loc[trade_days, col] = target.loc[trade_days, col]
    weights = weights.ffill().fillna(0.0).where(live, 0.0)

    # The cap applies to each trade's target; a held weight next to newly traded
    # ones can briefly take gross a little above max_gross. "gross" is what's held.
    if diagnostics:
        return weights, pd.DataFrame({"scaler": scaler, "gross": weights.abs().sum(axis=1)})
    return weights
