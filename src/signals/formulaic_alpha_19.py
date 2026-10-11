"""
src/signals/formulaic_alpha_19.py

Formulaic alpha_19 (Kakushadze & Tulchinsky, 2015, "101 Formulaic Alphas"):
fades the 7-day price move, scaled by a cross-sectional rank of 250-day
momentum -- names that have run hardest over the past year get a bigger
short-term mean-reversion bet against their most recent 7-day move.

Extracted from notebooks/signals/formulaic_alphas_batch1.ipynb, where it was
the standout of a 10-alpha screen on the current S&P 500 universe (highest
Sharpe, lowest turnover -- see that notebook and
memory/formulaic_alphas_screening.md for the full screening process and the
other 9 candidates). This module provides the reusable signal-construction
plumbing only; cross-sectional ranking into a tradable decile long/short
portfolio is a portfolio-layer concern, done by
src.experiments.portfolio.decile_long_short(), not here.

Known limitation (not fixed here, same as the screening notebook): the
asset_ids() query underlying this signal's universe is drawn from current
S&P 500 constituents only (src.experiments.universe.EquityUniverse) -- no
point-in-time index membership, so this is survivorship-biased. See that
notebook's "Known limitation" section.

Usage:
    from src.db.client import db
    from src.signals.formulaic_alpha_19 import SignalConfig, load_data, calculate_signal

    db.open()
    open_df, high_df, low_df, close_df, volume_df = load_data()
    signal_df = calculate_signal(open_df, high_df, low_df, close_df, volume_df, SignalConfig())
    db.close()
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from src.db.client import db


@dataclass(frozen=True)
class SignalConfig:
    """Parameters for alpha_19 signal construction.

    trend_window/momentum_window match the formula as published; they are
    not a research grid the way cot_commercial_positioning's windows are --
    the screening notebook evaluated this alpha at its original spec only.
    """

    trend_window: int = 7
    momentum_window: int = 250


# ── Data loading ──────────────────────────────────────────────────────────────

def load_data(start: str | None = None, end: str | None = None) -> tuple[
    pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame
]:
    """Load daily OHLCV for the current S&P 500 universe from the DB.

    Returns (open_df, high_df, low_df, close_df, volume_df), each a wide
    date x ticker panel. Caller manages db.open()/db.close().
    """
    clauses = ["a.asset_class = 'equity'", "a.id < 100000", "p.interval = '1d'"]
    params: list = []
    if start is not None:
        clauses.append("p.timestamp >= ?")
        params.append(start)
    if end is not None:
        clauses.append("p.timestamp <= ?")
        params.append(end)

    rows = db.query(
        f"""
        SELECT a.ticker AS ticker,
               CAST(p.timestamp AS DATE) AS d,
               p.open, p.high, p.low, p.close, p.volume
        FROM prices p
        JOIN assets a ON a.id = p.asset_id
        WHERE {' AND '.join(clauses)}
        ORDER BY d, ticker
        """,
        params,
    )
    raw = pd.DataFrame(rows)
    if raw.empty:
        empty = pd.DataFrame()
        return empty, empty, empty, empty, empty
    raw["d"] = pd.to_datetime(raw["d"])

    panels = tuple(
        raw.pivot(index="d", columns="ticker", values=field)
        for field in ("open", "high", "low", "close", "volume")
    )
    return panels


# ── Signal construction ───────────────────────────────────────────────────────

def calculate_signal(
    open_df: pd.DataFrame,
    high_df: pd.DataFrame,
    low_df: pd.DataFrame,
    close_df: pd.DataFrame,
    volume_df: pd.DataFrame,
    config: SignalConfig = SignalConfig(),
) -> pd.DataFrame:
    """Compute alpha_19 and return it in long format.

    alpha_19 = -sign(close - close[trend_window]) * (1 + rank(1 + sum(pct_change, momentum_window)))

    Coverage/minimum-names-per-day thresholds are a portfolio-construction
    concern (see decile_long_short's min_names), not applied here -- this
    returns one row per (date, ticker) present in close_df's columns, with
    alpha_19 = NaN wherever the rolling windows haven't warmed up yet.

    Returns columns: d, ticker, alpha_19.
    """
    returns_df = close_df.pct_change()
    trend = close_df - close_df.shift(config.trend_window)
    cum_momentum = returns_df.rolling(config.momentum_window).sum()
    momentum_rank = (1 + cum_momentum).rank(axis=1)

    score = -np.sign(trend) * (1 + momentum_rank)
    score.index.name = "d"

    return (
        score.reset_index()
        .melt(id_vars="d", var_name="ticker", value_name="alpha_19")
        .sort_values(["d", "ticker"])
        .reset_index(drop=True)
    )
