"""
src/signals/cot_commercial_positioning.py

COT commercial positioning signal: extreme commercial (producer/merchant)
net positioning in the CFTC disaggregated futures report, expressed as a
percentile rank over a rolling window, optionally filtered by a price
trend condition.

Extracted from notebooks/signals/cot_commercial_positioning.ipynb — see that
notebook for the full research process (IC analysis, regime dependence,
in-sample/out-of-sample variant selection). This module provides the
reusable signal-construction plumbing only. Which signal variant (window,
positioning measure, trend filter, threshold) actually survives OOS
validation is a research question the notebook investigates and explicitly
flags as not fully clean (full-sample IC was used to pick the headline
variant) — this module does not hardcode that choice as a default.

Usage:
    from src.db.client import db
    from src.signals.cot_commercial_positioning import (
        SignalConfig, load_data, calculate_signal,
    )

    db.open()
    cot_raw, prices_raw = load_data()
    signal_df = calculate_signal(cot_raw, prices_raw, SignalConfig())
    db.close()
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd
from scipy.stats import percentileofscore

from src.db.client import db

# ── Commodity universe ────────────────────────────────────────────────────────
# Maps each commodity to its CFTC disaggregated-futures series code
# (src/collectors/cot.py) and its front-month futures price ticker
# (src/collectors/commodity.py).

COMMODITY_MAP: dict[str, dict[str, str]] = {
    "Crude Oil":   {"cot_code": "CFTC.DISAGG_FUT.067651", "price_ticker": "CL1"},
    "Natural Gas": {"cot_code": "CFTC.DISAGG_FUT.023651", "price_ticker": "NG1"},
    "Gold":        {"cot_code": "CFTC.DISAGG_FUT.088691", "price_ticker": "GC1"},
    "Silver":      {"cot_code": "CFTC.DISAGG_FUT.084691", "price_ticker": "SI1"},
    "Copper":      {"cot_code": "CFTC.DISAGG_FUT.085692", "price_ticker": "HG1"},
    "Corn":        {"cot_code": "CFTC.DISAGG_FUT.002602", "price_ticker": "ZC1"},
    "Wheat":       {"cot_code": "CFTC.DISAGG_FUT.001602", "price_ticker": "ZW1"},
    "Soybeans":    {"cot_code": "CFTC.DISAGG_FUT.005602", "price_ticker": "ZS1"},
    "Coffee":      {"cot_code": "CFTC.DISAGG_FUT.083731", "price_ticker": "KC1"},
    "Sugar":       {"cot_code": "CFTC.DISAGG_FUT.080732", "price_ticker": "SB1"},
    "Cocoa":       {"cot_code": "CFTC.DISAGG_FUT.073732", "price_ticker": "CC1"},
}


@dataclass(frozen=True)
class SignalConfig:
    """Parameters for commercial-positioning signal construction.

    windows/horizons are the research grid swept in the notebook, not a
    validated "best" choice — see module docstring.
    """

    windows: tuple[int, ...] = (26, 52, 78, 104)   # weeks, for rolling percentile rank
    horizons: tuple[int, ...] = (1, 2, 4, 8, 12)   # weeks, for forward returns


# ── Data loading ──────────────────────────────────────────────────────────────

def load_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load raw COT disaggregated positioning and futures prices from the DB.

    Returns (cot_raw, prices_raw). Caller manages db.open()/db.close().
    """
    cot_rows = db.query(
        """
        SELECT
            cp.report_date,
            cp.series_id,
            ms.code   AS series_code,
            ms.name   AS series_name,
            cp.open_interest,
            cp.dis_pmpu_long,
            cp.dis_pmpu_short
        FROM cot_positions cp
        JOIN macro_series ms ON cp.series_id = ms.id
        WHERE cp.report_type = 'disagg_fut'
        """
    )
    cot_raw = pd.DataFrame(cot_rows)
    if not cot_raw.empty:
        cot_raw["report_date"] = pd.to_datetime(cot_raw["report_date"])

    price_rows = db.query(
        """
        SELECT
            a.id AS asset_id,
            a.ticker,
            CAST(p.timestamp AS DATE) AS timestamp,
            p.close
        FROM prices p
        JOIN assets a ON p.asset_id = a.id
        WHERE a.asset_class = 'futures'
          AND p.interval    = '1d'
        ORDER BY a.ticker, timestamp
        """
    )
    prices_raw = pd.DataFrame(price_rows)
    if not prices_raw.empty:
        prices_raw["timestamp"] = pd.to_datetime(prices_raw["timestamp"])

    return cot_raw, prices_raw


# ── Signal construction ───────────────────────────────────────────────────────

def net_commercial_positioning(cot_raw: pd.DataFrame) -> pd.DataFrame:
    """Filter to the commodity universe and compute net commercial positioning.

    Adds 'commodity' (mapped name), 'net_long' (raw contracts), and
    'net_long_oi' (open-interest-normalised) columns.
    """
    code_to_commodity = {cfg["cot_code"]: name for name, cfg in COMMODITY_MAP.items()}

    cot = cot_raw[cot_raw["series_code"].isin(code_to_commodity)].copy()
    cot["commodity"] = cot["series_code"].map(code_to_commodity)
    cot["net_long"] = cot["dis_pmpu_long"] - cot["dis_pmpu_short"]
    cot["net_long_oi"] = (cot["net_long"] / cot["open_interest"]).replace([np.inf, -np.inf], np.nan)

    return cot.sort_values(["commodity", "report_date"]).reset_index(drop=True)


def rolling_percentile(s: pd.Series, window: int) -> pd.Series:
    """Percentile rank of the current observation vs. the prior window (x[-1] vs x[:-1])."""
    return s.rolling(window, min_periods=window // 2).apply(
        lambda x: percentileofscore(x[:-1], x[-1], kind="rank"),
        raw=True,
    )


def add_percentile_signals(cot: pd.DataFrame, windows: tuple[int, ...]) -> pd.DataFrame:
    """Add raw_pctile_{w}w and oi_pctile_{w}w columns for each window, per commodity."""
    pieces = []
    for _, grp in cot.groupby("commodity", sort=True):
        grp = grp.sort_values("report_date").copy()
        for w in windows:
            grp[f"raw_pctile_{w}w"] = rolling_percentile(grp["net_long"], w)
            grp[f"oi_pctile_{w}w"] = rolling_percentile(grp["net_long_oi"], w)
        pieces.append(grp)

    return pd.concat(pieces).sort_values(["commodity", "report_date"]).reset_index(drop=True)


def weekly_prices_with_trend(prices_raw: pd.DataFrame) -> pd.DataFrame:
    """Resample daily futures closes to weekly (Monday close) and add trend filters.

    Trend columns: trend_ma_cross (10w SMA > 40w SMA), trend_price_above_ma
    (close > 40w SMA), trend_momentum (12w return > 0), trend_none (always True).
    """
    ticker_to_commodity = {cfg["price_ticker"]: name for name, cfg in COMMODITY_MAP.items()}
    target_tickers = set(ticker_to_commodity)

    px = prices_raw[prices_raw["ticker"].isin(target_tickers)].copy()
    if px.empty:
        return pd.DataFrame(columns=[
            "commodity", "week_date", "close",
            "trend_ma_cross", "trend_price_above_ma", "trend_momentum", "trend_none",
        ])

    weekly_pieces = []
    for ticker, grp in px.groupby("ticker"):
        weekly = (
            grp.set_index("timestamp")["close"]
            .resample("W-MON")
            .last()
            .dropna()
            .reset_index()
            .rename(columns={"timestamp": "week_date"})
        )
        weekly["commodity"] = ticker_to_commodity[ticker]
        weekly_pieces.append(weekly[["commodity", "week_date", "close"]])

    weekly = pd.concat(weekly_pieces).sort_values(["commodity", "week_date"]).reset_index(drop=True)

    trend_pieces = []
    for _, grp in weekly.groupby("commodity", sort=True):
        grp = grp.sort_values("week_date").copy()
        sma10 = grp["close"].rolling(10, min_periods=10).mean()
        sma40 = grp["close"].rolling(40, min_periods=40).mean()
        ret12 = grp["close"] / grp["close"].shift(12) - 1

        grp["trend_ma_cross"] = sma10 > sma40
        grp["trend_price_above_ma"] = grp["close"] > sma40
        grp["trend_momentum"] = ret12 > 0
        grp["trend_none"] = True
        trend_pieces.append(grp)

    return pd.concat(trend_pieces).sort_values(["commodity", "week_date"]).reset_index(drop=True)


def add_forward_returns(weekly: pd.DataFrame, horizons: tuple[int, ...]) -> pd.DataFrame:
    """Add fwd_{h}w forward return columns to a weekly price frame, per commodity."""
    pieces = []
    for _, grp in weekly.groupby("commodity", sort=True):
        grp = grp.sort_values("week_date").copy()
        for h in horizons:
            grp[f"fwd_{h}w"] = grp["close"].shift(-h) / grp["close"] - 1
        pieces.append(grp)

    return pd.concat(pieces).sort_values(["commodity", "week_date"]).reset_index(drop=True)


def calculate_signal(
    cot_raw: pd.DataFrame,
    prices_raw: pd.DataFrame,
    config: SignalConfig = SignalConfig(),
) -> pd.DataFrame:
    """Build the full commercial-positioning signal frame.

    Pipeline: filter to the commodity universe -> net positioning -> rolling
    percentile ranks -> weekly trend filters -> forward returns. Entry is
    modelled at the next Monday close after COT publication (positions as of
    Tuesday, released Friday afternoon) so the data is fully public before
    trading.

    Returns one row per (commodity, report_date) with columns:
        commodity, report_date, week_date, net_long, net_long_oi,
        {raw,oi}_pctile_{w}w for w in config.windows,
        trend_ma_cross, trend_price_above_ma, trend_momentum, trend_none,
        fwd_{h}w for h in config.horizons
    """
    cot = net_commercial_positioning(cot_raw)
    cot = add_percentile_signals(cot, config.windows)

    weekly = weekly_prices_with_trend(prices_raw)
    weekly = add_forward_returns(weekly, config.horizons)

    # report_date is Tuesday; next Monday close = +((7 - weekday) % 7) days
    cot["week_date"] = cot["report_date"] + pd.to_timedelta(
        (7 - cot["report_date"].dt.weekday) % 7, unit="D"
    )

    trend_cols = [
        "commodity", "week_date",
        "trend_ma_cross", "trend_price_above_ma", "trend_momentum", "trend_none",
    ]
    fwd_cols = ["commodity", "week_date"] + [f"fwd_{h}w" for h in config.horizons]

    cot = cot.merge(weekly[trend_cols], on=["commodity", "week_date"], how="left")
    cot = cot.merge(weekly[fwd_cols], on=["commodity", "week_date"], how="left")

    return cot
