"""
tests/signals/test_cot_commercial_positioning.py

Tests for src/signals/cot_commercial_positioning.py.

Pure pandas transforms — no DB or network access needed except for
test_load_data, which uses an in-memory DuckDB instance.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

import src.signals.cot_commercial_positioning as sig
from src.db.client import Database
from src.signals.cot_commercial_positioning import (
    SignalConfig,
    add_forward_returns,
    add_percentile_signals,
    calculate_signal,
    load_data,
    net_commercial_positioning,
    rolling_percentile,
    weekly_prices_with_trend,
)

CRUDE_CODE = "CFTC.DISAGG_FUT.067651"  # Crude Oil, per COMMODITY_MAP


# ── rolling_percentile ────────────────────────────────────────────────────────

def test_rolling_percentile_strictly_increasing_series_is_always_max():
    s = pd.Series(range(1, 11), dtype=float)  # 1..10
    result = rolling_percentile(s, window=4)

    # min_periods = window // 2 = 2 -> first valid output at index 1
    assert pd.isna(result.iloc[0])
    assert (result.iloc[1:] == 100.0).all()


# ── net_commercial_positioning ────────────────────────────────────────────────

def _raw_cot_rows() -> pd.DataFrame:
    return pd.DataFrame({
        "report_date": pd.to_datetime(["2024-01-02", "2024-01-09", "2024-01-02"]),
        "series_id": [1, 1, 2],
        "series_code": [CRUDE_CODE, CRUDE_CODE, "CFTC.DISAGG_FUT.999999"],  # last is unmapped
        "series_name": ["CRUDE OIL", "CRUDE OIL", "UNKNOWN"],
        "open_interest": [1000.0, 0.0, 500.0],
        "dis_pmpu_long": [600.0, 300.0, 100.0],
        "dis_pmpu_short": [400.0, 100.0, 50.0],
    })


def test_net_commercial_positioning_filters_and_computes_net():
    cot = net_commercial_positioning(_raw_cot_rows())

    assert set(cot["commodity"]) == {"Crude Oil"}  # unmapped series dropped
    assert len(cot) == 2

    row0 = cot.iloc[0]
    assert row0["net_long"] == 200.0  # 600 - 400
    assert row0["net_long_oi"] == pytest.approx(0.2)  # 200 / 1000

    row1 = cot.iloc[1]
    assert row1["net_long"] == 200.0  # 300 - 100
    assert pd.isna(row1["net_long_oi"])  # open_interest == 0 -> inf -> NaN


# ── add_percentile_signals ────────────────────────────────────────────────────

def test_add_percentile_signals_adds_expected_columns():
    cot = net_commercial_positioning(_raw_cot_rows())
    cot = add_percentile_signals(cot, windows=(2,))

    assert "raw_pctile_2w" in cot.columns
    assert "oi_pctile_2w" in cot.columns


# ── weekly_prices_with_trend ──────────────────────────────────────────────────

def _raw_price_rows() -> pd.DataFrame:
    dates = pd.date_range("2023-12-25", "2024-02-05", freq="D")
    closes = 100.0 + np.arange(len(dates))
    return pd.DataFrame({
        "asset_id": 1,
        "ticker": "CL1",
        "timestamp": dates,
        "close": closes,
    })


def test_weekly_prices_with_trend_maps_ticker_to_commodity():
    weekly = weekly_prices_with_trend(_raw_price_rows())

    assert set(weekly["commodity"]) == {"Crude Oil"}
    assert "trend_none" in weekly.columns
    assert (weekly["trend_none"]).all()
    assert weekly["week_date"].is_monotonic_increasing


def test_weekly_prices_with_trend_ignores_untracked_tickers():
    prices = _raw_price_rows()
    prices["ticker"] = "UNKNOWN1"
    weekly = weekly_prices_with_trend(prices)
    assert weekly.empty


# ── add_forward_returns ───────────────────────────────────────────────────────

def test_add_forward_returns_computes_shifted_pct_change():
    weekly = pd.DataFrame({
        "commodity": ["Crude Oil"] * 5,
        "week_date": pd.date_range("2024-01-08", periods=5, freq="W-MON"),
        "close": [100.0, 101.0, 102.0, 103.0, 104.0],
    })
    result = add_forward_returns(weekly, horizons=(1, 2))

    assert result["fwd_1w"].iloc[0] == pytest.approx(0.01)   # 101/100 - 1
    assert result["fwd_2w"].iloc[0] == pytest.approx(0.02)   # 102/100 - 1
    assert pd.isna(result["fwd_1w"].iloc[-1])  # no data beyond series end


# ── calculate_signal (integration) ────────────────────────────────────────────

def test_calculate_signal_builds_full_frame():
    cot_raw = pd.DataFrame({
        "report_date": pd.to_datetime(["2024-01-02", "2024-01-09", "2024-01-16", "2024-01-23"]),
        "series_id": [1, 1, 1, 1],
        "series_code": [CRUDE_CODE] * 4,
        "series_name": ["CRUDE OIL"] * 4,
        "open_interest": [1000.0, 1000.0, 1000.0, 1000.0],
        "dis_pmpu_long": [600.0, 620.0, 640.0, 660.0],
        "dis_pmpu_short": [400.0, 400.0, 400.0, 400.0],
    })
    prices_raw = _raw_price_rows()

    config = SignalConfig(windows=(2,), horizons=(1,))
    result = calculate_signal(cot_raw, prices_raw, config)

    expected_cols = {
        "commodity", "report_date", "week_date", "net_long", "net_long_oi",
        "raw_pctile_2w", "oi_pctile_2w",
        "trend_ma_cross", "trend_price_above_ma", "trend_momentum", "trend_none",
        "fwd_1w",
    }
    assert expected_cols.issubset(result.columns)
    assert len(result) == 4

    # report_date is a Tuesday; week_date must be the following Monday (+6 days)
    assert (result["week_date"] - result["report_date"]).dt.days.eq(6).all()


# ── load_data ──────────────────────────────────────────────────────────────

@pytest.fixture
def mem_db(monkeypatch) -> Database:
    database = Database(":memory:")
    database.open()
    monkeypatch.setattr(sig, "db", database)
    yield database
    database.close()


def test_load_data_reads_disagg_fut_and_futures_prices(mem_db):
    mem_db.run(
        """INSERT INTO macro_series (id, code, name, source, frequency, units)
           VALUES (1, ?, 'Crude Oil', 'CFTC', 'weekly', 'contracts')""",
        [CRUDE_CODE],
    )
    mem_db.run(
        """INSERT INTO cot_positions
             (series_id, report_date, release_date, report_type, open_interest,
              dis_pmpu_long, dis_pmpu_short)
           VALUES (1, '2024-01-02', '2024-01-05', 'disagg_fut', 1000, 600, 400)"""
    )
    mem_db.run(
        """INSERT INTO assets (id, ticker, name, asset_class, currency)
           VALUES (1, 'CL1', 'Crude Oil Front Month', 'futures', 'USD')"""
    )
    mem_db.run(
        """INSERT INTO prices
             (asset_id, interval, timestamp, open, high, low, close, volume, source)
           VALUES (1, '1d', '2024-01-02 00:00:00', 100, 101, 99, 100.5, 0, 'test')"""
    )

    cot_raw, prices_raw = load_data()

    assert len(cot_raw) == 1
    assert cot_raw.iloc[0]["series_code"] == CRUDE_CODE
    assert len(prices_raw) == 1
    assert prices_raw.iloc[0]["ticker"] == "CL1"


def test_load_data_empty_db_returns_empty_frames(mem_db):
    cot_raw, prices_raw = load_data()
    assert cot_raw.empty
    assert prices_raw.empty
