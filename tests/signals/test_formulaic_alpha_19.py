"""
tests/signals/test_formulaic_alpha_19.py

Tests for src/signals/formulaic_alpha_19.py.

Pure pandas transforms — no DB or network access needed except for
test_load_data, which uses an in-memory DuckDB instance.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

import src.signals.formulaic_alpha_19 as sig
from src.db.client import Database
from src.signals.formulaic_alpha_19 import SignalConfig, calculate_signal, load_data


def _panels(n_days: int, tickers: list[str]) -> tuple[pd.DataFrame, ...]:
    idx = pd.date_range("2024-01-01", periods=n_days, freq="D")
    rng = np.random.default_rng(0)
    close = pd.DataFrame(
        100 + np.cumsum(rng.normal(0, 1, size=(n_days, len(tickers))), axis=0),
        index=idx,
        columns=tickers,
    )
    open_df = close.shift(1).fillna(close.iloc[0])
    high_df = close + 1
    low_df = close - 1
    volume = pd.DataFrame(1_000_000, index=idx, columns=tickers, dtype=float)
    return open_df, high_df, low_df, close, volume


# ── calculate_signal ──────────────────────────────────────────────────────────

def test_calculate_signal_long_format_shape():
    open_df, high_df, low_df, close_df, volume_df = _panels(300, ["AAA", "BBB", "CCC"])

    signal_df = calculate_signal(open_df, high_df, low_df, close_df, volume_df, SignalConfig())

    assert list(signal_df.columns) == ["d", "ticker", "alpha_19"]
    assert set(signal_df["ticker"]) == {"AAA", "BBB", "CCC"}
    assert len(signal_df) == 300 * 3


def test_calculate_signal_nan_during_warmup():
    open_df, high_df, low_df, close_df, volume_df = _panels(10, ["AAA", "BBB"])

    signal_df = calculate_signal(open_df, high_df, low_df, close_df, volume_df, SignalConfig())

    # momentum_window=250 default never warms up over 10 days -> all NaN
    assert signal_df["alpha_19"].isna().all()


def test_calculate_signal_sign_is_fade_of_trend_window():
    tickers = ["AAA", "BBB"]
    idx = pd.date_range("2024-01-01", periods=12, freq="D")
    close_df = pd.DataFrame({"AAA": np.linspace(100, 111, 12), "BBB": np.linspace(100, 89, 12)}, index=idx)
    open_df = close_df.copy()
    high_df = close_df + 1
    low_df = close_df - 1
    volume_df = pd.DataFrame(1.0, index=idx, columns=tickers)

    config = SignalConfig(trend_window=7, momentum_window=5)
    signal_df = calculate_signal(open_df, high_df, low_df, close_df, volume_df, config)

    last_day = signal_df[signal_df["d"] == idx[-1]].set_index("ticker")["alpha_19"]
    # AAA trended up over the last 7 days -> fade is negative; BBB trended down -> fade is positive
    assert last_day["AAA"] < 0
    assert last_day["BBB"] > 0


# ── load_data ──────────────────────────────────────────────────────────────

@pytest.fixture
def mem_db(monkeypatch) -> Database:
    database = Database(":memory:")
    database.open()
    monkeypatch.setattr(sig, "db", database)
    yield database
    database.close()


def test_load_data_reads_equity_prices_below_100000(mem_db):
    mem_db.run(
        """INSERT INTO assets (id, ticker, name, asset_class, currency)
           VALUES (1, 'AAPL', 'Apple', 'equity', 'USD'),
                  (100001, '^GSPC', 'S&P 500 Index', 'equity', 'USD')"""
    )
    mem_db.run(
        """INSERT INTO prices
             (asset_id, interval, timestamp, open, high, low, close, volume, source)
           VALUES (1, '1d', '2024-01-02', 10, 11, 9, 10.5, 1000, 'test'),
                  (100001, '1d', '2024-01-02', 4000, 4010, 3990, 4005, 0, 'test')"""
    )

    open_df, high_df, low_df, close_df, volume_df = load_data()

    assert list(close_df.columns) == ["AAPL"]  # index-catalog id >= 100000 excluded
    assert close_df.iloc[0]["AAPL"] == 10.5


def test_load_data_empty_db_returns_empty_frames(mem_db):
    open_df, high_df, low_df, close_df, volume_df = load_data()
    assert close_df.empty
