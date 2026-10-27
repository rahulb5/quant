"""
tests/collectors/test_total_return.py

Tests for src/collectors/total_return. In-memory DuckDB; yfinance stubbed.
"""

from __future__ import annotations

import pandas as pd
import pytest

import src.collectors.base as base_module
import src.collectors.total_return as tr_module
from src.collectors.total_return import (
    TOTAL_RETURN_SOURCES,
    TotalReturnCollector,
    to_dividend_rows,
    to_price_rows,
)
from src.db.asset_ids import ID_BLOCKS
from src.db.client import Database


def _yahoo(dates, close, adj, tz="Asia/Tokyo", divs=None) -> pd.DataFrame:
    idx = pd.DatetimeIndex(pd.to_datetime(dates)).tz_localize(tz)
    return pd.DataFrame({"Open": close, "High": close, "Low": close, "Close": close,
                         "Adj Close": adj, "Volume": [1.0] * len(close),
                         "Dividends": divs or [0.0] * len(close)}, index=idx)


@pytest.fixture
def mem_db(monkeypatch) -> Database:
    database = Database(":memory:")
    database.open()
    monkeypatch.setattr(tr_module, "db", database)
    monkeypatch.setattr(base_module, "db", database)
    yield database
    database.close()


def test_catalogue_ids_sit_in_their_blocks():
    for a in TOTAL_RETURN_SOURCES:
        block = "etf" if a["id"] >= 500_001 else "equity_index"
        lo, hi = ID_BLOCKS[block]
        assert lo <= a["id"] <= hi, a


def test_to_price_rows_keeps_local_date():
    df = to_price_rows(_yahoo(["2026-10-23"], [100.0], [99.0]))
    ts = df["timestamp"].iloc[0]
    assert str(ts.tz) == "America/New_York"
    assert ts.date().isoformat() == "2026-10-23"   # Tokyo date, not shifted to the 22nd
    assert df["adj_close"].iloc[0] == 99.0


def test_to_price_rows_without_adj_close_uses_close():
    raw = _yahoo(["2026-10-23"], [100.0], [100.0]).drop(columns="Adj Close")
    assert to_price_rows(raw)["adj_close"].iloc[0] == 100.0


def test_to_dividend_rows_keeps_positive_amounts_on_local_date():
    df = to_dividend_rows(_yahoo(["2026-10-22", "2026-10-23"], [100.0, 101.0], [100.0, 101.0], divs=[0.0, 1.5]))
    assert df.to_dict("records") == [{"ex_date": pd.Timestamp("2026-10-23").date(), "amount": 1.5}]


def test_refetch_restates_prices_and_dividends(mem_db, monkeypatch):
    """A split restates Yahoo's history; a refetch must replace, not mix, stored rows."""
    frames = iter([
        _yahoo(["2026-10-22", "2026-10-23"], [100.0, 101.0], [100.0, 101.0], divs=[0.0, 10.0]),
        _yahoo(["2026-10-22", "2026-10-23"], [1.0, 1.01], [1.0, 1.01], divs=[0.0, 0.1]),
    ])

    class FakeTicker:
        def __init__(self, ticker):
            pass

        def history(self, **kwargs):
            return next(frames)

    monkeypatch.setattr(tr_module.yf, "Ticker", FakeTicker)
    c = TotalReturnCollector()
    c.fetch_all(tickers=["QQQ"])
    c.fetch_all(tickers=["QQQ"])

    rows = mem_db.query("SELECT close FROM prices WHERE asset_id = 500002 ORDER BY timestamp")
    assert [r["close"] for r in rows] == [1.0, 1.01]
    assert mem_db.query("SELECT amount FROM dividends WHERE asset_id = 500002") == [{"amount": 0.1}]
    assert mem_db.query("SELECT asset_class FROM assets WHERE id = 500002")[0]["asset_class"] == "equity"
