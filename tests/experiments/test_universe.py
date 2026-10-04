"""
tests/experiments/test_universe.py

Uses an in-memory DuckDB instance monkeypatched into universe.py, same
pattern as tests/collectors/test_cot.py.
"""

from __future__ import annotations

import pandas as pd
import pytest

import data.repository as repository_module
import src.experiments.universe as universe_module
from src.db.client import Database
from src.experiments.universe import CommodityUniverse, EquityUniverse
from src.signals.cot_commercial_positioning import weekly_prices_with_trend
from data.repository import Asset, Price, insert_asset, insert_price


@pytest.fixture
def mem_db(monkeypatch) -> Database:
    database = Database(":memory:")
    database.open()
    monkeypatch.setattr(universe_module, "db", database)
    monkeypatch.setattr(repository_module, "db", database)
    yield database
    database.close()


def _insert_price_series(database, asset_id, ticker, closes: dict[str, float]):
    for ts, close in closes.items():
        insert_price(Price(
            asset_id=asset_id, interval="1d", timestamp=ts,
            open=close, high=close, low=close, close=close, volume=0, source="test",
        ))


def test_commodity_universe_asset_ids(mem_db):
    insert_asset(Asset(id=300001, ticker="CL1", name="Crude Oil", asset_class="futures"))
    insert_asset(Asset(id=300002, ticker="NG1", name="Natural Gas", asset_class="futures"))

    ids = CommodityUniverse().asset_ids()

    assert ids["Crude Oil"] == 300001
    assert ids["Natural Gas"] == 300002
    assert "Gold" not in ids  # GC1 never inserted


def test_equity_universe_excludes_index_catalog(mem_db):
    insert_asset(Asset(id=1, ticker="AAPL", name="Apple", asset_class="equity"))
    insert_asset(Asset(id=100001, ticker="^GSPC", name="S&P 500", asset_class="equity"))

    ids = EquityUniverse().asset_ids()

    assert ids == {"AAPL": 1}


def test_returns_panel_native_frequency(mem_db):
    insert_asset(Asset(id=300001, ticker="CL1", name="Crude Oil", asset_class="futures"))
    _insert_price_series(mem_db, 300001, "CL1", {
        "2024-01-01T00:00:00Z": 10.0,
        "2024-01-02T00:00:00Z": 11.0,
        "2024-01-03T00:00:00Z": 9.9,
    })

    panel = CommodityUniverse().returns_panel()

    assert list(panel.columns) == ["Crude Oil"]
    assert panel.shape[0] == 2  # pct_change drops the first native row
    assert panel["Crude Oil"].iloc[0] == pytest.approx(0.1)


def test_returns_panel_resample_matches_weekly_prices_with_trend(mem_db):
    insert_asset(Asset(id=300001, ticker="CL1", name="Crude Oil", asset_class="futures"))

    closes = {}
    price = 10.0
    for i in range(30):
        ts = pd.Timestamp("2024-01-01") + pd.Timedelta(days=i)
        price *= 1.001
        closes[ts.isoformat()] = price
    _insert_price_series(mem_db, 300001, "CL1", closes)

    panel = CommodityUniverse().returns_panel(resample="W-MON")

    prices_raw = pd.DataFrame({
        "ticker": ["CL1"] * len(closes),
        "timestamp": list(closes.keys()),
        "close": list(closes.values()),
    })
    prices_raw["timestamp"] = pd.to_datetime(prices_raw["timestamp"])
    expected_weekly = weekly_prices_with_trend(prices_raw)
    expected_dates = set(expected_weekly["week_date"])

    # returns_panel's pct_change drops the first resampled date (all-NaN row);
    # every other date must match weekly_prices_with_trend()'s own resample exactly.
    assert set(panel.index) == expected_dates - {min(expected_dates)}


def test_returns_panel_empty_universe_returns_empty_frame(mem_db):
    panel = EquityUniverse().returns_panel()
    assert panel.empty
