"""
tests/collectors/test_currency.py

Tests for src/collectors/currency.CurrencyCollector.

Uses an in-memory DuckDB instance (monkeypatched into the module in place of
the global singleton) and stubs out the Twelve Data HTTP call, so no network
or filesystem access is required. Follows the pattern in tests/db/test_client.py.
"""

from __future__ import annotations

import pytest

import src.collectors.base as base_module
import src.collectors.currency as currency_module
from src.collectors.currency import CurrencyCollector
from src.db.client import Database

PAIR = {"id": 200001, "ticker": "EURUSD", "name": "Euro / US Dollar", "asset_class": "forex"}


@pytest.fixture
def mem_db(monkeypatch) -> Database:
    database = Database(":memory:")
    database.open()
    monkeypatch.setattr(currency_module, "db", database)
    monkeypatch.setattr(base_module, "db", database)  # _log_fetch lives in base.py
    yield database
    database.close()


@pytest.fixture
def collector() -> CurrencyCollector:
    return CurrencyCollector()


def test_to_td_symbol(collector):
    assert collector._to_td_symbol("EURUSD") == "EUR/USD"


def test_ensure_asset_creates_and_reuses(mem_db, collector):
    asset_id_1 = collector.ensure_asset(PAIR)
    asset_id_2 = collector.ensure_asset(PAIR)

    assert asset_id_1 == asset_id_2 == PAIR["id"]
    rows = mem_db.query("SELECT * FROM assets WHERE id = ?", [PAIR["id"]])
    assert len(rows) == 1
    assert rows[0]["asset_class"] == "forex"


def test_last_price_date_defaults_without_history(mem_db, collector):
    asset_id = collector.ensure_asset(PAIR)
    assert collector.last_price_date(asset_id) == "1980-01-01"


def test_collect_requires_asset_id(collector):
    with pytest.raises(ValueError, match="asset_id is required"):
        collector.collect(from_date="2024-01-01", to_date="2024-01-31", asset_id=None)


def test_collect_inserts_prices(mem_db, collector, monkeypatch):
    asset_id = collector.ensure_asset(PAIR)

    canned_rows = [
        {"datetime": "2024-01-02", "open": "1.10", "high": "1.11", "low": "1.09", "close": "1.105", "volume": "0"},
        {"datetime": "2024-01-03", "open": "1.105", "high": "1.12", "low": "1.10", "close": "1.115", "volume": "0"},
    ]
    monkeypatch.setattr(collector, "_fetch_ohlcv", lambda ticker, from_date, to_date: canned_rows)

    rows = collector.collect(from_date="2024-01-02", to_date="2024-01-03", asset_id=asset_id)

    assert rows == 2
    stored = mem_db.query(
        "SELECT * FROM prices WHERE asset_id = ? ORDER BY timestamp", [asset_id]
    )
    assert len(stored) == 2
    assert stored[0]["close"] == 1.105
    assert stored[0]["adj_close"] == 1.105  # adj_close = close for FX
    assert stored[0]["source"] == "twelvedata"


def test_collect_is_idempotent(mem_db, collector, monkeypatch):
    asset_id = collector.ensure_asset(PAIR)
    canned_rows = [
        {"datetime": "2024-01-02", "open": "1.10", "high": "1.11", "low": "1.09", "close": "1.105", "volume": "0"},
    ]
    monkeypatch.setattr(collector, "_fetch_ohlcv", lambda ticker, from_date, to_date: canned_rows)

    collector.collect(from_date="2024-01-02", to_date="2024-01-02", asset_id=asset_id)
    collector.collect(from_date="2024-01-02", to_date="2024-01-02", asset_id=asset_id)

    stored = mem_db.query("SELECT * FROM prices WHERE asset_id = ?", [asset_id])
    assert len(stored) == 1  # ON CONFLICT DO NOTHING


def test_fetch_all_skips_up_to_date_pairs(mem_db, collector, monkeypatch):
    from datetime import date

    asset_id = collector.ensure_asset(PAIR)
    today = date.today().isoformat()
    mem_db.run(
        """INSERT INTO prices
           (asset_id, interval, timestamp, open, high, low, close, volume, source)
           VALUES (?, '1d', ?, 1, 1, 1, 1, 0, 'test')""",
        [asset_id, f"{today} 00:00:00"],
    )

    called = {"n": 0}

    def fake_run(*args, **kwargs):
        called["n"] += 1
        return 0

    monkeypatch.setattr(collector, "run", fake_run)
    monkeypatch.setattr(currency_module.time, "sleep", lambda *_: None)

    results = collector.fetch_all(mode="update", pairs=[PAIR])

    assert called["n"] == 0  # already up to date — run() never called
    assert results == {}
