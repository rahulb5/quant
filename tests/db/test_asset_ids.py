"""
tests/db/test_asset_ids.py

Tests for src/db/asset_ids.next_asset_id and the equity/commodity collectors'
use of it, against an in-memory DuckDB.
"""

from __future__ import annotations

import pytest

import src.collectors.commodity as commodity_module
import src.collectors.equity as equity_module
from src.collectors.commodity import CommodityCollector
from src.collectors.equity import EquityCollector
from src.db.asset_ids import next_asset_id
from src.db.client import Database


@pytest.fixture
def mem_db(monkeypatch) -> Database:
    database = Database(":memory:")
    database.open()
    monkeypatch.setattr(equity_module, "db", database)
    monkeypatch.setattr(commodity_module, "db", database)
    yield database
    database.close()


def _asset(database, aid, ticker, asset_class):
    database.run(
        "INSERT INTO assets (id, ticker, name, asset_class) VALUES (?, ?, 'x', ?)",
        [aid, ticker, asset_class],
    )


def test_empty_block_starts_at_its_first_id(mem_db):
    assert next_asset_id(mem_db, "equity") == 1
    assert next_asset_id(mem_db, "futures") == 300_001


def test_ignores_ids_in_other_blocks(mem_db):
    _asset(mem_db, 503, "ZTS", "equity")
    _asset(mem_db, 100_015, "^BVSP", "equity")
    _asset(mem_db, 300_047, "ZS1", "futures")
    _asset(mem_db, 400_011, "SPGSCCP", "index")

    assert next_asset_id(mem_db, "equity") == 504
    assert next_asset_id(mem_db, "futures") == 300_048


def test_unknown_block_raises(mem_db):
    with pytest.raises(ValueError, match="Unknown asset id block"):
        next_asset_id(mem_db, "crypto")


def test_full_block_raises(mem_db):
    _asset(mem_db, 99_999, "LAST", "equity")
    with pytest.raises(ValueError, match="is full"):
        next_asset_id(mem_db, "equity")


def test_equity_collector_allocates_in_equity_block(mem_db):
    _asset(mem_db, 503, "ZTS", "equity")
    _asset(mem_db, 400_011, "SPGSCCP", "index")

    assert EquityCollector().ensure_asset("APP", "AppLovin") == 504


def test_commodity_collector_allocates_in_futures_block(mem_db):
    _asset(mem_db, 300_047, "ZS1", "futures")
    _asset(mem_db, 400_011, "SPGSCCP", "index")

    assert CommodityCollector().ensure_asset("HO1", "Heating Oil") == 300_048
