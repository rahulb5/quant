"""
tests/collectors/test_asset_id_cleanup.py

Tests for src/collectors/asset_id_cleanup against an in-memory DuckDB.
"""

from __future__ import annotations

import pytest

from src.collectors.asset_id_cleanup import misplaced_equities, renumber_misplaced_equities
from src.db.client import Database


@pytest.fixture
def mem_db() -> Database:
    database = Database(":memory:")
    database.open()
    _seed(database)
    yield database
    database.close()


def _asset(database, aid, ticker, asset_class="equity"):
    database.run(
        "INSERT INTO assets (id, ticker, name, asset_class) VALUES (?, ?, ?, ?)",
        [aid, ticker, f"{ticker} name", asset_class],
    )


def _price(database, aid, day, close):
    database.run(
        "INSERT INTO prices (asset_id, interval, timestamp, open, high, low, close, source) "
        "VALUES (?, '1d', ?, ?, ?, ?, ?, 'yahoo')",
        [aid, day, close, close, close, close],
    )


def _seed(database):
    _asset(database, 2, "ZTS")                     # constituent: untouched
    _asset(database, 100_001, "^GSPC")             # index catalog: untouched
    _asset(database, 400_011, "SPGSCCP", "index")  # GSCI: untouched
    _asset(database, 400_012, "APP")               # misplaced
    _asset(database, 400_013, "ARES")              # misplaced
    for aid in (2, 400_011, 400_012, 400_013):
        _price(database, aid, "2024-01-02", 10.0)
        _price(database, aid, "2024-01-03", 11.0)
    database.run(
        "INSERT INTO data_fetch_log (id, asset_id, fetched_from, fetched_to, source, status) "
        "VALUES (1, 400012, '2024-01-01', '2024-01-03', 'yahoo', 'success')"
    )


def _ids(database):
    return {r["ticker"]: r["id"] for r in database.query("SELECT id, ticker FROM assets")}


def test_dry_run_plans_and_changes_nothing(mem_db):
    plan = renumber_misplaced_equities(mem_db, dry_run=True)

    assert [(p["ticker"], p["old_id"], p["new_id"], p["prices"], p["data_fetch_log"]) for p in plan] == [
        ("APP", 400_012, 3, 2, 1),
        ("ARES", 400_013, 4, 2, 0),
    ]
    assert _ids(mem_db)["APP"] == 400_012


def test_renumber_moves_assets_prices_and_log(mem_db):
    renumber_misplaced_equities(mem_db, dry_run=False)

    ids = _ids(mem_db)
    assert ids == {"ZTS": 2, "^GSPC": 100_001, "SPGSCCP": 400_011, "APP": 3, "ARES": 4}
    closes = mem_db.query("SELECT close FROM prices WHERE asset_id = 3 ORDER BY timestamp")
    assert [r["close"] for r in closes] == [10.0, 11.0]
    assert mem_db.query("SELECT COUNT(*) AS n FROM prices WHERE asset_id >= 400012")[0]["n"] == 0
    assert mem_db.query("SELECT asset_id FROM data_fetch_log")[0]["asset_id"] == 3
    assert mem_db.query("SELECT name FROM assets WHERE id = 3")[0]["name"] == "APP name"
    assert misplaced_equities(mem_db) == []


def test_renumber_is_idempotent(mem_db):
    renumber_misplaced_equities(mem_db, dry_run=False)
    assert renumber_misplaced_equities(mem_db, dry_run=False) == []
    assert _ids(mem_db)["ARES"] == 4


def test_resumes_after_interrupted_run(mem_db):
    # Simulate a crash after the new asset row and half the prices were written.
    mem_db.run("INSERT INTO assets (id, ticker, name, asset_class) VALUES (3, 'APP', 'APP name', 'equity')")
    _price(mem_db, 3, "2024-01-02", 10.0)

    plan = renumber_misplaced_equities(mem_db, dry_run=False)

    assert [(p["ticker"], p["new_id"]) for p in plan] == [("APP", 3), ("ARES", 4)]
    assert mem_db.query("SELECT COUNT(*) AS n FROM prices WHERE asset_id = 3")[0]["n"] == 2
    assert misplaced_equities(mem_db) == []


def test_exchange_set_raises(mem_db):
    mem_db.run(
        "INSERT INTO assets (id, ticker, name, asset_class, exchange) "
        "VALUES (400014, 'BNY', 'BNY', 'equity', 'NYSE')"
    )
    with pytest.raises(ValueError, match="exchange set"):
        renumber_misplaced_equities(mem_db, dry_run=True)
