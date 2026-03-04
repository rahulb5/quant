"""
tests/db/test_client.py

Tests that migrations run correctly and produce the expected schema.
Uses an in-memory DuckDB instance so nothing touches the filesystem.

Run with: pytest
"""

import pytest
from src.db.client import Database
from src.db.migrations import MIGRATIONS


@pytest.fixture(scope="module")
def db():
    database = Database(":memory:")
    database.open()
    yield database
    database.close()


# ── Migrations ────────────────────────────────────────────────────────────────

def test_migrations_applied_without_error(db):
    # If the fixture didn't raise, migrations ran successfully
    assert True


def test_schema_version_records_all_migrations(db):
    rows = db.query("SELECT version, description FROM schema_version ORDER BY version")
    assert len(rows) == len(MIGRATIONS)
    assert rows[0]["version"] == 1
    assert rows[-1]["version"] == MIGRATIONS[-1].version


def test_migrations_idempotent():
    db2 = Database(":memory:")
    db2.open()
    db2.open()  # second call should be a no-op
    rows = db2.query("SELECT version FROM schema_version ORDER BY version")
    assert len(rows) == len(MIGRATIONS)
    db2.close()


# ── Schema — table existence ──────────────────────────────────────────────────

@pytest.mark.parametrize("table", [
    "schema_version",
    "assets",
    "prices",
    "macro_series",
    "macro_observations",
    "data_fetch_log",
])
def test_table_exists(db, table):
    rows = db.query(
        "SELECT table_name FROM information_schema.tables WHERE table_name = ?",
        [table],
    )
    assert len(rows) == 1


# ── Schema — column checks ────────────────────────────────────────────────────

def test_assets_columns(db):
    rows = db.query(
        "SELECT column_name FROM information_schema.columns WHERE table_name = 'assets'"
    )
    cols = [r["column_name"] for r in rows]
    assert "id" in cols
    assert "ticker" in cols
    assert "asset_class" in cols
    assert "is_active" in cols


def test_prices_columns(db):
    rows = db.query(
        "SELECT column_name FROM information_schema.columns WHERE table_name = 'prices'"
    )
    cols = [r["column_name"] for r in rows]
    assert "asset_id" in cols
    assert "interval" in cols
    assert "timestamp" in cols
    assert "adj_close" in cols
    assert "vwap" in cols


def test_macro_observations_columns(db):
    rows = db.query(
        "SELECT column_name FROM information_schema.columns WHERE table_name = 'macro_observations'"
    )
    cols = [r["column_name"] for r in rows]
    assert "period_date" in cols
    assert "release_date" in cols
    assert "is_final" in cols


# ── Database helpers ──────────────────────────────────────────────────────────

def test_query_returns_dicts(db):
    rows = db.query("SELECT 42 AS n")
    assert rows[0]["n"] == 42


def test_transaction_commits_on_success(db):
    def steps(q):
        q("INSERT INTO assets (id, ticker, name, asset_class, currency) VALUES (1, 'TEST', 'Test Asset', 'equity', 'USD')")

    db.transaction(steps)
    rows = db.query("SELECT ticker FROM assets WHERE ticker = 'TEST'")
    assert len(rows) == 1


def test_transaction_rolls_back_on_error(db):
    with pytest.raises(RuntimeError, match="forced rollback"):
        def steps(q):
            q("INSERT INTO assets (id, ticker, name, asset_class, currency) VALUES (2, 'ROLLBACK_TEST', 'Should Not Exist', 'equity', 'USD')")
            raise RuntimeError("forced rollback")

        db.transaction(steps)

    rows = db.query("SELECT ticker FROM assets WHERE ticker = 'ROLLBACK_TEST'")
    assert len(rows) == 0
