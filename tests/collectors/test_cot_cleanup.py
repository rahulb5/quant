"""
tests/collectors/test_cot_cleanup.py

Tests for src/collectors/cot_cleanup.purge_exchange_keyed against an
in-memory DuckDB.
"""

from __future__ import annotations

import pytest

from src.collectors.cot_cleanup import purge_exchange_keyed
from src.db.client import Database


@pytest.fixture
def mem_db() -> Database:
    database = Database(":memory:")
    database.open()
    _seed(database)
    yield database
    database.close()


def _series(database, sid, code, source="CFTC"):
    database.run(
        "INSERT INTO macro_series (id, code, name, source, frequency, units) "
        "VALUES (?, ?, 'x', ?, 'weekly', 'contracts')",
        [sid, code, source],
    )


def _seed(database):
    _series(database, 1, "CFTC.LEGACY_FUT.NYME")        # exchange-keyed
    _series(database, 2, "CFTC.LEGACY_FUT.CBT ")        # exchange-keyed, trailing space
    _series(database, 3, "CFTC.DISAGG_FUT.067651")      # contract-keyed
    _series(database, 4, "CFTC.TFF_FUT.13874A")         # contract-keyed, alphanumeric
    _series(database, 5, "DGS10", source="FRED")        # not CFTC: untouched
    for sid, rt in [(1, "legacy_fut"), (2, "legacy_fut"), (3, "disagg_fut"), (4, "tff_fut")]:
        database.run(
            "INSERT INTO cot_positions (series_id, report_date, release_date, report_type) "
            "VALUES (?, '2024-01-02', '2024-01-05', ?)",
            [sid, rt],
        )
    for i, sid in enumerate([1, 3], start=1):
        database.run(
            "INSERT INTO data_fetch_log (id, series_id, fetched_from, fetched_to, source, status) "
            "VALUES (?, ?, '2024-01-01', '2024-01-02', 'CFTC', 'success')",
            [i, sid],
        )


def _n(database, table):
    return database.query(f"SELECT COUNT(*) AS n FROM {table}")[0]["n"]


def test_dry_run_counts_and_changes_nothing(mem_db):
    counts = purge_exchange_keyed(mem_db, dry_run=True)

    assert counts["n_series"] == 2
    assert counts["cot_positions"] == 2
    assert counts["data_fetch_log"] == 1
    assert counts["cot_by_report_type"] == {"legacy_fut": 2}
    assert counts["kept_series"] == 2
    assert (_n(mem_db, "macro_series"), _n(mem_db, "cot_positions"), _n(mem_db, "data_fetch_log")) == (5, 4, 2)


def test_purge_removes_only_exchange_keyed(mem_db):
    purge_exchange_keyed(mem_db, dry_run=False)

    codes = sorted(r["code"] for r in mem_db.query("SELECT code FROM macro_series"))
    assert codes == ["CFTC.DISAGG_FUT.067651", "CFTC.TFF_FUT.13874A", "DGS10"]
    assert _n(mem_db, "cot_positions") == 2
    assert [r["series_id"] for r in mem_db.query("SELECT series_id FROM data_fetch_log")] == [3]


def test_purge_is_idempotent(mem_db):
    purge_exchange_keyed(mem_db, dry_run=False)
    again = purge_exchange_keyed(mem_db, dry_run=False)

    assert again["n_series"] == 0
    assert _n(mem_db, "macro_series") == 3
