"""
tests/collectors/test_fred_cleanup.py

Tests for src/collectors/fred_cleanup.purge_stamped_rows against an
in-memory DuckDB.
"""

from __future__ import annotations

import pytest

from src.collectors.fred_cleanup import fix_labels, purge_stamped_rows
from src.db.client import Database


@pytest.fixture
def mem_db() -> Database:
    database = Database(":memory:")
    database.open()
    _seed(database)
    yield database
    database.close()


def _series(database, sid, code, freq):
    database.run(
        "INSERT INTO macro_series (id, code, name, source, frequency, units) VALUES (?, ?, 'x', 'FRED', ?, 'n')",
        [sid, code, freq],
    )


def _obs(database, sid, period, release, value, final=False):
    database.run(
        "INSERT INTO macro_observations VALUES (?, ?, ?, ?, ?)", [sid, period, release, value, final]
    )


def _seed(database):
    _series(database, 1, "ICSA", "daily")        # mislabelled weekly
    _series(database, 2, "DGS10", "daily")       # genuine daily market series: untouched
    # 1995 week: stamped only (before vintages begin)
    _obs(database, 1, "1995-03-04", "1995-03-04", 330000, True)
    # 2024 week: stamped (revised value) + real first print + real revision
    _obs(database, 1, "2024-03-02", "2024-03-02", 212000, True)
    _obs(database, 1, "2024-03-02", "2024-03-07", 217000)
    _obs(database, 1, "2024-03-02", "2024-03-14", 210000)
    _obs(database, 2, "2024-03-01", "2024-03-01", 4.1, True)


def _rows(database, sid, period):
    return [(str(r["release_date"]), r["value"], r["is_final"]) for r in database.query(
        "SELECT release_date, value, is_final FROM macro_observations "
        "WHERE series_id = ? AND period_date = ? ORDER BY release_date", [sid, period])]


def test_dry_run_counts_and_changes_nothing(mem_db):
    res = purge_stamped_rows(mem_db, codes=["ICSA"], dry_run=True)

    assert res["ICSA"] == {"stamped_deleted": 1, "stamped_kept": 1, "real_rows": 2}
    assert len(_rows(mem_db, 1, "2024-03-02")) == 3


def test_purge_deletes_only_stamped_rows_with_a_real_release(mem_db):
    purge_stamped_rows(mem_db, codes=["ICSA"], dry_run=False)

    assert _rows(mem_db, 1, "2024-03-02") == [
        ("2024-03-07", 217000.0, False),   # first print survives: this is what "first" returns
        ("2024-03-14", 210000.0, True),
    ]
    assert _rows(mem_db, 1, "1995-03-04") == [("1995-03-04", 330000.0, True)]  # pre-vintage kept


def test_fix_labels_dry_run_then_apply(mem_db):
    assert fix_labels(mem_db, dry_run=True) == {"ICSA": ("daily", "weekly")}
    assert mem_db.query("SELECT frequency FROM macro_series WHERE id = 1")[0]["frequency"] == "daily"

    fix_labels(mem_db, dry_run=False)

    assert mem_db.query("SELECT frequency FROM macro_series WHERE id = 1")[0]["frequency"] == "weekly"
    assert mem_db.query("SELECT frequency FROM macro_series WHERE id = 2")[0]["frequency"] == "daily"
    assert fix_labels(mem_db, dry_run=False) == {}   # idempotent


def test_other_series_untouched(mem_db):
    purge_stamped_rows(mem_db, dry_run=False)   # default codes include ICSA, skip absent ones
    assert _rows(mem_db, 2, "2024-03-01") == [("2024-03-01", 4.1, True)]


def test_purge_is_idempotent(mem_db):
    purge_stamped_rows(mem_db, codes=["ICSA"], dry_run=False)
    again = purge_stamped_rows(mem_db, codes=["ICSA"], dry_run=False)

    assert again["ICSA"]["stamped_deleted"] == 0
    assert again["ICSA"]["stamped_kept"] == 1
    assert len(_rows(mem_db, 1, "2024-03-02")) == 2


def test_first_vintage_is_the_real_first_print_after_purge(mem_db):
    from src.macro.data import load_fred

    purge_stamped_rows(mem_db, codes=["ICSA"], dry_run=False)
    s = load_fred(mem_db, "ICSA", vintage="first")

    assert s.loc["2024-03-02"] == 217000.0
    assert str(s.attrs["release_date"].loc["2024-03-02"].date()) == "2024-03-07"
