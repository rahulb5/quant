"""
tests/collectors/test_cot.py

Tests for src/collectors/cot.CotCollector.

Uses an in-memory DuckDB instance (monkeypatched into the module in place of
the global singleton) and stubs out the CFTC Socrata HTTP call, so no network
or filesystem access is required.
"""

from __future__ import annotations

import pytest

import src.collectors.base as base_module
import src.collectors.cot as cot_module
from src.collectors.cot import ENDPOINTS, CotCollector
from src.db.client import Database


@pytest.fixture
def mem_db(monkeypatch) -> Database:
    database = Database(":memory:")
    database.open()
    monkeypatch.setattr(cot_module, "db", database)
    monkeypatch.setattr(base_module, "db", database)  # _log_fetch lives in base.py
    yield database
    database.close()


@pytest.fixture
def collector() -> CotCollector:
    return CotCollector()


_LEGACY_ROW = {
    "report_date_as_yyyy_mm_dd": "2024-01-02T00:00:00.000",
    "market_and_exchange_names": "CRUDE OIL, LIGHT SWEET - NEW YORK MERCANTILE EXCHANGE",
    "cftc_market_code": "067651",
    "commodity_code": "067",
    "exchange_name": "NYME",
    "commodity": "CRUDE OIL, LIGHT SWEET",
    "open_interest_fut": "1000000",
    "noncomm_positions_long_all": "500000",
    "noncomm_positions_short_all": "300000",
    "noncomm_postions_spread_all": "10000",
    "comm_positions_long_all": "400000",
    "comm_positions_short_all": "600000",
    "nonrept_positions_long_all": "5000",
    "nonrept_positions_short_all": "6000",
    "traders_noncomm_long_all": "100",
    "traders_noncomm_short_all": "90",
    "traders_noncomm_spread_all": "20",
    "traders_comm_long_all": "50",
    "traders_comm_short_all": "45",
}


def test_parse_float(collector):
    assert collector._parse_float("123.45") == 123.45
    assert collector._parse_float("") is None
    assert collector._parse_float(None) is None
    assert collector._parse_float("not-a-number") is None


def test_last_db_date_empty_returns_none(mem_db, collector):
    assert collector._last_db_date("legacy_fut") is None


def test_resolve_cutoff_backfill_is_none(collector):
    assert collector._resolve_cutoff("legacy_fut", "backfill") is None


def test_resolve_cutoff_incremental_is_two_weeks_ago(collector):
    from datetime import date, timedelta

    expected = (date.today() - timedelta(weeks=2)).isoformat()
    assert collector._resolve_cutoff("legacy_fut", "incremental") == expected


def test_resolve_cutoff_update_falls_back_when_no_history(mem_db, collector):
    from datetime import date, timedelta

    expected = (date.today() - timedelta(weeks=2)).isoformat()
    assert collector._resolve_cutoff("legacy_fut", "update") == expected


def test_resolve_cutoff_update_uses_last_db_date(mem_db, collector):
    series_id = collector._ensure_series("CFTC.LEGACY_FUT.067651", "Crude Oil")
    mem_db.run(
        """
        INSERT INTO cot_positions (series_id, report_date, release_date, report_type, open_interest)
        VALUES (?, '2024-06-01', '2024-06-04', 'legacy_fut', 1000)
        """,
        [series_id],
    )
    assert collector._resolve_cutoff("legacy_fut", "update") == "2024-06-01"


def test_fetch_report_type_upserts_and_logs(mem_db, collector, monkeypatch):
    monkeypatch.setattr(collector, "_fetch_all_rows", lambda url, cutoff, market_codes: [_LEGACY_ROW])

    rows = collector.fetch_report_type("legacy_fut", mode="backfill")

    assert rows == 1

    stored = mem_db.query(
        "SELECT * FROM cot_positions WHERE report_type = 'legacy_fut'"
    )
    assert len(stored) == 1
    assert stored[0]["leg_nc_long"] == 500000.0
    assert stored[0]["leg_comm_short"] == 600000.0
    assert stored[0]["cftc_market_code"] == "067651"

    series_rows = mem_db.query(
        "SELECT * FROM macro_series WHERE code = 'CFTC.LEGACY_FUT.067651'"
    )
    assert len(series_rows) == 1
    assert series_rows[0]["source"] == "CFTC"

    log_rows = mem_db.query("SELECT * FROM data_fetch_log WHERE source = 'CFTC'")
    assert len(log_rows) == 1
    assert log_rows[0]["status"] == "success"
    assert log_rows[0]["rows_inserted"] == 1


def test_fetch_report_type_upsert_updates_on_conflict(mem_db, collector, monkeypatch):
    monkeypatch.setattr(collector, "_fetch_all_rows", lambda url, cutoff, market_codes: [_LEGACY_ROW])
    collector.fetch_report_type("legacy_fut", mode="backfill")

    updated_row = dict(_LEGACY_ROW, noncomm_positions_long_all="999999")
    monkeypatch.setattr(collector, "_fetch_all_rows", lambda url, cutoff, market_codes: [updated_row])
    collector.fetch_report_type("legacy_fut", mode="backfill")

    stored = mem_db.query("SELECT * FROM cot_positions WHERE report_type = 'legacy_fut'")
    assert len(stored) == 1  # same (series_id, report_date, report_type) — updated, not duplicated
    assert stored[0]["leg_nc_long"] == 999999.0


def test_fetch_report_type_no_rows_returns_zero(mem_db, collector, monkeypatch):
    monkeypatch.setattr(collector, "_fetch_all_rows", lambda url, cutoff, market_codes: [])
    rows = collector.fetch_report_type("legacy_fut", mode="backfill")
    assert rows == 0


def test_fetch_all_tolerates_per_report_failures(mem_db, collector, monkeypatch):
    def fake_fetch_report_type(report_type, mode="update", market_codes=None, targeted=False):
        if report_type == "tff_fut":
            raise RuntimeError("boom")
        return 7

    monkeypatch.setattr(collector, "fetch_report_type", fake_fetch_report_type)

    results = collector.fetch_all(mode="update")

    assert "tff_fut" not in results
    assert all(v == 7 for k, v in results.items())
    assert len(results) == len(ENDPOINTS) - 1


def test_collect_raises_not_implemented(collector):
    with pytest.raises(NotImplementedError):
        collector.collect(from_date="2024-01-01", to_date="2024-01-31")
