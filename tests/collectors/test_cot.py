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
from src.collectors.cot import COT_CONTRACTS, ENDPOINTS, CotCollector, catalog_codes
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
    "cftc_contract_market_code": "067651",
    "cftc_market_code": "NYME",   # exchange code: must never be used as the series key
    "commodity_code": "067",
    "exchange_name": "NYME",
    "commodity": "CRUDE OIL, LIGHT SWEET",
    "open_interest_all": "1000000",
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
    assert stored[0]["cftc_market_code"] == "067651"   # column now holds the contract code
    assert stored[0]["exchange_name"] == "NYME"

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
    def fake_fetch_report_type(report_type, mode="update", market_codes=None):
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


# ── Contract keying ───────────────────────────────────────────────────────────

def test_two_contracts_on_one_exchange_stay_separate(mem_db, collector, monkeypatch):
    natgas = dict(
        _LEGACY_ROW,
        cftc_contract_market_code="023651",
        market_and_exchange_names="NAT GAS NYME - NEW YORK MERCANTILE EXCHANGE",
        noncomm_positions_long_all="111",
    )
    monkeypatch.setattr(collector, "_fetch_all_rows", lambda url, cutoff, market_codes: [_LEGACY_ROW, natgas])

    collector.fetch_report_type("legacy_fut", mode="backfill")

    stored = mem_db.query(
        "SELECT ms.code, p.leg_nc_long FROM cot_positions p "
        "JOIN macro_series ms ON ms.id = p.series_id ORDER BY ms.code"
    )
    assert [r["code"] for r in stored] == ["CFTC.LEGACY_FUT.023651", "CFTC.LEGACY_FUT.067651"]
    assert [r["leg_nc_long"] for r in stored] == [111.0, 500000.0]


def test_contract_code_whitespace_is_trimmed(mem_db, collector, monkeypatch):
    padded = dict(_LEGACY_ROW, cftc_contract_market_code=" 067651 ")
    monkeypatch.setattr(collector, "_fetch_all_rows", lambda url, cutoff, market_codes: [_LEGACY_ROW, padded])

    collector.fetch_report_type("legacy_fut", mode="backfill")

    codes = [r["code"] for r in mem_db.query("SELECT code FROM macro_series")]
    assert codes == ["CFTC.LEGACY_FUT.067651"]


def test_row_without_contract_code_is_skipped_with_warning(mem_db, collector, monkeypatch, caplog):
    missing = {k: v for k, v in _LEGACY_ROW.items() if k != "cftc_contract_market_code"}
    monkeypatch.setattr(collector, "_fetch_all_rows", lambda url, cutoff, market_codes: [missing])

    with caplog.at_level("WARNING"):
        rows = collector.fetch_report_type("legacy_fut", mode="backfill")

    assert rows == 0
    assert mem_db.query("SELECT COUNT(*) AS n FROM macro_series")[0]["n"] == 0
    assert "skipped 1 row" in caplog.text


def test_catalog_contract_gets_catalog_name_and_group(mem_db, collector, monkeypatch):
    monkeypatch.setattr(collector, "_fetch_all_rows", lambda url, cutoff, market_codes: [_LEGACY_ROW])

    collector.fetch_report_type("legacy_fut", mode="backfill")

    series = mem_db.query("SELECT name, description FROM macro_series")[0]
    assert series["name"] == "Crude Oil WTI"
    assert series["description"] == "COT energy"


# ── Catalog ───────────────────────────────────────────────────────────────────

def test_catalog_codes_filters_by_report_type():
    disagg = catalog_codes("disagg_fut")
    tff = catalog_codes("tff_fut")
    assert "067651" in disagg and "067651" not in tff
    assert "13874A" in tff and "13874A" not in disagg
    assert set(catalog_codes("legacy_fut")) == set(COT_CONTRACTS)


def test_catalog_codes_subset_and_unknown():
    assert catalog_codes("tff_fut", ["13874A", "067651"]) == ["13874A"]
    with pytest.raises(KeyError):
        catalog_codes("tff_fut", ["NOPE00"])


def test_catalog_codes_are_unique_six_char_codes():
    assert all(len(c) == 6 for c in COT_CONTRACTS)
    assert len(COT_CONTRACTS) == 35


def test_fetch_all_requests_only_catalog_codes(mem_db, collector, monkeypatch):
    seen: dict[str, list[str]] = {}

    def fake_fetch_all_rows(url, cutoff, market_codes):
        seen[url] = market_codes
        return []

    monkeypatch.setattr(collector, "_fetch_all_rows", fake_fetch_all_rows)

    collector.fetch_all(mode="backfill")

    by_type = {e["report_type"]: seen[e["url"]] for e in ENDPOINTS}
    assert set(by_type["disagg_fut"]) == set(catalog_codes("disagg_fut"))
    assert set(by_type["tff_fut"]) == set(catalog_codes("tff_fut"))


def test_fetch_all_contract_subset_skips_uncovered_report_types(mem_db, collector, monkeypatch):
    calls: list[tuple[str, list[str]]] = []

    def fake_fetch_report_type(report_type, mode="update", market_codes=None):
        calls.append((report_type, market_codes))
        return 0

    monkeypatch.setattr(collector, "fetch_report_type", fake_fetch_report_type)

    collector.fetch_all(mode="backfill", contracts=["13874A"])

    assert sorted(r for r, _ in calls) == ["legacy_comb", "legacy_fut", "tff_fut"]
    assert all(codes == ["13874A"] for _, codes in calls)


_TFF_ROW = {
    "report_date_as_yyyy_mm_dd": "2024-01-02T00:00:00.000",
    "market_and_exchange_names": "E-MINI S&P 500 - CHICAGO MERCANTILE EXCHANGE",
    "cftc_contract_market_code": "13874A",
    "cftc_market_code": "CME",
    "exchange_name": "CME",
    "open_interest_all": "2000000",
    # Real TFF field names: no `_all` suffix on these.
    "lev_money_positions_long": "120000",
    "lev_money_positions_short": "310000",
    "asset_mgr_positions_long": "900000",
    "asset_mgr_positions_short": "100000",
    "other_rept_positions_long": "5",
    "traders_lev_money_spread": "7",
    "dealer_positions_long_all": "4",
}


def test_tff_row_populates_positions_and_open_interest(mem_db, collector, monkeypatch):
    monkeypatch.setattr(collector, "_fetch_all_rows", lambda url, cutoff, market_codes: [_TFF_ROW])

    collector.fetch_report_type("tff_fut", mode="backfill")

    row = mem_db.query("SELECT * FROM cot_positions WHERE report_type = 'tff_fut'")[0]
    assert row["open_interest"] == 2_000_000.0
    assert row["tff_lf_long"] == 120000.0 and row["tff_lf_short"] == 310000.0
    assert row["tff_am_long"] == 900000.0
    assert row["tff_or_long"] == 5.0
    assert row["tff_lf_traders_spread"] == 7.0
    assert row["tff_dealer_long"] == 4.0


def test_legacy_futures_only_open_interest_populated(mem_db, collector, monkeypatch):
    row = {k: v for k, v in _LEGACY_ROW.items() if k != "open_interest_fut"}
    row["open_interest_all"] = "1234"
    monkeypatch.setattr(collector, "_fetch_all_rows", lambda url, cutoff, market_codes: [row])

    collector.fetch_report_type("legacy_fut", mode="backfill")

    assert mem_db.query("SELECT open_interest FROM cot_positions")[0]["open_interest"] == 1234.0
