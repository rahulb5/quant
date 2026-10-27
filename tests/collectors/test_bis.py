"""
tests/collectors/test_bis.py

Tests for src/collectors/bis.BisPolicyRateCollector. In-memory DuckDB, and
the HTTP fetch is stubbed, so no network access.
"""

from __future__ import annotations

import pytest

import src.collectors.base as base_module
import src.collectors.bis as bis_module
from src.collectors.bis import BisPolicyRateCollector, parse_csv
from src.db.client import Database

CSV = """FREQ,REF_AREA,TIME_PERIOD,OBS_VALUE
D,XM,2026-10-17,2.5
D,XM,2026-10-18,NaN
D,XM,2026-10-19,2.5
D,XM,2026-10-20,2.25
"""


@pytest.fixture
def mem_db(monkeypatch) -> Database:
    database = Database(":memory:")
    database.open()
    monkeypatch.setattr(bis_module, "db", database)
    monkeypatch.setattr(base_module, "db", database)
    yield database
    database.close()


def test_parse_csv_drops_missing_days():
    df = parse_csv(CSV)
    assert list(df["value"]) == [2.5, 2.5, 2.25]
    assert str(df["period_date"].iloc[-1]) == "2026-10-20"


def test_parse_csv_empty_text():
    assert parse_csv("").empty


def test_ensure_series_registers_once(mem_db):
    c = BisPolicyRateCollector()
    assert c.ensure_series("XM") == c.ensure_series("XM")
    row = mem_db.query("SELECT code, source, frequency FROM macro_series")[0]
    assert row == {"code": "BIS.CBPOL.XM", "source": "BIS", "frequency": "daily"}


def test_ensure_series_rejects_unknown_area(mem_db):
    with pytest.raises(ValueError):
        BisPolicyRateCollector().ensure_series("ZZ")


def test_fetch_all_upserts_and_is_idempotent(mem_db, monkeypatch):
    c = BisPolicyRateCollector()
    calls = []
    monkeypatch.setattr(c, "_fetch_csv", lambda area, start, end: calls.append((area, start)) or CSV)

    assert c.fetch_all(areas=["XM"]) == {"XM": 3}
    revised = CSV.replace("2026-10-20,2.25", "2026-10-20,2.0")
    monkeypatch.setattr(c, "_fetch_csv", lambda area, start, end: revised)
    c.fetch_all(areas=["XM"], full_history=True)

    rows = mem_db.query("SELECT period_date, release_date, value, is_final FROM macro_observations ORDER BY 1")
    assert len(rows) == 3
    assert rows[-1]["value"] == 2.0
    assert all(r["period_date"] == r["release_date"] and r["is_final"] for r in rows)
    assert calls[0][1] is not None   # incremental run passes a start date


def test_fetch_all_tolerates_failure(mem_db, monkeypatch):
    c = BisPolicyRateCollector()

    def fake(area, start, end):
        if area == "JP":
            raise RuntimeError("boom")
        return CSV

    monkeypatch.setattr(c, "_fetch_csv", fake)
    assert set(c.fetch_all(areas=["XM", "JP"])) == {"XM"}
    log = mem_db.query("SELECT status FROM data_fetch_log ORDER BY id")
    assert [r["status"] for r in log] == ["success", "failed"]
