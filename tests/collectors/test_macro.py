"""
tests/collectors/test_macro.py

Tests for src/collectors/macro.MacroCollector.

Uses an in-memory DuckDB instance (monkeypatched into the module in place of
the global singleton) and stubs out the FRED client, so no network or
filesystem access is required.
"""

from __future__ import annotations

import pandas as pd
import pytest

import src.collectors.base as base_module
import src.collectors.macro as macro_module
from src.collectors.macro import FULL_HISTORY_SENTINEL, MacroCollector
from src.db.client import Database


@pytest.fixture
def mem_db(monkeypatch) -> Database:
    database = Database(":memory:")
    database.open()
    monkeypatch.setattr(macro_module, "db", database)
    monkeypatch.setattr(base_module, "db", database)  # _log_fetch lives in base.py
    yield database
    database.close()


@pytest.fixture
def collector(monkeypatch) -> MacroCollector:
    monkeypatch.setenv("FRED_API_KEY", "dummy")
    return MacroCollector()


def _fake_series_info(**overrides) -> pd.Series:
    info = {
        "title": "Test Series",
        "frequency": "Monthly",
        "units_short": "Index",
        "seasonal_adjustment_short": "SA",
        "notes": "some notes",
    }
    info.update(overrides)
    return pd.Series(info)


def test_ensure_series_creates_and_reuses(mem_db, collector, monkeypatch):
    monkeypatch.setattr(collector.fred, "get_series_info", lambda code: _fake_series_info())

    series_id_1 = collector.ensure_series("TESTCODE")
    series_id_2 = collector.ensure_series("TESTCODE")

    assert series_id_1 == series_id_2
    rows = mem_db.query("SELECT * FROM macro_series WHERE code = 'TESTCODE'")
    assert len(rows) == 1
    assert rows[0]["frequency"] == "monthly"
    assert rows[0]["seasonal_adj"] is True


def test_ensure_series_maps_daily_frequency(mem_db, collector, monkeypatch):
    monkeypatch.setattr(
        collector.fred, "get_series_info",
        lambda code: _fake_series_info(frequency="Daily", seasonal_adjustment_short="NSA"),
    )
    series_id = collector.ensure_series("DAILYCODE")
    assert collector._is_daily(series_id) is True
    rows = mem_db.query("SELECT seasonal_adj FROM macro_series WHERE id = ?", [series_id])
    assert rows[0]["seasonal_adj"] is False


def test_get_code_round_trip(mem_db, collector, monkeypatch):
    monkeypatch.setattr(collector.fred, "get_series_info", lambda code: _fake_series_info())
    series_id = collector.ensure_series("ROUNDTRIP")
    assert collector._get_code(series_id) == "ROUNDTRIP"


def test_fetch_daily_sets_release_date_equal_to_period_date(mem_db, collector, monkeypatch):
    monkeypatch.setattr(
        collector.fred, "get_series_info",
        lambda code: _fake_series_info(frequency="Daily"),
    )
    series_id = collector.ensure_series("DAILYCODE")

    fake_series = pd.Series(
        [1.0, 2.0],
        index=pd.to_datetime(["2024-01-01", "2024-01-02"]),
    )
    monkeypatch.setattr(collector.fred, "get_series", lambda code, observation_start=None: fake_series)

    df = collector._fetch_daily(series_id, cutoff=None)

    assert {"period_date", "value", "release_date"}.issubset(df.columns)
    assert (df["period_date"] == df["release_date"]).all()
    assert len(df) == 2


def test_fetch_vintages_applies_cutoff(mem_db, collector, monkeypatch):
    monkeypatch.setattr(collector.fred, "get_series_info", lambda code: _fake_series_info())
    series_id = collector.ensure_series("VINTAGECODE")

    raw = pd.DataFrame({
        "date": pd.to_datetime(["2023-01-01", "2024-01-01", "2024-02-01"]),
        "realtime_start": pd.to_datetime(["2023-01-05", "2024-01-05", "2024-02-05"]),
        "value": [1.0, 2.0, 3.0],
    })
    monkeypatch.setattr(collector.fred, "get_series_all_releases", lambda code: raw)

    df = collector._fetch_vintages(series_id, cutoff="2024-01-01")

    assert len(df) == 2
    assert set(df.columns) == {"period_date", "release_date", "value"}


def test_upsert_observations_marks_latest_release_final(mem_db, collector, monkeypatch):
    monkeypatch.setattr(collector.fred, "get_series_info", lambda code: _fake_series_info())
    series_id = collector.ensure_series("REVCODE")

    df = pd.DataFrame({
        "period_date": pd.to_datetime(["2024-01-01", "2024-01-01"]),
        "release_date": pd.to_datetime(["2024-01-05", "2024-02-05"]),
        "value": [100.0, 101.0],
    })

    count = collector._upsert_observations(series_id, df)
    assert count == 2

    rows = mem_db.query(
        "SELECT release_date, value, is_final FROM macro_observations "
        "WHERE series_id = ? ORDER BY release_date",
        [series_id],
    )
    assert rows[0]["is_final"] is False
    assert rows[1]["is_final"] is True
    assert rows[1]["value"] == 101.0


def test_collect_dispatches_to_daily_fetch(mem_db, collector, monkeypatch):
    monkeypatch.setattr(
        collector.fred, "get_series_info",
        lambda code: _fake_series_info(frequency="Daily"),
    )
    series_id = collector.ensure_series("DAILYCODE")

    called = {}

    def fake_fetch_daily(sid, cutoff):
        called["sid"] = sid
        called["cutoff"] = cutoff
        return pd.DataFrame({"period_date": [], "release_date": [], "value": []})

    monkeypatch.setattr(collector, "_fetch_daily", fake_fetch_daily)

    rows = collector.collect(from_date=FULL_HISTORY_SENTINEL, to_date="2024-01-01", series_id=series_id)

    assert rows == 0
    assert called["sid"] == series_id
    assert called["cutoff"] is None  # sentinel maps to no cutoff


def test_collect_requires_series_id(collector):
    with pytest.raises(ValueError, match="series_id is required"):
        collector.collect(from_date="2024-01-01", to_date="2024-01-31", series_id=None)
