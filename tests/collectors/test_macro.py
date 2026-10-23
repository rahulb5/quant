"""
tests/collectors/test_macro.py

Tests for src/collectors/macro.MacroCollector.

Uses an in-memory DuckDB instance (monkeypatched into the module in place of
the global singleton) and stubs out the FRED client, so no network or
filesystem access is required.
"""

from __future__ import annotations

import json

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
    monkeypatch.setattr(collector, "_fetch_all_releases", lambda code: raw)

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


# ── Frequency mapping / vintage mode (stamped-row bug) ────────────────────────

@pytest.mark.parametrize("raw,expected", [
    ("Daily", "daily"),
    ("Daily, 7-Day", "daily"),
    ("Weekly, Ending Saturday", "weekly"),
    ("Weekly, As of Wednesday", "weekly"),
    ("Monthly, End of Period", "monthly"),
    ("Quarterly", "quarterly"),
    ("Annual", "annual"),
])
def test_frequency_from_fred_matches_leading_word(raw, expected):
    from src.collectors.macro import frequency_from_fred
    assert frequency_from_fred(raw) == expected


def test_frequency_from_fred_rejects_unknown():
    from src.collectors.macro import frequency_from_fred
    with pytest.raises(ValueError, match="Unrecognised FRED frequency"):
        frequency_from_fred("Fortnightly")


def test_weekly_series_with_qualified_frequency_uses_vintage_mode(mem_db, collector, monkeypatch):
    monkeypatch.setattr(
        collector.fred, "get_series_info",
        lambda code: _fake_series_info(frequency="Weekly, Ending Saturday"),
    )
    series_id = collector.ensure_series("ICSA")

    assert mem_db.query("SELECT frequency FROM macro_series WHERE id = ?", [series_id])[0]["frequency"] == "weekly"
    assert collector._is_daily(series_id) is False


def test_revised_daily_series_uses_vintage_mode(mem_db, collector, monkeypatch):
    monkeypatch.setattr(collector.fred, "get_series_info", lambda code: _fake_series_info(frequency="Daily"))
    dollar = collector.ensure_series("DTWEXBGS")
    market = collector.ensure_series("DGS10")

    assert collector._is_daily(dollar) is False
    assert collector._is_daily(market) is True


def test_series_catalogue_is_consistent():
    from src.collectors.macro import REVISED_DAILY, SERIES_CODES
    from src.macro.pit import RELEASE_LAG_OVERRIDES, UNREVISED_SERIES

    assert len(SERIES_CODES) == len(set(SERIES_CODES))
    assert REVISED_DAILY <= set(SERIES_CODES)
    assert set(UNREVISED_SERIES) <= set(SERIES_CODES)
    # ICE OAS history is archived (FRED serves 3 years); overrides must name catalogued series
    assert set(RELEASE_LAG_OVERRIDES) - {"BAMLEMCBPIOAS"} <= set(SERIES_CODES)


# ── Paginated vintage fetch (FRED caps a request at 100,000 rows) ─────────────

class _FakeResponse:
    def __init__(self, payload):
        import io
        self._buf = io.BytesIO(json.dumps(payload).encode())

    def read(self, *a):
        return self._buf.read(*a)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def test_fetch_all_releases_pages_until_count(collector, monkeypatch):
    obs = [{"date": f"2024-01-0{i}", "realtime_start": "2024-02-01", "realtime_end": "9999-12-31",
            "value": str(float(i))} for i in range(1, 6)]
    obs.append({"date": "2024-01-06", "realtime_start": "2024-02-01", "realtime_end": "9999-12-31", "value": "."})
    pages = {0: obs[:4], 4: obs[4:]}
    seen = []

    def fake_urlopen(url):
        offset = int(url.split("offset=")[1].split("&")[0])
        seen.append(offset)
        return _FakeResponse({"count": len(obs), "observations": pages[offset]})

    monkeypatch.setattr(macro_module, "urlopen", fake_urlopen)
    monkeypatch.setattr(collector.fred, "api_key", "k")

    df = collector._fetch_all_releases("NFCI")

    assert seen == [0, 4]
    assert len(df) == 6 and df["value"].isna().sum() == 1
    assert list(df.columns) == ["date", "realtime_start", "value"]


def test_fetch_all_releases_empty_raises(collector, monkeypatch):
    monkeypatch.setattr(macro_module, "urlopen", lambda url: _FakeResponse({"count": 0, "observations": []}))
    with pytest.raises(ValueError, match="No data exists"):
        collector._fetch_all_releases("NOPE")


def test_upsert_is_idempotent_and_dedupes(mem_db, collector, monkeypatch):
    monkeypatch.setattr(collector.fred, "get_series_info", lambda code: _fake_series_info())
    series_id = collector.ensure_series("BULK")
    df = pd.DataFrame({
        "period_date": pd.to_datetime(["2024-01-01", "2024-01-01", "2024-01-01"]),
        "release_date": pd.to_datetime(["2024-01-05", "2024-02-05", "2024-02-05"]),   # duplicate key
        "value": [100.0, 101.0, 102.0],
    })
    assert collector._upsert_observations(series_id, df) == 2
    assert collector._upsert_observations(series_id, df) == 2
    rows = mem_db.query("SELECT release_date, value, is_final FROM macro_observations ORDER BY release_date")
    assert [(r["value"], r["is_final"]) for r in rows] == [(100.0, False), (102.0, True)]
