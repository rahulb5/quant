"""
tests/backtest/test_reference.py

Tests for src/backtest/reference.ReferenceData.

Uses an in-memory DuckDB instance (injected via the database= parameter) so no
filesystem access is required.  Follows the pattern in tests/db/test_client.py.

Run with: pytest tests/backtest/test_reference.py -v
"""

from __future__ import annotations

import logging

import pandas as pd
import pytest

from src.backtest.reference import DEFAULT_FACTORS, Factor, ReferenceData
from src.db.client import Database


# ── Fixture data constants ────────────────────────────────────────────────────

# equities: 5 daily closes → 4 simple returns (Jan 3, 4, 5, 8)
_EQUITY_CLOSES = [
    ("2024-01-02", 100.0),
    ("2024-01-03", 102.0),
    ("2024-01-04", 101.0),
    ("2024-01-05", 103.0),
    ("2024-01-08", 105.0),
]

# dollar: levels on Jan 2, 3, 4, 5 → 3 returns (Jan 3, 4, 5).
# Jan 2 has two releases; the later one (100.5) should win.
_DOLLAR_OBS = [
    # (period_date, release_date, value)
    ("2024-01-02", "2024-01-03", 100.0),   # earlier revision — must be ignored
    ("2024-01-02", "2024-01-05", 100.5),   # latest revision  — must be used
    ("2024-01-03", "2024-01-04", 101.0),
    ("2024-01-04", "2024-01-05", 100.5),
    ("2024-01-05", "2024-01-06", 101.5),
    # note: no Jan-08 entry → dollar returns end at Jan 5
]


# ── Shared in-memory DB fixture ───────────────────────────────────────────────

@pytest.fixture(scope="module")
def mem_db() -> Database:
    """In-memory DuckDB with migrations applied and fixture rows inserted."""
    database = Database(":memory:")
    database.open()

    # --- assets ---
    database.run(
        "INSERT INTO assets (id, ticker, name, asset_class, currency) VALUES (?, ?, ?, ?, ?)",
        [100001, "SPX", "S&P 500", "equity", "USD"],
    )

    # --- prices for equities ---
    for d, close in _EQUITY_CLOSES:
        database.run(
            """INSERT INTO prices
               (asset_id, interval, timestamp, open, high, low, close, volume, source)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [100001, "1d", f"{d} 00:00:00+00", close, close, close, close, 0.0, "test"],
        )

    # --- macro_series for dollar ---
    database.run(
        """INSERT INTO macro_series (id, code, name, source, frequency, units)
           VALUES (?, ?, ?, ?, ?, ?)""",
        [1, "DTWEXBGS", "Broad USD Index", "FRED", "daily", "Index"],
    )

    # --- macro_observations for dollar ---
    for period, release, val in _DOLLAR_OBS:
        database.run(
            """INSERT INTO macro_observations (series_id, period_date, release_date, value)
               VALUES (?, ?, ?, ?)""",
            [1, period, release, val],
        )

    # gold (400003) and crude (400001) are intentionally NOT inserted.

    yield database
    database.close()


# ── Helper: build a minimal two-factor list for most tests ───────────────────

_TWO_FACTORS = [
    f for f in DEFAULT_FACTORS if f.name in ("equities", "dollar")
]


# ── Price factor correctness ─────────────────────────────────────────────────

class TestPriceFactor:
    def test_returns_length(self, mem_db: Database):
        rd = ReferenceData(factors=_TWO_FACTORS, database=mem_db).load()
        assert len(rd.returns("equities")) == len(_EQUITY_CLOSES) - 1

    def test_returns_values_match_pct_change(self, mem_db: Database):
        rd = ReferenceData(factors=_TWO_FACTORS, database=mem_db).load()
        closes = [c for _, c in _EQUITY_CLOSES]
        expected = [
            (closes[i] - closes[i - 1]) / closes[i - 1]
            for i in range(1, len(closes))
        ]
        got = rd.returns("equities").tolist()
        assert len(got) == len(expected)
        for g, e in zip(got, expected):
            assert pytest.approx(g, rel=1e-9) == e

    def test_levels_length(self, mem_db: Database):
        rd = ReferenceData(factors=_TWO_FACTORS, database=mem_db).load()
        assert len(rd.levels("equities")) == len(_EQUITY_CLOSES)

    def test_returns_name(self, mem_db: Database):
        rd = ReferenceData(factors=_TWO_FACTORS, database=mem_db).load()
        assert rd.returns("equities").name == "equities"

    def test_levels_name(self, mem_db: Database):
        rd = ReferenceData(factors=_TWO_FACTORS, database=mem_db).load()
        assert rd.levels("equities").name == "equities"

    def test_returns_no_nans(self, mem_db: Database):
        rd = ReferenceData(factors=_TWO_FACTORS, database=mem_db).load()
        assert not rd.returns("equities").isna().any()

    def test_index_is_datetime(self, mem_db: Database):
        rd = ReferenceData(factors=_TWO_FACTORS, database=mem_db).load()
        assert isinstance(rd.returns("equities").index, pd.DatetimeIndex)


# ── Macro factor + revision handling ────────────────────────────────────────

class TestMacroFactor:
    def test_returns_length(self, mem_db: Database):
        rd = ReferenceData(factors=_TWO_FACTORS, database=mem_db).load()
        # 4 level observations → 3 returns
        assert len(rd.returns("dollar")) == len(set(o[0] for o in _DOLLAR_OBS)) - 1

    def test_latest_revision_wins(self, mem_db: Database):
        rd = ReferenceData(factors=_TWO_FACTORS, database=mem_db).load()
        lvl = rd.levels("dollar")
        # Jan 2 has two releases: 100.0 (Jan 3) and 100.5 (Jan 5).  Must use 100.5.
        assert pytest.approx(lvl["2024-01-02"], rel=1e-9) == 100.5

    def test_levels_sorted(self, mem_db: Database):
        rd = ReferenceData(factors=_TWO_FACTORS, database=mem_db).load()
        lvl = rd.levels("dollar")
        assert lvl.index.is_monotonic_increasing

    def test_returns_no_nans(self, mem_db: Database):
        rd = ReferenceData(factors=_TWO_FACTORS, database=mem_db).load()
        assert not rd.returns("dollar").isna().any()


# ── Skip-and-warn: gold and crude absent ────────────────────────────────────

class TestSkipAndWarn:
    def test_available_excludes_missing_factors(self, mem_db: Database):
        rd = ReferenceData(factors=DEFAULT_FACTORS, database=mem_db).load()
        avail = rd.available()
        assert "gold" not in avail
        assert "crude" not in avail

    def test_available_includes_loaded_factors(self, mem_db: Database):
        rd = ReferenceData(factors=DEFAULT_FACTORS, database=mem_db).load()
        avail = rd.available()
        assert "equities" in avail
        assert "dollar" in avail

    def test_no_exception_raised_for_missing_factors(self, mem_db: Database):
        # Should complete without raising even though gold/crude have no rows.
        ReferenceData(factors=DEFAULT_FACTORS, database=mem_db).load()

    def test_panel_excludes_missing_factors(self, mem_db: Database):
        rd = ReferenceData(factors=DEFAULT_FACTORS, database=mem_db).load()
        df = rd.panel()
        assert "gold" not in df.columns
        assert "crude" not in df.columns


# ── panel() ───────────────────────────────────────────────────────────────��──

class TestPanel:
    def test_inner_join_no_nans(self, mem_db: Database):
        rd = ReferenceData(factors=_TWO_FACTORS, database=mem_db).load()
        df = rd.panel(how="inner")
        assert not df.isna().any().any()

    def test_inner_join_columns(self, mem_db: Database):
        rd = ReferenceData(factors=_TWO_FACTORS, database=mem_db).load()
        df = rd.panel(how="inner")
        assert set(df.columns) == {"equities", "dollar"}

    def test_inner_join_dates_intersection(self, mem_db: Database):
        """Inner join should contain only dates common to both factors."""
        rd = ReferenceData(factors=_TWO_FACTORS, database=mem_db).load()
        df = rd.panel(how="inner")
        eq_idx = set(rd.returns("equities").index)
        dol_idx = set(rd.returns("dollar").index)
        expected = eq_idx & dol_idx
        assert set(df.index) == expected

    def test_outer_join_has_nans(self, mem_db: Database):
        """Outer join must keep Jan 8 equities return with NaN for dollar."""
        rd = ReferenceData(factors=_TWO_FACTORS, database=mem_db).load()
        df = rd.panel(how="outer")
        jan8 = pd.Timestamp("2024-01-08")
        assert jan8 in df.index
        assert pd.isna(df.loc[jan8, "dollar"])
        assert not pd.isna(df.loc[jan8, "equities"])

    def test_outer_join_no_rows_dropped(self, mem_db: Database):
        rd = ReferenceData(factors=_TWO_FACTORS, database=mem_db).load()
        df_outer = rd.panel(how="outer")
        df_inner = rd.panel(how="inner")
        assert len(df_outer) > len(df_inner)

    def test_inner_join_warns_on_mismatch(
        self, mem_db: Database, caplog: pytest.LogCaptureFixture
    ):
        """1 row dropped out of 4 (equities) = 25% > 20% threshold → warning."""
        rd = ReferenceData(factors=_TWO_FACTORS, database=mem_db).load()
        with caplog.at_level(logging.WARNING, logger="quant"):
            rd.panel(how="inner")
        assert any("mismatch" in msg.lower() or "dropped" in msg.lower()
                   for msg in caplog.messages)

    def test_panel_with_custom_index(self, mem_db: Database):
        rd = ReferenceData(factors=_TWO_FACTORS, database=mem_db).load()
        idx = pd.date_range("2024-01-03", "2024-01-05", freq="D")
        df = rd.panel(index=idx, how="outer")
        assert list(df.index) == list(idx)

    def test_empty_db_returns_empty_dataframe(self):
        empty = Database(":memory:")
        empty.open()
        try:
            rd = ReferenceData(factors=DEFAULT_FACTORS, database=empty).load()
            df = rd.panel()
            assert isinstance(df, pd.DataFrame)
            assert df.empty
        finally:
            empty.close()


# ── start/end filtering ───────────────────────────────────────────────────────

class TestStartEndFiltering:
    def test_start_filter_restricts_levels(self, mem_db: Database):
        rd = ReferenceData(
            factors=[f for f in DEFAULT_FACTORS if f.name == "equities"],
            start="2024-01-04",
            database=mem_db,
        ).load()
        # Only Jan 4, 5, 8 price levels → 2 returns (Jan 5, 8)
        assert len(rd.returns("equities")) == 2

    def test_end_filter_restricts_levels(self, mem_db: Database):
        rd = ReferenceData(
            factors=[f for f in DEFAULT_FACTORS if f.name == "equities"],
            end="2024-01-04",
            database=mem_db,
        ).load()
        # Jan 2, 3, 4 levels → 2 returns (Jan 3, 4)
        assert len(rd.returns("equities")) == 2

    def test_start_end_range(self, mem_db: Database):
        rd = ReferenceData(
            factors=[f for f in DEFAULT_FACTORS if f.name == "equities"],
            start="2024-01-03",
            end="2024-01-05",
            database=mem_db,
        ).load()
        # Jan 3, 4, 5 levels → 2 returns (Jan 4, 5)
        assert len(rd.returns("equities")) == 2

    def test_macro_start_filter(self, mem_db: Database):
        rd = ReferenceData(
            factors=[f for f in DEFAULT_FACTORS if f.name == "dollar"],
            start="2024-01-03",
            database=mem_db,
        ).load()
        # Dollar levels on Jan 3, 4, 5 → 2 returns (Jan 4, 5)
        assert len(rd.returns("dollar")) == 2


# ── KeyError on unavailable name ─────────────────────────────────────────────

class TestKeyError:
    def test_levels_unavailable_raises_key_error(self, mem_db: Database):
        rd = ReferenceData(factors=_TWO_FACTORS, database=mem_db).load()
        with pytest.raises(KeyError, match="gold"):
            rd.levels("gold")

    def test_returns_unavailable_raises_key_error(self, mem_db: Database):
        rd = ReferenceData(factors=_TWO_FACTORS, database=mem_db).load()
        with pytest.raises(KeyError, match="crude"):
            rd.returns("crude")

    def test_error_message_lists_available(self, mem_db: Database):
        rd = ReferenceData(factors=_TWO_FACTORS, database=mem_db).load()
        with pytest.raises(KeyError) as exc_info:
            rd.levels("bonds")
        assert "equities" in str(exc_info.value) or "dollar" in str(exc_info.value)

    def test_empty_available_is_empty_list(self):
        empty = Database(":memory:")
        empty.open()
        try:
            rd = ReferenceData(factors=DEFAULT_FACTORS, database=empty).load()
            assert rd.available() == []
        finally:
            empty.close()
