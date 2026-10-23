"""Shared fixture: a temp DuckDB file seeded with one FRED and two CFTC series."""

from __future__ import annotations

import pandas as pd
import pytest

from src.db.client import Database


@pytest.fixture
def macro_db_path(tmp_path) -> str:
    path = str(tmp_path / "quant.db")
    db = Database(path)
    db.open()
    db.run("""INSERT INTO macro_series (id, code, name, source, frequency, units) VALUES
        (1, 'CPI',  'Test CPI',   'FRED', 'monthly', 'Index'),
        (2, 'CFTC.DISAGG_FUT.067651', 'CRUDE OIL', 'CFTC', 'weekly', 'contracts'),
        (3, 'CFTC.DISAGG_FUT.NYME',   'MIXED',     'CFTC', 'weekly', 'contracts')""")

    # 10 years of monthly CPI growing 0.2%/month; each period has a first
    # release and a revision one month later that is 1.0 higher.
    months = pd.date_range("2015-01-01", periods=120, freq="MS")
    for i, d in enumerate(months):
        first = 100 * 1.002 ** i
        db.run("INSERT INTO macro_observations VALUES (1, ?, ?, ?, false)",
               [d.date(), (d + pd.DateOffset(days=14)).date(), first])
        db.run("INSERT INTO macro_observations VALUES (1, ?, ?, ?, true)",
               [d.date(), (d + pd.DateOffset(days=45)).date(), first + 1.0])

    weeks = pd.date_range("2020-01-07", periods=150, freq="7D")
    for sid in (2, 3):
        for i, d in enumerate(weeks):
            db.run("""INSERT INTO cot_positions
                (series_id, report_date, release_date, report_type, market_name,
                 open_interest, dis_mm_long, dis_mm_short)
                VALUES (?, ?, ?, 'disagg_fut', 'CRUDE OIL', 1000, ?, 100)""",
                [sid, d.date(), (d + pd.DateOffset(days=3)).date(), 100 + i])
    db.close()
    return path
