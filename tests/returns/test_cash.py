"""
tests/returns/test_cash.py

Tests for src/returns/cash. Also holds the in-memory DB helpers the other
tests/returns modules import (add_rate, add_prices).
"""

from __future__ import annotations

import pandas as pd
import pytest

from src.db.client import Database
from src.returns.cash import accrue, cash_rate, caveats_for, policy_rate

# ── Shared helpers ────────────────────────────────────────────────────────────


@pytest.fixture
def mem_db():
    database = Database(":memory:")
    database.open()
    yield database
    database.close()


def add_rate(db: Database, code: str, values: pd.Series) -> None:
    """Register a daily percent series `code` and store `values` (index = dates)."""
    sid = int(db.query("SELECT COALESCE(MAX(id), 0) + 1 AS n FROM macro_series")[0]["n"])
    db.run("INSERT INTO macro_series (id, code, name, source, frequency, units) VALUES (?, ?, ?, 'TEST', 'daily', 'Percent')",
           [sid, code, code])
    df = pd.DataFrame({"sid": sid, "d": pd.DatetimeIndex(values.index).date, "v": values.to_numpy(dtype=float)})
    db.run_df("INSERT INTO macro_observations SELECT sid, d, d, v, true FROM obs", df, name="obs")


def add_prices(db: Database, asset_id: int, close: pd.Series) -> None:
    """Register asset_id (if new) and store daily closes at New York midnight."""
    if not db.query("SELECT 1 FROM assets WHERE id = ?", [asset_id]):
        db.run("INSERT INTO assets (id, ticker, name, asset_class) VALUES (?, ?, ?, 'equity')",
               [asset_id, f"T{asset_id}", f"T{asset_id}"])
    df = pd.DataFrame({"a": asset_id, "ts": pd.DatetimeIndex(close.index).tz_localize("America/New_York"),
                       "c": close.to_numpy(dtype=float)})
    db.run_df("INSERT INTO prices (asset_id, interval, timestamp, open, high, low, close, source) "
              "SELECT a, '1d', ts, c, c, c, c, 'test' FROM px", df, name="px")


def flat(value: float, start: str = "1990-01-01", end: str = "2001-12-31", freq: str = "D") -> pd.Series:
    return pd.Series(value, index=pd.date_range(start, end, freq=freq))


# ── accrue ────────────────────────────────────────────────────────────────────

def test_accrue_uses_prior_rate_and_calendar_days():
    rate = pd.Series([0.036, 0.072, 0.108], index=pd.to_datetime(["2024-01-05", "2024-01-08", "2024-01-09"]))
    dates = pd.to_datetime(["2024-01-05", "2024-01-08", "2024-01-09"])
    r = accrue(rate, dates, 360)
    assert pd.isna(r.iloc[0])
    assert r.iloc[1] == pytest.approx(0.036 * 3 / 360)   # Friday's rate over the weekend
    assert r.iloc[2] == pytest.approx(0.072 * 1 / 360)   # not Tuesday's


def test_accrue_nan_before_rate_starts():
    rate = pd.Series([0.05], index=pd.to_datetime(["2024-01-10"]))
    r = accrue(rate, pd.to_datetime(["2024-01-08", "2024-01-09", "2024-01-10", "2024-01-11"]), 365)
    assert r.iloc[:3].isna().all()
    assert r.iloc[3] == pytest.approx(0.05 / 365)


# ── rate loaders ──────────────────────────────────────────────────────────────

def test_policy_rate_splices_eur_at_1999(mem_db):
    add_rate(mem_db, "BIS.CBPOL.DE", flat(4.0, "1998-12-01", "1998-12-31"))
    add_rate(mem_db, "BIS.CBPOL.XM", flat(3.0, "1999-01-01", "1999-01-31"))
    s = policy_rate(mem_db, "EUR")
    assert s["1998-12-31"] == pytest.approx(0.04)
    assert s["1999-01-01"] == pytest.approx(0.03)
    assert s.name == "EUR"


def test_cash_rate_usd_prefers_bill_and_fills_with_fed_funds(mem_db):
    add_rate(mem_db, "DGS3MO", pd.Series([5.0, 5.1], index=pd.to_datetime(["2024-01-02", "2024-01-04"])))
    add_rate(mem_db, "DFF", pd.Series([5.3, 5.3, 5.3], index=pd.to_datetime(["2024-01-02", "2024-01-03", "2024-01-04"])))
    s = cash_rate(mem_db, "USD")
    assert list(s.round(4)) == [0.05, 0.053, 0.051]
    assert s.attrs == {}


def test_cash_rate_non_usd_is_policy_rate(mem_db):
    add_rate(mem_db, "BIS.CBPOL.GB", flat(2.0, "2020-01-01", "2020-01-10"))
    pd.testing.assert_series_equal(cash_rate(mem_db, "GBP"), policy_rate(mem_db, "GBP"))


def test_policy_rate_unknown_currency():
    with pytest.raises(ValueError):
        policy_rate(None, "XXX")


def test_caveats_for():
    assert any("MRO" in why for _, _, why in caveats_for("EUR"))
    assert caveats_for("AUD") == []
