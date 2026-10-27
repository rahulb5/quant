"""
tests/returns/test_layer.py

Tests for src/returns/layer: key mapping, clipping, attrs.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from src.returns.layer import ALL_KEYS, excess_returns
from tests.returns.test_cash import add_prices, add_rate, flat, mem_db  # noqa: F401  (fixture)

DAYS = pd.bdate_range("2020-01-01", periods=40)


@pytest.fixture
def small_db(mem_db):
    add_prices(mem_db, 100006, pd.Series(np.linspace(100, 104, len(DAYS)), index=DAYS))   # DAX
    add_prices(mem_db, 200001, pd.Series(np.linspace(1.10, 1.12, len(DAYS)), index=DAYS))  # EURUSD
    for code in ("DGS1", "DGS2", "DGS5", "DGS10", "DGS30", "DGS3MO", "DFF"):
        add_rate(mem_db, code, pd.Series(2.0, index=DAYS))
    for area in ("US", "XM", "DE"):
        add_rate(mem_db, f"BIS.CBPOL.{area}", flat(1.0, "1970-01-01", "2020-12-31"))
    return mem_db


def test_keys_map_to_sleeves(small_db):
    xr = excess_returns(small_db, ["EQ.DAX", "RT.UST10", "FX.EUR"])
    assert list(xr.columns) == ["EQ.DAX", "RT.UST10", "FX.EUR"]
    assert xr.attrs["sleeve"] == {"EQ.DAX": "EQ", "RT.UST10": "RT", "FX.EUR": "FX"}
    assert xr.attrs["equity.cash_currency"] == {"EQ.DAX": "EUR"}
    assert "FX.EUR" in xr.attrs["fx.carry_approx"]
    assert xr.notna().sum().min() > 30


def test_mixed_calendars_do_not_propagate_nan(mem_db):
    """A holiday in one sleeve leaves NaN only in that column, and the others keep every day."""
    eq_days = DAYS.delete([10, 11])                                             # equity holiday
    fx_days = DAYS.delete([20])                                                 # FX data gap (1 day)
    dax = pd.Series(np.linspace(100, 104, len(eq_days)), index=eq_days)
    add_prices(mem_db, 100006, dax)
    add_prices(mem_db, 200001, pd.Series(np.linspace(1.10, 1.12, len(fx_days)), index=fx_days))
    for code in ("DGS1", "DGS2", "DGS5", "DGS10", "DGS30", "DGS3MO", "DFF"):
        add_rate(mem_db, code, pd.Series(2.0, index=DAYS))
    for area in ("US", "XM", "DE"):
        add_rate(mem_db, f"BIS.CBPOL.{area}", flat(1.0, "1970-01-01", "2020-12-31"))

    xr = excess_returns(mem_db, ["EQ.DAX", "RT.UST10", "FX.EUR"])
    assert xr.loc[DAYS[10], "EQ.DAX"] != xr.loc[DAYS[10], "EQ.DAX"]           # NaN on its holiday
    assert xr.loc[DAYS[10]:DAYS[11], ["RT.UST10", "FX.EUR"]].notna().all().all()
    assert pd.isna(xr.loc[DAYS[20], "FX.EUR"])
    assert xr.loc[DAYS[20], ["EQ.DAX", "RT.UST10"]].notna().all()
    assert xr["RT.UST10"].iloc[1:].notna().all()
    # The return after the equity holiday spans it (3 business days of cash).
    expected = dax[DAYS[12]] / dax[DAYS[9]] - 1 - 0.01 * (DAYS[12] - DAYS[9]).days / 360
    assert xr.loc[DAYS[12], "EQ.DAX"] == pytest.approx(expected)


def test_start_end_clip(small_db):
    xr = excess_returns(small_db, ["RT.UST2"], start="2020-01-10", end="2020-01-31")
    assert xr.index.min() >= pd.Timestamp("2020-01-10")
    assert xr.index.max() <= pd.Timestamp("2020-01-31")


def test_unknown_key(small_db):
    with pytest.raises(ValueError, match="Unknown instrument keys"):
        excess_returns(small_db, ["EQ.XYZ"])


def test_all_keys_are_prefixed():
    assert all(k.split(".")[0] in {"EQ", "RT", "FX"} for k in ALL_KEYS)
    assert len(ALL_KEYS) == len(set(ALL_KEYS)) == 23
