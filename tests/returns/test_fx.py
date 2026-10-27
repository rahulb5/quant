"""
tests/returns/test_fx.py

Tests for src/returns/fx: spot cleaning, quote inversion, carry.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from src.returns.fx import fx_excess_returns, null_long_gaps, remove_spikes
from tests.returns.test_cash import add_prices, add_rate, flat, mem_db  # noqa: F401  (fixture)

DAYS = pd.bdate_range("2020-01-01", periods=30)


def _rates(db, usd=1.8, **foreign):
    add_rate(db, "BIS.CBPOL.US", flat(usd, "1970-01-01", "2020-12-31"))
    for area, v in foreign.items():
        add_rate(db, f"BIS.CBPOL.{area}", flat(v, "1970-01-01", "2020-12-31"))


def test_remove_spikes_drops_only_revert_pattern():
    p = pd.Series(1.0, index=DAYS)
    p.iloc[10] = 1.17          # spike and revert
    p.iloc[20:] = 0.85         # genuine jump that stays (CHF 2015 style)
    cleaned, removed = remove_spikes(p)
    assert list(removed) == [DAYS[10]]
    assert cleaned[DAYS[10]] == pytest.approx(1.0)            # midpoint of 1.0 and 1.0
    assert cleaned[DAYS[20]] == pytest.approx(0.85)


def test_null_long_gaps():
    idx = DAYS.delete(range(10, 15))                          # five weekdays missing
    r = pd.Series(0.001, index=idx)
    out, nulled = null_long_gaps(r)
    assert list(nulled) == [DAYS[15]]
    assert pd.isna(out[DAYS[15]])
    assert out.drop(DAYS[15]).notna().all()
    _, none = null_long_gaps(pd.Series(0.001, index=DAYS.delete([10, 11])))   # two missing: kept
    assert none.empty


def test_usdxxx_quote_is_inverted_and_carry_added(mem_db):
    usdjpy = pd.Series(100.0, index=DAYS)
    usdjpy.iloc[15:] = 80.0                      # yen strengthens 25%
    add_prices(mem_db, 200002, usdjpy)
    _rates(mem_db, usd=3.6, JP=0.365)

    xr = fx_excess_returns(mem_db, ["JPY"])["JPY"]
    days = DAYS.to_series().diff().dt.days
    carry = 0.00365 * days / 365 - 0.036 * days / 360
    assert xr.iloc[15] == pytest.approx(0.25 + carry.iloc[15])
    np.testing.assert_allclose(xr.iloc[1:15], carry.iloc[1:15])


def test_start_respects_float_date_and_rate_history(mem_db):
    idx = pd.bdate_range("1983-12-01", "1983-12-30")
    add_prices(mem_db, 200004, pd.Series(1.0, index=idx))
    _rates(mem_db, AU=5.0)
    xr = fx_excess_returns(mem_db, ["AUD"])["AUD"].dropna()
    assert xr.index[0] > pd.Timestamp("1983-12-12")


def test_weekend_rows_dropped(mem_db):
    idx = pd.date_range("2024-01-01", periods=21, freq="D")          # includes weekends
    add_prices(mem_db, 200001, pd.Series(np.linspace(1.0, 1.1, len(idx)), index=idx))
    _rates(mem_db, XM=0.0, DE=0.0, usd=0.0)
    xr = fx_excess_returns(mem_db, ["EUR"])["EUR"].dropna()
    assert (xr.index.dayofweek < 5).all()
    monday = xr.index[xr.index.dayofweek == 0][0]
    assert xr[monday] > xr[xr.index[1]] * 2                          # Fri->Mon spans three days of drift


def test_attrs_report_spikes_and_caveats(mem_db):
    p = pd.Series(1.0, index=DAYS)
    p.iloc[10] = 1.2
    add_prices(mem_db, 200006, 1 / p)                                 # USDCHF
    _rates(mem_db, CH=0.0)
    xr = fx_excess_returns(mem_db, ["CHF"])
    assert xr.attrs["spikes_removed"]["CHF"] == [DAYS[10]]
    assert xr.attrs["carry_approx"]["CHF"]


def test_unknown_currency(mem_db):
    with pytest.raises(ValueError):
        fx_excess_returns(mem_db, ["XXX"])
