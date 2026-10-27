"""
tests/returns/test_equity.py

Tests for src/returns/equity: total return minus local cash.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from src.returns.equity import equity_excess_returns
from tests.returns.test_cash import add_prices, add_rate, flat, mem_db  # noqa: F401  (fixture)

DAYS = pd.bdate_range("2020-01-01", periods=40)


def test_dax_excess_is_price_return_minus_eur_cash(mem_db):
    price = pd.Series(np.linspace(100, 104, len(DAYS)), index=DAYS)
    add_prices(mem_db, 100006, price)
    add_rate(mem_db, "BIS.CBPOL.DE", flat(9.0, "1998-01-01", "1998-12-31"))
    add_rate(mem_db, "BIS.CBPOL.XM", flat(3.6, "2019-12-01", "2020-03-31"))

    xr = equity_excess_returns(mem_db, [100006])
    days = DAYS.to_series().diff().dt.days
    expected = price.pct_change() - 0.036 * days / 360
    assert pd.isna(xr[100006].iloc[1])                          # first return interval has no cash leg
    np.testing.assert_allclose(xr[100006].iloc[2:], expected.iloc[2:], rtol=1e-9)
    assert xr.attrs["cash_currency"] == {100006: "EUR"}
    assert xr.attrs["method"] == {100006: "performance"}


def test_hsi_uses_usd_cash(mem_db):
    add_prices(mem_db, 100010, pd.Series(100.0, index=DAYS))
    add_rate(mem_db, "DGS3MO", flat(3.6, "2019-12-01", "2020-03-31"))
    add_rate(mem_db, "DFF", flat(3.6, "2019-12-01", "2020-03-31"))
    xr = equity_excess_returns(mem_db, [100010])
    assert xr.attrs["cash_currency"] == {100010: "USD"}
    assert xr[100010].isna().all()          # no ETF dividends stored -> no total return


def test_spike_and_revert_print_is_repaired(mem_db):
    price = pd.Series(100.0, index=DAYS)
    price.iloc[20] = 74.0                       # bad print, back the next day
    add_prices(mem_db, 100006, price)
    add_rate(mem_db, "BIS.CBPOL.DE", flat(0.0, "1998-01-01", "1998-12-31"))
    add_rate(mem_db, "BIS.CBPOL.XM", flat(0.0, "2019-12-01", "2020-03-31"))
    xr = equity_excess_returns(mem_db, [100006])
    assert xr.attrs["spikes_removed"] == {100006: [DAYS[20]]}
    assert xr[100006].iloc[20] == pytest.approx(0.0)
    assert xr[100006].iloc[21] == pytest.approx(0.0)


def test_unknown_index(mem_db):
    with pytest.raises(ValueError):
        equity_excess_returns(mem_db, [100011])
