"""
tests/returns/test_dividends.py

Tests for src/returns/dividends: synthetic prices with known dividends.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from src.db.client import Database
from src.returns.dividends import (
    PERIODS_PER_YEAR,
    accrued_total_return,
    dividend_events,
    load_index_total_returns,
    trailing_yield,
)

DAYS = pd.bdate_range("2020-01-01", "2022-12-30")


def _etf(div_per_quarter: float = 0.5):
    """Flat close at 100; a cash dividend every 63 business days."""
    close = pd.Series(100.0, index=DAYS, name=1)
    amounts = pd.Series(div_per_quarter, index=DAYS[63::63])
    return close, amounts


def test_dividend_events_divides_by_prior_close():
    close = pd.Series([100.0, 50.0, 51.0], index=DAYS[:3], name=1)
    ev = dividend_events(close, pd.Series([1.0], index=[DAYS[1]]))
    assert list(ev) == [0.0, 0.01, 0.0]      # 1 / 100, not 1 / 50


def test_dividend_events_moves_non_trading_ex_date_forward():
    close = pd.Series(100.0, index=DAYS[:5], name=1)
    saturday = DAYS[0] + pd.Timedelta(days=(5 - DAYS[0].weekday()) % 7)
    ev = dividend_events(close, pd.Series([1.0], index=[saturday]))
    assert ev[ev > 0].index[0] == close.index[close.index > saturday][0]


def test_dividend_events_skips_ex_date_without_prior_close():
    close = pd.Series(100.0, index=DAYS[:3], name=1)
    assert dividend_events(close, pd.Series([1.0], index=[DAYS[0]])).eq(0).all()


def test_dividend_events_drops_implausible_amount():
    close, _ = _etf()
    with pytest.warns(UserWarning, match="implausible"):
        ev = dividend_events(close, pd.Series([617.0, 0.5], index=DAYS[[100, 200]]))   # unsplit amount
    assert ev.sum() == pytest.approx(0.005)


def test_trailing_yield_nan_until_full_window():
    close, amounts = _etf()
    y = trailing_yield(dividend_events(close, amounts))
    assert y[: "2020-12-30"].isna().all()
    assert y["2021-06-30":].notna().all()
    assert y["2021-06-30":].round(6).isin([0.02, 0.025]).all()   # 4-5 dividends of 0.5% per 365 days


def test_accrued_total_return_uses_yesterdays_yield():
    price = pd.Series([100.0, 101.0, 102.01], index=DAYS[:3])
    yld = pd.Series([0.0, 0.0252, 0.5], index=DAYS[:3])   # the jump on day 3 must not be used on day 3
    r = accrued_total_return(price, yld)
    assert r.iloc[1] == pytest.approx(0.01)
    assert r.iloc[2] == pytest.approx(0.01 + 0.0252 / PERIODS_PER_YEAR)


# ── Loader, against an in-memory DB ──────────────────────────────────────────

def _insert(db: Database, asset_id: int, close: pd.Series, amounts: pd.Series | None = None) -> None:
    db.run("INSERT INTO assets (id, ticker, name, asset_class) VALUES (?, ?, ?, 'equity')",
           [asset_id, f"T{asset_id}", f"T{asset_id}"])
    df = pd.DataFrame({"asset_id": asset_id,
                       "timestamp": close.index.tz_localize("America/New_York"),
                       "close": close.to_numpy()})
    db.run_df("""INSERT INTO prices (asset_id, interval, timestamp, open, high, low, close, source)
                 SELECT asset_id, '1d', timestamp, close, close, close, close, 'test' FROM df""", df)
    if amounts is not None:
        dv = pd.DataFrame({"asset_id": asset_id, "ex_date": amounts.index.date, "amount": amounts.to_numpy()})
        db.run_df("INSERT INTO dividends SELECT asset_id, ex_date, amount, 'test' FROM dv", dv, name="dv")


@pytest.fixture
def mem_db():
    database = Database(":memory:")
    database.open()
    yield database
    database.close()


def test_loader_etf_method(mem_db):
    price = pd.Series(np.linspace(100, 110, len(DAYS)), index=DAYS)
    close, amounts = _etf()
    _insert(mem_db, 100004, price)
    _insert(mem_db, 500003, close, amounts)

    tr = load_index_total_returns(mem_db, [100004])
    assert tr.attrs["method"] == {100004: "etf"}
    assert tr[100004][:"2020-12-31"].isna().all()           # drop before a full year of dividends
    expected = trailing_yield(dividend_events(close, amounts)).shift(1) / PERIODS_PER_YEAR
    np.testing.assert_allclose(tr[100004]["2022-01-03":] - price.pct_change()["2022-01-03":],
                               expected["2022-01-03":])

    held = load_index_total_returns(mem_db, [100004], pre_coverage="hold_first")
    assert held[100004].iloc[1:].notna().all()
    assert held.attrs["approx_until"][100004] == pd.Timestamp("2020-12-31")


def test_loader_etf_without_data_is_nan(mem_db):
    price = pd.Series(np.linspace(100, 110, len(DAYS)), index=DAYS)
    _insert(mem_db, 100005, price)
    assert load_index_total_returns(mem_db, [100005])[100005].isna().all()


def test_loader_performance_index_is_unadjusted(mem_db):
    price = pd.Series(np.linspace(100, 110, len(DAYS)), index=DAYS)
    _insert(mem_db, 100006, price)
    tr = load_index_total_returns(mem_db, [100006])
    pd.testing.assert_series_equal(tr[100006], price.pct_change(), check_names=False, check_freq=False)


def test_loader_tr_index_exact_then_hold_first_before(mem_db):
    price = pd.Series(np.linspace(100, 110, len(DAYS)), index=DAYS)
    tr_idx = (price.pct_change().fillna(0) + 0.02 / PERIODS_PER_YEAR).add(1).cumprod()["2021-01-04":] * 100
    _insert(mem_db, 100001, price)
    _insert(mem_db, 100016, tr_idx)

    exact = load_index_total_returns(mem_db, [100001])[100001]
    assert exact[:"2021-01-04"].isna().all()
    np.testing.assert_allclose(exact["2021-01-05":], tr_idx.pct_change()["2021-01-05":])

    held = load_index_total_returns(mem_db, [100001], pre_coverage="hold_first")[100001]
    # The implied yield sums a 365-day window: 261 synthetic business days, vs ~252 on a real calendar.
    days_in_window = len(DAYS[(DAYS > "2021-01-04") & (DAYS <= "2022-01-04")])
    np.testing.assert_allclose(held["2020-01-03":"2021-01-01"] - price.pct_change()["2020-01-03":"2021-01-01"],
                               0.02 / PERIODS_PER_YEAR * days_in_window / PERIODS_PER_YEAR, rtol=1e-3)
    np.testing.assert_allclose(held["2021-01-05":], exact["2021-01-05":])


def test_loader_rejects_unknown_index(mem_db):
    with pytest.raises(ValueError, match="No total-return method"):
        load_index_total_returns(mem_db, [100011])
