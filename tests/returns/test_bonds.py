"""
tests/returns/test_bonds.py

Tests for src/returns/bonds on synthetic yield curves.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from src.returns.bonds import (
    MAX_GAP_DAYS,
    YEAR_DAYS,
    bond_price,
    constant_maturity_returns,
    ust_excess_returns,
)
from tests.returns.test_cash import add_rate, mem_db  # noqa: F401  (fixture)

DAYS = pd.bdate_range("2020-01-01", periods=30)


def _curve(levels: dict[float, float | np.ndarray], index=DAYS) -> pd.DataFrame:
    return pd.DataFrame({m: np.broadcast_to(v, len(index)).astype(float) for m, v in levels.items()}, index=index)


def test_bond_price_par_and_zero_yield():
    assert bond_price(0.04, 10, 0.04) == pytest.approx(1.0)
    assert bond_price(0.04, 9.5, 0.04) == pytest.approx(1.0)
    assert bond_price(0.0, 10, 0.0) == pytest.approx(1.0)
    assert bond_price(0.02, 10, 0.0) == pytest.approx(1.2)        # undiscounted coupons + face
    assert bond_price(0.04, 10, 0.05) < 1.0 < bond_price(0.04, 10, 0.03)


def test_flat_static_curve_earns_the_yield():
    r = constant_maturity_returns(_curve({1: 0.04, 2: 0.04, 5: 0.04, 10: 0.04, 30: 0.04}))
    dt = DAYS.to_series().diff().dt.days / YEAR_DAYS
    for m in (2, 5, 10, 30):
        np.testing.assert_allclose(r[m].iloc[1:], 0.04 * dt.iloc[1:], rtol=1e-9)
        assert pd.isna(r[m].iloc[0])


def test_parallel_shift_matches_duration():
    y10 = np.full(len(DAYS), 0.04)
    y10[5:] = 0.0401                                   # +1 bp on day 5
    r = constant_maturity_returns(_curve({5: y10, 10: y10}), tenors=(10,))[10]
    dt = (DAYS[5] - DAYS[4]).days / YEAR_DAYS
    mod_dur = (1 - 1.02 ** -20) / 0.04                 # par bond, semi-annual: annuity / (1 + y/2) * ... ~8.18
    assert r.iloc[5] - 0.04 * dt == pytest.approx(-mod_dur * 0.0001, rel=0.02)
    # Timing: the move shows up on its own day only (no repricing with tomorrow's curve),
    # and the next day earns the new coupon.
    dt4 = (DAYS[4] - DAYS[3]).days / YEAR_DAYS
    assert r.iloc[4] == pytest.approx(0.04 * dt4)
    dt6 = (DAYS[6] - DAYS[5]).days / YEAR_DAYS
    assert r.iloc[6] == pytest.approx(0.0401 * dt6)


def test_rolldown_uses_same_day_shorter_tenor():
    y5 = np.full(len(DAYS), 0.03)
    y5[8:] = 0.02                                      # steeper 5s10s from day 8
    r = constant_maturity_returns(_curve({5: y5, 10: 0.04}), tenors=(10,))[10]
    dt = DAYS.to_series().diff().dt.days / YEAR_DAYS
    extra = r - 0.04 * dt
    assert extra.iloc[8] > extra.iloc[7] * 1.5          # day 8 rolls down the day-8 curve, not day 7's


def test_upward_sloping_curve_adds_rolldown():
    r = constant_maturity_returns(_curve({5: 0.03, 10: 0.04}), tenors=(10,))[10]
    dt = DAYS.to_series().diff().dt.days / YEAR_DAYS
    assert (r.iloc[1:] > 0.04 * dt.iloc[1:]).all()


def test_missing_shorter_tenor_means_no_rolldown():
    curve = _curve({5: 0.03, 10: 0.04})
    curve.loc[DAYS[3], 5] = np.nan
    r = constant_maturity_returns(curve, tenors=(10,))[10]
    dt = (DAYS[3] - DAYS[2]).days / YEAR_DAYS
    assert r.iloc[3] == pytest.approx(0.04 * dt)


def test_long_gap_gives_nan():
    idx = DAYS.append(pd.DatetimeIndex([DAYS[-1] + pd.Timedelta(days=MAX_GAP_DAYS + 5)]))
    r = constant_maturity_returns(_curve({10: 0.04}, index=idx), tenors=(10,))[10]
    assert pd.isna(r.iloc[-1])
    assert r.iloc[-2] > 0


def test_ust_excess_returns_subtracts_usd_cash(mem_db):
    days = pd.bdate_range("2020-01-01", periods=20)
    for code in ("DGS1", "DGS2", "DGS5", "DGS10", "DGS30"):
        add_rate(mem_db, code, pd.Series(4.0, index=days))
    add_rate(mem_db, "DGS3MO", pd.Series(1.0, index=days))
    add_rate(mem_db, "DFF", pd.Series(1.0, index=days))
    xr = ust_excess_returns(mem_db)
    dt = days.to_series().diff().dt.days
    np.testing.assert_allclose(xr[10].iloc[1:], (0.04 * dt / YEAR_DAYS - 0.01 * dt / 360).iloc[1:], rtol=1e-9)
