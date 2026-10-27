"""
src/returns/bonds.py

Daily returns of constant-maturity US Treasury positions, built from FRED
constant-maturity (CMT) par yields.

Each day the position holds a par bond of maturity M: coupon = yesterday's
M-year yield, price 1. A day later the bond has M - dt years left and is
repriced at today's curve, interpolated at M - dt:

    return = price(c, M - dt, y_t(M - dt)) - 1 + c * dt

so it has three parts: coupon carry (c * dt), rolldown (the curve read at a
slightly shorter maturity), and the price change from the yield move.
Semi-annual coupons; price() treats a fractional number of periods
continuously, which is exact on coupon dates and accurate to well under 1 bp
a day otherwise. dt is calendar days / 365.25.

Rolldown interpolates linearly between M and the next shorter CMT tenor
(2y: 1y, 5y: 2y, 10y: 5y, 30y: 10y); if that tenor is missing on the day,
there is no rolldown that day. A gap longer than MAX_GAP_DAYS between two
yields gives NaN rather than one return spanning the gap. (FRED's DGS30 has
values through the 2002-2006 suspension of the 30-year bond, so this guard
doesn't trigger there; those values' construction wasn't checked.)

Excess return = total return - USD cash (cash.cash_rate("USD"), act/360).
Earliest dates: 2y 1976-06, 5y/10y 1962-01, 30y 1977-02; USD cash from 1954.

Not modelled: futures basis / cheapest-to-deliver, the on-the-run premium,
and transaction costs. Checked against unit cases only; ETF validation
(IEF/TLT) was deferred because it needs new data.

Usage:
    from src.returns.bonds import ust_excess_returns
    xr = ust_excess_returns(db)          # columns 2, 5, 10, 30
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from src.db.client import Database
from src.returns.cash import DAY_COUNT, accrue, cash_rate, load_rate

CURVE_CODES: dict[float, str] = {1: "DGS1", 2: "DGS2", 5: "DGS5", 10: "DGS10", 30: "DGS30"}
TENORS: tuple[int, ...] = (2, 5, 10, 30)
ROLL_FROM: dict[int, float] = {2: 1, 5: 2, 10: 5, 30: 10}   # next shorter tenor for rolldown
COUPON_FREQ = 2
YEAR_DAYS = 365.25
MAX_GAP_DAYS = 10


def bond_price(coupon, maturity, yld, freq: int = COUPON_FREQ):
    """Price per unit face of a bond with `maturity` years left, at yield `yld`.

    Vectorised over numpy arrays / pandas objects. Handles yld == 0.
    """
    coupon, maturity, yld = (np.asarray(x, dtype=float) for x in (coupon, maturity, yld))
    n = maturity * freq
    c = coupon / freq
    y = yld / freq
    v = np.power(1.0 + y, -n)
    small = np.abs(y) < 1e-12
    annuity = np.where(small, n, (1.0 - v) / np.where(small, 1.0, y))
    return c * annuity + v


def constant_maturity_returns(curve: pd.DataFrame, tenors: tuple[int, ...] = TENORS) -> pd.DataFrame:
    """Daily total returns of constant-maturity par bonds.

    Args:
        curve: decimal yields, DatetimeIndex, columns = maturities in years
               (must include each tenor and its ROLL_FROM tenor where available).
        tenors: maturities to return.

    Returns:
        DataFrame indexed like `curve`, one column per tenor; NaN where the
        tenor's yield is missing, on its first date, and after a gap > MAX_GAP_DAYS.
    """
    out = {}
    for m in tenors:
        y = curve[m].dropna()
        dates = y.index
        dt = dates.to_series().diff().dt.days / YEAR_DAYS
        coupon = y.shift(1)
        remaining = m - dt

        lower = ROLL_FROM[m]
        if lower in curve.columns:
            y_low = curve[lower].reindex(dates)
            y_roll = y - (y - y_low) * dt / (m - lower)
            y_roll = y_roll.fillna(y)            # no shorter tenor that day: no rolldown
        else:
            y_roll = y

        r = pd.Series(bond_price(coupon, remaining, y_roll), index=dates) - 1.0 + coupon * dt
        r[dt * YEAR_DAYS > MAX_GAP_DAYS] = np.nan
        out[m] = r
    return pd.DataFrame(out).reindex(curve.index)


def load_ust_curve(db: Database) -> pd.DataFrame:
    """FRED CMT yields as decimals, columns = maturities in years (1, 2, 5, 10, 30)."""
    return pd.DataFrame({m: load_rate(db, code) for m, code in CURVE_CODES.items()}).sort_index()


def ust_total_returns(db: Database, tenors: tuple[int, ...] = TENORS) -> pd.DataFrame:
    return constant_maturity_returns(load_ust_curve(db), tenors)


def ust_excess_returns(db: Database, tenors: tuple[int, ...] = TENORS) -> pd.DataFrame:
    """Total return minus USD cash accrued over the same interval, per tenor.

    Each tenor accrues cash over its own return intervals, so a holiday in
    one series doesn't misalign the cash leg.
    """
    curve = load_ust_curve(db)
    total = constant_maturity_returns(curve, tenors)
    usd = cash_rate(db, "USD")
    out = {}
    for m in tenors:
        cash = accrue(usd, curve[m].dropna().index, DAY_COUNT["USD"])   # same intervals as the bond
        out[m] = total[m] - cash.reindex(total.index)
    return pd.DataFrame(out)
