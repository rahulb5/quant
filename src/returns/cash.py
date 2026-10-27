"""
src/returns/cash.py

Short rates per currency, and turning them into per-period cash returns.

- policy_rate(): the BIS central bank policy rate (macro_observations,
  BIS.CBPOL.<area>). EUR is spliced: Bundesbank (DE) to 1998, euro area (XM)
  from 1999-01-01. Used on both legs of FX carry, so the differential compares
  like with like.
- cash_rate(): what a funded position pays or earns. USD is the 3-month
  T-bill (DGS3MO, from 1981-09) with Fed funds (DFF) before and in its gaps;
  other currencies use policy_rate().

Rates come back as decimals (0.05 = 5%) on each source's own dates. accrue()
turns a rate into a return over each interval of a target calendar, using
the rate known on the interval's start date (no lookahead).

RATE_CAVEATS lists periods where the BIS series isn't a money-market rate
(from the BIS compilation notes, checked 2026-10-25). They bias carry, so
downstream code reports them; nothing here adjusts for them.

Usage:
    from src.returns.cash import cash_rate, accrue, DAY_COUNT
    r = cash_rate(db, "GBP")
    cash_ret = accrue(r, equity_dates, DAY_COUNT["GBP"])
"""

from __future__ import annotations

import pandas as pd

from src.db.client import Database
from src.macro.data import load_fred

CURRENCY_AREA: dict[str, str] = {
    "USD": "US", "EUR": "XM", "JPY": "JP", "GBP": "GB", "CHF": "CH", "CAD": "CA",
    "AUD": "AU", "NZD": "NZ", "SEK": "SE", "NOK": "NO", "DKK": "DK",
}
EUR_SPLICE_DATE = pd.Timestamp("1999-01-01")

# Money-market day-count denominators.
DAY_COUNT: dict[str, int] = {
    "USD": 360, "EUR": 360, "CHF": 360, "SEK": 360, "NOK": 360, "DKK": 360,
    "GBP": 365, "JPY": 365, "AUD": 365, "NZD": 365, "CAD": 365,
}

# (currency, first date, last date, reason). None = open-ended.
RATE_CAVEATS: list[tuple[str, str | None, str | None, str]] = [
    ("CHF", None, "1999-12-31", "SNB discount rate, well below money-market rates around 1990"),
    ("JPY", None, "1998-09-08", "values follow the BoJ official discount rate, not the call rate"),
    ("JPY", "1999-02-12", "2006-03-08", "BIS holds 0.25/0.15% through zero-rate and QE years; call rate ~0"),
    ("JPY", "2013-04-04", "2016-09-20", "no BoJ policy rate; BIS value carried forward (negative rates from 2016-02)"),
    ("DKK", None, "1992-03-31", "official discount rate"),
    ("SEK", None, "1987-01-29", "discount rate"),
    ("EUR", None, "1998-12-31", "Bundesbank rate (BIS DE); definition not checked"),
    ("EUR", "2008-10-15", "2024-09-17", "ECB MRO rate; overnight rates traded 50-75 bp below it under full allotment"),
]


def load_rate(db: Database, code: str) -> pd.Series:
    """A percent series from macro_observations as a decimal, without load_fred's
    release_date attrs (unrevised daily series: they equal the index, and pandas
    can't combine Series that carry them)."""
    s = load_fred(db, code) / 100.0
    s.attrs = {}
    return s


def _load_bis(db: Database, area: str) -> pd.Series:
    return load_rate(db, f"BIS.CBPOL.{area}")


def policy_rate(db: Database, currency: str) -> pd.Series:
    """BIS policy rate as a decimal, calendar-daily. EUR spliced DE -> XM at 1999-01-01."""
    if currency not in CURRENCY_AREA:
        raise ValueError(f"No policy rate for {currency!r}; expected one of {list(CURRENCY_AREA)}")
    if currency == "EUR":
        de, xm = _load_bis(db, "DE"), _load_bis(db, "XM")
        s = pd.concat([de[de.index < EUR_SPLICE_DATE], xm[xm.index >= EUR_SPLICE_DATE]])
    else:
        s = _load_bis(db, CURRENCY_AREA[currency])
    return s.rename(currency)


def cash_rate(db: Database, currency: str) -> pd.Series:
    """Funding rate as a decimal. USD: DGS3MO, falling back to DFF; others: policy_rate()."""
    if currency != "USD":
        return policy_rate(db, currency)
    bill = load_rate(db, "DGS3MO")
    ff = load_rate(db, "DFF")
    return bill.combine_first(ff).rename("USD")


def accrue(rate: pd.Series, dates: pd.DatetimeIndex, day_count: int) -> pd.Series:
    """Cash return over each interval (dates[i-1], dates[i]] at the rate known on dates[i-1].

    rate is forward-filled from its own dates onto `dates`; the first date, and
    any date before the rate starts, is NaN.
    """
    dates = pd.DatetimeIndex(dates).sort_values()
    known = rate.sort_index().reindex(rate.index.union(dates)).ffill().reindex(dates)
    days = dates.to_series().diff().dt.days
    return (known.shift(1) * days / day_count).set_axis(dates).rename(rate.name)


def caveats_for(currency: str) -> list[tuple[str | None, str | None, str]]:
    """RATE_CAVEATS entries for one currency, as (start, end, reason)."""
    return [(s, e, why) for c, s, e, why in RATE_CAVEATS if c == currency]
