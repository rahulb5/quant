"""
src/macro/inputs.py

Release-dated, point-in-time input series for the macro state vector. This is
the only place that combines DB loading (src.macro.data) with the vintage
logic in src.macro.pit; src.macro.state is pure and takes the dict returned
here. Definitions are frozen in docs/research/macro_state_definitions_v1.md.

Every transform below is causal (the value at period p uses only periods <= p),
as pit.realtime() requires, and is applied to each vintage snapshot.

Usage:
    from src.macro.inputs import load_state_inputs
    from src.macro.state import macro_state

    inputs = load_state_inputs(ro)
    state = macro_state(inputs, pd.bdate_range("1960-01-03", "2026-10-23"))
"""

from __future__ import annotations

from typing import Callable

import pandas as pd

from src.db.client import Database
from src.macro.data import load_vintages
from src.macro.pit import PreVintage, pit_series
from src.macro.transforms import transform

Fn = Callable[[pd.Series], pd.Series]


# ── Causal transforms (applied to one snapshot) ───────────────────────────────

def yoy(s: pd.Series) -> pd.Series:
    """YoY % against the last observation on or before the same date a year earlier."""
    return transform(s, "yoy_pct")


def level(s: pd.Series) -> pd.Series:
    return s


def change(n: int) -> Fn:
    """n-period difference."""
    return lambda s: s - s.shift(n)


def yoy_then_change(n: int) -> Fn:
    """n-period change of the YoY %, e.g. n=6 for monthly series = 6-month change."""
    def fn(s: pd.Series) -> pd.Series:
        y = yoy(s)
        return y - y.shift(n)
    return fn


def avg_yoy(weeks: int) -> Fn:
    """YoY % of a trailing `weeks`-week average (weekly series)."""
    return lambda s: yoy(s.rolling(weeks).mean())


def avg_yoy_then_change(weeks: int, n: int) -> Fn:
    def fn(s: pd.Series) -> pd.Series:
        y = avg_yoy(weeks)(s)
        return y - y.shift(n)
    return fn


def gap_to_expanding_median(s: pd.Series) -> pd.Series:
    return s - s.expanding().median()


def pct_change(n: int) -> Fn:
    return lambda s: s.pct_change(n, fill_method=None) * 100


# input name -> (FRED code, transform, daily). Keys are what state.py consumes.
SPECS: dict[str, tuple[str, Fn, bool]] = {
    # growth
    "ip_yoy":            ("INDPRO",   yoy, False),
    "ip_yoy_d6":         ("INDPRO",   yoy_then_change(6), False),
    "pay_yoy":           ("PAYEMS",   yoy, False),
    "pay_yoy_d6":        ("PAYEMS",   yoy_then_change(6), False),
    "unr_gap":           ("UNRATE",   gap_to_expanding_median, False),
    "unr_d6":            ("UNRATE",   change(6), False),
    "claims_yoy13_d26":  ("ICNSA",    avg_yoy_then_change(13, 26), False),
    # labor
    "unr_d3":            ("UNRATE",   change(3), False),
    "claims_yoy13":      ("ICNSA",    avg_yoy(13), False),
    "cont_yoy13":        ("CCSA",     avg_yoy(13), False),
    "temp_6m":           ("TEMPHELPS", pct_change(6), False),
    "hours_d6":          ("AWHAETP",  change(6), False),
    # inflation
    "core_yoy":          ("CPILFENS", yoy, False),
    "core_yoy_d6":       ("CPILFENS", yoy_then_change(6), False),
    # policy, financial conditions
    "dff":               ("DFF",           level, True),
    "hy_oas":            ("BAMLH0A0HYM2",  level, True),
    "vix":               ("VIXCLS",        level, True),
    "t10y3m":            ("T10Y3M",        level, True),
    # credit supply
    "sloos":             ("DRTSCILM", level, False),
    "bank_credit_yoy":   ("TOTBKCR",  yoy, False),
}


RATES_CODES = [
    "DGS10", "DFII10", "T10YIE", "DGS5", "DFII5", "T5YIE", "T5YIFR", "THREEFYTP10",
    "T10Y2Y", "T10Y3M", "DGS2", "DGS1", "DFF",
]


def load_rates_inputs(db: Database, pre_vintage: PreVintage = "drop") -> dict[str, pd.Series]:
    """Daily release-dated levels for src.macro.rates, keyed by FRED code.

    Codes with no data in the DB are omitted (rates functions return NaN columns).
    """
    out: dict[str, pd.Series] = {}
    for code in RATES_CODES:
        v = load_vintages(db, code)
        if v.empty:
            continue
        s = pit_series(v, level, code, pre_vintage=pre_vintage, daily=True)
        if not s.empty:
            out[code] = s
    return out


def load_state_inputs(
    db: Database,
    names: list[str] | None = None,
    pre_vintage: PreVintage = "drop",
) -> dict[str, pd.Series]:
    """Build every input in SPECS (or just `names`) as a release-dated Series.

    Each Series is indexed by the date its value became usable. Raw vintages
    are loaded once per FRED code. An input whose series has no data in the DB
    is omitted (macro_state() then runs without it).
    """
    wanted = names if names is not None else list(SPECS)
    raw: dict[str, pd.DataFrame] = {}
    out: dict[str, pd.Series] = {}
    for name in wanted:
        code, fn, daily = SPECS[name]
        if code not in raw:
            raw[code] = load_vintages(db, code)
        if raw[code].empty:
            continue
        s = pit_series(raw[code], fn, code, pre_vintage=pre_vintage, daily=daily)
        if not s.empty:
            out[name] = s
    return out
