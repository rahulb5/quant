"""
src/returns/dividends.py

Daily total returns (local currency) for the equity price indices.

Each index gets one method (INDEX_TOTAL_RETURN):

- "tr_index":    a published total-return index (^SP500TR, ^RUTTR). Exact.
- "etf":         price-index return + trailing 12-month dividend yield / 252.
                 The yield comes from a local-currency distributing ETF: each
                 cash dividend (dividends table) / the ETF's prior close.
- "performance": the index already reinvests dividends (DAX). No adjustment.

Point in time: the yield used on day t is the trailing yield known at t-1.
ETF dividends are paid net of fund costs and, for some domiciles, withholding
tax, so "etf" understates gross dividends by roughly 0.1-0.5 pp a year. That's
close to what an index futures position earns, which is the target. Checked
on the S&P 500: SPY's dividends reproduce ^SP500TR to -0.06 pp/yr, TE 0.23%.

Before a source's coverage starts (ETFs from ~2008, ^SP500TR from 1988),
pre_coverage="drop" leaves NaN (the default, as with E2 in the macro plan);
"hold_first" back-fills the first available trailing yield and records the
first non-back-filled date in .attrs["approx_until"].

Usage:
    from src.db.client import db
    from src.returns.dividends import load_index_total_returns

    db.open()
    tr = load_index_total_returns(db)                 # all covered indices
    tr = load_index_total_returns(db, [100004], pre_coverage="hold_first")
    db.close()
"""

from __future__ import annotations

import warnings
from typing import Literal

import pandas as pd

from src.db.client import Database

PERIODS_PER_YEAR = 252
YIELD_WINDOW = "365D"
MAX_EVENT_YIELD = 0.05   # a single dividend above 5% of price is treated as a data error

Method = Literal["tr_index", "etf", "performance"]
PreCoverage = Literal["drop", "hold_first"]

# price index id -> (method, source asset id)
INDEX_TOTAL_RETURN: dict[int, tuple[Method, int | None]] = {
    100001: ("tr_index", 100016),     # ^GSPC     <- ^SP500TR
    100002: ("etf", 500002),          # ^NDX      <- QQQ
    100003: ("tr_index", 100017),     # ^RUT      <- ^RUTTR
    100004: ("etf", 500003),          # ^STOXX50E <- EXW1.DE
    100005: ("etf", 500004),          # ^FTSE     <- ISF.L
    100006: ("performance", None),    # ^GDAXI is a total-return index
    100009: ("etf", 500009),          # ^N225     <- 1329.T
    100010: ("etf", 500008),          # ^HSI      <- 2800.HK
    100013: ("etf", 500006),          # ^GSPTSE   <- XIU.TO (TSX 60 yield)
    100014: ("etf", 500007),          # ^AXJO     <- STW.AX
}


# ── Pure functions ────────────────────────────────────────────────────────────

def dividend_events(close: pd.Series, amounts: pd.Series) -> pd.Series:
    """Dividend / prior close on each ex-date, 0 on other days, on close's dates.

    An ex-date that isn't a trading day moves to the next one. Events above
    MAX_EVENT_YIELD (unit or split errors in the source) are zeroed with a
    warning.
    """
    ev = pd.Series(0.0, index=close.index, name=close.name)
    if amounts.empty:
        return ev
    pos = close.index.searchsorted(pd.DatetimeIndex(amounts.index))
    keep = (pos > 0) & (pos < len(close))      # need a prior close
    for p, amt in zip(pos[keep], amounts.to_numpy()[keep]):
        ev.iloc[p] += amt / close.iloc[p - 1]
    bad = ev > MAX_EVENT_YIELD
    if bad.any():
        warnings.warn(
            f"{close.name}: dropped {int(bad.sum())} implausible dividend event(s) "
            f"on {[d.date().isoformat() for d in ev.index[bad]][:5]}"
        )
        ev = ev.mask(bad, 0.0)
    return ev


def trailing_yield(daily_dividends: pd.Series, window: str = YIELD_WINDOW) -> pd.Series:
    """Trailing sum of daily dividend fractions; NaN until one full window exists."""
    s = daily_dividends.sort_index()
    out = s.rolling(window).sum()
    first_full = s.index[0] + pd.Timedelta(window)
    return out.where(out.index >= first_full)


def accrued_total_return(price: pd.Series, div_yield: pd.Series) -> pd.Series:
    """Price return + yesterday's trailing yield / PERIODS_PER_YEAR, on price's dates."""
    y = div_yield.reindex(price.index.union(div_yield.index)).ffill().reindex(price.index)
    return price.pct_change() + y.shift(1) / PERIODS_PER_YEAR


def _hold_first(yld: pd.Series, index: pd.DatetimeIndex) -> tuple[pd.Series, pd.Timestamp | None]:
    y = yld.reindex(index.union(yld.index)).ffill().reindex(index)
    first = y.first_valid_index()
    if first is None:
        return y, None
    return y.fillna(y.loc[first]), first


# ── DB loaders ────────────────────────────────────────────────────────────────

def _load_closes(db: Database, ids: list[int]) -> dict[int, pd.Series]:
    df = db.query_df(
        f"""
        SELECT asset_id, timezone('UTC', timestamp)::DATE AS d, close
        FROM prices
        WHERE interval = '1d' AND asset_id IN ({",".join("?" * len(ids))})
        ORDER BY asset_id, d
        """,
        ids,
    )
    df["d"] = pd.to_datetime(df["d"])
    out = {}
    for i in ids:
        sub = df[df["asset_id"] == i]
        out[i] = pd.Series(sub["close"].to_numpy(dtype=float), index=pd.DatetimeIndex(sub["d"]), name=i)
    return out


def load_dividends(db: Database, asset_id: int) -> pd.Series:
    """Cash dividend per share by ex-date, in the asset's price units."""
    df = db.query_df(
        "SELECT ex_date, amount FROM dividends WHERE asset_id = ? ORDER BY ex_date", [asset_id]
    )
    return pd.Series(df["amount"].to_numpy(dtype=float), index=pd.DatetimeIndex(df["ex_date"]), name=asset_id)


def load_index_total_returns(
    db: Database,
    index_ids: list[int] | None = None,
    pre_coverage: PreCoverage = "drop",
) -> pd.DataFrame:
    """Daily local-currency total returns, columns = price index ids.

    .attrs["method"]: id -> method. With pre_coverage="hold_first",
    .attrs["approx_until"]: id -> first date not using a back-filled yield.
    """
    if pre_coverage not in ("drop", "hold_first"):
        raise ValueError(f"pre_coverage must be 'drop' or 'hold_first', got {pre_coverage!r}")
    index_ids = list(INDEX_TOTAL_RETURN) if index_ids is None else index_ids
    unknown = [i for i in index_ids if i not in INDEX_TOTAL_RETURN]
    if unknown:
        raise ValueError(f"No total-return method for index ids {unknown}")

    sources = [INDEX_TOTAL_RETURN[i][1] for i in index_ids if INDEX_TOTAL_RETURN[i][1] is not None]
    closes = _load_closes(db, sorted(set(index_ids) | set(sources)))

    cols: dict[int, pd.Series] = {}
    methods: dict[int, str] = {}
    approx_until: dict[int, pd.Timestamp] = {}
    for idx_id in index_ids:
        method, src = INDEX_TOTAL_RETURN[idx_id]
        price = closes[idx_id]
        methods[idx_id] = method

        if method == "performance":
            cols[idx_id] = price.pct_change()
            continue

        src_close = closes[src]
        if method == "tr_index":
            tr = src_close.pct_change().reindex(price.index)
            if pre_coverage == "hold_first" and tr.notna().any():
                # Before the TR index starts: accrue the yield implied by its first year.
                implied = (tr - price.pct_change()).dropna()
                filled, _ = _hold_first(trailing_yield(implied), price.index)
                approx_until[idx_id] = tr.first_valid_index()
                tr = tr.where(tr.index >= approx_until[idx_id], accrued_total_return(price, filled))
            cols[idx_id] = tr
            continue

        if src_close.empty:
            yld = pd.Series(dtype=float)
        else:
            yld = trailing_yield(dividend_events(src_close, load_dividends(db, src)))
        if pre_coverage == "hold_first":
            yld, first = _hold_first(yld, price.index)
            if first is not None:
                approx_until[idx_id] = first
        cols[idx_id] = accrued_total_return(price, yld)

    out = pd.DataFrame(cols)
    out.attrs["method"] = methods
    if pre_coverage == "hold_first":
        out.attrs["approx_until"] = approx_until
    return out
