"""
src/macro/rates.py

Rates decomposition (C4). Pure pandas, no DB access: it takes the release-dated
daily series built by src.macro.inputs.load_rates_inputs. Definitions are frozen
in docs/research/macro_state_definitions_v1.md section 2.

All series are forward-filled onto a business-day grid; changes are over
CHANGE_WINDOW = 126 business days (~6 months). Yields are in percent, so 10 bp = 0.10.

Usage:
    inputs = load_rates_inputs(ro)
    idx = pd.bdate_range("1990-01-02", "2026-10-23")
    dec = nominal_decomposition(inputs, idx)          # 10Y; tenor=5 for 5Y
    curve(inputs, idx)[["T10Y2Y", "curve_state"]].dropna().tail()
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from src.macro.pit import to_daily

CHANGE_WINDOW = 126
ATTRIBUTION_MIN_MOVE = 0.10     # percent: shares only reported when |d_nominal| exceeds this
PROXY_DURATION = 8.5            # constant modified duration of the Treasury proxy, years
PROXY_DAYS = 252

_TENOR = {
    10: ("DGS10", "DFII10", "T10YIE"),
    5: ("DGS5", "DFII5", "T5YIE"),
}


def _daily(inputs: dict[str, pd.Series], code: str, index: pd.DatetimeIndex) -> pd.Series:
    if code not in inputs:
        return pd.Series(np.nan, index=index, dtype=float, name=code)
    return to_daily(inputs[code], index).rename(code)


def nominal_decomposition(
    inputs: dict[str, pd.Series], index: pd.DatetimeIndex, tenor: int = 10, window: int = CHANGE_WINDOW,
) -> pd.DataFrame:
    """nominal = real + breakeven, with `window`-day changes attributed to each part.

    Columns: nominal, real, breakeven, resid (= nominal - real - breakeven, ~0 by
    construction), d_nominal, d_real, d_breakeven, share_real, share_breakeven.
    Shares are NaN unless |d_nominal| > 10 bp. 5y5y forward is T5YIFR in `forward_5y5y`.
    Valid from 2003 (TIPS series). Caveat: TIPS were illiquid in late 2008, so
    real yields and breakevens are distorted then (breakevens collapsed with no
    inflation news); don't read that episode as an inflation signal.
    """
    nom_c, real_c, be_c = _TENOR[tenor]
    nom, real, be = (_daily(inputs, c, index) for c in (nom_c, real_c, be_c))
    out = pd.DataFrame({"nominal": nom, "real": real, "breakeven": be})
    out["resid"] = nom - real - be
    for c in ("nominal", "real", "breakeven"):
        out[f"d_{c}"] = out[c] - out[c].shift(window)
    big = out["d_nominal"].abs() > ATTRIBUTION_MIN_MOVE
    out["share_real"] = (out["d_real"] / out["d_nominal"]).where(big)
    out["share_breakeven"] = (out["d_breakeven"] / out["d_nominal"]).where(big)
    out["forward_5y5y"] = _daily(inputs, "T5YIFR", index)
    return out


def term_premium(inputs: dict[str, pd.Series], index: pd.DatetimeIndex, window: int = CHANGE_WINDOW) -> pd.DataFrame:
    """THREEFYTP10 level and change. A model estimate (real-time from 2016 only; earlier
    values are revised history, so under the default pre_vintage="drop" the loader
    returns it from 2016 only; use "latest_lagged" for exploration). Shown beside the decomposition, never added to it:
    the model's term premium is already inside both the real and breakeven parts."""
    tp = _daily(inputs, "THREEFYTP10", index)
    return pd.DataFrame({"term_premium": tp, "d_term_premium": tp - tp.shift(window)})


def curve(inputs: dict[str, pd.Series], index: pd.DatetimeIndex, window: int = CHANGE_WINDOW) -> pd.DataFrame:
    """Curve levels, 6-month changes and the bull/bear steepener/flattener state.

    bull/bear = d_DGS10 < 0 / >= 0; steepener/flattener = d_T10Y2Y >= 0 / < 0.
    State is NaN where either change is unavailable. T10Y2Y from 1976, T10Y3M from 1982.
    """
    dgs10 = _daily(inputs, "DGS10", index)
    t10y2y = _daily(inputs, "T10Y2Y", index)
    t10y3m = _daily(inputs, "T10Y3M", index)
    out = pd.DataFrame({
        "T10Y2Y": t10y2y, "T10Y3M": t10y3m,
        "d_DGS10": dgs10 - dgs10.shift(window), "d_T10Y2Y": t10y2y - t10y2y.shift(window),
    })
    known = out["d_DGS10"].notna() & out["d_T10Y2Y"].notna()
    side = np.where(out["d_DGS10"] < 0, "bull", "bear")
    shape = np.where(out["d_T10Y2Y"] >= 0, "steepener", "flattener")
    out["curve_state"] = pd.Series(side, index=index).str.cat(pd.Series(shape, index=index), sep="_").where(known)
    return out


def policy_expectations(inputs: dict[str, pd.Series], index: pd.DatetimeIndex, window: int = CHANGE_WINDOW) -> pd.DataFrame:
    """DGS2 - DFF and DGS1 - DFF: a free proxy for OIS-implied policy over 1-2 years.
    It includes term premium, so it is a proxy, not an expectation. From 1976."""
    dff = _daily(inputs, "DFF", index)
    out = pd.DataFrame({
        "policy_exp_2y": _daily(inputs, "DGS2", index) - dff,
        "policy_exp_1y": _daily(inputs, "DGS1", index) - dff,
    })
    out["d_policy_exp_2y"] = out["policy_exp_2y"] - out["policy_exp_2y"].shift(window)
    return out


def treasury_proxy_returns(inputs: dict[str, pd.Series], index: pd.DatetimeIndex) -> pd.Series:
    """Constant-duration Treasury proxy from the 10Y yield (D4):

        r_t = DGS10_{t-1}/100/252 - 8.5 * (DGS10_t - DGS10_{t-1})/100

    Carry plus a duration-weighted price change. Days when DGS10 is unchanged from a
    holiday forward-fill earn carry only. From 1962. An approximation, not a bond index.
    """
    y = _daily(inputs, "DGS10", index)
    r = y.shift(1) / 100 / PROXY_DAYS - PROXY_DURATION * (y - y.shift(1)) / 100
    return r.rename("treasury_proxy")
