"""
src/macro/market.py

Market state and positioning (C5). Not macro state: these are what markets are
pricing, used for the pricing comparison (C7) and as conditioning variables.
Pure pandas, no DB access: it takes the dict built by
src.macro.inputs.load_market_inputs. Definitions are frozen in
docs/research/macro_state_definitions_v1.md section 3.

Inputs (keys of the dict, each a release-dated Series): FRED codes
BAMLH0A0HYM2, BAMLC0A0CM, BAMLC0A4CBBB, DTWEXBGS, VIXCLS, T10Y2Y, T10YIE, DGS2, DFF;
prices spx, rut, copper, gold; positioning keys from POSITIONING.

Usage:
    inputs = load_market_inputs(ro)
    idx = pd.bdate_range("2000-01-03", "2026-10-23")
    m = market_state(inputs, idx)
    m[["hy_oas", "strong_usd", "vol_regime", "growth_pricing"]].dropna().tail()
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from src.backtest.regimes import expanding_bucket_labels, ma_trend_labels
from src.macro.pit import to_daily
from src.macro.state import BURN_IN, CHANGE_WINDOW, _composite, expanding_z

MA_WINDOW = 200
VOL_BURN_IN = 756
VOL_LABELS = ["low_vol", "mid_vol", "high_vol"]    # same as src.backtest.regimes.VolRegime
COT_AVAILABLE_LAG_DAYS = 3                         # report is for Tuesday, published Friday

# Daily z-score composites: (input key, frequency, sign). Growth pricing is built
# from derived 63-day changes, see growth_pricing().
# Positioning: key -> (series code, measure, label). Leveraged funds on TFF for
# rates / equity index / FX, managed money on disaggregated for commodities.
POSITIONING: dict[str, tuple[str, str, str]] = {
    "pos_ust10y": ("CFTC.TFF_FUT.043602", "lf_net", "10Y note, leveraged funds"),
    "pos_es":     ("CFTC.TFF_FUT.13874A", "lf_net", "E-mini S&P, leveraged funds"),
    "pos_eur":    ("CFTC.TFF_FUT.099741", "lf_net", "EUR, leveraged funds"),
    "pos_jpy":    ("CFTC.TFF_FUT.097741", "lf_net", "JPY, leveraged funds"),
    "pos_crude":  ("CFTC.DISAGG_FUT.067651", "mm_net", "WTI crude, managed money"),
    "pos_gold":   ("CFTC.DISAGG_FUT.088691", "mm_net", "Gold, managed money"),
    "pos_copper": ("CFTC.DISAGG_FUT.085692", "mm_net", "Copper, managed money"),
}


def _d(inputs: dict[str, pd.Series], key: str, index: pd.DatetimeIndex) -> pd.Series:
    if key not in inputs or inputs[key].empty:
        return pd.Series(np.nan, index=index, dtype=float, name=key)
    return to_daily(inputs[key], index).rename(key)


def _chg(s: pd.Series, n: int = CHANGE_WINDOW) -> pd.Series:
    return s - s.shift(n)


def credit(inputs: dict[str, pd.Series], index: pd.DatetimeIndex) -> pd.DataFrame:
    """HY OAS level and 63-day change, and BBB OAS - IG OAS (all in percent). From 1997."""
    hy = _d(inputs, "BAMLH0A0HYM2", index)
    return pd.DataFrame({
        "hy_oas": hy, "d_hy_oas": _chg(hy),
        "bbb_minus_ig": _d(inputs, "BAMLC0A4CBBB", index) - _d(inputs, "BAMLC0A0CM", index),
    })


def dollar(inputs: dict[str, pd.Series], index: pd.DatetimeIndex) -> pd.DataFrame:
    """DTWEXBGS / its 200-day mean - 1; > 0 is `strong_usd`, else `weak_usd`. A value of
    exactly 0 is `weak_usd`. From 2006 (plus 200 days).

    Approximate before the series' first vintage (2019-02-04): those 2006-2018 values are
    the Fed's back-filled latest values, available 10 days after each period, not what was
    published then (see pit.PRE_VINTAGE_LATEST_LAGGED). `usd_approx` is True on those days;
    against real-time vintages the label differs on ~4.5% of days (2019-2026)."""
    usd = _d(inputs, "DTWEXBGS", index)
    first_vintage = inputs["DTWEXBGS"].attrs.get("first_vintage") if "DTWEXBGS" in inputs else None
    gap = usd / usd.rolling(MA_WINDOW, min_periods=MA_WINDOW).mean() - 1
    label = pd.Series(np.where(gap > 0, "strong_usd", "weak_usd"), index=index).where(gap.notna())
    approx = pd.Series(index < pd.Timestamp(first_vintage), index=index) if first_vintage is not None \
        else pd.Series(False, index=index)
    return pd.DataFrame({"usd_gap": gap, "usd_state": label, "usd_approx": approx.where(gap.notna(), False)})


def volatility(inputs: dict[str, pd.Series], index: pd.DatetimeIndex) -> pd.DataFrame:
    """VIX level and expanding tercile (burn-in 756 daily observations, as VolRegime
    but on the release-dated series). From 1990 (+ burn-in)."""
    vix = _d(inputs, "VIXCLS", index)
    out = pd.DataFrame({"vix": vix})
    src = inputs.get("VIXCLS")
    if src is None or src.empty:
        out["vol_regime"] = np.nan
        return out
    lab = expanding_bucket_labels(src[~src.index.duplicated(keep="last")].dropna(),
                                  burn_in=VOL_BURN_IN, labels=VOL_LABELS)
    lab = lab.dropna()
    out["vol_regime"] = lab.reindex(lab.index.union(index)).ffill().reindex(index) if len(lab) else np.nan
    return out


def equity_trend(inputs: dict[str, pd.Series], index: pd.DatetimeIndex) -> pd.Series:
    """S&P 500 vs its 200-day mean: `risk_on` above, `risk_off` otherwise (as TrendRegime)."""
    spx = inputs.get("spx")
    if spx is None or spx.empty:
        return pd.Series(np.nan, index=index, dtype=object, name="equity_trend")
    s = spx[~spx.index.duplicated(keep="last")].dropna()
    lab = ma_trend_labels(s, MA_WINDOW, ("risk_off", "risk_on")).dropna()
    return lab.reindex(lab.index.union(index)).ffill().reindex(index).rename("equity_trend")


def growth_pricing(inputs: dict[str, pd.Series], index: pd.DatetimeIndex) -> pd.DataFrame:
    """What markets price for growth: mean of expanding z-scores (burn-in 756) of 63-day
    changes in T10Y2Y, -HY OAS, log(Russell 2000 / S&P 500) and log(copper / gold).
    Positive = markets price stronger growth. Needs >= 2 components. From ~2000.
    Copper and gold are front-month futures, not roll-adjusted, so the ratio carries
    small roll gaps."""
    comps: dict[str, pd.Series] = {}

    def add(name: str, s: pd.Series, sign: int = 1) -> None:
        d = _chg(to_daily(s.dropna(), index)) if not s.empty else s
        comps[name] = d.where(d.notna()).dropna()
        comps[name] = comps[name] * sign

    for name, key, sign in (("curve", "T10Y2Y", 1), ("credit", "BAMLH0A0HYM2", -1)):
        if key in inputs:
            add(name, inputs[key], sign)
    for name, a, b in (("small_vs_large", "rut", "spx"), ("copper_gold", "copper", "gold")):
        if a in inputs and b in inputs:
            ratio = np.log(to_daily(inputs[a], index) / to_daily(inputs[b], index))
            comps[name] = _chg(ratio).dropna()
    spec = [(k, "daily", +1) for k in comps]
    res = _composite(comps, spec, index, 2)
    return pd.DataFrame({"growth_pricing": res["value"], "growth_pricing_n": res["n"]})


def _z_of_change(s: pd.Series, index: pd.DatetimeIndex) -> pd.Series:
    d = _chg(to_daily(s, index)).dropna()
    return to_daily(expanding_z(d, BURN_IN["daily"]), index)


def inflation_pricing(inputs: dict[str, pd.Series], index: pd.DatetimeIndex) -> pd.Series:
    """63-day change in T10YIE, expanding z (burn-in 756). From 2003 (+ burn-in)."""
    if "T10YIE" not in inputs:
        return pd.Series(np.nan, index=index, name="inflation_pricing")
    return _z_of_change(inputs["T10YIE"], index).rename("inflation_pricing")


def policy_pricing(inputs: dict[str, pd.Series], index: pd.DatetimeIndex) -> pd.Series:
    """63-day change in DGS2 - DFF, expanding z (burn-in 756). From 1976 (+ burn-in)."""
    if "DGS2" not in inputs or "DFF" not in inputs:
        return pd.Series(np.nan, index=index, name="policy_pricing")
    spread = (_d(inputs, "DGS2", index) - _d(inputs, "DFF", index)).dropna()
    return _z_of_change(spread, index).rename("policy_pricing")


def positioning(inputs: dict[str, pd.Series], index: pd.DatetimeIndex) -> pd.DataFrame:
    """Net % of open interest as an expanding z-score (burn-in 156 weeks), one column per
    POSITIONING key. Inputs must already be dated when usable (report date + 3 days,
    see inputs.load_market_inputs). From 2006."""
    cols = {}
    for key in POSITIONING:
        s = inputs.get(key)
        if s is None or s.empty:
            cols[key] = pd.Series(np.nan, index=index, dtype=float)
        else:
            cols[key] = to_daily(expanding_z(s, BURN_IN["weekly"]), index)
    return pd.DataFrame(cols, index=index)


def market_state(inputs: dict[str, pd.Series], index: pd.DatetimeIndex) -> pd.DataFrame:
    """Every market variable above on a business-day `index`.

    Columns: hy_oas, d_hy_oas, bbb_minus_ig, usd_gap, usd_state, usd_approx, vix, vol_regime,
    equity_trend, growth_pricing, growth_pricing_n, inflation_pricing,
    policy_pricing, and one z-scored column per positioning key.
    """
    parts = [
        credit(inputs, index), dollar(inputs, index), volatility(inputs, index),
        equity_trend(inputs, index), growth_pricing(inputs, index),
        inflation_pricing(inputs, index), policy_pricing(inputs, index),
        positioning(inputs, index),
    ]
    return pd.concat(parts, axis=1)
