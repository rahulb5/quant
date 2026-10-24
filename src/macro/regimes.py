"""
src/macro/regimes.py

The macro regime layer (C6): `Regime` subclasses that read the C3/C4/C5 state
and compute no inputs of their own. Definitions are frozen in
docs/research/macro_state_definitions_v1.md section 4.

    layer "macro":   growth_inflation, growth_level, policy, financial_conditions
    layer "market":  curve_dynamics, dollar_trend

Lookahead. Every label on date t uses only the state on or before t (the state is
built from release-dated, point-in-time inputs). The `min_duration` rule is causal:
a new label takes effect only once it has held for `min_duration` consecutive
business days, so it lags the change instead of rewriting history.

Boundary rule. A signed composite exactly at 0 keeps the previous label.

All regimes in a list share one state panel (`StatePanel`), built once from the DB
(about 6 s) or injected for DB-free use:

    regimes = macro_regimes(database=ro)                  # all six, sharing a panel
    Conditioner(returns, regimes=regimes, ...)

    reg = GrowthInflationRegime(panel=StatePanel.from_frame(df))   # tests / notebooks
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from src.backtest.regimes import (
    Regime,
    RatesRegime,
    TrendRegime,
    VolRegime,
    expanding_bucket_labels,
)

FC_BURN_IN = 756
GROWTH_INFLATION_MIN_DURATION = 21
POLICY_DEADBAND = 0.25          # percentage points of the 6-month change in DFF


# ── Pure label functions ──────────────────────────────────────────────────────

def sign_hold(x: pd.Series) -> pd.Series:
    """+1 where x > 0, -1 where x < 0; a value of exactly 0 keeps the previous sign.
    NaN where x is NaN (and where there is no earlier sign to keep)."""
    s = np.sign(x).where(x.notna())
    return s.where(s != 0).ffill().where(x.notna())


def min_duration_labels(raw: pd.Series, min_duration: int) -> pd.Series:
    """Causal hysteresis on a categorical series.

    The first valid label is adopted at once. After that a different label becomes
    current only when it has been the raw label for `min_duration` consecutive
    observations (on the day it completes the run, never retroactively). A NaN
    resets the state. `min_duration <= 1` returns `raw` unchanged.
    """
    if min_duration <= 1:
        return raw
    out: list = []
    cur, cand, run = None, None, 0
    for v in raw.to_numpy(dtype=object):
        if v is None or (isinstance(v, float) and np.isnan(v)):
            cur, cand, run = None, None, 0
            out.append(np.nan)
            continue
        if cur is None:
            cur = v
        elif v == cur:
            cand, run = None, 0
        else:
            run = run + 1 if v == cand else 1
            cand = v
            if run >= min_duration:
                cur, cand, run = v, None, 0
        out.append(cur)
    return pd.Series(out, index=raw.index, dtype=object)


def growth_inflation_labels(g_impulse: pd.Series, pi_impulse: pd.Series,
                            min_duration: int = GROWTH_INFLATION_MIN_DURATION) -> pd.Series:
    """sign(G_impulse) x sign(pi_impulse): goldilocks (G up, pi down), reflation (up, up),
    stagflation (down, up), deflation (down, down); then the min_duration rule."""
    g, p = sign_hold(g_impulse), sign_hold(pi_impulse)
    raw = pd.Series(np.nan, index=g.index, dtype=object)
    for gs, ps, name in ((1, -1, "goldilocks"), (1, 1, "reflation"), (-1, 1, "stagflation"), (-1, -1, "deflation")):
        raw[(g == gs) & (p == ps)] = name
    return min_duration_labels(raw, min_duration)


def growth_level_labels(g_level: pd.Series) -> pd.Series:
    """above_trend where G_level > 0 (its own expanding mean is ~0 by construction)."""
    s = sign_hold(g_level)
    return pd.Series(np.where(s > 0, "above_trend", "below_trend"), index=s.index, dtype=object).where(s.notna())


def policy_labels(p_stance: pd.Series, p_impulse: pd.Series, deadband: float = POLICY_DEADBAND) -> pd.Series:
    """Stance (sign of P_stance: restrictive > 0, else accommodative) x direction.

    Direction becomes `tightening` once P_impulse >= +deadband and `easing` once
    <= -deadband; in between the previous direction holds. It is undefined (NaN
    label) until the first crossing.
    """
    stance = sign_hold(p_stance)
    direction = pd.Series(np.nan, index=p_impulse.index, dtype=object)
    direction[p_impulse >= deadband] = "tightening"
    direction[p_impulse <= -deadband] = "easing"
    direction = direction.ffill().where(p_impulse.notna())
    stance = stance.reindex(direction.index)
    word = pd.Series(np.where(stance > 0, "restrictive", "accommodative"), index=stance.index, dtype=object)
    return (word + "_" + direction).where(stance.notna() & direction.notna())


def financial_conditions_labels(fc: pd.Series, burn_in: int = FC_BURN_IN) -> pd.Series:
    """Expanding terciles of FC: loose (lowest), normal, tight (highest)."""
    s = fc.dropna()
    if s.empty:
        return pd.Series(dtype=object)
    lab = expanding_bucket_labels(s, n_buckets=3, burn_in=burn_in, labels=["loose", "normal", "tight"])
    return lab.reindex(fc.index)


# ── State panel (the only DB-touching part) ───────────────────────────────────

PANEL_COLUMNS = ["G_level", "G_impulse", "pi_impulse", "P_stance", "P_impulse", "FC", "L_impulse",
                 "curve_state", "usd_state"]


class StatePanel:
    """A business-day DataFrame of every state column the regimes and flags read.

    Build with StatePanel.from_db() (cached per instance) or StatePanel.from_frame().
    """

    def __init__(self, frame: pd.DataFrame | None = None, database=None,
                 start: str | None = None, end: str | None = None) -> None:
        self._frame = frame
        self._database = database
        self._start = start
        self._end = end

    @classmethod
    def from_frame(cls, frame: pd.DataFrame) -> "StatePanel":
        return cls(frame=frame)

    @property
    def frame(self) -> pd.DataFrame:
        if self._frame is None:
            self._frame = self._build()
        return self._frame

    def _build(self) -> pd.DataFrame:
        from src.db.client import db as singleton
        from src.macro.flags import event_flags
        from src.macro.inputs import (
            load_flag_inputs, load_market_inputs, load_rates_inputs, load_state_inputs,
        )
        from src.macro.market import dollar
        from src.macro.rates import curve
        from src.macro.state import macro_state

        database = self._database if self._database is not None else singleton
        index = pd.bdate_range(self._start or "1954-07-01", self._end or pd.Timestamp.today().normalize())
        state_in = load_state_inputs(database)
        state = macro_state(state_in, index)
        flag_in = load_flag_inputs(database)
        flag_in.setdefault("t10y3m", state_in.get("t10y3m", pd.Series(dtype=float)))
        out = state.copy()
        out["curve_state"] = curve(load_rates_inputs(database), index)["curve_state"]
        out["usd_state"] = dollar(load_market_inputs(database), index)["usd_state"]
        flags = event_flags(state, flag_in, index)
        return pd.concat([out, flags], axis=1)


# ── Regime classes ────────────────────────────────────────────────────────────

class MacroRegime(Regime):
    """Base for regimes that read a StatePanel instead of loading a level series."""

    layer: str = "macro"
    columns: tuple[str, ...] = ()

    def __init__(self, panel: StatePanel | None = None, database=None,
                 start: str | None = None, end: str | None = None) -> None:
        super().__init__(database=database, start=start, end=end)
        self._panel = panel if panel is not None else StatePanel(database=database, start=start, end=end)

    def _cols(self) -> pd.DataFrame:
        f = self._panel.frame
        missing = [c for c in self.columns if c not in f.columns]
        if missing:
            raise KeyError(f"[{self.name}] state panel is missing columns {missing}")
        f = f.loc[self._start:self._end] if (self._start or self._end) else f
        return f[list(self.columns)]


class GrowthInflationRegime(MacroRegime):
    """goldilocks / reflation / stagflation / deflation, min_duration 21 business days."""
    name = "growth_inflation"
    columns = ("G_impulse", "pi_impulse")

    def __init__(self, min_duration: int = GROWTH_INFLATION_MIN_DURATION, **kw) -> None:
        super().__init__(**kw)
        self._min_duration = min_duration

    def labels(self) -> pd.Series:
        c = self._cols()
        return growth_inflation_labels(c["G_impulse"], c["pi_impulse"], self._min_duration)


class GrowthLevelRegime(MacroRegime):
    """above_trend / below_trend by the sign of G_level."""
    name = "growth_level"
    columns = ("G_level",)

    def labels(self) -> pd.Series:
        return growth_level_labels(self._cols()["G_level"])


class PolicyRegime(MacroRegime):
    """restrictive / accommodative x tightening / easing (±25 bp dead-band, direction held)."""
    name = "policy"
    columns = ("P_stance", "P_impulse")

    def labels(self) -> pd.Series:
        c = self._cols()
        return policy_labels(c["P_stance"], c["P_impulse"])


class FinancialConditionsRegime(MacroRegime):
    """loose / normal / tight: expanding terciles of FC."""
    name = "financial_conditions"
    columns = ("FC",)

    def labels(self) -> pd.Series:
        return financial_conditions_labels(self._cols()["FC"])


class CurveDynamicsRegime(MacroRegime):
    """bull/bear x steepener/flattener from the 126-day (6-month) changes in DGS10 and T10Y2Y."""
    layer = "market"
    name = "curve_dynamics"
    columns = ("curve_state",)

    def labels(self) -> pd.Series:
        return self._cols()["curve_state"].astype(object)


class DollarTrendRegime(MacroRegime):
    """strong_usd / weak_usd: DTWEXBGS vs its 200-day mean. Approximate before 2019-02."""
    layer = "market"
    name = "dollar_trend"
    columns = ("usd_state",)

    def labels(self) -> pd.Series:
        return self._cols()["usd_state"].astype(object)


# Existing market regimes get a layer so the Diagnostics tab can group everything.
for _cls in (VolRegime, RatesRegime, TrendRegime):
    _cls.layer = "market"

REGIME_REGISTRY: dict[str, type[Regime]] = {
    cls.name: cls for cls in (
        GrowthInflationRegime, GrowthLevelRegime, PolicyRegime, FinancialConditionsRegime,
        CurveDynamicsRegime, DollarTrendRegime, VolRegime, RatesRegime, TrendRegime,
    )
}

MACRO_REGIME_NAMES = ["growth_inflation", "growth_level", "policy", "financial_conditions",
                      "curve_dynamics", "dollar_trend"]


def macro_regimes(database=None, start: str | None = None, end: str | None = None,
                  names: list[str] | None = None, panel: StatePanel | None = None) -> list[Regime]:
    """The macro-layer regimes (or `names` from MACRO_REGIME_NAMES), sharing one state panel."""
    panel = panel if panel is not None else StatePanel(database=database, start=start, end=end)
    chosen = names or MACRO_REGIME_NAMES
    unknown = [n for n in chosen if n not in MACRO_REGIME_NAMES]
    if unknown:
        raise KeyError(f"unknown macro regimes {unknown}; expected {MACRO_REGIME_NAMES}")
    return [REGIME_REGISTRY[n](panel=panel, database=database) for n in chosen]
