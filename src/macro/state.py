"""
src/macro/state.py

The macro state vector: for each dimension a level (where are we?) and an
impulse (which way are we moving?). Pure pandas, no DB access: it takes the
release-dated inputs built by src.macro.inputs. Definitions are frozen in
docs/research/macro_state_definitions_v1.md; this module only implements them.

Conventions (spec section 0):
  * z-scores are expanding (mean and std of everything up to and including
    the current release), computed once per release on the release-dated
    series, clipped at +/-3, and only after a burn-in;
  * a composite is the mean of its components' z-scores on the business-day
    grid, each signed so positive = more of the named thing;
  * outputs carry the component count (`*_n`) and the days since the newest
    input release (`*_staleness`).

Usage:
    state = macro_state(inputs, pd.bdate_range("1960-01-03", "2026-10-23"))
    state[["G_level", "G_impulse"]].dropna().tail()
"""

from __future__ import annotations

import pandas as pd

from src.macro.pit import staleness, to_daily

# Burn-in observations before a z-score is emitted, by sampling frequency.
BURN_IN: dict[str, int] = {"monthly": 60, "weekly": 156, "daily": 756, "quarterly": 20}
Z_CLIP = 3.0

# Business-day windows for market-style changes.
D126, D63 = 126, 63

# (input name, frequency, sign). Positive sign = more of what the composite is named for.
GROWTH_LEVEL = [("ip_yoy", "monthly", +1), ("pay_yoy", "monthly", +1), ("unr_gap", "monthly", -1)]
GROWTH_IMPULSE = [
    ("ip_yoy_d6", "monthly", +1), ("pay_yoy_d6", "monthly", +1),
    ("unr_d6", "monthly", -1), ("claims_yoy13_d26", "weekly", -1),
]
LABOR_IMPULSE = [
    ("unr_d3", "monthly", +1), ("claims_yoy13", "weekly", +1), ("cont_yoy13", "weekly", +1),
    ("temp_6m", "monthly", -1), ("hours_d6", "monthly", -1),
]
FIN_CONDITIONS = [("hy_oas", "daily", +1), ("vix", "daily", +1), ("t10y3m", "daily", -1)]
CREDIT_SUPPLY = [("sloos", "quarterly", +1), ("bank_credit_yoy", "weekly", -1)]

POLICY_MEDIAN_BURN_IN = 60   # monthly CPI releases
INFLATION_TARGET = 2.0


def expanding_z(s: pd.Series, burn_in: int, clip: float = Z_CLIP) -> pd.Series:
    """Expanding z-score of a release-dated series; NaN until `burn_in` observations."""
    s = s.dropna()
    mean = s.expanding(min_periods=burn_in).mean()
    std = s.expanding(min_periods=burn_in).std()
    return ((s - mean) / std.where(std > 0)).clip(-clip, clip).dropna()


def _composite(
    inputs: dict[str, pd.Series],
    spec: list[tuple[str, str, int]],
    index: pd.DatetimeIndex,
    min_components: int,
) -> pd.DataFrame:
    """Mean of signed daily z-scores. Columns: value, n, staleness."""
    zs, stale = [], []
    for name, freq, sign in spec:
        if name not in inputs or inputs[name].empty:
            continue
        z = expanding_z(inputs[name], BURN_IN[freq]) * sign
        if z.empty:
            continue
        zs.append(to_daily(z, index))
        stale.append(staleness(z, index))
    if not zs:
        nan = pd.Series(float("nan"), index=index)
        return pd.DataFrame({"value": nan, "n": 0, "staleness": nan})
    panel = pd.concat(zs, axis=1)
    n = panel.notna().sum(axis=1)
    value = panel.mean(axis=1).where(n >= min_components)
    return pd.DataFrame({
        "value": value,
        "n": n.where(value.notna(), 0).astype(int),
        "staleness": pd.concat(stale, axis=1).min(axis=1).where(value.notna()),
    })


def _asof(s: pd.Series, idx: pd.DatetimeIndex) -> pd.Series:
    """Last value of s on or before each date in idx."""
    return s.reindex(s.index.union(idx)).ffill().reindex(idx)


def macro_state(inputs: dict[str, pd.Series], index: pd.DatetimeIndex) -> pd.DataFrame:
    """The state vector on a business-day `index`.

    Columns: G_level, G_impulse, pi_level, pi_impulse, L_level, L_impulse,
    P_stance_raw, P_stance, P_impulse, FC, FC_impulse, CS, CS_impulse; the
    component count G_level_n, G_impulse_n, L_impulse_n, FC_n, CS_n; and
    {G,pi,L,P,FC,CS}_staleness (days since the newest input release).
    Missing inputs leave their columns NaN. DataFrame.attrs["valid_from"] maps
    each value column to its first non-NaN date, and DataFrame.attrs["approximate_before"]
    maps each input that uses back-filled latest values before its first vintage
    (pit.PRE_VINTAGE_LATEST_LAGGED, e.g. cont_yoy13 -> L_impulse) to that vintage date:
    the columns it feeds are approximate before then.
    """
    out: dict[str, pd.Series] = {}

    g_level = _composite(inputs, GROWTH_LEVEL, index, 2)
    g_imp = _composite(inputs, GROWTH_IMPULSE, index, 2)
    l_imp = _composite(inputs, LABOR_IMPULSE, index, 2)
    fc = _composite(inputs, FIN_CONDITIONS, index, len(FIN_CONDITIONS))   # same recipe all eras
    cs = _composite(inputs, CREDIT_SUPPLY, index, len(CREDIT_SUPPLY))

    out["G_level"], out["G_level_n"] = g_level["value"], g_level["n"]
    out["G_impulse"], out["G_impulse_n"] = g_imp["value"], g_imp["n"]
    out["L_impulse"], out["L_impulse_n"] = l_imp["value"], l_imp["n"]
    out["FC"], out["FC_n"] = fc["value"], fc["n"]
    out["FC_impulse"] = fc["value"] - fc["value"].shift(D63)
    out["CS"], out["CS_n"] = cs["value"], cs["n"]
    out["CS_impulse"] = cs["value"] - cs["value"].shift(D126)

    nan = pd.Series(float("nan"), index=index)

    # Inflation (raw pp, not z-scored): level vs the 2% target, impulse = 6-month change in YoY
    core = to_daily(inputs["core_yoy"], index) if "core_yoy" in inputs else nan
    out["pi_level"] = core - INFLATION_TARGET
    out["pi_impulse"] = to_daily(inputs["core_yoy_d6"], index) if "core_yoy_d6" in inputs else nan

    # Labor level: unemployment gap to its expanding median (pp)
    out["L_level"] = to_daily(inputs["unr_gap"], index) if "unr_gap" in inputs else nan

    # Policy: real policy rate relative to its own expanding median (on the CPI release grid)
    dff = to_daily(inputs["dff"], index) if "dff" in inputs else nan
    raw = dff - core
    out["P_stance_raw"] = raw
    if "dff" in inputs and "core_yoy" in inputs:
        cpi_rel = inputs["core_yoy"].index
        raw_m = (_asof(inputs["dff"], cpi_rel) - inputs["core_yoy"]).dropna()
        med = raw_m.expanding(min_periods=POLICY_MEDIAN_BURN_IN).median().dropna()
        out["P_stance"] = raw - to_daily(med, index)
    else:
        out["P_stance"] = nan
    out["P_impulse"] = dff - dff.shift(D126)

    # Staleness: days since the newest input release, per dimension
    def _stale(names: list[str]) -> pd.Series:
        cols = [staleness(inputs[n], index) for n in names if n in inputs]
        return pd.concat(cols, axis=1).min(axis=1) if cols else nan

    out["G_staleness"] = g_level["staleness"].combine_first(g_imp["staleness"])
    out["pi_staleness"] = _stale(["core_yoy"])
    out["L_staleness"] = l_imp["staleness"]
    out["P_staleness"] = _stale(["dff", "core_yoy"]).where(out["P_stance"].notna())
    out["FC_staleness"] = fc["staleness"]
    out["CS_staleness"] = cs["staleness"]

    cols = ["G_level", "G_impulse", "pi_level", "pi_impulse", "L_level", "L_impulse",
            "P_stance_raw", "P_stance", "P_impulse", "FC", "FC_impulse", "CS", "CS_impulse",
            "G_level_n", "G_impulse_n", "L_impulse_n", "FC_n", "CS_n",
            "G_staleness", "pi_staleness", "L_staleness", "P_staleness", "FC_staleness", "CS_staleness"]
    df = pd.DataFrame({c: out[c] for c in cols}, index=index)
    value_cols = [c for c in cols if not c.endswith(("_n", "_staleness"))]
    df.attrs["valid_from"] = {c: (df[c].first_valid_index()) for c in value_cols}
    df.attrs["approximate_before"] = {
        name: s.attrs["first_vintage"] for name, s in inputs.items()
        if s.attrs.get("pre_vintage") == "latest_lagged" and s.attrs.get("first_vintage") is not None
    }
    return df
