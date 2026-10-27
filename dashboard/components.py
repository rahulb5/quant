"""dashboard/components.py — shared helpers for dashboard pages."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from src.db.client import Database
from src.experiments import artifacts
from src.macro import data as macro_data
from src.shared.config import config

# Streamlit badge colours; the word "approved" is deliberately never shown.
VERDICT_COLORS = {
    "KILLED": "red",
    "KILL": "red",
    "NEEDS_REVIEW": "orange",
    "FLAG": "orange",
    "PASS": "green",
    "SKIPPED": "gray",
}

PALETTE = {"primary": "#2f6fdb", "secondary": "#8b95a5", "negative": "#d1453b", "positive": "#2e9e6b"}


def verdict_badge(verdict: str | None) -> str:
    """Markdown badge for a verdict; 'not run' when there is none."""
    if not verdict:
        return ":gray-badge[not run]"
    return f":{VERDICT_COLORS.get(verdict, 'gray')}-badge[{verdict}]"


def chip(text: str | None) -> str:
    return f":blue-badge[{text}]" if text else ":gray-badge[unclassified]"


def fmt_num(value: Any, spec: str = ".2f") -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "—"
    return format(value, spec)


def fmt_pct(value: Any, spec: str = ".1%") -> str:
    return fmt_num(value, spec)


def style_fig(fig: go.Figure, height: int = 340) -> go.Figure:
    fig.update_layout(
        height=height,
        margin=dict(l=10, r=10, t=30, b=10),
        legend=dict(orientation="h", yanchor="bottom", y=1.0, x=0),
        hovermode="x unified",
    )
    return fig


def plot(container: Any, fig: go.Figure, title: str, height: int = 340) -> None:
    """Draw a chart with its title as page text above it, not inside the figure."""
    container.markdown(f"**{title}**")
    container.plotly_chart(style_fig(fig, height), width="stretch")


@st.cache_data(show_spinner=False)
def cached_list(mtime: float):
    return artifacts.list_experiments()


@st.cache_data(show_spinner=False)
def cached_load(experiment_id: str, mtime: float):
    return artifacts.load_experiment(experiment_id)


def list_experiments():
    return cached_list(artifacts.artifacts_mtime())


def load_experiment(experiment_id: str):
    return cached_load(experiment_id, artifacts.artifacts_mtime())


# ── Macro (read-only DuckDB) ─────────────────────────────────────────────────


def _db() -> Database:
    """Fresh read-only handle; query_df() opens and closes it per call so the
    dashboard never holds a lock a collector needs. DB_PATH is re-read so tests
    can point at a temp file."""
    return Database(os.environ.get("DB_PATH", config.db_path), read_only=True)


def _db_mtime() -> float:
    path = Path(_db().path)
    return path.stat().st_mtime if path.exists() else 0.0


@st.cache_data(show_spinner=False)
def _cached_catalog(path: str, mtime: float) -> pd.DataFrame:
    return macro_data.list_series(_db())


@st.cache_data(show_spinner=False)
def _cached_series(path: str, mtime: float, source: str, code: str, vintage: str,
                   measure: str | None, pct_of_oi: bool) -> pd.Series:
    if source in ("FRED", "BIS"):  # both live in macro_observations
        return macro_data.load_fred(_db(), code, vintage=vintage)
    return macro_data.load_cot(_db(), code, measure, pct_of_oi=pct_of_oi)


def load_catalog() -> pd.DataFrame:
    return _cached_catalog(_db().path, _db_mtime())


def load_series(source: str, code: str, vintage: str = "latest",
                measure: str | None = None, pct_of_oi: bool = False) -> pd.Series:
    return _cached_series(_db().path, _db_mtime(), source, code, vintage, measure, pct_of_oi)


# ── Macro state page ─────────────────────────────────────────────────────────

# title, level column, impulse column, staleness column, level unit, impulse unit, level is already a z-score
STATE_CARDS = [
    ("Growth", "G_level", "G_impulse", "G_staleness", "z", "z", True),
    ("Inflation", "pi_level", "pi_impulse", "pi_staleness", "pp", "pp", False),
    ("Labor", "L_level", "L_impulse", "L_staleness", "pp", "z", False),
    ("Policy", "P_stance", "P_impulse", "P_staleness", "pp", "pp", False),
    ("Financial conditions", "FC", "FC_impulse", "FC_staleness", "z", "z", True),
    ("Credit supply", "CS", "CS_impulse", "CS_staleness", "z", "z", True),
]
MACRO_REGIME_LABELS = {   # definitional order = fixed colour order on the timeline
    "growth_inflation": ["goldilocks", "reflation", "stagflation", "deflation"],
    "growth_level": ["above_trend", "below_trend"],
    "policy": ["restrictive_tightening", "restrictive_easing", "accommodative_tightening", "accommodative_easing"],
    "financial_conditions": ["loose", "normal", "tight"],
    "curve_dynamics": ["bull_steepener", "bear_steepener", "bull_flattener", "bear_flattener"],
    "dollar_trend": ["strong_usd", "weak_usd"],
}


def _last_valid(s: pd.Series) -> tuple[float | None, pd.Timestamp | None]:
    s = s.dropna()
    return (float(s.iloc[-1]), s.index[-1]) if len(s) else (None, None)


def state_cards(panel: pd.DataFrame) -> list[dict[str, Any]]:
    """One dict per macro dimension for the cards: the latest level, impulse, the level as an
    expanding z-score (daily, burn-in 756; the level itself where it already is one), the
    staleness in days of the newest input, and the date the level was last valid."""
    from src.macro.state import BURN_IN, expanding_z

    cards = []
    for title, level, impulse, stale, lunit, iunit, is_z in STATE_CARDS:
        if level not in panel:
            continue
        lv, as_of = _last_valid(panel[level])
        z = lv
        if not is_z and lv is not None:
            z, _ = _last_valid(expanding_z(panel[level], BURN_IN["daily"]))
        iv, _ = _last_valid(panel[impulse]) if impulse in panel else (None, None)
        sv, _ = _last_valid(panel[stale]) if stale in panel else (None, None)
        cards.append({"title": title, "level": lv, "level_unit": lunit, "impulse": iv, "impulse_unit": iunit,
                      "z": z, "staleness": sv, "as_of": as_of})
    return cards


def asset_betas(state: pd.DataFrame, returns: pd.Series, horizon: int) -> pd.DataFrame:
    """Univariate forward-return regressions of `returns` on each state variable (STATE_VECTOR) at
    one horizon, with Newey-West HAC errors at lag = horizon (src.macro.analysis.forward_regression).

    Each variable is scaled by its own standard deviation (full-sample, display only; it changes
    coefficients, not t-statistics), so `beta_per_sd` is the forward return per +1 s.d. of the state
    and rows are comparable across variables (one s.d. of the variable's whole history, not of the
    regression sample). `ci_low`/`ci_high` are beta ± 1.96 HAC standard errors.
    Columns: variable, beta_per_sd, ci_low, ci_high, t_stat_hac, n_obs; rows with no estimate are dropped.
    """
    from src.macro.analysis import STATE_VECTOR, forward_regression

    rows = []
    for col in (c for c in STATE_VECTOR if c in state):
        sd = state[col].std()
        if not sd or pd.isna(sd):
            continue
        fit = forward_regression((state[col] / sd).rename(col), returns, horizons=(horizon,))
        r = fit.loc[(horizon, col)]
        if pd.isna(r["coef"]) or not r["t_stat_hac"]:
            continue
        se = abs(r["coef"] / r["t_stat_hac"])
        rows.append({"variable": col, "beta_per_sd": r["coef"], "ci_low": r["coef"] - 1.96 * se,
                     "ci_high": r["coef"] + 1.96 * se, "t_stat_hac": r["t_stat_hac"], "n_obs": int(r["n_obs"])})
    return pd.DataFrame(rows, columns=["variable", "beta_per_sd", "ci_low", "ci_high", "t_stat_hac", "n_obs"])


def macro_state_bundle() -> dict[str, Any] | None:
    """Everything the macro state page shows, built once from the read-only DB (about 10 s):
    {"as_of", "state" (StatePanel frame), "market" (market_state), "rates" (decomposition + term
    premium + curve), "regimes" (macro regime labels), "divergence" (pricing_divergence per
    dimension), "assets" (reference-asset returns), "latest_input" (input name -> date of its newest
    raw release)}. None if the database has no macro state data."""
    from src.macro.analysis import pricing_divergence
    from src.macro.inputs import load_market_inputs, load_rates_inputs, load_reference_returns
    from src.macro.market import market_state
    from src.macro.rates import curve, nominal_decomposition, term_premium
    from src.macro.regimes import StatePanel, macro_regimes

    db = _db()
    panel = StatePanel(database=db)
    state = panel.frame
    if state.dropna(how="all").empty:
        return None
    index = state.index
    market_in = load_market_inputs(db)
    market = market_state(market_in, index)
    rates_in = load_rates_inputs(db)
    # Newest raw (release-dated) observation per input: the daily panels forward-fill, so "the last
    # value" is always today's fill; this says how old the underlying release is.
    latest_input = {k: v.dropna().index[-1] for k, v in {**market_in, **rates_in}.items() if v.notna().any()}
    rates = pd.concat([nominal_decomposition(rates_in, index), term_premium(rates_in, index),
                       curve(rates_in, index)], axis=1)
    rates = rates.loc[:, ~rates.columns.duplicated()]
    regimes = {r.name: r.labels() for r in macro_regimes(database=db, panel=panel)}
    return {"as_of": index[-1], "state": state, "market": market, "rates": rates, "regimes": regimes,
            "divergence": pricing_divergence(state, market), "assets": load_reference_returns(db),
            "latest_input": latest_input}


@st.cache_data(show_spinner="Building the macro state (about 10 seconds)...")
def _cached_macro_state(path: str, mtime: float):
    return macro_state_bundle()


def load_macro_state() -> dict[str, Any] | None:
    """macro_state_bundle(), cached on the DB file's modification time."""
    return _cached_macro_state(_db().path, _db_mtime())
