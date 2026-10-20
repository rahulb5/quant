"""Experiments list page (default)."""

import pandas as pd
import streamlit as st

from dashboard.components import list_experiments
from src.experiments.config import DIRECTIONS, FREQUENCIES, MARKETS, SIGNAL_FAMILIES

st.title("Experiments")

summaries = list_experiments()
if not summaries:
    st.info("No experiments found.")
    st.stop()

STATUSES = ("draft", "running", "complete", "failed")
VERDICTS = ("PASS", "NEEDS_REVIEW", "KILLED", "not run")


def _field(s, name):
    return getattr(s.classification, name) if s.classification else "unclassified"


with st.sidebar:
    st.header("Filters")
    market = st.multiselect("Market", MARKETS)
    family = st.multiselect("Signal family", SIGNAL_FAMILIES)
    freq = st.multiselect("Frequency", FREQUENCIES)
    direction = st.multiselect("Direction", DIRECTIONS)
    status = st.multiselect("Status", STATUSES)
    verdict = st.multiselect("Verdict", VERDICTS)


def _keep(s) -> bool:
    return (
        (not market or _field(s, "asset_class") in market)
        and (not family or _field(s, "signal_family") in family)
        and (not freq or _field(s, "frequency") in freq)
        and (not direction or _field(s, "direction") in direction)
        and (not status or s.status in status)
        and (not verdict or (s.robustness_verdict or "not run") in verdict)
    )


shown = [s for s in summaries if _keep(s)]
st.caption(f"{len(shown)} of {len(summaries)} experiments")

df = pd.DataFrame([
    {
        "ID": s.experiment_id,
        "Signal": s.signal_name,
        "Market": _field(s, "asset_class"),
        "Universe": _field(s, "universe"),
        "Family": _field(s, "signal_family"),
        "Freq": _field(s, "frequency"),
        "Direction": _field(s, "direction"),
        "Status": s.status,
        "Verdict": s.robustness_verdict or "not run",
        "Sharpe": s.sharpe,
        "2x-cost Sharpe": s.sharpe_2x_cost,
        "Deflated Sharpe": s.deflated_sharpe,
        "CI low": s.sharpe_ci_low,
        "Max DD": s.max_drawdown,
        "Ann. turnover": s.ann_turnover,
        "Breakeven bps": s.breakeven_bps,
        "Holdout": s.holdout_state,
        "Data as-of": s.data_as_of or "—",
        "Git SHA": (s.git_sha or "—")[:8],
    }
    for s in shown
])

event = st.dataframe(
    df,
    hide_index=True,
    width="stretch",
    on_select="rerun",
    selection_mode="single-row",
    column_config={
        "Sharpe": st.column_config.NumberColumn(format="%.2f"),
        "2x-cost Sharpe": st.column_config.NumberColumn(format="%.2f"),
        "Deflated Sharpe": st.column_config.NumberColumn(format="%.2f"),
        "CI low": st.column_config.NumberColumn(format="%.2f"),
        "Max DD": st.column_config.NumberColumn(format="percent"),
        "Ann. turnover": st.column_config.NumberColumn(format="%.1f"),
        "Breakeven bps": st.column_config.NumberColumn(format="%.1f"),
    },
)

rows = event.selection.rows if event and event.selection else []
if rows:
    chosen = df.iloc[rows[0]]["ID"]
    st.switch_page("research/experiment.py", query_params={"id": chosen})
else:
    st.caption("Select a row to open the experiment.")
