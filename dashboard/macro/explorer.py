"""Macro explorer: pick a FRED or CFTC series, transform it, see its rolling z-score."""

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from dashboard.components import PALETTE, fmt_num, load_catalog, load_series, plot
from src.macro.data import cot_measures
from src.macro.transforms import (
    CHANGE_TRANSFORMS, TRANSFORMS, frequency_label, history_years, periods_per_year,
    rolling_zscore, transform,
)

st.title("Macro explorer")

try:
    catalog = load_catalog()
except Exception as e:  # missing file, or the DB is write-locked by a collector
    st.error(f"Could not open the database read-only: {e}")
    st.stop()
if catalog.empty:
    st.info("No macro or positioning series in the database.")
    st.stop()

SOURCES = {"FRED": "Macro (FRED)", "CFTC": "Positioning (CFTC COT)"}

# ── Selection ────────────────────────────────────────────────────────────────

with st.sidebar:
    st.header("Series")
    source = st.radio("Source", [s for s in SOURCES if s in set(catalog["source"])], format_func=SOURCES.get)
    rows = catalog[catalog["source"] == source]
    if source == "CFTC":
        report_type = st.selectbox("Report", sorted(rows["report_type"].unique()))
        rows = rows[rows["report_type"] == report_type]
        rows = rows[rows["contract_keyed"]]  # safety net; the collector only writes contract-keyed series
        groups = sorted(rows["asset_group"].dropna().unique())
        if groups:
            group = st.selectbox("Asset group", ["all", *groups])
            if group != "all":
                rows = rows[rows["asset_group"] == group]
        if rows.empty:
            st.warning("No series for this report.")
            st.stop()
    rows = rows.set_index("code")
    code = st.selectbox("Series", list(rows.index), format_func=lambda c: f"{c} — {rows.at[c, 'name']}")
    meta = rows.loc[code]

    if source == "FRED":
        vintage = st.radio(
            "Vintage", ["latest", "first"], horizontal=True,
            format_func={"latest": "Latest revision", "first": "First release"}.get,
            help="First release = the value as originally published (what was known in real time).",
        )
        measure, pct_of_oi = None, False
    else:
        measures = cot_measures(meta["report_type"])
        measure = st.selectbox("Measure", list(measures), format_func=measures.get)
        pct_of_oi = measure != "open_interest" and st.checkbox("As % of open interest", key="pct_oi")
        vintage = "latest"

    kind = st.selectbox("Transform", list(TRANSFORMS), format_func=TRANSFORMS.get)

raw = load_series(source, code, vintage=vintage, measure=measure, pct_of_oi=pct_of_oi)
if raw.empty:
    st.warning("No observations for this selection.")
    st.stop()

ppy = periods_per_year(raw.index)
series = transform(raw, kind)
if len(series) < 3:
    st.warning("Too few observations after the transform to plot.")
    st.stop()

# ── Header ───────────────────────────────────────────────────────────────────

st.subheader(meta["name"])
units = "% of OI" if pct_of_oi else meta["units"]
st.caption(
    f"`{code}` · {source} · {frequency_label(raw.index)} (inferred; stored as {meta['frequency']}) · "
    f"units: {units} · {raw.index[0]:%Y-%m-%d} → {raw.index[-1]:%Y-%m-%d} · {len(raw):,} obs"
)

# ── Z-score lookback (bounded by the history this transform leaves) ──────────

years = history_years(series.index)
max_lb = max(0.5, round(years * 2) / 2)
default_lb = min(5.0, max(0.5, (max_lb / 2 // 0.5) * 0.5))
c1, c2 = st.columns([3, 1])
if max_lb > 0.5:
    lookback = c1.slider("Z-score lookback (years)", 0.5, max_lb, default_lb, 0.5)
else:
    lookback = 0.5
    c1.caption("History is under a year; z-score uses a 6-month lookback.")
window = max(3, round(lookback * ppy))
c2.metric("Window", f"{window:,} obs")
z = rolling_zscore(series, window)

# ── Tiles ────────────────────────────────────────────────────────────────────

last_z = z.iloc[-1] if not z.empty and z.index[-1] == series.index[-1] else None
pctile = (series <= series.iloc[-1]).mean()
tiles = [
    ("Latest", fmt_num(series.iloc[-1], ",.2f")),
    ("As of", f"{series.index[-1]:%Y-%m-%d}"),
    ("Z-score", fmt_num(last_z, "+.2f")),
    ("Percentile (full history)", f"{pctile:.0%}"),
]
if source == "FRED":
    tiles.append(("Released", f"{raw.attrs['release_date'].iloc[-1]:%Y-%m-%d}"))
for col, (label, value) in zip(st.columns(len(tiles)), tiles):
    col.metric(label, value)

# ── Charts ───────────────────────────────────────────────────────────────────

as_bars = st.toggle("Bar chart", value=kind in CHANGE_TRANSFORMS and ppy <= 52)
label = f"{TRANSFORMS[kind]}" + ("" if kind == "level" else f" of {code}")

fig = go.Figure()
if as_bars:
    fig.add_bar(x=series.index, y=series, name=label, marker_color=PALETTE["primary"])
else:
    fig.add_scatter(x=series.index, y=series, name=label, mode="lines",
                    line=dict(color=PALETTE["primary"], width=2))
fig.update_layout(showlegend=False)
plot(st, fig, label)

if z.empty:
    st.info(f"Not enough history for a {window}-observation z-score; shorten the lookback.")
else:
    zfig = go.Figure()
    for level in (-2, -1, 1, 2):
        zfig.add_hline(y=level, line=dict(color=PALETTE["secondary"], width=1, dash="dot" if abs(level) == 1 else "dash"))
    zfig.add_hline(y=0, line=dict(color=PALETTE["secondary"], width=1))
    zfig.add_scatter(x=z.index, y=z, name="Z-score", mode="lines", line=dict(color=PALETTE["primary"], width=2))
    zfig.update_xaxes(range=[series.index[0], series.index[-1]])
    zfig.update_layout(showlegend=False)
    plot(st, zfig, f"Rolling z-score · {lookback:g}y lookback ({window:,} obs, trailing)", height=260)

with st.expander("Data"):
    st.dataframe(
        pd.DataFrame({label: series, "z-score": z}).sort_index(ascending=False),
        width="stretch",
    )
