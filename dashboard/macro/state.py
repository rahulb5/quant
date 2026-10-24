"""Macro state: where the macro dimensions are, what is driving yields, whether the market has priced it,
how crowded positioning is, which regimes we are in, and how assets have responded.

State, regimes, rates, divergence and positioning are point-in-time (what was known on their date);
the asset returns in the last panel are ex-post. Read-only: built from the DB through
dashboard.components.load_macro_state(), cached on the DB file's modification time.
"""

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from dashboard.components import (
    MACRO_REGIME_LABELS, PALETTE, asset_betas, fmt_num, load_macro_state, plot, state_cards,
)
from src.macro.analysis import (
    ENTRY_HORIZONS, GAP_LABELS, HORIZONS, current_regime, divergence_forward_returns, episodes,
    performance_by_regime, regime_durations,
)
from src.macro.inputs import REFERENCE_LABELS
from src.macro.market import POSITIONING

# Categorical slots 1-4 of the reference palette (validated adjacent pairs; aqua and yellow are below
# 3:1 on a light surface, so every chart here carries a legend or labels and a table sits beside it).
S = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100"]
WINDOWS = {"5 years": 5, "10 years": 10, "20 years": 20, "All history": None}
DIVERGENCE_TITLES = {"growth": "Growth (G_impulse vs growth pricing)",
                     "inflation": "Inflation (pi_impulse vs 63-day change in breakevens)",
                     "policy": "Policy (P_impulse vs 63-day change in 2Y - fed funds)"}


def pretty(name: str) -> str:
    return name.replace("_", " ")


st.title("Macro state")

try:
    bundle = load_macro_state()
except Exception as e:  # missing file, or the DB is write-locked by a collector
    st.error(f"Could not build the macro state from the database: {e}")
    st.stop()
if bundle is None:
    st.info("No macro state data in the database yet.")
    st.stop()

state, market, rates = bundle["state"], bundle["market"], bundle["rates"]
regimes, divergence, assets = bundle["regimes"], bundle["divergence"], bundle["assets"]
as_of, latest_input = bundle["as_of"], bundle.get("latest_input", {})


def newest(key: str) -> str:
    d = latest_input.get(key)
    return f"{d:%Y-%m-%d}" if d is not None else "unknown"

with st.sidebar:
    st.header("View")
    window = st.selectbox("History shown", list(WINDOWS))
    asset_cols = [c for c in REFERENCE_LABELS if c in assets]
    asset = st.selectbox("Asset", asset_cols, format_func=REFERENCE_LABELS.get) if asset_cols else None

years = WINDOWS[window]
start = as_of - pd.DateOffset(years=years) if years else pd.Timestamp.min


def recent(df: pd.DataFrame | pd.Series, weekly: bool = True):
    out = df.loc[df.index >= start]
    return out.resample("W-FRI").last() if weekly else out


st.caption(
    f"As of {as_of:%Y-%m-%d}. State, rates, divergence, positioning and regimes are point-in-time: each value "
    "is what was known on its date (daily series are carried forward, so check the newest-input dates). The dollar is "
    "approximate before 2019-02 and labor impulse before 2009-09 (back-filled history, not real-time vintages). "
    "Asset returns in the last panel are ex-post."
)

# ── 1. State vector ──────────────────────────────────────────────────────────

st.subheader("State vector")
cards = state_cards(state)
for row in range(0, len(cards), 3):
    for col, c in zip(st.columns(3), cards[row:row + 3]):
        with col.container(border=True):
            st.metric(c["title"], f"{fmt_num(c['level'], '+.2f')} {c['level_unit']}" if c["level"] is not None else "—")
            st.caption(
                f"impulse {fmt_num(c['impulse'], '+.2f')} {c['impulse_unit']} · level z {fmt_num(c['z'], '+.1f')} · "
                f"newest input {fmt_num(c['staleness'], '.0f')} days old"
            )
st.caption("Impulse: positive = accelerating, rising or tightening (labor: deteriorating). Level z: growth, "
           "financial conditions and credit supply are already means of expanding z-scores, shown as they are; "
           "inflation, labor and policy levels (pp) are z-scored daily with an expanding window.")

# ── 2. Rates decomposition ───────────────────────────────────────────────────

st.subheader("What is driving the 10Y yield?")
if "d_real" in rates and rates["d_real"].notna().any():
    r = recent(rates)
    last = rates.dropna(subset=["nominal"]).iloc[-1] if rates["nominal"].notna().any() else None
    if last is not None:
        m = st.columns(5)
        m[0].metric("10Y nominal", f"{last['nominal']:.2f}%")
        m[1].metric("10Y real", f"{last['real']:.2f}%")
        m[2].metric("10Y breakeven", f"{last['breakeven']:.2f}%")
        m[3].metric("Real share of 63-day move", fmt_num(last["share_real"], ".0%"),
                    help="Shown only when the nominal move exceeds 10 bp.")
        m[4].metric("Term premium, 63-day change", fmt_num(last.get("d_term_premium"), "+.2f") + " pp")
    left, right = st.columns(2)
    fig = go.Figure()
    fig.add_bar(x=r.index, y=r["d_real"], name="Real yield", marker_color=S[0])
    fig.add_bar(x=r.index, y=r["d_breakeven"], name="Breakeven", marker_color=S[1])
    fig.add_scatter(x=r.index, y=r["d_nominal"], name="Nominal (total)", line=dict(color=PALETTE["secondary"], width=2))
    fig.update_layout(barmode="relative", bargap=0, yaxis_title="pp over 63 days")
    plot(left, fig, "63-day change in the 10Y yield: real vs breakeven")
    fig = go.Figure(go.Scatter(x=r.index, y=r["term_premium"], name="Term premium", line=dict(color=S[0], width=2)))
    fig.update_layout(yaxis_title="%", showlegend=False)
    plot(right, fig, "Term premium (model estimate, real-time from 2016)")
    st.caption(f"Newest 10Y yield observation: {newest('DGS10')}. Newest term premium: {newest('THREEFYTP10')}.")
else:
    st.info("No rates data.")

# ── 3. Macro vs market-implied ───────────────────────────────────────────────

st.subheader("Has the market priced what the data shows?")
if divergence:
    dim = st.radio("Dimension", list(divergence), format_func=lambda d: DIVERGENCE_TITLES[d], horizontal=True)
    d = divergence[dim]
    now = d.dropna(subset=["gap_z"]).iloc[-1] if d["gap_z"].notna().any() else None
    left, right = st.columns([3, 2])
    fig = go.Figure()
    r = recent(d[["macro_z", "market_z"]])
    fig.add_scatter(x=r.index, y=r["macro_z"], name="Macro data", line=dict(color=S[0], width=2))
    fig.add_scatter(x=r.index, y=r["market_z"], name="Market-implied", line=dict(color=S[1], width=2))
    fig.update_layout(yaxis_title="z-score")
    plot(left, fig, "Macro vs market-implied (expanding z-scores)")
    if now is not None:
        right.metric("Gap (data minus market), z", fmt_num(now["gap_z"], "+.2f"))
        right.write(f"Now: **{pretty(str(now['gap_tercile']))}**" if pd.notna(now["gap_tercile"]) else "Tercile: not yet defined")
    right.caption("Positive gap = the data is stronger than the market is pricing. Terciles are expanding.")
    if asset is not None:
        fwd = divergence_forward_returns(d, assets[asset].dropna(), horizons=ENTRY_HORIZONS)
        mean = fwd["mean_fwd"].unstack("horizon").reindex(GAP_LABELS + ["spread"])
        tstat = fwd["t_stat_hac"].unstack("horizon").reindex(GAP_LABELS + ["spread"])
        tbl = pd.concat({**{f"{h}-day mean": mean[h] for h in mean.columns},
                         **{f"{h}-day t": tstat[h] for h in mean.columns}}, axis=1)
        tbl = tbl[[c for h in mean.columns for c in (f"{h}-day mean", f"{h}-day t")]]
        tbl.index = [pretty(i) for i in tbl.index]
        right.markdown(f"**{REFERENCE_LABELS[asset]} forward return by gap tercile**")
        right.dataframe(tbl.style.format({c: "{:+.2%}" if c.endswith("mean") else "{:+.1f}" for c in tbl.columns}),
                        width="stretch")
else:
    st.info("No divergence data.")

# ── 4. Positioning ───────────────────────────────────────────────────────────

st.subheader("Is positioning crowded?")
pos_keys = [k for k in POSITIONING if k in market and market[k].notna().any()]
if pos_keys:
    latest = pd.Series({POSITIONING[k][2]: market[k].dropna().iloc[-1] for k in pos_keys})
    left, right = st.columns(2)
    fig = go.Figure(go.Bar(
        x=latest.values, y=latest.index, orientation="h", text=[f"{v:+.1f}" for v in latest.values],
        marker_color=[S[0] if v >= 0 else S[1] for v in latest.values],
    ))
    fig.update_layout(showlegend=False, xaxis_title="z-score of net % of open interest", yaxis=dict(autorange="reversed"))
    plot(left, fig, "Net positioning now vs its own history (blue = above average, orange = below)", height=340)
    key = right.selectbox("History", pos_keys, format_func=lambda k: POSITIONING[k][2])
    r = recent(market[key].dropna())
    fig = go.Figure(go.Scatter(x=r.index, y=r.values, name=POSITIONING[key][2], line=dict(color=S[0], width=2)))
    fig.add_hline(y=0, line_color=PALETTE["secondary"], line_width=1)
    fig.update_layout(showlegend=False, yaxis_title="z-score")
    plot(right, fig, POSITIONING[key][2])
    st.caption("Expanding z-score of net position as % of open interest, usable 3 days after the report date. "
               "Newest report in use: " + ", ".join(f"{POSITIONING[k][2]} {newest(k)}" for k in pos_keys) + ". "
               "Positive = more net long than usual, not necessarily net long.")
else:
    st.info("No positioning data.")

# ── 5. Regimes ───────────────────────────────────────────────────────────────

st.subheader("Which regime are we in?")
rows = []
for name, labels in regimes.items():
    cur = current_regime(labels)
    dur = regime_durations(labels)
    med = dur["median"].get(cur["label"]) if cur["label"] is not None and not dur.empty else None
    rows.append({"Regime": pretty(name), "Now": pretty(cur["label"]) if cur["label"] else "—",
                 "Since": cur["since"].date() if cur["since"] is not None else None,
                 "Business days in": cur["days_in_regime"], "Typical run of this label (median)": med})
left, right = st.columns([3, 2])
left.dataframe(pd.DataFrame(rows).set_index("Regime"), width="stretch")
flags = [("sahm_triggered", "Sahm rule"), ("labor_deteriorating", "Labor deteriorating"), ("curve_inverted", "Curve inverted (10Y-3M)")]
right.markdown("**Event flags**")
for col, label in flags:
    on = col in state and state[col].dropna().astype(bool).iloc[-1] if col in state and state[col].notna().any() else None
    right.markdown(f"{'● ' if on else '○ '}{label}: " + (":red-badge[on]" if on else ":gray-badge[off]" if on is not None else ":gray-badge[no data]"))

grid = st.columns(2)
for i, (name, labels) in enumerate(regimes.items()):
    order = MACRO_REGIME_LABELS.get(name, [])
    ep = episodes(labels)
    if ep.empty:
        grid[i % 2].caption(f"{pretty(name)}: no labels yet")
        continue
    ep = ep[ep["end"] >= start]
    fig = go.Figure()
    for j, lbl in enumerate(order):
        e = ep[ep["label"] == lbl]
        if e.empty:
            continue
        fig.add_bar(
            base=e["start"], x=(e["end"] - e["start"]).dt.total_seconds() * 1000 + 86_400_000, y=[pretty(name)] * len(e),
            orientation="h", name=pretty(lbl), marker_color=S[j % len(S)], marker_line_width=0,
            customdata=e[["start", "end"]].apply(lambda c: c.dt.strftime("%Y-%m-%d")), hovertemplate=(
                f"{pretty(lbl)}<br>%{{customdata[0]}} to %{{customdata[1]}}<extra></extra>"),
        )
    fig.update_layout(barmode="overlay", yaxis=dict(visible=False), hovermode="closest",
                      xaxis=dict(range=[max(start, labels.dropna().index.min()) if labels.notna().any() else start, as_of]))
    plot(grid[i % 2], fig, pretty(name), height=130)

# ── 6. Asset response ────────────────────────────────────────────────────────

st.subheader("How have assets responded?")
if asset is None:
    st.info("No reference asset returns in the database.")
else:
    returns = assets[asset].dropna()
    st.caption(f"{REFERENCE_LABELS[asset]}, history from {returns.index[0]:%Y-%m-%d}. Gold and crude are front-month "
               "futures, not roll-adjusted. Ex-post and descriptive: not a forecast, and not corrected for the "
               "many state variables and horizons shown.")
    left, right = st.columns(2)
    h = left.radio("Forward horizon (trading days)", HORIZONS, index=1, horizontal=True)
    betas = asset_betas(state, returns, h)
    if betas.empty:
        left.info("Too little overlap between the asset and the state for a regression.")
    else:
        fig = go.Figure(go.Scatter(
            x=betas["beta_per_sd"], y=betas["variable"], mode="markers", marker=dict(color=S[0], size=9),
            error_x=dict(type="data", symmetric=False, array=betas["ci_high"] - betas["beta_per_sd"],
                         arrayminus=betas["beta_per_sd"] - betas["ci_low"], color=S[0], thickness=2),
            customdata=betas[["t_stat_hac", "n_obs"]],
            hovertemplate="%{y}: %{x:+.2%} (t=%{customdata[0]:.1f}, n=%{customdata[1]})<extra></extra>",
        ))
        fig.add_vline(x=0, line_color=PALETTE["secondary"], line_width=1)
        fig.update_layout(showlegend=False, xaxis=dict(tickformat=".1%", title=f"forward {h}-day return per +1 s.d. of the variable (its full history)"),
                          yaxis=dict(autorange="reversed"), hovermode="closest")
        plot(left, fig, f"Univariate betas, 95% Newey-West interval (lag {h})", height=420)
        with left.expander("Table"):
            st.dataframe(betas.set_index("variable").style.format(
                {"beta_per_sd": "{:+.2%}", "ci_low": "{:+.2%}", "ci_high": "{:+.2%}", "t_stat_hac": "{:+.1f}"}), width="stretch")
    regime_name = right.selectbox("Regime", list(regimes), format_func=pretty)
    table = performance_by_regime(regimes[regime_name], returns)
    right.markdown(f"**{REFERENCE_LABELS[asset]} by {pretty(regime_name)} label**")
    right.caption("Returns on the days that carry the label (same day, not forward), so the day's own move can help set the label.")
    if table.empty:
        right.info("No overlap between this regime's labels and the asset.")
    else:
        view = table[["ann_return", "ann_vol", "sharpe", "pct_time", "n_obs", "n_episodes", "mean_episode_length"]]
        view.index = [pretty(i) for i in view.index]
        right.dataframe(view.style.format({"ann_return": "{:+.1%}", "ann_vol": "{:.1%}", "sharpe": "{:+.2f}", "pct_time": "{:.0%}",
                                           "n_obs": "{:,.0f}", "n_episodes": "{:.0f}", "mean_episode_length": "{:.0f}"}),
                        width="stretch")
        few = [pretty(i) for i in table.index[table["n_episodes"] < 5]]
        right.caption("A regime has as many independent observations as episodes: read n_episodes, not n_obs."
                      + (f" Fewer than 5 episodes: {', '.join(few)}." if few else ""))
