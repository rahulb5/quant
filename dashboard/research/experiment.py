"""Experiment detail page, selected by ?id=EXP-NNNN."""

from dataclasses import asdict

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
import yaml

from dashboard.components import (
    PALETTE, chip, fmt_num, fmt_pct, list_experiments, load_experiment, plot, verdict_badge,
)

experiment_id = st.query_params.get("id")
known = [s.experiment_id for s in list_experiments()]
if not experiment_id or experiment_id not in known:
    st.warning("Pick an experiment from the list." if not experiment_id else f"Unknown experiment {experiment_id!r}.")
    if st.button("← Experiments"):
        st.switch_page("research/experiments.py")
    st.stop()

art = load_experiment(experiment_id)
s, cfg = art.summary, art.config

if st.button("← Experiments"):
    st.switch_page("research/experiments.py")

st.title(f"{s.experiment_id} · {s.signal_name}")
c = s.classification
chips = [c.asset_class, c.universe, c.signal_family, c.frequency, c.direction] if c else [None]
st.markdown(" ".join(chip(x) for x in chips) + f"  ·  status: **{s.status}**  ·  robustness: {verdict_badge(s.robustness_verdict)}")
st.write(s.hypothesis)
if not s.git_sha or not s.data_as_of:
    st.warning("git_sha or data_as_of is not recorded — not reproducible from ID alone.")

tab_perf, tab_rob, tab_diag, tab_note, tab_cfg = st.tabs(["Performance", "Robustness", "Diagnostics", "Note", "Config"])


# ── Performance ──────────────────────────────────────────────────────────────

with tab_perf:
    m = (art.results or {}).get("metrics", {})
    ex = (art.results or {}).get("execution_summary", {})
    if art.performance is None:
        st.info("No performance.parquet for this experiment.")
    else:
        tiles = [
            ("Sharpe", fmt_num(m.get("sharpe"))), ("CAGR", fmt_pct(m.get("cagr"))),
            ("Ann. vol", fmt_pct(m.get("ann_vol"))), ("Max DD", fmt_pct(m.get("max_drawdown"))),
            ("Sortino", fmt_num(m.get("sortino"))), ("Calmar", fmt_num(m.get("calmar"))),
            ("Hit rate", fmt_pct(m.get("hit_rate"))), ("Ann. turnover", fmt_num(ex.get("ann_turnover"), ".1f")),
        ]
        for col, (label, value) in zip(st.columns(len(tiles)), tiles):
            col.metric(label, value)

        perf = art.performance
        gross, net = perf["gross_returns"], perf["net_returns"]
        ppy = cfg.backtest.periods_per_year

        log_scale = st.toggle("Log scale", value=False, key="equity_log_scale")
        fig = go.Figure()
        fig.add_scatter(x=perf.index, y=(1 + gross).cumprod(), name="Gross", line=dict(color=PALETTE["secondary"]))
        fig.add_scatter(x=perf.index, y=(1 + net).cumprod(), name="Net", line=dict(color=PALETTE["primary"]))
        fig.update_yaxes(type="log" if log_scale else "linear")
        plot(st, fig, f"Equity curve ({'log' if log_scale else 'linear'} scale)")

        equity = (1 + net).cumprod()
        dd = equity / equity.cummax() - 1
        fig = go.Figure(go.Scatter(x=dd.index, y=dd, fill="tozeroy", line=dict(color=PALETTE["negative"]), name="Drawdown"))
        fig.update_yaxes(tickformat=".0%")
        plot(st, fig, "Drawdown (net)", 260)

        roll = net.rolling(ppy).mean() / net.rolling(ppy).std() * np.sqrt(ppy)
        fig = go.Figure(go.Scatter(x=roll.index, y=roll, line=dict(color=PALETTE["primary"]), name="Rolling Sharpe"))
        fig.add_hline(y=0, line_color=PALETTE["secondary"])
        plot(st, fig, f"Rolling Sharpe ({ppy} periods)", 260)

        yearly = (1 + net).groupby(net.index.year).prod() - 1
        fig = go.Figure(go.Bar(
            x=yearly.index, y=yearly,
            marker_color=[PALETTE["positive"] if v >= 0 else PALETTE["negative"] for v in yearly], name="Net return",
        ))
        fig.update_yaxes(tickformat=".0%")
        plot(st, fig, "Calendar-year net return", 260)


# ── Robustness ───────────────────────────────────────────────────────────────

with tab_rob:
    rob = art.robustness
    if rob is None:
        st.info(f"Not run — `scripts/run_robustness.py experiments/{s.experiment_id}/config.yaml`")
    else:
        checks = rob["checks"]
        st.markdown(f"Overall: {verdict_badge(rob['overall'])} · n_trials {rob['n_trials']} · generated {rob['generated_at']}")
        st.dataframe(
            pd.DataFrame([{"Check": k, "Verdict": v["verdict"], "Reason": v["reason"]} for k, v in checks.items()]),
            hide_index=True, width="stretch",
        )

        left, right = st.columns(2)

        cost = checks["cost_stress"]["detail"]
        if cost:
            mult = cost["sharpe_by_multiplier"]
            fig = go.Figure(go.Scatter(x=[float(k) for k in mult], y=list(mult.values()), mode="lines+markers", line=dict(color=PALETTE["primary"])))
            fig.add_hline(y=0, line_color=PALETTE["secondary"])
            fig.update_xaxes(title="Cost multiplier")
            plot(left, fig, f"Cost ladder — breakeven {fmt_num(cost['breakeven_bps'], '.1f')} bps")

        lag = checks["lag_sensitivity"]["detail"]
        if lag:
            by_lag = lag["sharpe_by_lag"]
            fig = go.Figure(go.Bar(x=[f"lag {k}" for k in by_lag], y=list(by_lag.values()), marker_color=PALETTE["primary"]))
            plot(right, fig, "Sharpe by lag")

        par = checks["parameters"]["detail"]
        if par and par.get("grid"):
            keys, grid = par["keys"], par["grid"]
            if len(keys) == 1:
                xs = [str(g["params"][keys[0]]) for g in grid]
                fig = go.Figure(go.Scatter(x=xs, y=[g["sharpe"] for g in grid], mode="lines+markers", line=dict(color=PALETTE["primary"])))
                fig.update_xaxes(title=keys[0], type="category")
                plot(st, fig, "Parameter sweep (Sharpe)", 300)
            elif len(keys) == 2:
                pdf = pd.DataFrame([{"x": str(g["params"][keys[0]]), "y": str(g["params"][keys[1]]), "sharpe": g["sharpe"]} for g in grid])
                piv = pdf.pivot(index="y", columns="x", values="sharpe")
                fig = go.Figure(go.Heatmap(z=piv.values, x=list(piv.columns), y=list(piv.index), colorscale="RdBu", zmid=0, text=np.round(piv.values, 2), texttemplate="%{text}"))
                fig.update_xaxes(title=keys[0], type="category")
                fig.update_yaxes(title=keys[1], type="category")
                plot(st, fig, "Parameter grid (Sharpe)", 360)
            else:
                st.dataframe(pd.DataFrame([{**g["params"], "sharpe": g["sharpe"]} for g in grid]), hide_index=True)

        sub = checks["subperiods"]["detail"]
        if sub:
            st.subheader("Subperiods")
            rows = (
                [{"Period": f"half: {k}", "Value": v} for k, v in sub["halves"].items()]
                + [{"Period": f"decade: {k}", "Value": v} for k, v in sub["decades"].items()]
                + [{"Period": f"crisis: {k}", "Value": v["total_return"]} for k, v in sub.get("crisis_windows", {}).items()]
            )
            st.caption("Halves/decades are Sharpe; crisis windows are total return.")
            st.dataframe(pd.DataFrame(rows), hide_index=True, width="stretch")

        stat = checks["statistical"]["detail"]
        out = checks["outliers"]["detail"]
        sl, sr = st.columns(2)
        if stat:
            sl.subheader("Statistical")
            sl.metric("Sharpe", fmt_num(stat["sharpe"]))
            sl.write(f"Bootstrap 95% CI: [{fmt_num(stat['ci_low'])}, {fmt_num(stat['ci_high'])}]")
            sl.write(f"Deflated Sharpe: {fmt_num(stat['deflated_sharpe'])} at n_trials={stat['n_trials']}")
        if out:
            sr.subheader("Outliers")
            sr.metric(f"Sharpe excl. tails (±{out['trim_frac']:.0%})", fmt_num(out["sharpe_excluding_tails"]))
            sr.write(f"Excluding top only: {fmt_num(out['sharpe_excluding_top'])} (vs {fmt_num(out['sharpe_excluding_top_if_gaussian'])} if Gaussian)")
            sr.caption("One-sided trimming is biased: removing the best periods lowers Sharpe even for pure noise.")

    if art.holdout is not None:
        st.subheader("Holdout (frozen)")
        st.json(art.holdout)
    else:
        st.caption(f"Holdout: {s.holdout_state}")


# ── Diagnostics ──────────────────────────────────────────────────────────────

with tab_diag:
    d = art.diagnostics
    if d is None:
        st.info("No diagnostics.json for this experiment.")
    else:
        st.subheader("Performance by regime")
        regime_tbl = pd.concat(
            {name: pd.DataFrame(labels) for name, labels in d["by_regime"].items()},
            axis=1,
        )
        regime_tbl.columns = [f"{name}: {label}" for name, label in regime_tbl.columns]
        st.dataframe(regime_tbl, width="stretch")

        fr = d["factor_regression"]
        st.subheader("Factor regression")
        st.write(f"Annualised alpha {fmt_pct(fr['ann_alpha'])} · R² {fmt_num(fr['r_squared'], '.3f')} · n={fr['n_obs']} · Newey-West lags {fr['hac_maxlags']}")
        st.dataframe(pd.DataFrame(fr["table"]).T.rename(columns={"t_stat_hac": "t (Newey-West)"}), width="stretch")

        cl, cr = st.columns(2)
        cl.subheader("Correlations")
        cl.dataframe(pd.Series(d["correlations"], name="corr").to_frame(), width="stretch")
        cr.subheader("Betas")
        cr.dataframe(pd.Series(d["betas"], name="beta").to_frame(), width="stretch")

        st.subheader("Conditional performance (factor up / down)")
        st.dataframe(pd.DataFrame(d["conditional_perf"]).T, width="stretch")

        rc = pd.Series(d["rolling_corr"])
        if len(rc):
            rc.index = pd.to_datetime(rc.index)
            fig = go.Figure(go.Scatter(x=rc.index, y=rc, line=dict(color=PALETTE["primary"])))
            fig.add_hline(y=0, line_color=PALETTE["secondary"])
            plot(st, fig, "Rolling correlation to equities")


with tab_note:
    if art.note:
        st.markdown(art.note)
    else:
        st.info("No research note.")

with tab_cfg:
    st.code(yaml.safe_dump(asdict(cfg), sort_keys=False), language="yaml")
