"""
scripts/macro_state_validation.py

Computes every number in docs/research/macro_state_validation.md (plan step C8). No asset returns are
used. Run from the project root; prints markdown tables and, with --charts, writes figures to
docs/research/macro_state_v1/.

    python scripts/macro_state_validation.py [--section sanity|vintage|overlap|all] [--charts]

Caller needs no open DB; the script opens it read/write like the other scripts.
"""

import argparse
import pickle
from pathlib import Path

import numpy as np
import pandas as pd

from src.db.client import db
from src.macro import validation as val
from src.macro.data import load_fred
from src.macro.regimes import MACRO_REGIME_NAMES, REGIME_REGISTRY, StatePanel, macro_regimes

pd.set_option("display.width", 220)
pd.set_option("display.max_columns", 40)

INDEX_START = "1954-07-01"

# Episodes used for the narrative checks (calendar windows, inclusive).
RECESSION_WINDOWS = {
    "1974": ("1973-11-01", "1975-03-31"), "1980-82": ("1980-01-01", "1982-11-30"),
    "2001": ("2001-03-01", "2001-11-30"), "2008": ("2007-12-01", "2009-06-30"),
    "2020": ("2020-02-01", "2020-04-30"), "2022": ("2022-01-01", "2022-12-31"),
}
STRESS_WINDOWS = {
    "1998 LTCM": ("1998-08-01", "1998-12-31"), "2001": ("2001-03-01", "2002-12-31"),
    "2008": ("2008-09-01", "2009-06-30"), "2011": ("2011-07-01", "2011-12-31"),
    "2020": ("2020-02-15", "2020-06-30"), "2022": ("2022-01-01", "2022-12-31"),
    "2023 SVB": ("2023-03-01", "2023-06-30"),
}
FED_CYCLES = {   # (label expected, window)
    "1994-95 tightening": ("1994-02-01", "1995-02-28"), "1999-2000 tightening": ("1999-06-01", "2000-05-31"),
    "2001 easing": ("2001-01-01", "2001-12-31"), "2004-06 tightening": ("2004-06-01", "2006-06-30"),
    "2007-09 easing": ("2007-09-01", "2008-12-31"), "2015-18 tightening": ("2015-12-01", "2018-12-31"),
    "2019 easing": ("2019-07-01", "2019-12-31"), "2020 emergency easing": ("2020-03-01", "2020-12-31"),
    "2022-23 tightening": ("2022-03-01", "2023-07-31"),
}


def md(df: pd.DataFrame, fmt: str = "{:.2f}") -> str:
    df = df.copy()
    cols = [df.index.name or ""] + [str(c) for c in df.columns]
    lines = ["| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)]
    for idx, row in df.iterrows():
        cells = [str(idx)]
        for v in row:
            if isinstance(v, (float, np.floating)):
                cells.append("" if pd.isna(v) else fmt.format(v))
            else:
                cells.append("" if v is None or (isinstance(v, float) and pd.isna(v)) else str(v))
        lines.append("| " + " | ".join(cells) + " |")
    return "\n".join(lines)


def load_everything(cache: str | None):
    if cache and Path(cache).exists():
        return pickle.loads(Path(cache).read_bytes())
    db.open()
    try:
        panel = StatePanel(database=db, start=INDEX_START)
        frame = panel.frame
        regimes = macro_regimes(database=db, panel=panel)
        labels = {r.name: r.labels() for r in regimes}
        for name in ("volatility", "rates_curve", "equity_trend"):
            labels[name] = REGIME_REGISTRY[name](database=db).labels()
        usrec = load_fred(db, "USREC")
    finally:
        db.close()
    out = {"frame": frame, "labels": labels, "usrec": usrec}
    if cache:
        Path(cache).write_bytes(pickle.dumps(out))
    return out


def sanity(d: dict) -> None:
    f, usrec = d["frame"], d["usrec"]
    idx = f.index
    rec = val.usrec_daily(usrec, idx)
    wins = val.recession_windows(usrec)
    wins = wins[wins["end"] >= pd.Timestamp("1960-04-01")]
    sahm = f["sahm_triggered"]

    print("\n### Recession table (G_level, G_impulse, Sahm) ###")
    print(md(val.recession_table(wins, f["G_level"], f["G_impulse"], sahm).set_index("start")))
    print("\n### AUC against USREC (contemporaneous, business days) ###")
    rows = {}
    for name, score in {"-G_level": -f["G_level"], "-G_impulse": -f["G_impulse"], "-L_level (UNRATE gap)": f["L_level"],
                        "L_impulse": f["L_impulse"], "sahm_triggered": sahm.astype("float"), "FC": f["FC"],
                        "CS": f["CS"]}.items():
        both = pd.concat([score, rec], axis=1).dropna()
        rows[name] = {"auc": val.auc(score, rec), "from": both.index.min().date(), "n_days": len(both)}
    print(md(pd.DataFrame(rows).T))
    print("\n### Sahm runs outside any recession (grace 365d) ###")
    print(val.false_flag_runs(sahm, wins))
    print("\n### Labor deteriorating / curve inverted vs recession: share of recession days flagged, share of other days ###")
    for c in ("sahm_triggered", "labor_deteriorating", "curve_inverted"):
        x = pd.concat([f[c].astype("float"), rec], axis=1, keys=["f", "r"]).dropna()
        print(f"{c}: in recession {x[x.r == 1].f.mean():.2f}, outside {x[x.r == 0].f.mean():.2f}, from {x.index.min().date()}")

    print("\n### FC / CS / FC_impulse peaks in stress windows (percentile = rank within full history) ###")
    for c in ("FC", "CS"):
        print(c); print(md(val.window_extremes(f[c], STRESS_WINDOWS)))

    print("\n### Macro regime label shares in recession windows ###")
    for name in ("growth_inflation", "growth_level", "financial_conditions"):
        print(name); print(md(val.label_shares_by_window(d["labels"][name], RECESSION_WINDOWS)))
    print("\n### Policy regime label shares in Fed cycles ###")
    print(md(val.label_shares_by_window(d["labels"]["policy"], FED_CYCLES)))
    print("\n### Policy raw: P_stance / P_impulse means in Fed cycles ###")
    rows = {k: {"P_stance": f["P_stance"].loc[a:b].mean(), "P_impulse": f["P_impulse"].loc[a:b].mean()} for k, (a, b) in FED_CYCLES.items()}
    print(md(pd.DataFrame(rows).T))


def latest_vintage_inputs(database, names=None) -> dict:
    """What a naive backtest would use: every period's *latest* value (all revisions included),
    available at the period's real first-release date. Periods bulk-loaded on a series' first
    vintage date are dropped, as in the real-time build. Unrevised series use their normal build."""
    from src.macro import inputs as I
    from src.macro.data import load_vintages
    from src.macro.pit import UNREVISED_SERIES, _clean, latest, pit_series

    out = {}
    raw = {}
    for name in (names or list(I.SPECS)):
        code, fn, daily = I.SPECS[name]
        if code not in raw:
            raw[code] = load_vintages(database, code)
        v = raw[code]
        if v.empty:
            continue
        if code in UNREVISED_SERIES:
            out[name] = pit_series(v, fn, code, daily=daily)
            continue
        res = fn(latest(v)).dropna()
        first = _clean(v).groupby("period_date")["release_date"].min()
        avail = first.reindex(res.index)
        keep = avail > first.min() if not daily else avail.notna()
        res, avail = res[keep], avail[keep]
        s = pd.Series(res.to_numpy(dtype=float), index=pd.DatetimeIndex(avail.to_numpy()))
        out[name] = s[~s.index.duplicated(keep="last")].sort_index()
    return out


def vintage(d: dict) -> None:
    from src.macro import inputs as I
    from src.macro import regimes as rg
    from src.macro.data import load_vintages
    from src.macro.pit import UNREVISED_SERIES, _clean, first_vintage_date, latest, pit_series
    from src.macro.state import macro_state

    f = d["frame"]
    idx = f.index
    db.open()
    try:
        rt_in = I.load_state_inputs(db)
        lv_in = latest_vintage_inputs(db)
        raw = {c: load_vintages(db, c) for c in {s[0] for s in I.SPECS.values()}}
        nfci_v = load_vintages(db, "NFCI")
        nfci = pit_series(nfci_v, I.level, "NFCI", daily=False) if not nfci_v.empty else pd.Series(dtype=float)
    finally:
        db.close()
    rt, lv = macro_state(rt_in, idx), macro_state(lv_in, idx)
    cols = ["G_level", "G_impulse", "pi_level", "pi_impulse", "L_level", "L_impulse", "P_stance", "P_impulse", "FC", "FC_impulse", "CS", "CS_impulse"]
    print("\n### Real-time vs latest-vintage state (per component, common non-NaN days) ###")
    print(md(val.column_correlations(rt[cols], lv[cols]), "{:.3f}"))
    print("\n### Share of days the regime label differs: real-time vs latest-vintage ###")
    lab_rt = {
        "growth_inflation": rg.growth_inflation_labels(rt["G_impulse"], rt["pi_impulse"]),
        "growth_level": rg.growth_level_labels(rt["G_level"]),
        "policy": rg.policy_labels(rt["P_stance"], rt["P_impulse"]),
        "financial_conditions": rg.financial_conditions_labels(rt["FC"]),
    }
    lab_lv = {
        "growth_inflation": rg.growth_inflation_labels(lv["G_impulse"], lv["pi_impulse"]),
        "growth_level": rg.growth_level_labels(lv["G_level"]),
        "policy": rg.policy_labels(lv["P_stance"], lv["P_impulse"]),
        "financial_conditions": rg.financial_conditions_labels(lv["FC"]),
    }
    rows = {}
    for k in lab_rt:
        share, n = val.label_disagreement(lab_rt[k], lab_lv[k])
        rows[k] = {"disagree": share, "n_days": n}
    print(md(pd.DataFrame(rows).T, "{:.3f}"))
    # post-2000 only (revisions matter most where vintages are rich)
    print("\n(1996 onward, where the vintage record is richest)")
    rows = {}
    for k in lab_rt:
        share, n = val.label_disagreement(lab_rt[k].loc["1996":], lab_lv[k].loc["1996":])
        rows[k] = {"disagree": share, "n_days": n}
    print(md(pd.DataFrame(rows).T, "{:.3f}"))
    print("\nsaved: stored panel regimes equal rebuilt real-time labels:",
          {k: bool(d["labels"][k].dropna().equals(lab_rt[k].reindex(d["labels"][k].index).dropna())) for k in lab_rt})

    print("\n### Unrevised-series assumption: value known at the availability date vs the latest value the rule uses ###")
    print("(first print = the advance estimate; as-of = the value in the vintage record on period + configured lag)")
    rows = {}
    for name, (code, fn, daily) in I.SPECS.items():
        if code not in UNREVISED_SERIES:
            continue
        v = _clean(raw[code])
        if v.empty:
            continue
        lag = UNREVISED_SERIES[code]
        fv = v["release_date"].min()
        firsts = v.sort_values("release_date").groupby("period_date").first()
        era = firsts.index[firsts["release_date"] > fv]
        avail = pd.Series(firsts.index + pd.Timedelta(days=lag), index=firsts.index)

        def snapshot(use_avail):
            vv = v.assign(avail=v["period_date"].map(avail))
            if use_avail:
                vv = vv[vv["release_date"] <= vv["avail"]]
            g = vv.sort_values("release_date").groupby("period_date")["value"]
            return pd.Series((g.last() if use_avail else g.first()).to_numpy(), index=g.last().index)

        lat = fn(latest(v))
        out = {"series": code, "lag": lag}
        for label, ser in (("first_print", fn(snapshot(False))), ("as_of_lag", fn(snapshot(True)))):
            both = pd.concat([ser, lat], axis=1, keys=["x", "latest"]).dropna()
            both = both[both.index.isin(era)]
            diff = (both["x"] - both["latest"]).abs()
            out[f"{label}_mean"], out[f"{label}_p95"], out[f"{label}_max"] = diff.mean(), diff.quantile(0.95), diff.max()
            out["n_periods"] = len(both)
        rows[name] = out
    print(md(pd.DataFrame(rows).T, "{:.3f}"))
    print("\nActual first-release lag vs configured lag (days from period date), vintage-era periods:")
    lagrows = {}
    for code, lag in UNREVISED_SERIES.items():
        v = _clean(raw.get(code, load_vintages_safe(code)))
        if v.empty:
            continue
        fv = v["release_date"].min()
        firsts = v.groupby("period_date")["release_date"].min()
        firsts = firsts[firsts > fv]
        actual = (firsts - firsts.index.to_series()).dt.days
        actual = actual[actual <= 3 * lag]
        lagrows[code] = {"configured": lag, "n": len(actual), "median": actual.median(), "max": actual.max(),
                         "share_over_configured": float((actual > lag).mean())}
    print(md(pd.DataFrame(lagrows).T, "{:.2f}"))

    print("\nPeriods released later than the configured lag (vintage era):")
    for code in ("ICNSA", "CCSA", "SAHMREALTIME", "CPIAUCNS"):
        v = _clean(raw.get(code, load_vintages_safe(code)))
        lag = UNREVISED_SERIES[code]
        firsts = v.groupby("period_date")["release_date"].min()
        firsts = firsts[firsts > v["release_date"].min()]
        actual = (firsts - firsts.index.to_series()).dt.days
        late = actual[(actual > lag) & (actual <= 3 * lag)]
        print(code, "configured", lag, "late periods:", len(late), "by year:", late.groupby(late.index.year).size().to_dict(),
              "max lag", int(late.max()) if len(late) else None)
    sl = _clean(raw["DRTSCILM"])
    sl_first = sl.groupby("period_date")["value"].first()
    sl_last = sl.groupby("period_date")["value"].last()
    chg = (sl_last - sl_first).abs()
    chg = chg[chg > 0.05]
    print(f"\nSLOOS (DRTSCILM) periods whose value changed between releases: {len(chg)} "
          f"({chg.index.min().date()} to {chg.index.max().date()}), mean abs change {chg.mean():.2f}, max {chg.max():.2f}; "
          f"restated on {sl[sl['period_date'].isin(chg.index)].sort_values('release_date').groupby('period_date')['release_date'].last().dt.date.value_counts().to_dict()}")

    no_ccsa = macro_state({k: v for k, v in rt_in.items() if k != "cont_yoy13"}, idx)
    both = pd.concat([rt["L_impulse"], no_ccsa["L_impulse"]], axis=1, keys=["with", "without"]).dropna()
    print(f"L_impulse with vs without CCSA (continuing claims): corr {both['with'].corr(both['without']):.3f}, "
          f"mean abs diff {(both['with'] - both['without']).abs().mean():.3f}, n {len(both)}")
    from src.macro.flags import labor_deteriorating
    fa, fb = labor_deteriorating(rt["L_impulse"]), labor_deteriorating(no_ccsa["L_impulse"])
    x = pd.concat([fa, fb], axis=1).dropna().astype(bool)
    print(f"labor_deteriorating flag differs on {(x.iloc[:, 0] != x.iloc[:, 1]).mean():.3f} of {len(x)} days")

    print("\n### Thin real-time history: two eras ###")
    full = macro_state(rt_in, idx)
    thin_in = {k: v for k, v in rt_in.items() if k not in ("temp_6m", "hours_d6")}
    thin = macro_state(thin_in, idx)
    both = pd.concat([full["L_impulse"], thin["L_impulse"]], axis=1, keys=["full", "thin"]).dropna().loc["2012":]
    print(f"L_impulse, 5 components vs 3 (no temp help, hours), 2012+: corr {both.full.corr(both.thin):.3f}, "
          f"mean abs diff {(both.full - both.thin).abs().mean():.3f}, n {len(both)}")
    print("L_impulse components over time:", full["L_impulse_n"].resample("10YE").max().to_dict())
    if not nfci.empty:
        from src.macro.pit import to_daily
        n = to_daily(nfci, idx)
        both = pd.concat([f["FC"], n], axis=1, keys=["FC", "NFCI"]).dropna()
        print(f"FC vs real-time NFCI (2011+): corr {both.FC.corr(both.NFCI):.3f}, n {len(both)}, from {both.index.min().date()}")
        # NFCI real-time vs latest, label of positive (tight)
        nl = latest(nfci_v)
        print("NFCI first vintage:", first_vintage_date(nfci_v).date())


def load_vintages_safe(code):
    from src.macro.data import load_vintages
    db.open()
    try:
        return load_vintages(db, code)
    finally:
        db.close()


def truncation(d: dict, cuts=("2012-12-31", "2019-12-31")) -> None:
    """Rebuild every state input as if the database ended on `cut` and compare with the full build
    on dates up to `cut`. Real-time (vintage-reconstructed) series must match exactly."""
    from src.macro import inputs as I
    from src.macro.pit import UNREVISED_SERIES
    import src.macro.inputs as inputs_module

    db.open()
    try:
        full = I.load_state_inputs(db)
        real_load = inputs_module.load_vintages
        for cut in cuts:
            c = pd.Timestamp(cut)
            inputs_module.load_vintages = lambda database, code, _l=real_load, _c=c: (
                lambda v: v[pd.to_datetime(v["release_date"]) <= _c])(_l(database, code))
            try:
                trunc = I.load_state_inputs(db)
            finally:
                inputs_module.load_vintages = real_load
            rows = {}
            for name, s in trunc.items():
                code = I.SPECS[name][0]
                a = s.loc[:c]
                b = full[name].loc[:c]
                joined = pd.concat([a, b], axis=1, keys=["trunc", "full"])
                same_dates = joined.notna().all(axis=1)
                diff = (joined["trunc"] - joined["full"]).abs()[same_dates]
                rows[name] = {"series": code, "unrevised_rule": code in UNREVISED_SERIES, "n_dates": len(a),
                              "dates_only_in_full": int((joined["full"].notna() & joined["trunc"].isna()).sum()),
                              "max_abs_diff": diff.max() if len(diff) else np.nan}
            print(f"\n### Truncation at {cut}: inputs rebuilt from the DB as it stood then ###")
            print(md(pd.DataFrame(rows).T, "{:.6f}"))
    finally:
        db.close()


def stability(d: dict) -> None:
    """What the (causal) min_duration rule does to episode counts and lengths. Label stability only;
    no returns are involved."""
    from src.macro.regimes import min_duration_labels

    print("\n### Effect of min_duration on episode length (label stability only) ###")
    rows = {}
    for name in ("growth_inflation", "growth_level", "policy", "financial_conditions", "curve_dynamics", "dollar_trend"):
        raw = d["labels"][name]
        if name == "growth_inflation":
            from src.macro.regimes import growth_inflation_labels
            f = d["frame"]
            raw = growth_inflation_labels(f["G_impulse"], f["pi_impulse"], min_duration=1)
        for md_ in (1, 21, 63):
            lab = min_duration_labels(raw, md_)
            ep = val.episodes(lab)
            smry = val.episode_summary(lab)
            rows[(name, md_)] = {"episodes": len(ep), "median_days": ep["n_days"].median(), "mean_days": ep["n_days"].mean(),
                                 "labels_usable": f"{int(smry['usable'].sum())}/{len(smry)}",
                                 "valid_from": lab.dropna().index.min().date()}
    df = pd.DataFrame(rows).T
    df.index = [f"{a} / {b}" for a, b in df.index]
    print(md(df, "{:.1f}"))
    print("\n(min_duration lags every regime change by that many business days.)")


# ── Figures (single-axis panels, thin marks, validated categorical slots 1-4, light surface) ──
SURFACE, INK, INK2, GRID, BAND = "#fcfcfb", "#0b0b0b", "#52514e", "#e1e0d9", "#e6e5de"
BLUE, ORANGE, AQUA, YELLOW = "#2a78d6", "#eb6834", "#1baf7a", "#eda100"
ORDINAL_BLUE = {"loose": "#bcd6f5", "normal": "#6aa0e6", "tight": "#1d5fb0"}
LABEL_COLORS = {
    "growth_inflation": {"goldilocks": BLUE, "reflation": ORANGE, "stagflation": AQUA, "deflation": YELLOW},
    "growth_level": {"above_trend": BLUE, "below_trend": BAND},
    "policy": {"restrictive_tightening": BLUE, "restrictive_easing": ORANGE,
               "accommodative_tightening": AQUA, "accommodative_easing": YELLOW},
    "financial_conditions": ORDINAL_BLUE,
    "curve_dynamics": {"bull_steepener": BLUE, "bear_steepener": ORANGE, "bull_flattener": AQUA, "bear_flattener": YELLOW},
    "dollar_trend": {"strong_usd": BLUE, "weak_usd": BAND},
}


def _style(ax):
    ax.set_facecolor(SURFACE)
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    for s in ("left", "bottom"):
        ax.spines[s].set_color(GRID)
    ax.tick_params(colors=INK2, labelsize=9, length=0)
    ax.grid(axis="y", color=GRID, linewidth=0.6)
    ax.set_axisbelow(True)


def _shade(ax, windows, color=BAND):
    for _, w in windows.iterrows():
        ax.axvspan(w["start"], w["end"], color=color, linewidth=0, zorder=0)


def charts(d: dict, outdir: str) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.patches import Patch

    out = Path(outdir)
    out.mkdir(parents=True, exist_ok=True)
    f, labels, usrec = d["frame"], d["labels"], d["usrec"]
    wins = val.recession_windows(usrec)
    wins = wins[wins["end"] >= pd.Timestamp("1960-04-01")]
    plt.rcParams.update({"font.family": "sans-serif", "figure.facecolor": SURFACE, "savefig.facecolor": SURFACE})

    # 1. growth state and the Sahm flag against NBER recessions
    fig, axes = plt.subplots(3, 1, figsize=(10, 6.4), sharex=True, gridspec_kw={"height_ratios": [3, 3, 1]})
    for ax, col, title in ((axes[0], "G_level", "Growth level (G_level, z-score)"),
                           (axes[1], "G_impulse", "Growth impulse (G_impulse, z-score)")):
        _style(ax); _shade(ax, wins)
        ax.plot(f.index, f[col], color=BLUE, linewidth=1.0)
        ax.axhline(0, color=INK2, linewidth=0.6)
        ax.set_title(title, loc="left", fontsize=10, color=INK)
    flag = f["sahm_triggered"].fillna(False).astype(bool)
    ax = axes[2]; _style(ax); _shade(ax, wins); ax.grid(False)
    ax.fill_between(f.index, 0, flag.astype(float), step="post", color=ORANGE, linewidth=0)
    ax.set_yticks([]); ax.set_ylim(0, 1)
    ax.set_title("Sahm rule triggered (real time)", loc="left", fontsize=10, color=INK)
    axes[2].set_xlim(pd.Timestamp("1960-01-01"), f.index.max())
    fig.text(0.01, 0.005, "Grey bands: NBER recessions (USREC). Series are point-in-time; z-scores are expanding.", fontsize=8, color=INK2)
    fig.tight_layout(rect=(0, 0.02, 1, 1))
    fig.savefig(out / "growth_vs_recessions.png", dpi=150); plt.close(fig)

    # 2. financial conditions and credit supply with stress windows
    fig, axes = plt.subplots(2, 1, figsize=(10, 5.4), sharex=True)
    for ax, col, title in ((axes[0], "FC", "Financial conditions (FC, z-score; higher = tighter)"),
                           (axes[1], "CS", "Credit supply (CS, z-score; higher = tighter)")):
        _style(ax); _shade(ax, wins)
        s = f[col].loc["1999":]
        ax.plot(s.index, s, color=BLUE, linewidth=1.0)
        ax.axhline(0, color=INK2, linewidth=0.6)
        ax.set_title(title, loc="left", fontsize=10, color=INK)
    for name, (a, b) in STRESS_WINDOWS.items():
        if pd.Timestamp(a) > pd.Timestamp("1999-12-01"):
            axes[0].annotate(name.split(" ")[0] if name[0] != "2" or " " not in name else name.split(" ")[0],
                             (pd.Timestamp(a) + (pd.Timestamp(b) - pd.Timestamp(a)) / 2, f["FC"].loc[a:b].max()),
                             textcoords="offset points", xytext=(0, 5), ha="center", fontsize=8, color=INK2)
    axes[1].set_xlim(pd.Timestamp("1999-06-01"), f.index.max())
    fig.text(0.01, 0.005, "Grey bands: NBER recessions. FC starts 1999-11 (HY OAS archive + burn-in); CS is quarterly (SLOOS) so it steps.", fontsize=8, color=INK2)
    fig.tight_layout(rect=(0, 0.02, 1, 1))
    fig.savefig(out / "financial_conditions_credit.png", dpi=150); plt.close(fig)

    # 3. regime timelines, one strip each, with direct labels
    names = ["growth_inflation", "growth_level", "policy", "financial_conditions", "curve_dynamics", "dollar_trend"]
    fig, axes = plt.subplots(len(names), 1, figsize=(10, 7.2), sharex=True)
    for ax, name in zip(axes, names):
        lab = labels[name].dropna()
        pal = LABEL_COLORS[name]
        for _, r in val.episodes(lab).iterrows():
            ax.axvspan(r["start"], r["end"] + pd.Timedelta(days=1), color=pal[r["label"]], linewidth=0)
        ax.set_yticks([]); ax.set_facecolor(SURFACE)
        for s in ax.spines.values():
            s.set_visible(False)
        ax.tick_params(colors=INK2, labelsize=9, length=0)
        ax.set_ylabel(name.replace("_", "\n"), rotation=0, ha="right", va="center", fontsize=9, color=INK)
        ax.legend(handles=[Patch(facecolor=c, label=l.replace("_", " ")) for l, c in pal.items()],
                  loc="center left", bbox_to_anchor=(1.01, 0.5), frameon=False, fontsize=8, labelcolor=INK2,
                  handlelength=1.2, handleheight=1.0, borderaxespad=0)
        ax.set_xlim(pd.Timestamp("1960-01-01"), f.index.max())
    _shade_all = [ax.axvspan(w["start"], w["end"], ymin=0, ymax=0.08, color=INK2, linewidth=0) for ax in axes[-1:] for _, w in wins.iterrows()]
    fig.text(0.01, 0.005, "Bottom strip's dark ticks mark NBER recessions. Episodes are runs of one label; blank = before the label is valid. "
             "dollar_trend is approximate before 2019-02.", fontsize=8, color=INK2)
    fig.tight_layout(rect=(0, 0.02, 0.86, 1))
    fig.savefig(out / "regime_timelines.png", dpi=150); plt.close(fig)


def overlap(d: dict) -> None:
    f, labels = d["frame"], d["labels"]
    print("\n### Episode summary per regime label ###")
    for name, lab in labels.items():
        s = val.episode_summary(lab)
        print(f"\n{name}  (valid {lab.dropna().index.min().date()} to {lab.dropna().index.max().date()})")
        print(md(s, "{:.2f}"))
    print("\n### Cramer's V between regime label series ###")
    v, n = val.label_association(labels)
    print(md(v)); print("min common days:", int(n.values[n.values > 0].min()))
    print("\n### Correlation matrix of the state vector ###")
    cols = ["G_level", "G_impulse", "pi_level", "pi_impulse", "L_level", "L_impulse", "P_stance", "P_impulse", "FC", "FC_impulse", "CS"]
    print(md(f[cols].corr()))
    print("\n### Pairs with |corr| > 0.6 ###")
    c = f[cols].corr()
    for i, a in enumerate(cols):
        for b in cols[i + 1:]:
            if abs(c.loc[a, b]) > 0.6:
                print(a, b, round(c.loc[a, b], 2))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--section", default="all", choices=["sanity", "vintage", "truncation", "overlap", "stability", "all"])
    ap.add_argument("--charts", action="store_true", help="write figures to docs/research/macro_state_v1/")
    ap.add_argument("--cache", default=None, help="pickle path to cache the panel and labels between runs")
    args = ap.parse_args()
    data = load_everything(args.cache)
    if args.charts:
        charts(data, "docs/research/macro_state_v1")
    if args.section in ("sanity", "all"):
        sanity(data)
    if args.section in ("vintage", "all"):
        vintage(data)
    if args.section in ("truncation", "all"):
        truncation(data)
    if args.section in ("overlap", "all"):
        overlap(data)
    if args.section in ("stability", "all"):
        stability(data)
