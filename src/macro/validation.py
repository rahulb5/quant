"""
src/macro/validation.py

Pure helpers for the C8 validation write-up (docs/research/macro_state_validation.md):
episode counting, association between regime labels, recession sanity checks and
real-time vs latest-vintage comparisons. No DB access and no asset returns.

Usage:
    ep = episodes(labels)                       # one row per contiguous run of a label
    summary = episode_summary(labels)           # per label: n_episodes, days, usable
    v, n = label_association({"a": la, "b": lb})
"""

from __future__ import annotations

import numpy as np
import pandas as pd

MIN_EPISODES = 5     # labels with fewer episodes are reported but not used in experiment verdicts


# ── Episodes ──────────────────────────────────────────────────────────────────

def episodes(labels: pd.Series) -> pd.DataFrame:
    """Contiguous runs of each label (a NaN, or a change of label, ends a run).

    Columns: label, start, end, n_days (observations in the run).
    """
    s = labels.dropna()
    if s.empty:
        return pd.DataFrame(columns=["label", "start", "end", "n_days"])
    # a gap in the original index (NaN rows) must also break a run
    pos = pd.Series(np.arange(len(labels)), index=labels.index).loc[s.index].to_numpy()
    new = (s.to_numpy() != np.roll(s.to_numpy(), 1)) | (np.diff(pos, prepend=pos[0] - 1) > 1)
    new[0] = True
    run = np.cumsum(new)
    g = pd.DataFrame({"label": s.to_numpy(), "date": s.index, "run": run}).groupby("run")
    out = g.agg(label=("label", "first"), start=("date", "min"), end=("date", "max"), n_days=("date", "size"))
    return out.reset_index(drop=True)


def episode_summary(labels: pd.Series, min_episodes: int = MIN_EPISODES) -> pd.DataFrame:
    """Per label: n_episodes, n_days, share of labelled days, median and mean run length (days),
    and `usable` (n_episodes >= min_episodes)."""
    ep = episodes(labels)
    total = int(labels.notna().sum())
    if ep.empty:
        return pd.DataFrame(columns=["n_episodes", "n_days", "share", "median_days", "mean_days", "usable"])
    g = ep.groupby("label")["n_days"]
    out = pd.DataFrame({
        "n_episodes": g.size(), "n_days": g.sum(), "share": g.sum() / total,
        "median_days": g.median(), "mean_days": g.mean(),
    })
    out["usable"] = out["n_episodes"] >= min_episodes
    return out.sort_values("n_days", ascending=False)


# ── Association between label series ─────────────────────────────────────────

def _table(a: pd.Series, b: pd.Series) -> pd.DataFrame:
    both = pd.concat([a, b], axis=1, keys=["a", "b"]).dropna()
    return pd.crosstab(both["a"], both["b"])


def cramers_v(a: pd.Series, b: pd.Series) -> float:
    """Cramer's V between two label series on their common dates (0 = unrelated, 1 = identical
    partitions). NaN if either has fewer than two labels in the overlap."""
    t = _table(a, b)
    if t.shape[0] < 2 or t.shape[1] < 2:
        return float("nan")
    n = t.to_numpy().sum()
    expected = np.outer(t.sum(axis=1), t.sum(axis=0)) / n
    chi2 = ((t.to_numpy() - expected) ** 2 / expected).sum()
    return float(np.sqrt(chi2 / (n * (min(t.shape) - 1))))


def label_association(regimes: dict[str, pd.Series]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Pairwise Cramer's V and common-day counts across regime label series."""
    names = list(regimes)
    v = pd.DataFrame(np.nan, index=names, columns=names)
    n = pd.DataFrame(0, index=names, columns=names)
    for i, a in enumerate(names):
        for b in names[i:]:
            both = pd.concat([regimes[a], regimes[b]], axis=1).dropna()
            n.loc[a, b] = n.loc[b, a] = len(both)
            v.loc[a, b] = v.loc[b, a] = 1.0 if a == b else cramers_v(regimes[a], regimes[b])
    return v, n


def label_disagreement(a: pd.Series, b: pd.Series) -> tuple[float, int]:
    """Share of common labelled days on which two label series differ, and the day count."""
    both = pd.concat([a, b], axis=1, keys=["a", "b"]).dropna()
    if both.empty:
        return float("nan"), 0
    return float((both["a"] != both["b"]).mean()), len(both)


def column_correlations(a: pd.DataFrame, b: pd.DataFrame) -> pd.DataFrame:
    """Per shared column: correlation and mean/max absolute difference over common non-NaN days,
    and the number of days."""
    rows = {}
    for c in a.columns.intersection(b.columns):
        both = pd.concat([a[c], b[c]], axis=1, keys=["a", "b"]).dropna()
        if len(both) < 3:
            continue
        d = (both["a"] - both["b"]).abs()
        rows[c] = {"corr": both["a"].corr(both["b"]), "mean_abs_diff": d.mean(), "max_abs_diff": d.max(),
                   "n_days": len(both)}
    return pd.DataFrame(rows).T


# ── Recessions ────────────────────────────────────────────────────────────────

def recession_windows(usrec: pd.Series) -> pd.DataFrame:
    """NBER recessions from a monthly 0/1 series indexed by month start: columns start (first
    day of the first month), end (last day of the last month)."""
    s = usrec.dropna().astype(int)
    new = s.ne(s.shift()).cumsum()
    rows = []
    for _, g in s.groupby(new):
        if g.iloc[0] == 1:
            rows.append({"start": g.index[0], "end": g.index[-1] + pd.offsets.MonthEnd(0)})
    return pd.DataFrame(rows, columns=["start", "end"])


def usrec_daily(usrec: pd.Series, index: pd.DatetimeIndex) -> pd.Series:
    """The monthly recession indicator on a business-day index (0/1; NaN outside its range)."""
    m = usrec.dropna().astype(float)
    out = m.reindex(m.index.union(index)).ffill().reindex(index)
    return out.where(index <= m.index.max() + pd.offsets.MonthEnd(0))


def auc(score: pd.Series, outcome: pd.Series) -> float:
    """Probability that a randomly chosen outcome=1 day has a higher score than an outcome=0 day
    (rank-based; 0.5 = no information). Ties count half."""
    both = pd.concat([score, outcome], axis=1, keys=["s", "y"]).dropna()
    y = both["y"].to_numpy().astype(bool)
    if y.all() or (~y).all():
        return float("nan")
    ranks = both["s"].rank(method="average").to_numpy()
    n1, n0 = y.sum(), (~y).sum()
    return float((ranks[y].sum() - n1 * (n1 + 1) / 2) / (n1 * n0))


def recession_table(
    windows: pd.DataFrame, g_level: pd.Series, g_impulse: pd.Series, sahm: pd.Series,
    lead_days: int = 120, after_days: int = 90,
) -> pd.DataFrame:
    """Per recession: mean G_level / G_impulse inside the window, share of days with G_level < 0,
    and the first day (from `lead_days` before the start to `after_days` after the end) the Sahm flag was
    True, with the lag in calendar days from the recession's first month (negative = before it started).
    The window extends past the end because a short recession can end before its jobs data is published."""
    rows = []
    for _, w in windows.iterrows():
        inside = slice(w["start"], w["end"])
        gl, gi = g_level.loc[inside].dropna(), g_impulse.loc[inside].dropna()
        look = sahm.loc[w["start"] - pd.Timedelta(days=lead_days): w["end"] + pd.Timedelta(days=after_days)]
        hit = look[look.fillna(False).astype(bool)]
        first = hit.index[0] if len(hit) else pd.NaT
        rows.append({
            "start": w["start"].date(), "end": w["end"].date(),
            "G_level_mean": gl.mean() if len(gl) else np.nan,
            "G_level_below0": float((gl < 0).mean()) if len(gl) else np.nan,
            "G_impulse_mean": gi.mean() if len(gi) else np.nan,
            "sahm_first": None if pd.isna(first) else first.date(),
            "sahm_lag_days": np.nan if pd.isna(first) else (first - w["start"]).days,
        })
    return pd.DataFrame(rows)


def false_flag_runs(flag: pd.Series, windows: pd.DataFrame, grace_days: int = 365) -> pd.DataFrame:
    """Runs of a True flag that start more than `grace_days` before, or after the end of, every recession
    window (episodes the flag signalled outside a recession)."""
    f = flag.fillna(False).astype(bool)
    runs = episodes(pd.Series(np.where(f, "on", None), index=f.index, dtype=object))
    keep = []
    for _, r in runs.iterrows():
        near = ((windows["start"] - pd.Timedelta(days=grace_days) <= r["start"]) &
                (r["start"] <= windows["end"] + pd.Timedelta(days=grace_days))).any()
        if not near:
            keep.append(r)
    return pd.DataFrame(keep, columns=runs.columns).reset_index(drop=True)


def window_extremes(s: pd.Series, windows: dict[str, tuple[str, str]], full_sample: pd.Series | None = None) -> pd.DataFrame:
    """Per named window: the peak (max) value and its date, the window mean, and the percentile
    of the peak within `full_sample` (default: the whole series) ... NaN if no data in the window."""
    ref = (full_sample if full_sample is not None else s).dropna()
    rows = {}
    for name, (a, b) in windows.items():
        w = s.loc[a:b].dropna()
        if w.empty:
            rows[name] = {"peak": np.nan, "peak_date": None, "mean": np.nan, "peak_pctile": np.nan}
            continue
        rows[name] = {"peak": w.max(), "peak_date": w.idxmax().date(), "mean": w.mean(),
                      "peak_pctile": float((ref <= w.max()).mean())}
    return pd.DataFrame(rows).T


def label_shares_by_window(labels: pd.Series, windows: dict[str, tuple[str, str]]) -> pd.DataFrame:
    """Share of labelled days carrying each label, per named window (rows = windows)."""
    rows = {}
    for name, (a, b) in windows.items():
        w = labels.loc[a:b].dropna()
        rows[name] = w.value_counts(normalize=True) if len(w) else pd.Series(dtype=float)
    return pd.DataFrame(rows).T.fillna(0.0)
