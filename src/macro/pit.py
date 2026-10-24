"""
src/macro/pit.py

Point-in-time (as-of vintage) construction of macro series. Pure pandas: no
DB access, no Streamlit. Vintages come from src.macro.data.load_vintages().

A value used on date t must be computed from the dataset as it was published
on t, revisions included. Stitching each period's first release is not enough:
a YoY, rolling mean or acceleration mixes vintages (this month's print against
a last-year base that has since been revised). `realtime()` therefore rebuilds
the series at every release date and applies the transform to that snapshot.

    vintages  -> realtime(vintages, fn)        revisable series (the default)
              -> unrevised(vintages, fn, lag)  series documented as unrevised
    sparse release-dated series -> to_daily()  business-day index, forward-filled

Pre-vintage history. ALFRED loads the history it already held under a single
early release date (e.g. CPI back to 1947 appears on 1972-07-21), so nothing
before that date was ever "released" in real time. `realtime()` drops it by
default (pre_vintage="drop"): a snapshot only emits the value at its latest
period. pre_vintage="latest_lagged" instead emits latest-vintage values at
period + release_lag_days; use it for exploration only, and the output is
flagged in `.attrs`.

Causality. `fn` must be causal, i.e. its value at period p depends only on
periods <= p (YoY, trailing means, expanding statistics). `realtime()` only
keeps the value at the latest period, so a non-causal fn is wrong; `unrevised()`
applies fn once to the whole series and so relies on causality outright.

Usage:
    from src.macro.data import load_vintages
    from src.macro.pit import realtime, to_daily

    v = load_vintages(db, "PAYEMS")
    yoy = realtime(v, lambda s: s.pct_change(12) * 100)
    daily = to_daily(yoy, pd.bdate_range("2000-01-03", "2026-10-22"))
"""

from __future__ import annotations

from typing import Callable, Literal

import pandas as pd

from src.shared.utils import logger

PreVintage = Literal["drop", "latest_lagged"]

# Series documented as unrevised (checked against ALFRED, see
# docs/plans/macro_research_layer.md): code -> release lag in days, a
# conservative upper bound counted from the period start (monthly/quarterly)
# or the week end (weekly). These skip vintage reconstruction and use latest
# values shifted by the lag, which gives them history before their vintages
# begin. Add a series only with a measured revision check.
UNREVISED_SERIES: dict[str, int] = {
    "CPILFENS": 62,       # core CPI, NSA
    "CPIAUCNS": 62,       # headline CPI, NSA
    "DRTSCILM": 60,       # SLOOS C&I tightening
    "CCSA": 12,           # continuing claims
    "ICNSA": 19,          # initial claims NSA: advance estimate is revised the next week
                          # (residual ~0.5% of level after 19 days, mostly annual re-benchmark)
    "SAHMREALTIME": 40,   # real-time by construction
}

# The subset of UNREVISED_SERIES whose first print is the final value (no advance
# estimate), so the real first-release date can replace the conservative lag wherever
# ALFRED recorded one. Not ICNSA / CCSA: their early prints are revised.
USE_FIRST_RELEASE_DATE: frozenset[str] = frozenset({"CPILFENS", "CPIAUCNS", "DRTSCILM", "SAHMREALTIME"})

# Series whose vintage history starts late but whose backfilled history is used anyway, by a
# documented researcher decision: code -> lag in days (from the period) for the backfilled values.
# Before the first vintage the latest values are used at period + lag; from the first vintage on
# the real-time vintages (with their real release dates) are used. Output is flagged
# pre_vintage="latest_lagged" in .attrs. Add a series only with a written decision.
#   DTWEXBGS: first vintage 2019-02-04 (the Fed back-filled 2006-2018). Revised (weights update):
#   against the real-time label, the latest-vintage 200-day-gap label differs on 4.5% of days
#   (2019-2026). Real-time release lag is median 5, max 10 days; the backfill uses 10.
PRE_VINTAGE_LATEST_LAGGED: dict[str, int] = {
    "DTWEXBGS": 10,
}

# Series whose stored release_date is known to precede publication.
# code -> extra business days. The ICE OAS series appear the next morning.
RELEASE_LAG_OVERRIDES: dict[str, int] = {
    "BAMLH0A0HYM2": 1,
    "BAMLC0A0CM": 1,
    "BAMLC0A4CBBB": 1,
    "BAMLEMCBPIOAS": 1,
}

_REQUIRED = ("period_date", "release_date", "value")


def _clean(vintages: pd.DataFrame) -> pd.DataFrame:
    missing = [c for c in _REQUIRED if c not in vintages.columns]
    if missing:
        raise ValueError(f"vintages is missing columns {missing}; expected {list(_REQUIRED)}")
    v = vintages[list(_REQUIRED)].dropna().copy()
    v["period_date"] = pd.to_datetime(v["period_date"])
    v["release_date"] = pd.to_datetime(v["release_date"])
    return v.sort_values(["release_date", "period_date"], kind="stable").reset_index(drop=True)


def _empty() -> pd.Series:
    return pd.Series(dtype=float, index=pd.DatetimeIndex([]))


def first_vintage_date(vintages: pd.DataFrame) -> pd.Timestamp | None:
    """Earliest release date in the data, i.e. when real-time history begins."""
    v = _clean(vintages)
    return None if v.empty else v["release_date"].min()


def as_of(vintages: pd.DataFrame, when: str | pd.Timestamp) -> pd.Series:
    """The series as published on `when`: the latest release <= when for each period.

    Returns an empty Series if nothing had been released by then.
    """
    v = _clean(vintages)
    v = v[v["release_date"] <= pd.Timestamp(when)]
    if v.empty:
        return _empty()
    s = v.groupby("period_date")["value"].last()   # rows are sorted by release_date
    s.index.name = None
    return s.sort_index()


def latest(vintages: pd.DataFrame) -> pd.Series:
    """Latest-vintage value of every period (what FRED shows today)."""
    v = _clean(vintages)
    if v.empty:
        return _empty()
    s = v.groupby("period_date")["value"].last()
    s.index.name = None
    return s.sort_index()


def _warn_if_stamped(v: pd.DataFrame, label: str) -> None:
    n = int((v["release_date"] <= v["period_date"]).sum())
    if n:
        logger.warning(
            f"[pit] {label}: {n} row(s) have release_date <= period_date on a non-daily series; "
            "these look like unvintaged rows (see src/collectors/fred_cleanup.py) and would look ahead"
        )


def realtime(
    vintages: pd.DataFrame,
    fn: Callable[[pd.Series], pd.Series],
    release_lag_days: int = 0,
    pre_vintage: PreVintage = "drop",
    daily: bool = False,
    label: str = "series",
    pre_vintage_lag_days: int | None = None,
) -> pd.Series:
    """Apply `fn` to the series as published at each release date; keep the newest value.

    For every distinct release date v, the snapshot is the latest release <= v
    of each period, `fn(snapshot)` is evaluated, and its value at the snapshot's
    latest period is recorded at v + release_lag_days. A release that only
    revises old periods is therefore still recorded when it changes that value.

    Args:
        vintages:         DataFrame with period_date, release_date, value.
        fn:               Causal transform of a period-indexed Series, returning a
                          Series on the same (or a subset of the) index.
        release_lag_days: Extra days between the stored release date and when the
                          value is usable (per-series override).
        pre_vintage:      "drop" (default) or "latest_lagged" (see module docstring).
        daily:            True for daily series, which legitimately have
                          release_date == period_date; suppresses the stamped-row warning and,
                          if no period has more than one release, takes a single-pass fast path.
        label:            Name used in warnings.
        pre_vintage_lag_days: Lag for the backfilled values when pre_vintage="latest_lagged"
                          (default: release_lag_days). Real-time values keep release_lag_days.

    Returns:
        Series indexed by availability date (unique, sorted), with
        .attrs {"first_vintage", "n_vintages", "pre_vintage"}. Empty if there is no data.
    """
    if pre_vintage not in ("drop", "latest_lagged"):
        raise ValueError(f"pre_vintage must be 'drop' or 'latest_lagged', got {pre_vintage!r}")
    v = _clean(vintages)
    if v.empty:
        return _empty()
    if not daily:
        _warn_if_stamped(v, label)

    lag = pd.Timedelta(days=release_lag_days)

    if daily and pre_vintage == "drop" and not v.duplicated("period_date").any():
        # Unrevised daily series (one release per period): for a causal fn the snapshot
        # at each release is just the series up to it, so apply fn once.
        res = fn(pd.Series(v["value"].to_numpy(dtype=float), index=pd.DatetimeIndex(v["period_date"])).sort_index())
        released = pd.Series(v["release_date"].to_numpy(), index=pd.DatetimeIndex(v["period_date"]))
        res = res.dropna()
        out = pd.Series(res.to_numpy(dtype=float), index=pd.DatetimeIndex(released.reindex(res.index).to_numpy()) + lag)
        out = out[~out.index.duplicated(keep="last")].sort_index()
        out.attrs.update(first_vintage=v["release_date"].iloc[0], n_vintages=int(v["release_date"].nunique()),
                         pre_vintage=pre_vintage)
        return out

    current: dict[pd.Timestamp, float] = {}
    when: list[pd.Timestamp] = []
    vals: list[float] = []
    first_release = v["release_date"].iloc[0]
    backfill: pd.Series | None = None

    for release, grp in v.groupby("release_date", sort=True):
        current.update(zip(grp["period_date"], grp["value"]))
        snap = pd.Series(current).sort_index()
        res = fn(snap)
        if release == first_release and pre_vintage == "latest_lagged":
            backfill = res
        if len(res) and res.index[-1] == snap.index[-1] and pd.notna(res.iloc[-1]):
            when.append(release + lag)
            vals.append(float(res.iloc[-1]))

    out = pd.Series(vals, index=pd.DatetimeIndex(when), dtype=float)
    out = out[~out.index.duplicated(keep="last")]

    if backfill is not None:
        pre = backfill.dropna()
        pre_lag = release_lag_days if pre_vintage_lag_days is None else pre_vintage_lag_days
        pre.index = pre.index + pd.Timedelta(days=pre_lag)
        pre = pre[pre.index < first_release + lag]
        out = pd.concat([pre, out[~out.index.isin(pre.index)]]).sort_index()

    out.attrs.update(first_vintage=first_release, n_vintages=int(v["release_date"].nunique()),
                     pre_vintage=pre_vintage)
    return out


def unrevised(
    vintages: pd.DataFrame,
    fn: Callable[[pd.Series], pd.Series],
    lag_days: int,
    use_first_release: bool = False,
) -> pd.Series:
    """Latest values, transformed once, available `lag_days` after each period.

    For series in UNREVISED_SERIES. `fn` must be causal. The lag should be an
    upper bound on the publication delay so the result cannot look ahead.

    With use_first_release (series whose first print is final), a period that
    was actually released within 2 x lag_days of its start becomes available on
    its real first-release date; older periods, and any released on the series'
    first vintage date (ALFRED's bulk load), use period + lag_days. This removes the staleness of the conservative lag in the
    vintage era without ever looking ahead.
    """
    s = latest(vintages)
    if s.empty:
        return _empty()
    res = fn(s).dropna()
    avail = pd.Series(res.index + pd.Timedelta(days=lag_days), index=res.index)
    if use_first_release:
        clean = _clean(vintages)
        first = clean.groupby("period_date")["release_date"].min().reindex(res.index)
        # Periods first released on the series' first vintage date were bulk-loaded history,
        # whatever their age: their release date says nothing about publication.
        real = (first > clean["release_date"].min()) & ((first - res.index.to_series()).dt.days <= 2 * lag_days)
        avail = avail.where(~real, first)
    out = pd.Series(res.to_numpy(dtype=float), index=pd.DatetimeIndex(avail.to_numpy()))
    out = out[~out.index.duplicated(keep="last")]
    out.attrs.update(first_vintage=None, n_vintages=0, pre_vintage="unrevised")
    return out


def pit_series(
    vintages: pd.DataFrame,
    fn: Callable[[pd.Series], pd.Series],
    code: str,
    pre_vintage: PreVintage = "drop",
    daily: bool = False,
) -> pd.Series:
    """Dispatch: unrevised() for codes in UNREVISED_SERIES, else realtime().

    Applies RELEASE_LAG_OVERRIDES[code] as the extra lag for realtime(). Codes in
    PRE_VINTAGE_LATEST_LAGGED use their backfilled history (at that lag) before the
    first vintage, whatever `pre_vintage` is.
    """
    if code in PRE_VINTAGE_LATEST_LAGGED:
        return realtime(vintages, fn, release_lag_days=RELEASE_LAG_OVERRIDES.get(code, 0),
                        pre_vintage="latest_lagged", daily=daily, label=code,
                        pre_vintage_lag_days=PRE_VINTAGE_LATEST_LAGGED[code])
    if code in UNREVISED_SERIES:
        return unrevised(vintages, fn, UNREVISED_SERIES[code], use_first_release=code in USE_FIRST_RELEASE_DATE)
    return realtime(vintages, fn, release_lag_days=RELEASE_LAG_OVERRIDES.get(code, 0),
                    pre_vintage=pre_vintage, daily=daily, label=code)


def to_daily(s: pd.Series, index: pd.DatetimeIndex) -> pd.Series:
    """Forward-fill a release-dated series onto `index` (normally business days).

    The value on a day is the latest one released on or before it, so a release
    on a weekend lands on the next business day in `index`. Days before the
    first release are NaN.
    """
    if s.empty:
        return pd.Series(float("nan"), index=index, dtype=float)
    s = s[~s.index.duplicated(keep="last")].sort_index()
    out = s.reindex(s.index.union(index)).ffill().reindex(index)
    out.attrs.update(s.attrs)
    return out


def staleness(s: pd.Series, index: pd.DatetimeIndex) -> pd.Series:
    """Days since the last release as of each date in `index` (NaN before the first)."""
    if s.empty:
        return pd.Series(float("nan"), index=index, dtype=float)
    s = s[~s.index.duplicated(keep="last")].sort_index()
    stamps = pd.Series(s.index, index=s.index)
    last = stamps.reindex(stamps.index.union(index)).ffill().reindex(index)
    return pd.Series((index - pd.DatetimeIndex(last)).days.astype(float), index=index)
