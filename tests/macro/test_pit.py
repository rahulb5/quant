"""Tests for src/macro/pit.py: as-of vintage reconstruction on synthetic data."""

import logging

import numpy as np
import pandas as pd
import pytest

from src.db.client import Database
from src.macro.data import load_vintages
from src.macro.pit import (
    RELEASE_LAG_OVERRIDES,
    UNREVISED_SERIES,
    as_of,
    first_vintage_date,
    latest,
    pit_series,
    realtime,
    staleness,
    to_daily,
    unrevised,
)

yoy = lambda s: s.pct_change(12, fill_method=None) * 100  # noqa: E731  (causal)


def _vint(rows):
    return pd.DataFrame(rows, columns=["period_date", "release_date", "value"])


def _monthly(n=36, start="2020-01-01", lag=15, revise=True, seed=0):
    """n monthly periods; each released `lag` days after its start, and (if revise)
    again 30 days later with a small deterministic change, plus the previous
    period revised at each release."""
    rng = np.random.default_rng(seed)
    months = pd.date_range(start, periods=n, freq="MS")
    vals = 100 + np.cumsum(rng.normal(0.3, 0.5, n))
    rows = []
    for i, d in enumerate(months):
        rows.append((d, d + pd.Timedelta(days=lag), vals[i]))
        if revise:
            rows.append((d, d + pd.Timedelta(days=lag + 30), vals[i] + rng.normal(0, 0.3)))
    return _vint(rows)


# ── as_of / latest ────────────────────────────────────────────────────────────

def test_as_of_picks_latest_release_not_after_date():
    v = _vint([
        ("2024-01-01", "2024-02-10", 100.0),
        ("2024-01-01", "2024-03-10", 101.0),   # revision
        ("2024-02-01", "2024-03-10", 102.0),
    ])
    assert as_of(v, "2024-02-09").empty
    s = as_of(v, "2024-02-15")
    assert s.to_dict() == {pd.Timestamp("2024-01-01"): 100.0}
    s = as_of(v, "2024-03-10")                       # release date is inclusive
    assert s.to_dict() == {pd.Timestamp("2024-01-01"): 101.0, pd.Timestamp("2024-02-01"): 102.0}
    assert latest(v).iloc[0] == 101.0


def test_first_vintage_date():
    v = _vint([("2024-01-01", "2024-02-10", 1.0), ("2024-01-01", "2024-01-20", 2.0)])
    assert first_vintage_date(v) == pd.Timestamp("2024-01-20")
    assert first_vintage_date(_vint([])) is None


# ── realtime ──────────────────────────────────────────────────────────────────

def test_realtime_uses_the_revised_base_known_at_the_time():
    # Jan-2022 first printed 100, revised to 105 on the day Jan-2023 (110) is released.
    rows = [(d, d + pd.Timedelta(days=15), 100.0) for d in pd.date_range("2022-01-01", periods=12, freq="MS")]
    rows.append(("2023-01-01", "2023-01-16", 110.0))
    rows.append(("2022-01-01", "2023-01-16", 105.0))
    v = _vint(rows)

    out = realtime(v, yoy)

    stitched = 110 / 100 * 100 - 100                       # first-release stitching: 10%
    assert out.loc["2023-01-16"] == pytest.approx((110 / 105 - 1) * 100)
    assert out.loc["2023-01-16"] != pytest.approx(stitched)


def test_realtime_revision_of_old_period_does_not_rewrite_stored_value():
    v = _vint(
        [(d, d + pd.Timedelta(days=15), 100.0 + i) for i, d in enumerate(pd.date_range("2022-01-01", periods=14, freq="MS"))]
        + [("2022-06-01", "2023-06-01", 999.0)]            # late revision of an old period
    )
    out = realtime(v, yoy)
    before = out.loc[:"2023-05-31"]
    # the value stored at an earlier release date is what was known then
    assert before.loc["2023-02-16"] == pytest.approx((113 / 101 - 1) * 100)
    assert 999.0 not in out.round(6).tolist()


def test_realtime_truncation_invariance():
    v = _monthly(n=48)
    full = realtime(v, yoy)
    for k in ["2021-06-30", "2022-03-15", "2023-01-01"]:
        cut = realtime(v[v["release_date"] <= pd.Timestamp(k)], yoy)
        pd.testing.assert_series_equal(cut, full.loc[:k], check_freq=False, check_names=False)
        assert len(cut) > 0


def test_realtime_without_revisions_equals_plain_transform():
    v = _monthly(n=30, revise=False)
    out = realtime(v, yoy)
    plain = yoy(v.set_index("period_date")["value"]).dropna()
    assert out.round(9).tolist() == plain.round(9).tolist()
    assert (out.index == plain.index + pd.Timedelta(days=15)).all()


def test_realtime_release_lag_shifts_index():
    v = _monthly(n=20, revise=False)
    a = realtime(v, lambda s: s)
    b = realtime(v, lambda s: s, release_lag_days=1)
    assert (b.index - a.index == pd.Timedelta(days=1)).all()


def test_realtime_bulk_history_only_emits_latest_period_when_dropping():
    # 60 periods of history all loaded on 2010-01-05, then one new period per month.
    hist = [(d, "2010-01-05", float(i)) for i, d in enumerate(pd.date_range("2005-01-01", periods=60, freq="MS"))]
    new = [("2010-01-01", "2010-02-15", 61.0)]
    out = realtime(_vint(hist + new), lambda s: s)
    assert list(out.index) == [pd.Timestamp("2010-01-05"), pd.Timestamp("2010-02-15")]
    assert out.attrs["first_vintage"] == pd.Timestamp("2010-01-05")
    assert out.attrs["pre_vintage"] == "drop"


def test_realtime_latest_lagged_backfills_before_first_vintage():
    hist = [(d, "2010-01-05", float(i)) for i, d in enumerate(pd.date_range("2005-01-01", periods=60, freq="MS"))]
    out = realtime(_vint(hist + [("2010-01-01", "2010-02-15", 61.0)]), lambda s: s,
                   release_lag_days=20, pre_vintage="latest_lagged")
    assert out.index.min() == pd.Timestamp("2005-01-21")             # period + lag
    assert (out.index[:-2] < pd.Timestamp("2010-01-05")).all()
    assert out.index.is_monotonic_increasing and out.index.is_unique
    assert out.attrs["pre_vintage"] == "latest_lagged"


def test_realtime_rejects_bad_pre_vintage():
    with pytest.raises(ValueError, match="pre_vintage"):
        realtime(_monthly(6), lambda s: s, pre_vintage="nope")


def test_realtime_skips_when_fn_has_no_value_at_latest_period():
    out = realtime(_monthly(n=14, revise=False), yoy)
    assert out.index.min() == pd.Timestamp("2021-01-16")             # first YoY exists at period 13


def test_realtime_empty_and_missing_columns():
    assert realtime(_vint([]), yoy).empty
    with pytest.raises(ValueError, match="missing columns"):
        realtime(pd.DataFrame({"period_date": [], "value": []}), yoy)


def test_realtime_warns_on_stamped_rows_unless_daily(caplog):
    v = _vint([("2024-01-01", "2024-01-01", 1.0), ("2024-02-01", "2024-03-01", 2.0)])
    with caplog.at_level(logging.WARNING, logger="quant"):
        realtime(v, lambda s: s, label="ICSA")
    assert "ICSA" in caplog.text and "release_date <= period_date" in caplog.text
    caplog.clear()
    with caplog.at_level(logging.WARNING, logger="quant"):
        realtime(v, lambda s: s, daily=True)
    assert caplog.text == ""


# ── unrevised / dispatch ──────────────────────────────────────────────────────

def test_unrevised_uses_latest_values_with_lag():
    v = _monthly(n=30, revise=True)
    out = unrevised(v, yoy, lag_days=62)
    plain = yoy(latest(v)).dropna()
    assert out.round(9).tolist() == plain.round(9).tolist()
    assert (out.index == plain.index + pd.Timedelta(days=62)).all()
    assert out.attrs["pre_vintage"] == "unrevised"


def test_pit_series_dispatch_and_overrides():
    v = _monthly(n=30, revise=False)
    assert "CPILFENS" in UNREVISED_SERIES
    pd.testing.assert_series_equal(pit_series(v, yoy, "CPILFENS"), unrevised(v, yoy, 62), check_names=False)
    pd.testing.assert_series_equal(pit_series(v, yoy, "SOMETHING"), realtime(v, yoy), check_names=False)
    assert RELEASE_LAG_OVERRIDES["BAMLH0A0HYM2"] == 1
    d = _vint([("2024-01-02", "2024-01-02", 3.0)])
    assert pit_series(d, lambda s: s, "BAMLH0A0HYM2", daily=True).index[0] == pd.Timestamp("2024-01-03")


# ── to_daily / staleness ──────────────────────────────────────────────────────

def test_to_daily_weekend_release_lands_on_next_business_day():
    s = pd.Series([1.0, 2.0], index=pd.to_datetime(["2024-03-01", "2024-03-09"]))   # Fri, Sat
    idx = pd.bdate_range("2024-03-01", "2024-03-13")
    d = to_daily(s, idx)
    assert d.loc["2024-03-08"] == 1.0                    # Friday: Saturday's print not yet known
    assert d.loc["2024-03-11"] == 2.0                    # Monday
    assert to_daily(s, pd.bdate_range("2024-02-26", "2024-03-01")).loc["2024-02-28"] != 1.0


def test_to_daily_before_first_release_is_nan_and_empty_input():
    s = pd.Series([1.0], index=pd.to_datetime(["2024-03-05"]))
    d = to_daily(s, pd.bdate_range("2024-03-01", "2024-03-08"))
    assert d.loc[:"2024-03-04"].isna().all() and d.loc["2024-03-05":].eq(1.0).all()
    assert to_daily(_vint([]).set_index("period_date")["value"], pd.bdate_range("2024-03-01", "2024-03-05")).isna().all()


def test_staleness_counts_days_since_last_release():
    s = pd.Series([1.0, 2.0], index=pd.to_datetime(["2024-03-05", "2024-03-12"]))
    idx = pd.bdate_range("2024-03-04", "2024-03-14")
    st = staleness(s, idx)
    assert np.isnan(st.loc["2024-03-04"])
    assert st.loc["2024-03-05"] == 0 and st.loc["2024-03-11"] == 6
    assert st.loc["2024-03-12"] == 0 and st.loc["2024-03-14"] == 2


# ── load_vintages ─────────────────────────────────────────────────────────────

def test_load_vintages_roundtrip(macro_db_path):
    ro = Database(macro_db_path, read_only=True)
    v = load_vintages(ro, "CPI")
    assert len(v) == 240 and list(v.columns) == ["period_date", "release_date", "value"]
    assert v["release_date"].is_monotonic_increasing
    out = realtime(v, yoy)
    assert len(out) > 100
    assert load_vintages(ro, "NOPE").empty
