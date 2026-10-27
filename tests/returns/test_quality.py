"""
tests/returns/test_quality.py

Tests for src/returns/quality.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from src.returns.quality import quality_report, remove_return_spikes

DAYS = pd.bdate_range("2020-01-01", periods=60)


def test_remove_return_spikes_splits_the_move_across_both_days():
    r = pd.Series(0.001, index=DAYS)
    r.iloc[20], r.iloc[21] = -0.26, 0.33
    fixed, removed = remove_return_spikes(r, 0.10)
    assert list(removed) == [DAYS[20]]
    half = np.sqrt(0.74 * 1.33) - 1
    assert fixed.iloc[20] == pytest.approx(half)
    assert fixed.iloc[21] == pytest.approx(half)
    assert (1 + fixed.iloc[20]) * (1 + fixed.iloc[21]) == pytest.approx(0.74 * 1.33)


def test_spike_repair_is_not_truncation_invariant():
    """Documents the one-day look-ahead: only the repaired date differs, nothing earlier."""
    r = pd.Series(0.001, index=DAYS)
    r.iloc[20], r.iloc[21] = -0.26, 0.33
    full, _ = remove_return_spikes(r, 0.10)
    trunc, removed = remove_return_spikes(r.iloc[:21], 0.10)
    assert removed.empty                                   # last observation is never checked
    assert not np.isclose(trunc.iloc[20], full.iloc[20])
    pd.testing.assert_series_equal(trunc.iloc[:20], full.iloc[:20])


def test_remove_return_spikes_keeps_genuine_crash_and_rebound():
    r = pd.Series(0.0, index=DAYS)
    r.iloc[20], r.iloc[21] = -0.12, 0.08         # net -5%: not a revert
    fixed, removed = remove_return_spikes(r, 0.10)
    assert removed.empty
    pd.testing.assert_series_equal(fixed, r)


def test_remove_return_spikes_skips_nan_days():
    r = pd.Series(np.nan, index=DAYS)
    r.iloc[::2] = 0.0
    r.iloc[20], r.iloc[22] = -0.3, 0.42
    _, removed = remove_return_spikes(r, 0.10)
    assert list(removed) == [DAYS[20]]


def test_quality_report_flags():
    rng = np.random.default_rng(0)
    df = pd.DataFrame({"ok": rng.normal(0, 0.01, len(DAYS)),
                       "bad": rng.normal(0, 0.01, len(DAYS))}, index=DAYS)
    df.loc[DAYS[10], "bad"] = 0.5
    df.loc[DAYS[30:40], "bad"] = np.nan
    rep = quality_report(df)
    assert rep.loc["ok", "flags"] == ""
    assert rep.loc["bad", "flags"] == "abs>25%, gap>5bd"
    assert rep.loc["bad", "max_gap_bdays"] == 11
    assert rep.loc["bad", "worst_days"].startswith(f"{DAYS[10].date()} +50.0%")
    assert rep.loc["ok", "n_obs"] == len(DAYS)


def test_quality_report_empty_column():
    rep = quality_report(pd.DataFrame({"x": [np.nan, np.nan]}, index=DAYS[:2]))
    assert rep.loc["x", "flags"] == "no data"
