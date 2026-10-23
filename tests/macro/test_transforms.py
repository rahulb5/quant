"""Unit tests for src/macro/transforms.py."""

import numpy as np
import pandas as pd
import pytest

from src.macro.transforms import (
    frequency_label, history_years, periods_per_year, rolling_zscore, transform,
)


def _series(freq: str, n: int, values=None) -> pd.Series:
    idx = pd.date_range("2010-01-01", periods=n, freq=freq)
    return pd.Series(np.arange(1, n + 1, dtype=float) if values is None else values, index=idx)


@pytest.mark.parametrize("freq,ppy,label", [
    ("B", 252, "daily"), ("W-FRI", 52, "weekly"), ("MS", 12, "monthly"),
    ("QS", 4, "quarterly"), ("YS", 1, "annual"),
])
def test_periods_per_year_inferred_from_dates(freq, ppy, label):
    s = _series(freq, 40)
    assert periods_per_year(s.index) == ppy
    assert frequency_label(s.index) == label


def test_history_years():
    s = _series("MS", 121)
    assert history_years(s.index) == pytest.approx(10.0, abs=0.01)


def test_level_and_one_period_changes():
    s = _series("MS", 5, [100.0, 110.0, 99.0, 99.0, 108.9])
    assert transform(s, "level").equals(s)
    assert transform(s, "diff").tolist() == pytest.approx([10.0, -11.0, 0.0, 9.9])
    assert transform(s, "pct").tolist() == pytest.approx([10.0, -10.0, 0.0, 10.0])


def test_yoy_uses_calendar_year_not_row_count():
    # Monthly series with a missing month: a 12-row shift would be wrong.
    s = _series("MS", 30, np.arange(30, dtype=float) + 100)
    s = s.drop(s.index[5])
    yoy = transform(s, "yoy_diff")
    assert yoy.index[0] == pd.Timestamp("2011-01-01")
    # 2011-06's year-ago month is missing, so it falls back to 2010-05 (stale by one month).
    assert yoy.loc["2011-06-01"] == 13.0
    assert (yoy.drop(pd.Timestamp("2011-06-01")) == 12.0).all()
    assert transform(s, "yoy_pct").iloc[0] == pytest.approx(12.0)


def test_unknown_transform_raises():
    with pytest.raises(ValueError):
        transform(_series("MS", 5), "log")


def test_rolling_zscore_matches_manual_and_is_trailing():
    rng = np.random.default_rng(0)
    s = _series("MS", 60, rng.normal(size=60))
    z = rolling_zscore(s, window=12)
    assert z.index[0] == s.index[11]
    w = s.iloc[48:60]
    assert z.iloc[-1] == pytest.approx((w.iloc[-1] - w.mean()) / w.std())
    # Changing a future value must not change past z-scores.
    s2 = s.copy()
    s2.iloc[-1] = 99.0
    assert rolling_zscore(s2, 12).iloc[:-1].equals(z.iloc[:-1])


def test_rolling_zscore_flat_window_is_dropped():
    z = rolling_zscore(_series("MS", 10, np.ones(10)), window=5)
    assert z.empty


def test_rolling_zscore_rejects_tiny_window():
    with pytest.raises(ValueError):
        rolling_zscore(_series("MS", 10), window=1)
