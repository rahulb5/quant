"""
tests/backtest/test_regimes.py

Tests for src/backtest/regimes — pure label functions and Regime subclasses.

In-memory DB tests follow the same fixture pattern as test_reference.py.

Run with: pytest tests/backtest/test_regimes.py -v
"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd
import pytest

from src.backtest.regimes import (
    RatesRegime,
    TrendRegime,
    VolRegime,
    default_regimes,
    expanding_bucket_labels,
    ma_trend_labels,
    threshold_labels,
)
from src.db.client import Database


# ═══════════════════════════════════════════════════════════════════════════════
# Pure label function tests  (no DB)
# ═══════════════════════════════════════════════════════════════════════════════

class TestExpandingBucketLabels:
    """Tests for expanding_bucket_labels."""

    # ── Lookahead-clean property ──────────────────────────────────────────────

    def test_no_lookahead_prefix_identical_to_truncated(self):
        """Labels on the full series must match labels on any prefix."""
        rng = np.random.default_rng(42)
        n = 600
        s = pd.Series(
            rng.normal(0, 1, n),
            index=pd.date_range("2020-01-01", periods=n, freq="B"),
        )
        burn_in = 100
        labels_full = expanding_bucket_labels(s, burn_in=burn_in)
        k = 400
        labels_trunc = expanding_bucket_labels(s.iloc[:k], burn_in=burn_in)

        for i in range(k):
            f = labels_full.iloc[i]
            t = labels_trunc.iloc[i]
            both_nan = (isinstance(f, float) and pd.isna(f)) and \
                       (isinstance(t, float) and pd.isna(t))
            if both_nan:
                continue
            assert f == t, f"Lookahead mismatch at position {i}: full={f!r} trunc={t!r}"

    # ── Burn-in ───────────────────────────────────────────────────────────────

    def test_burn_in_dates_are_nan(self):
        n = 300
        s = pd.Series(range(n), index=pd.date_range("2020-01-01", periods=n))
        result = expanding_bucket_labels(s, burn_in=252)
        # First 251 labels (indices 0..250) must be NaN
        assert result.iloc[:251].isna().all()

    def test_first_post_burn_in_label_is_not_nan(self):
        n = 300
        s = pd.Series(range(n), index=pd.date_range("2020-01-01", periods=n))
        result = expanding_bucket_labels(s, burn_in=252)
        assert not pd.isna(result.iloc[251])

    # ── Increasing series → top bucket ───────────────────────────────────────

    def test_increasing_series_always_hits_top_bucket(self):
        """On a monotonically increasing series the current value is always
        the maximum of the expanding window, placing it in the top bucket."""
        n = 300
        s = pd.Series(
            range(n), index=pd.date_range("2020-01-01", periods=n), dtype=float
        )
        result = expanding_bucket_labels(s, burn_in=10)
        post_burn = result.iloc[10:]
        assert (post_burn == "high").all()

    # ── Label length mismatch raises ─────────────────────────────────────────

    def test_wrong_label_length_raises_value_error(self):
        s = pd.Series([1.0, 2.0, 3.0])
        with pytest.raises(ValueError, match="n_buckets"):
            expanding_bucket_labels(s, n_buckets=3, labels=["a", "b"])

    # ── Values ───────────────────────────────────────────────────────────────

    def test_default_labels_are_low_mid_high(self):
        n = 200
        s = pd.Series(range(n), index=pd.date_range("2020-01-01", periods=n), dtype=float)
        result = expanding_bucket_labels(s, n_buckets=3, burn_in=10)
        valid = result.dropna()
        assert set(valid.unique()).issubset({"low", "mid", "high"})

    def test_custom_labels_used(self):
        n = 200
        s = pd.Series(range(n), index=pd.date_range("2020-01-01", periods=n), dtype=float)
        result = expanding_bucket_labels(s, n_buckets=3, burn_in=10,
                                         labels=["Q1", "Q2", "Q3"])
        valid = result.dropna()
        assert set(valid.unique()).issubset({"Q1", "Q2", "Q3"})

    def test_n_buckets_2(self):
        n = 200
        s = pd.Series(range(n), index=pd.date_range("2020-01-01", periods=n), dtype=float)
        result = expanding_bucket_labels(s, n_buckets=2, burn_in=10,
                                         labels=["bottom", "top"])
        valid = result.dropna()
        assert set(valid.unique()).issubset({"bottom", "top"})

    def test_rolling_window_variant(self):
        n = 300
        s = pd.Series(range(n), index=pd.date_range("2020-01-01", periods=n), dtype=float)
        result = expanding_bucket_labels(s, burn_in=10, window=50)
        # Should still produce non-NaN labels after burn-in
        assert result.iloc[50:].notna().all()

    def test_nan_in_series_propagates(self):
        s = pd.Series([float("nan"), 1.0, 2.0, 3.0, 4.0])
        result = expanding_bucket_labels(s, burn_in=2)
        assert pd.isna(result.iloc[0])

    def test_all_buckets_populated_on_random_series(self):
        rng = np.random.default_rng(7)
        n = 600
        s = pd.Series(rng.normal(0, 1, n), index=pd.date_range("2020-01-01", periods=n))
        result = expanding_bucket_labels(s, burn_in=100)
        valid = result.dropna()
        assert set(valid.unique()) == {"low", "mid", "high"}


class TestThresholdLabels:
    """Tests for threshold_labels."""

    def test_basic_two_bucket(self):
        s = pd.Series([-1.0, -0.5, 0.0, 0.5, 1.0])
        result = threshold_labels(s, [0.0], ["negative", "positive"])
        # < 0 → "negative"; >= 0 → "positive"
        assert result[0] == "negative"
        assert result[1] == "negative"
        assert result[2] == "positive"  # 0.0 is NOT < 0
        assert result[3] == "positive"
        assert result[4] == "positive"

    def test_inverted_yield_curve(self):
        """Standard RatesRegime pattern: T10Y2Y < 0 → 'inverted'."""
        s = pd.Series([-0.5, 0.0, 0.5, 1.0])
        result = threshold_labels(s, [0.0], ["inverted", "normal"])
        assert result[0] == "inverted"
        assert result[1] == "normal"   # exactly 0: normal (>= threshold)
        assert result[2] == "normal"
        assert result[3] == "normal"

    def test_nan_input_gives_nan_output(self):
        s = pd.Series([float("nan"), 1.0, -1.0])
        result = threshold_labels(s, [0.0], ["neg", "pos"])
        assert pd.isna(result[0])
        assert result[1] == "pos"
        assert result[2] == "neg"

    def test_three_bucket(self):
        s = pd.Series([0.5, 1.5, 2.5])
        result = threshold_labels(s, [1.0, 2.0], ["low", "mid", "high"])
        assert result[0] == "low"   # 0.5 < 1.0
        assert result[1] == "mid"   # 1.0 <= 1.5 < 2.0
        assert result[2] == "high"  # 2.5 >= 2.0

    def test_empty_series(self):
        s = pd.Series([], dtype=float)
        result = threshold_labels(s, [0.0], ["neg", "pos"])
        assert result.empty

    def test_all_nan_series(self):
        s = pd.Series([float("nan"), float("nan")])
        result = threshold_labels(s, [0.0], ["neg", "pos"])
        assert result.isna().all()


class TestMaTrendLabels:
    """Tests for ma_trend_labels."""

    def test_pre_window_is_nan(self):
        n = 20
        s = pd.Series(range(n), index=pd.date_range("2020-01-01", periods=n), dtype=float)
        result = ma_trend_labels(s, ma_window=5)
        # First 4 observations (indices 0–3) should be NaN
        assert result.iloc[:4].isna().all()

    def test_first_post_window_not_nan(self):
        n = 20
        s = pd.Series(range(n), index=pd.date_range("2020-01-01", periods=n), dtype=float)
        result = ma_trend_labels(s, ma_window=5)
        assert not pd.isna(result.iloc[4])

    def test_increasing_series_is_above_ma(self):
        """For a strictly increasing series, the current value is always
        strictly greater than the trailing mean of strictly smaller past values."""
        n = 250
        s = pd.Series(
            range(n), index=pd.date_range("2020-01-01", periods=n), dtype=float
        )
        result = ma_trend_labels(s, ma_window=5)
        # After burn-in window, all should be "above"
        assert (result.iloc[5:] == "above").all()

    def test_decreasing_series_is_below_ma(self):
        n = 250
        s = pd.Series(
            range(n, 0, -1),
            index=pd.date_range("2020-01-01", periods=n),
            dtype=float,
        )
        result = ma_trend_labels(s, ma_window=5)
        # After burn-in, current value is smallest in window → below MA
        assert (result.iloc[5:] == "below").all()

    def test_constant_series_is_below_ma(self):
        """Constant value equals the MA → 'below' (series > ma is False)."""
        n = 100
        s = pd.Series(
            [50.0] * n, index=pd.date_range("2020-01-01", periods=n)
        )
        result = ma_trend_labels(s, ma_window=5)
        post = result.dropna()
        assert (post == "below").all()

    def test_custom_labels(self):
        n = 100
        s = pd.Series(range(n), index=pd.date_range("2020-01-01", periods=n), dtype=float)
        result = ma_trend_labels(s, ma_window=5, labels=("risk_off", "risk_on"))
        valid = result.dropna()
        assert set(valid.unique()).issubset({"risk_off", "risk_on"})

    def test_nan_in_series_propagates(self):
        n = 20
        vals = list(range(n))
        vals[10] = float("nan")
        s = pd.Series(vals, index=pd.date_range("2020-01-01", periods=n), dtype=float)
        result = ma_trend_labels(s, ma_window=3)
        # Position 10 should be NaN
        assert pd.isna(result.iloc[10])


# ═══════════════════════════════════════════════════════════════════════════════
# Regime class tests (in-memory DB)
# ═══════════════════════════════════════════════════════════════════════════════

# ── Fixture ───────────────────────────────────────────────────────────────────

_N = 300  # number of trading days to insert
_DATES = pd.date_range("2022-01-03", periods=_N, freq="B")


@pytest.fixture(scope="module")
def mem_db() -> Database:
    """In-memory DB with migrations, test asset, and macro series."""
    database = Database(":memory:")
    database.open()

    # Asset for TrendRegime (S&P 500 proxy)
    database.run(
        "INSERT INTO assets (id, ticker, name, asset_class, currency) VALUES (?, ?, ?, ?, ?)",
        [100001, "^GSPC", "S&P 500", "equity", "USD"],
    )

    # Macro series
    for sid, code, name in [
        (1, "VIXCLS", "CBOE VIX"),
        (2, "T10Y2Y", "10Y-2Y Spread"),
    ]:
        database.run(
            """INSERT INTO macro_series (id, code, name, source, frequency, units)
               VALUES (?, ?, ?, ?, ?, ?)""",
            [sid, code, name, "FRED", "daily", "Index"],
        )

    # Generate values
    rng = np.random.default_rng(99)
    vix_vals = rng.uniform(10, 80, _N)
    # T10Y2Y: mix of negative and positive to cover both buckets
    t10y2y_vals = rng.uniform(-1.5, 2.5, _N)
    # Prices: generally trending up with noise
    prices = np.cumsum(rng.normal(5, 15, _N)) + 2000.0

    for i, d in enumerate(_DATES):
        d_str = d.strftime("%Y-%m-%d")
        # prices (noon UTC to avoid timezone-shift issues)
        database.run(
            """INSERT INTO prices
               (asset_id, interval, timestamp, open, high, low, close, volume, source)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [
                100001, "1d", f"{d_str} 12:00:00+00",
                float(prices[i]), float(prices[i]),
                float(prices[i]), float(prices[i]),
                0.0, "test",
            ],
        )
        # VIX
        database.run(
            """INSERT INTO macro_observations
               (series_id, period_date, release_date, value)
               VALUES (?, ?, ?, ?)""",
            [1, d_str, d_str, float(vix_vals[i])],
        )
        # T10Y2Y
        database.run(
            """INSERT INTO macro_observations
               (series_id, period_date, release_date, value)
               VALUES (?, ?, ?, ?)""",
            [2, d_str, d_str, float(t10y2y_vals[i])],
        )

    yield database
    database.close()


# ── VolRegime ─────────────────────────────────────────────────────────────────

class TestVolRegime:
    def test_labels_non_empty(self, mem_db: Database):
        regime = VolRegime(burn_in=50, database=mem_db)
        lbls = regime.labels()
        assert not lbls.empty

    def test_labels_only_expected_values_or_nan(self, mem_db: Database):
        regime = VolRegime(burn_in=50, database=mem_db)
        lbls = regime.labels()
        valid = lbls.dropna()
        assert set(valid.unique()).issubset({"low_vol", "mid_vol", "high_vol"})

    def test_labels_all_three_buckets_populated(self, mem_db: Database):
        """With random VIX data, all three tercile labels should appear."""
        regime = VolRegime(burn_in=50, database=mem_db)
        lbls = regime.labels()
        valid = lbls.dropna()
        assert set(valid.unique()) == {"low_vol", "mid_vol", "high_vol"}

    def test_labels_pre_burn_in_are_nan(self, mem_db: Database):
        regime = VolRegime(burn_in=100, database=mem_db)
        lbls = regime.labels()
        assert lbls.iloc[:99].isna().all()

    def test_missing_data_returns_empty_series(self, caplog: pytest.LogCaptureFixture):
        empty = Database(":memory:")
        empty.open()
        try:
            with caplog.at_level(logging.WARNING, logger="quant"):
                lbls = VolRegime(database=empty).labels()
            assert isinstance(lbls, pd.Series)
            assert lbls.empty
            assert any(
                "No" in msg and ("data" in msg or "level" in msg)
                for msg in caplog.messages
            )
        finally:
            empty.close()

    def test_name_attribute(self):
        assert VolRegime.name == "volatility"


# ── RatesRegime ───────────────────────────────────────────────────────────────

class TestRatesRegime:
    def test_labels_non_empty(self, mem_db: Database):
        regime = RatesRegime(database=mem_db)
        lbls = regime.labels()
        assert not lbls.empty

    def test_labels_only_expected_values(self, mem_db: Database):
        regime = RatesRegime(database=mem_db)
        lbls = regime.labels()
        valid = lbls.dropna()
        assert set(valid.unique()).issubset({"inverted", "normal"})

    def test_labels_both_buckets_present(self, mem_db: Database):
        """T10Y2Y fixture spans negative and positive → both labels appear."""
        regime = RatesRegime(database=mem_db)
        lbls = regime.labels()
        valid = lbls.dropna()
        assert "inverted" in valid.values
        assert "normal" in valid.values

    def test_no_nan_in_output(self, mem_db: Database):
        """threshold_labels has no burn-in, so no NaN for non-NaN input."""
        regime = RatesRegime(database=mem_db)
        lbls = regime.labels()
        assert not lbls.isna().any()

    def test_missing_data_returns_empty_series(self, caplog: pytest.LogCaptureFixture):
        empty = Database(":memory:")
        empty.open()
        try:
            with caplog.at_level(logging.WARNING, logger="quant"):
                lbls = RatesRegime(database=empty).labels()
            assert lbls.empty
        finally:
            empty.close()

    def test_name_attribute(self):
        assert RatesRegime.name == "rates_curve"


# ── TrendRegime ───────────────────────────────────────────────────────────────

class TestTrendRegime:
    def test_labels_non_empty(self, mem_db: Database):
        regime = TrendRegime(ma_window=50, database=mem_db)
        lbls = regime.labels()
        assert not lbls.empty

    def test_labels_only_expected_values_or_nan(self, mem_db: Database):
        regime = TrendRegime(ma_window=50, database=mem_db)
        lbls = regime.labels()
        valid = lbls.dropna()
        assert set(valid.unique()).issubset({"risk_off", "risk_on"})

    def test_pre_window_is_nan(self, mem_db: Database):
        ma_window = 50
        regime = TrendRegime(ma_window=ma_window, database=mem_db)
        lbls = regime.labels()
        assert lbls.iloc[:ma_window - 1].isna().all()
        assert not pd.isna(lbls.iloc[ma_window - 1])

    def test_missing_data_returns_empty_series(self, caplog: pytest.LogCaptureFixture):
        empty = Database(":memory:")
        empty.open()
        try:
            with caplog.at_level(logging.WARNING, logger="quant"):
                lbls = TrendRegime(database=empty).labels()
            assert lbls.empty
        finally:
            empty.close()

    def test_name_attribute(self):
        assert TrendRegime.name == "equity_trend"


# ── default_regimes ───────────────────────────────────────────────────────────

class TestDefaultRegimes:
    def test_returns_three_regimes(self, mem_db: Database):
        regimes = default_regimes(database=mem_db)
        assert len(regimes) == 3

    def test_expected_names(self, mem_db: Database):
        regimes = default_regimes(database=mem_db)
        names = [r.name for r in regimes]
        assert names == ["volatility", "rates_curve", "equity_trend"]

    def test_regime_types(self, mem_db: Database):
        regimes = default_regimes(database=mem_db)
        assert isinstance(regimes[0], VolRegime)
        assert isinstance(regimes[1], RatesRegime)
        assert isinstance(regimes[2], TrendRegime)

    def test_database_threaded_through(self, mem_db: Database):
        """All regimes should use the injected database, not the singleton."""
        regimes = default_regimes(database=mem_db)
        for r in regimes:
            assert r._db is mem_db

    def test_all_labels_callable_without_exception(self, mem_db: Database):
        for r in default_regimes(database=mem_db, start="2022-01-01"):
            # Use small burn_in for VolRegime by calling with defaults
            lbls = r.labels() if not isinstance(r, VolRegime) else \
                   VolRegime(burn_in=50, database=mem_db).labels()
            assert isinstance(lbls, pd.Series)
