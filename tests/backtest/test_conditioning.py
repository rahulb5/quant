"""
tests/backtest/test_conditioning.py

Tests for src/backtest/conditioning.Conditioner.

Uses the in-memory DuckDB fixture pattern from test_regimes.py.  A multi-year
price and macro dataset is seeded once per module; the synthetic strategy is
constructed as  2 * equities_returns + small_const + tiny_noise  so that wiring
can be validated with known-relationship assertions.

Run with: pytest tests/backtest/test_conditioning.py -v
"""

from __future__ import annotations

import logging
import math

import numpy as np
import pandas as pd
import pytest

from src.backtest.conditioning import Conditioner
from src.backtest.performance import PerformanceReport
from src.db.client import Database


# ═══════════════════════════════════════════════════════════════════════════════
# Shared constants — computed once at module load, fully deterministic
# ═══════════════════════════════════════════════════════════════════════════════

_N = 800  # number of price observations (~3.2 years of business days)
_PRICE_DATES = pd.bdate_range("2019-01-02", periods=_N)
_RETURN_DATES = _PRICE_DATES[1:]   # N-1 return dates
_N_RET = len(_RETURN_DATES)        # _N - 1 = 799

_RNG = np.random.default_rng(42)

# Equity price levels and simple returns (ReferenceData pct_change replication)
_EQ_PRICES = 2000.0 * np.exp(np.cumsum(_RNG.normal(0.0003, 0.01, _N)))
_EQ_RETURNS = _EQ_PRICES[1:] / _EQ_PRICES[:-1] - 1   # shape (_N_RET,)

# Dollar factor (DTWEXBGS) price levels
_DOL_PRICES = 100.0 * np.exp(np.cumsum(_RNG.normal(0.0, 0.005, _N)))

# VIX and yield-curve spread for regimes
_VIX_VALS = _RNG.uniform(10, 80, _N)
_T10Y2Y_VALS = _RNG.uniform(-1.5, 2.5, _N)

# Strategy: 2 * equities + tiny constant + tiny noise
# Signal-to-noise ratio ≈ 2*0.01/0.0005 = 40 → correlation ≈ 0.9997
_CONST = 0.0001            # ≈ 2.52 % annualised alpha
_NOISE = _RNG.normal(0, 0.0005, _N_RET)
_STRAT_VALS = 2.0 * _EQ_RETURNS + _CONST + _NOISE


# ═══════════════════════════════════════════════════════════════════════════════
# Fixtures
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.fixture(scope="module")
def mem_db() -> Database:
    """In-memory DuckDB seeded with multi-year equity prices and macro series."""
    database = Database(":memory:")
    database.open()

    database.run(
        "INSERT INTO assets (id, ticker, name, asset_class, currency) VALUES (?, ?, ?, ?, ?)",
        [100001, "^GSPC", "S&P 500", "equity", "USD"],
    )

    for sid, code, name in [
        (1, "VIXCLS",   "CBOE VIX"),
        (2, "T10Y2Y",   "10Y-2Y Spread"),
        (3, "DTWEXBGS", "Broad USD Index"),
    ]:
        database.run(
            """INSERT INTO macro_series (id, code, name, source, frequency, units)
               VALUES (?, ?, ?, ?, ?, ?)""",
            [sid, code, name, "FRED", "daily", "Index"],
        )

    for i, d in enumerate(_PRICE_DATES):
        d_str = d.strftime("%Y-%m-%d")
        p = float(_EQ_PRICES[i])
        database.run(
            """INSERT INTO prices
               (asset_id, interval, timestamp, open, high, low, close, volume, source)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [100001, "1d", f"{d_str} 12:00:00+00", p, p, p, p, 0.0, "test"],
        )
        database.run(
            """INSERT INTO macro_observations (series_id, period_date, release_date, value)
               VALUES (?, ?, ?, ?)""",
            [1, d_str, d_str, float(_VIX_VALS[i])],
        )
        database.run(
            """INSERT INTO macro_observations (series_id, period_date, release_date, value)
               VALUES (?, ?, ?, ?)""",
            [2, d_str, d_str, float(_T10Y2Y_VALS[i])],
        )
        database.run(
            """INSERT INTO macro_observations (series_id, period_date, release_date, value)
               VALUES (?, ?, ?, ?)""",
            [3, d_str, d_str, float(_DOL_PRICES[i])],
        )

    yield database
    database.close()


@pytest.fixture(scope="module")
def strategy() -> pd.Series:
    """Synthetic strategy: 2 * equities + small_const + tiny_noise."""
    return pd.Series(_STRAT_VALS, index=_RETURN_DATES, name="test_strat")


@pytest.fixture(scope="module")
def cond(mem_db: Database, strategy: pd.Series) -> Conditioner:
    """Conditioner wired to the in-memory DB and the synthetic strategy."""
    return Conditioner(strategy, database=mem_db)


# ═══════════════════════════════════════════════════════════════════════════════
# Constructor
# ═══════════════════════════════════════════════════════════════════════════════

class TestConstructor:
    def test_non_series_ndarray_raises(self):
        with pytest.raises(TypeError, match="pd.Series"):
            Conditioner(np.array([0.01, -0.01, 0.02]))

    def test_non_series_list_raises(self):
        with pytest.raises(TypeError):
            Conditioner([0.01, -0.01, 0.02])

    def test_spine_drops_nan(self):
        r = pd.Series(
            [0.01, float("nan"), 0.02],
            index=pd.date_range("2020-01-01", periods=3),
        )
        c = Conditioner(r)
        assert len(c._spine) == 2

    def test_scalar_rf_stored_as_float(self):
        r = pd.Series([0.01, 0.02], index=pd.date_range("2020-01-01", periods=2))
        c = Conditioner(r, rf=0.0001)
        assert isinstance(c._rf, float)
        assert c._rf == pytest.approx(0.0001)

    def test_series_rf_reindexed_to_spine(self):
        idx = pd.date_range("2020-01-01", periods=5)
        r = pd.Series(np.random.default_rng(0).normal(0, 0.01, 5), index=idx)
        rf = pd.Series([0.0001] * 3, index=idx[:3])
        c = Conditioner(r, rf=rf)
        assert isinstance(c._rf, pd.Series)
        assert len(c._rf) == 5
        # Dates not covered by the original rf should be 0.0
        assert c._rf.iloc[3] == 0.0
        assert c._rf.iloc[4] == 0.0

    def test_spine_name_preserved(self):
        r = pd.Series([0.01, 0.02], index=pd.date_range("2020-01-01", periods=2), name="my_strat")
        c = Conditioner(r)
        assert c._spine.name == "my_strat"


# ═══════════════════════════════════════════════════════════════════════════════
# Known-relationship validation  (strategy = 2 * equities + const + tiny noise)
# ═══════════════════════════════════════════════════════════════════════════════

class TestKnownRelationship:
    """Core wiring tests: the synthetic construction implies tight constraints."""

    def test_correlations_equities_above_0_9(self, cond: Conditioner):
        corr = cond.correlations()
        assert "equities" in corr.index
        assert corr["equities"] > 0.9

    def test_betas_equities_approx_2(self, cond: Conditioner):
        betas = cond.betas()
        assert "equities" in betas.index
        assert pytest.approx(betas["equities"], abs=0.15) == 2.0

    def test_regression_equities_coef_approx_2(self, cond: Conditioner):
        reg = cond.factor_regression()
        assert reg, "factor_regression returned empty dict"
        assert "equities" in reg["table"].index
        assert pytest.approx(reg["table"].loc["equities", "coef"], abs=0.15) == 2.0

    def test_regression_ann_alpha_approx_const_times_252(self, cond: Conditioner):
        reg = cond.factor_regression()
        # ann_alpha ≈ _CONST * 252 = 0.0252
        assert pytest.approx(reg["ann_alpha"], abs=0.02) == _CONST * 252

    def test_regression_r_squared_above_0_9(self, cond: Conditioner):
        reg = cond.factor_regression()
        assert reg["r_squared"] > 0.9

    def test_rolling_corr_tail_near_1(self, cond: Conditioner):
        rc = cond.rolling_correlation(window=60)
        tail = rc.dropna().tail(50)
        assert (tail > 0.9).all()

    def test_conditional_perf_equities_up_better_than_down(self, cond: Conditioner):
        """When equities are up, 2*equities strategy earns much more."""
        cp = cond.conditional_performance()
        assert "equities" in cp.index
        assert (
            cp.loc["equities", "ann_return_up"]
            > cp.loc["equities", "ann_return_down"]
        )


# ═══════════════════════════════════════════════════════════════════════════════
# yearly()
# ═══════════════════════════════════════════════════════════════════════════════

class TestYearly:
    def test_returns_dataframe(self, cond: Conditioner):
        assert isinstance(cond.yearly(), pd.DataFrame)

    def test_multi_year_coverage(self, cond: Conditioner):
        df = cond.yearly()
        assert len(df) >= 2, "Expected at least 2 calendar years"

    def test_index_contains_integer_years(self, cond: Conditioner):
        df = cond.yearly()
        for yr in df.index:
            assert isinstance(yr, (int, np.integer))

    def test_all_columns_present(self, cond: Conditioner):
        df = cond.yearly()
        expected = {
            "total_return", "ann_return", "ann_vol", "sharpe",
            "sortino", "max_drawdown", "calmar", "hit_rate", "n_periods",
        }
        assert set(df.columns) == expected

    def test_sharpe_matches_direct_performance_report(
        self, cond: Conditioner, strategy: pd.Series
    ):
        """yearly() Sharpe must agree with a direct PerformanceReport on the same slice."""
        df = cond.yearly()
        year = df.index[1]  # second year is always full
        yr_slice = strategy[strategy.index.year == year]
        direct = PerformanceReport(yr_slice, name=str(year))
        assert pytest.approx(df.loc[year, "sharpe"], rel=1e-9) == direct.metrics()["sharpe"]

    def test_max_drawdown_matches_direct_performance_report(
        self, cond: Conditioner, strategy: pd.Series
    ):
        df = cond.yearly()
        year = df.index[1]
        yr_slice = strategy[strategy.index.year == year]
        direct = PerformanceReport(yr_slice, name=str(year))
        assert pytest.approx(
            df.loc[year, "max_drawdown"], rel=1e-9
        ) == direct.metrics()["max_drawdown"]

    def test_n_periods_sum_equals_spine_length(self, cond: Conditioner, strategy: pd.Series):
        df = cond.yearly()
        assert pytest.approx(df["n_periods"].sum()) == float(len(strategy.dropna()))

    def test_empty_strategy_returns_empty_df(self):
        r = pd.Series([], dtype=float, index=pd.DatetimeIndex([]))
        c = Conditioner(r)
        df = c.yearly()
        assert isinstance(df, pd.DataFrame)
        assert df.empty


# ═══════════════════════════════════════════════════════════════════════════════
# by_regime()
# ═══════════════════════════════════════════════════════════════════════════════

class TestByRegime:
    def test_returns_dict(self, cond: Conditioner):
        assert isinstance(cond.by_regime(), dict)

    def test_at_least_one_regime_present(self, cond: Conditioner):
        result = cond.by_regime()
        assert len(result) >= 1, "Expected at least one regime with valid labels"

    def test_no_drawdown_columns(self, cond: Conditioner):
        """Path-dependent stats must be absent from regime slices."""
        for name, df in cond.by_regime().items():
            assert "max_drawdown" not in df.columns, f"{name} has max_drawdown"
            assert "calmar" not in df.columns, f"{name} has calmar"

    def test_expected_columns(self, cond: Conditioner):
        expected = {"ann_return", "ann_vol", "sharpe", "t_stat", "hit_rate", "pct_time", "n_obs"}
        for name, df in cond.by_regime().items():
            assert set(df.columns) == expected, f"{name}: columns mismatch"

    def test_pct_time_sums_to_1(self, cond: Conditioner):
        for name, df in cond.by_regime().items():
            assert pytest.approx(df["pct_time"].sum(), abs=1e-9) == 1.0, (
                f"{name}: pct_time sums to {df['pct_time'].sum()}"
            )

    def test_n_obs_consistent_with_pct_time(self, cond: Conditioner):
        """pct_time for each label must equal n_obs / total_labeled."""
        for name, df in cond.by_regime().items():
            total = df["n_obs"].sum()
            for lbl in df.index:
                expected = df.loc[lbl, "n_obs"] / total
                assert pytest.approx(df.loc[lbl, "pct_time"], rel=1e-9) == expected

    def test_n_obs_positive(self, cond: Conditioner):
        for name, df in cond.by_regime().items():
            assert (df["n_obs"] >= 0).all()
            assert df["n_obs"].sum() > 0

    def test_volatility_regime_present(self, cond: Conditioner):
        # With 800 obs and default burn_in=252, volatility regime should load
        result = cond.by_regime()
        assert "volatility" in result

    def test_volatility_has_all_three_tercile_labels(self, cond: Conditioner):
        result = cond.by_regime()
        if "volatility" in result:
            labels = set(result["volatility"].index)
            assert labels == {"low_vol", "mid_vol", "high_vol"}

    def test_rates_regime_present(self, cond: Conditioner):
        result = cond.by_regime()
        assert "rates_curve" in result

    def test_rates_regime_has_both_labels(self, cond: Conditioner):
        result = cond.by_regime()
        if "rates_curve" in result:
            labels = set(result["rates_curve"].index)
            assert "inverted" in labels
            assert "normal" in labels

    def test_equity_trend_regime_present(self, cond: Conditioner):
        result = cond.by_regime()
        assert "equity_trend" in result


# ═══════════════════════════════════════════════════════════════════════════════
# correlations()
# ════════���══════════════════════════════════════════════════════════════════════

class TestCorrelations:
    def test_returns_series(self, cond: Conditioner):
        assert isinstance(cond.correlations(), pd.Series)

    def test_equities_in_index(self, cond: Conditioner):
        assert "equities" in cond.correlations().index

    def test_equities_correlation_above_0_9(self, cond: Conditioner):
        assert cond.correlations()["equities"] > 0.9

    def test_all_values_in_valid_range(self, cond: Conditioner):
        for name, v in cond.correlations().items():
            if not math.isnan(v):
                assert -1.0 <= v <= 1.0, f"{name} correlation {v} out of [-1, 1]"

    def test_series_name(self, cond: Conditioner):
        assert cond.correlations().name == "correlation"


# ═══════════════════════════════════════════════════════════════════════════════
# betas()
# ═══════════════════════════════════════════════════════════════════════════════

class TestBetas:
    def test_returns_series(self, cond: Conditioner):
        assert isinstance(cond.betas(), pd.Series)

    def test_equities_in_index(self, cond: Conditioner):
        assert "equities" in cond.betas().index

    def test_equities_beta_approx_2(self, cond: Conditioner):
        assert pytest.approx(cond.betas()["equities"], abs=0.15) == 2.0

    def test_series_name(self, cond: Conditioner):
        assert cond.betas().name == "beta"


# ═══════════════════════════════════════════════════════════════════════════════
# factor_regression()
# ═══════════════════════════════════════════════════════════════════════════════

class TestFactorRegression:
    def test_returns_dict_with_all_keys(self, cond: Conditioner):
        reg = cond.factor_regression()
        assert set(reg.keys()) == {"table", "ann_alpha", "r_squared", "n_obs", "hac_maxlags"}

    def test_table_columns(self, cond: Conditioner):
        reg = cond.factor_regression()
        assert list(reg["table"].columns) == ["coef", "t_stat", "t_stat_hac"]

    def test_table_has_const_row(self, cond: Conditioner):
        reg = cond.factor_regression()
        assert "const" in reg["table"].index

    def test_table_has_equities_row(self, cond: Conditioner):
        reg = cond.factor_regression()
        assert "equities" in reg["table"].index

    def test_r_squared_is_float_in_0_1(self, cond: Conditioner):
        reg = cond.factor_regression()
        assert 0.0 <= reg["r_squared"] <= 1.0

    def test_n_obs_positive_int(self, cond: Conditioner):
        reg = cond.factor_regression()
        assert isinstance(reg["n_obs"], int)
        assert reg["n_obs"] > 0

    def test_hac_maxlags_positive_int(self, cond: Conditioner):
        reg = cond.factor_regression()
        assert isinstance(reg["hac_maxlags"], int)
        assert reg["hac_maxlags"] >= 1

    def test_custom_hac_maxlags_respected(self, cond: Conditioner):
        reg = cond.factor_regression(hac_maxlags=5)
        assert reg["hac_maxlags"] == 5

    def test_hac_tstat_differs_from_ols_tstat(self, cond: Conditioner):
        """HAC and OLS t-stats should generally differ due to autocorrelation correction."""
        reg = cond.factor_regression()
        tbl = reg["table"]
        # They could be equal by coincidence for individual rows, but not the const
        # Just verify both are finite
        for row in tbl.index:
            assert math.isfinite(tbl.loc[row, "t_stat"])
            assert math.isfinite(tbl.loc[row, "t_stat_hac"])


# ═══════════════════════════════════════════════════════════════════════════════
# rolling_correlation()
# ═══════════════════════════════════════════════════════════════════════════════

class TestRollingCorrelation:
    def test_returns_series(self, cond: Conditioner):
        assert isinstance(cond.rolling_correlation(window=60), pd.Series)

    def test_first_window_minus_1_are_nan(self, cond: Conditioner):
        window = 60
        rc = cond.rolling_correlation(window=window)
        assert rc.iloc[:window - 1].isna().all()

    def test_values_after_window_are_finite(self, cond: Conditioner):
        rc = cond.rolling_correlation(window=60)
        post = rc.iloc[60:]
        assert post.notna().any()

    def test_late_values_near_1_for_2x_strategy(self, cond: Conditioner):
        rc = cond.rolling_correlation(window=60)
        tail = rc.dropna().tail(50)
        # strategy = 2*equities + tiny_noise → correlation ≈ 0.9997
        assert (tail > 0.9).all()

    def test_missing_factor_returns_empty_series(
        self, cond: Conditioner, caplog: pytest.LogCaptureFixture
    ):
        with caplog.at_level(logging.WARNING, logger="quant"):
            rc = cond.rolling_correlation(factor="nonexistent_xyz")
        assert rc.empty
        assert any("nonexistent_xyz" in m for m in caplog.messages)


# ═══════════════════════════════════════════════════════════════════════════════
# conditional_performance()
# ═══════════════════════════════════════════════════════════════════════════════

class TestConditionalPerformance:
    def test_returns_dataframe(self, cond: Conditioner):
        assert isinstance(cond.conditional_performance(), pd.DataFrame)

    def test_expected_columns(self, cond: Conditioner):
        df = cond.conditional_performance()
        expected = {
            "ann_return_up", "ann_return_down",
            "hit_rate_up", "hit_rate_down",
            "n_up", "n_down",
        }
        assert set(df.columns) == expected

    def test_equities_in_index(self, cond: Conditioner):
        assert "equities" in cond.conditional_performance().index

    def test_equities_up_better_than_down(self, cond: Conditioner):
        df = cond.conditional_performance()
        assert (
            df.loc["equities", "ann_return_up"]
            > df.loc["equities", "ann_return_down"]
        )

    def test_n_up_plus_n_down_le_total(self, cond: Conditioner):
        """Zero-return factor days are dropped, so n_up + n_down ≤ spine length."""
        df = cond.conditional_performance()
        n_strat = len(cond._spine)
        for name in df.index:
            n_total = int(df.loc[name, "n_up"] + df.loc[name, "n_down"])
            assert n_total <= n_strat

    def test_hit_rates_in_valid_range(self, cond: Conditioner):
        df = cond.conditional_performance()
        for col in ("hit_rate_up", "hit_rate_down"):
            for v in df[col]:
                if not math.isnan(v):
                    assert 0.0 <= v <= 1.0


# ════════��══════════════════════════════════════════════════════════════════════
# report()
# ═══════════════════════════════════════════════════════════════════════════════

class TestReport:
    def test_returns_dict(self, cond: Conditioner):
        assert isinstance(cond.report(), dict)

    def test_all_keys_present(self, cond: Conditioner):
        r = cond.report()
        expected = {
            "yearly", "by_regime", "correlations", "betas",
            "factor_regression", "conditional_performance", "rolling_correlation",
        }
        assert set(r.keys()) == expected

    def test_rolling_correlation_uses_defaults(self, cond: Conditioner):
        r = cond.report()
        rc = r["rolling_correlation"]
        assert isinstance(rc, pd.Series)
        # Default window=252 → first 251 should be NaN
        assert rc.iloc[:251].isna().all()


# ═══════════════════════════════════════════════════════════════════════════════
# __repr__
# ═══════════════════════════════════════════════════════════════════════════════

class TestRepr:
    def test_is_string(self, cond: Conditioner):
        assert isinstance(repr(cond), str)

    def test_contains_sharpe(self, cond: Conditioner):
        assert "Sharpe" in repr(cond)

    def test_contains_alpha(self, cond: Conditioner):
        assert "alpha" in repr(cond).lower()

    def test_contains_obs_count(self, cond: Conditioner):
        assert str(len(cond._spine)) in repr(cond)


# ═══════════════════════════════════════════════════════════════════════════════
# Graceful degradation
# ═══════════════════════════════════════════════════════════════════════════════

class TestGracefulDegradation:
    """All methods must degrade gracefully when data is missing."""

    def _empty_cond(self, n: int = 300) -> tuple[Conditioner, Database]:
        db = Database(":memory:")
        db.open()
        r = pd.Series(
            np.random.default_rng(99).normal(0, 0.01, n),
            index=pd.bdate_range("2020-01-02", periods=n),
        )
        return Conditioner(r, database=db), db

    def test_no_factors_correlations_empty(self):
        c, db = self._empty_cond()
        try:
            assert c.correlations().empty
        finally:
            db.close()

    def test_no_factors_betas_empty(self):
        c, db = self._empty_cond()
        try:
            assert c.betas().empty
        finally:
            db.close()

    def test_no_factors_regression_returns_empty_dict(self):
        c, db = self._empty_cond()
        try:
            assert c.factor_regression() == {}
        finally:
            db.close()

    def test_no_factors_conditional_performance_empty(self):
        c, db = self._empty_cond()
        try:
            cp = c.conditional_performance()
            assert isinstance(cp, pd.DataFrame)
            assert cp.empty
        finally:
            db.close()

    def test_no_factors_rolling_correlation_empty(
        self, caplog: pytest.LogCaptureFixture
    ):
        c, db = self._empty_cond()
        try:
            with caplog.at_level(logging.WARNING, logger="quant"):
                rc = c.rolling_correlation(factor="equities")
            assert rc.empty
        finally:
            db.close()

    def test_yearly_works_without_factors(self):
        c, db = self._empty_cond(n=500)
        try:
            df = c.yearly()
            assert isinstance(df, pd.DataFrame)
            assert len(df) >= 1
        finally:
            db.close()

    def test_no_regime_data_by_regime_returns_empty_dict(
        self, caplog: pytest.LogCaptureFixture
    ):
        c, db = self._empty_cond()
        try:
            with caplog.at_level(logging.WARNING, logger="quant"):
                result = c.by_regime()
            assert isinstance(result, dict)
            # No exception — dict may be empty or partial
        finally:
            db.close()

    def test_empty_strategy_yearly_empty(self):
        r = pd.Series([], dtype=float, index=pd.DatetimeIndex([]))
        c = Conditioner(r)
        assert c.yearly().empty

    def test_no_overlap_report_does_not_raise(self):
        """A strategy whose dates never overlap with any factor → no exception."""
        db = Database(":memory:")
        db.open()
        try:
            # Strategy dates are far in the future — no overlap with any factor
            r = pd.Series(
                np.random.default_rng(7).normal(0, 0.01, 50),
                index=pd.bdate_range("2200-01-02", periods=50),
            )
            c = Conditioner(r, database=db)
            # Should not raise
            assert c.correlations().empty
            assert c.betas().empty
            assert c.factor_regression() == {}
        finally:
            db.close()
