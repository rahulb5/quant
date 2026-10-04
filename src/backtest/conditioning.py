"""
src/backtest/conditioning.py

Conditioner: ties PerformanceReport, ReferenceData, and Regime layers together,
reporting how a strategy's performance varies across calendar years, market
regimes, and factor exposures.

Design notes
------------
Regime slices:
    Regime-conditional return sets are non-contiguous in time — the dates
    belonging to a given label are scattered across years.  Path-dependent
    statistics (max drawdown, Calmar ratio, drawdown duration) are not
    meaningful on non-contiguous series and are intentionally omitted from
    by_regime().  Only contiguity-independent statistics are computed there:
    annualized return/vol, Sharpe, t-stat, and hit rate.

Yearly slices:
    Calendar-year slices are contiguous blocks, so the full PerformanceReport
    metric set — including drawdown statistics — is computed and reported.

Factor statistics:
    Correlation, beta, and regression are ex-post descriptive statistics of the
    realized sample.  Full-sample alignment is correct here; the lookahead
    discipline lives in the regime labels (expanding quantiles, trailing MA),
    not in these factor statistics.

DB lifecycle:
    The caller is responsible for db.open() / db.close() before constructing a
    Conditioner that uses the default reference/regimes (which query the DB).
    Pre-built reference and regimes objects may be injected to avoid DB access.

Alignment rules:
    Pairwise (max data per pair) — correlations(), betas(),
        rolling_correlation(), conditional_performance():
        align strategy with one factor at a time and drop NaN pairs only.
    Joint inner — factor_regression():
        strategy + all factors on common non-NaN dates (multivariate OLS
        requires all variables on the same observations).
        Warn if the joint inner join retains fewer than 60 % of strategy obs.
"""

from __future__ import annotations

import math
from typing import Any

import numpy as np
import pandas as pd
import statsmodels.api as sm

from src.backtest.metrics import (
    annualized_return,
    annualized_volatility,
    hit_rate,
    sharpe_ratio,
    sharpe_tstat,
)
from src.backtest.performance import PerformanceReport
from src.backtest.reference import ReferenceData
from src.backtest.regimes import Regime, default_regimes
from src.shared.utils import logger

_JOINT_COVERAGE_WARN = 0.60  # warn if joint inner join < 60 % of strategy obs
_MIN_REG_OBS = 30             # minimum observations required for OLS


class Conditioner:
    """Align factor data and regime labels to a strategy and report conditional performance.

    Args:
        returns: Daily return series for the strategy (must be pd.Series).
        reference: Pre-loaded ReferenceData instance.  If None, builds
            ``ReferenceData(database=database).load()`` on first use — the
            caller must have called ``db.open()`` before that point.
        regimes: List of Regime objects.  If None, uses
            ``default_regimes(database=database)`` on first use — same DB
            lifecycle note applies.
        rf: Per-period risk-free rate.  Scalar float or pd.Series aligned to
            the strategy's index.  Series values are reindexed to the strategy
            spine and missing dates are filled with 0.0.
        periods_per_year: Trading periods per year used for annualisation.
        database: DB instance injected into ReferenceData / default_regimes
            when those are built lazily.  Ignored if both reference and regimes
            are provided.

    Raises:
        TypeError: If ``returns`` is not a pd.Series.
    """

    def __init__(
        self,
        returns: pd.Series,
        reference: ReferenceData | None = None,
        regimes: list[Regime] | None = None,
        rf: float | pd.Series = 0.0,
        periods_per_year: int = 252,
        database=None,
    ) -> None:
        if not isinstance(returns, pd.Series):
            raise TypeError(
                f"returns must be a pd.Series, got {type(returns).__name__}"
            )

        self._spine: pd.Series = returns.dropna()
        self._ppy = periods_per_year
        self._database = database

        if isinstance(rf, pd.Series):
            self._rf: float | pd.Series = (
                rf.reindex(self._spine.index).fillna(0.0)
            )
        else:
            self._rf = float(rf)

        # Injected or lazily built
        self._reference: ReferenceData | None = reference
        self._regimes: list[Regime] | None = regimes

        # Private caches
        self.__regime_labels: dict[str, pd.Series] | None = None

    # ── Private helpers ───────────────────────────────────────────────────────

    def _rf_for(self, index: pd.Index) -> float | pd.Series:
        """Return the risk-free rate aligned to the given index.

        Args:
            index: DatetimeIndex of the slice to align to.

        Returns:
            The scalar rf if rf is a float; otherwise the rf Series reindexed
            to ``index`` with missing values filled to 0.0.
        """
        if isinstance(self._rf, pd.Series):
            return self._rf.reindex(index).fillna(0.0)
        return self._rf

    def _get_reference(self) -> ReferenceData:
        """Return the ReferenceData instance, building it lazily if needed."""
        if self._reference is None:
            self._reference = ReferenceData(database=self._database).load()
        return self._reference

    def _get_regimes(self) -> list[Regime]:
        """Return the regime list, building it lazily if needed."""
        if self._regimes is None:
            self._regimes = default_regimes(database=self._database)
        return self._regimes

    @property
    def _regime_labels(self) -> dict[str, pd.Series]:
        """Regime labels, each reindexed to the strategy spine (cached).

        Returns:
            Dict mapping regime name to a pd.Series of categorical string
            labels indexed by the strategy dates.  Dates outside the regime's
            history or inside its burn-in period are NaN.  Regimes with no
            underlying data are omitted (warning logged).
        """
        if self.__regime_labels is None:
            result: dict[str, pd.Series] = {}
            for regime in self._get_regimes():
                lbls = regime.labels()
                if lbls.empty:
                    logger.warning(
                        f"[Conditioner] Regime '{regime.name}' returned empty "
                        f"labels — omitting from by_regime()."
                    )
                    continue
                result[regime.name] = lbls.reindex(self._spine.index)
            self.__regime_labels = result
        return self.__regime_labels

    # ── Public methods ────────────────────────────────────────────────────────

    def yearly(self) -> pd.DataFrame:
        """Performance metrics broken down by calendar year.

        Yearly slices are contiguous, so the full PerformanceReport metric
        set — including drawdown statistics — is included.

        Returns:
            DataFrame indexed by year (int), columns:
            total_return, ann_return, ann_vol, sharpe, sortino,
            max_drawdown, calmar, hit_rate, n_periods.
            Empty DataFrame (with column headers) if the spine has no data.
        """
        cols = [
            "total_return", "ann_return", "ann_vol", "sharpe",
            "sortino", "max_drawdown", "calmar", "hit_rate", "n_periods",
        ]
        if self._spine.empty:
            return pd.DataFrame(columns=cols)

        rows: dict[int, dict] = {}
        for year, group in self._spine.groupby(self._spine.index.year):
            rf_slice = self._rf_for(group.index)
            rpt = PerformanceReport(
                group,
                rf=rf_slice,
                periods_per_year=self._ppy,
                name=str(year),
            )
            mets = rpt.metrics()
            rows[int(year)] = {k: mets[k] for k in cols}

        return pd.DataFrame.from_dict(rows, orient="index")[cols]

    def by_regime(self) -> dict[str, pd.DataFrame]:
        """Strategy performance broken down by regime label.

        Regime slices are non-contiguous in time, so path-dependent statistics
        (max drawdown, Calmar, drawdown duration) are intentionally excluded.
        Only contiguity-independent statistics are computed.

        Returns:
            Dict mapping regime name to a DataFrame indexed by label, columns:
            ann_return, ann_vol, sharpe, t_stat, hit_rate, pct_time, n_obs.
            Labels are ordered by chronological first appearance in the strategy
            window.  Regimes with no valid labels after alignment are omitted
            (warning logged).
        """
        cols = [
            "ann_return", "ann_vol", "sharpe", "t_stat",
            "hit_rate", "pct_time", "n_obs",
        ]
        result: dict[str, pd.DataFrame] = {}

        for regime_name, aligned_labels in self._regime_labels.items():
            valid_labels = aligned_labels.dropna()
            if valid_labels.empty:
                logger.warning(
                    f"[Conditioner] Regime '{regime_name}' has no valid labels "
                    f"in the strategy window — omitting."
                )
                continue

            total_labeled = len(valid_labels)

            # Determine label order by chronological first appearance
            seen: dict[str, None] = {}
            for lbl in valid_labels:
                if lbl not in seen:
                    seen[lbl] = None
            label_order = list(seen.keys())

            rows: dict[str, dict] = {}
            for lbl in label_order:
                lbl_dates = valid_labels.index[valid_labels == lbl]
                slice_ret = self._spine.reindex(lbl_dates).dropna()
                n_obs = len(slice_ret)
                pct_time = len(lbl_dates) / total_labeled

                if n_obs < 2:
                    rows[lbl] = {
                        "ann_return": float("nan"),
                        "ann_vol": float("nan"),
                        "sharpe": float("nan"),
                        "t_stat": float("nan"),
                        "hit_rate": float("nan"),
                        "pct_time": pct_time,
                        "n_obs": float(n_obs),
                    }
                    continue

                rf_s = self._rf_for(slice_ret.index)
                rows[lbl] = {
                    "ann_return": annualized_return(slice_ret, self._ppy),
                    "ann_vol": annualized_volatility(slice_ret, self._ppy),
                    "sharpe": sharpe_ratio(slice_ret, rf_s, self._ppy),
                    "t_stat": sharpe_tstat(slice_ret, rf_s),
                    "hit_rate": hit_rate(slice_ret),
                    "pct_time": pct_time,
                    "n_obs": float(n_obs),
                }

            if rows:
                result[regime_name] = pd.DataFrame.from_dict(
                    rows, orient="index"
                )[cols]

        return result

    def correlations(self) -> pd.Series:
        """Pairwise Pearson correlation of the strategy to each factor.

        Uses pairwise alignment — strategy and one factor at a time, dropping
        any dates where either is NaN.

        Returns:
            pd.Series indexed by factor name (name="correlation").
            Empty Series if no factors are available.
        """
        rd = self._get_reference()
        values: dict[str, float] = {}
        for name in rd.available():
            factor_ret = rd.returns(name)
            aligned = pd.concat(
                [self._spine.rename("strat"), factor_ret],
                axis=1, join="inner",
            ).dropna()
            if len(aligned) < 2:
                values[name] = float("nan")
                continue
            values[name] = float(aligned["strat"].corr(aligned[name]))
        return pd.Series(values, name="correlation")

    def betas(self) -> pd.Series:
        """Pairwise univariate beta: cov(strategy, factor) / var(factor).

        Uses pairwise alignment — strategy and one factor at a time, dropping
        any dates where either is NaN.

        Returns:
            pd.Series indexed by factor name (name="beta").
            Empty Series if no factors are available.
        """
        rd = self._get_reference()
        values: dict[str, float] = {}
        for name in rd.available():
            factor_ret = rd.returns(name)
            aligned = pd.concat(
                [self._spine.rename("strat"), factor_ret],
                axis=1, join="inner",
            ).dropna()
            if len(aligned) < 2:
                values[name] = float("nan")
                continue
            var_f = float(np.var(aligned[name], ddof=1))
            if var_f == 0.0:
                values[name] = float("nan")
                continue
            cov = float(np.cov(aligned["strat"], aligned[name], ddof=1)[0, 1])
            values[name] = cov / var_f
        return pd.Series(values, name="beta")

    def factor_regression(
        self, hac_maxlags: int | None = None
    ) -> dict[str, Any]:
        """Multivariate OLS regression of strategy returns on the factor panel.

        Uses joint-inner alignment — all factors and the strategy on their
        common non-NaN dates.  Warns if the inner join retains fewer than 60 %
        of the strategy's observations.

        Two fits are run: standard OLS (for R² and default t-stats) and OLS
        with Newey-West HAC standard errors (for robust t-stats).

        Args:
            hac_maxlags: Maximum lag for the Newey-West HAC covariance.
                Defaults to ``ceil(n ** 0.25)`` where n is the number of
                jointly aligned observations.

        Returns:
            Dict with keys:

            - ``table``:       DataFrame indexed by ["const", *factor_names],
                               columns ["coef", "t_stat", "t_stat_hac"].
            - ``ann_alpha``:   const coef * periods_per_year (float).
            - ``r_squared``:   float.
            - ``n_obs``:       int (number of jointly aligned observations).
            - ``hac_maxlags``: int (lag used for HAC).

            Empty dict (with warning logged) if no factors are available or
            there are too few jointly aligned observations.
        """
        rd = self._get_reference()
        avail = rd.available()
        if not avail:
            logger.warning(
                "[Conditioner.factor_regression] No factors available "
                "— returning {}."
            )
            return {}

        # Joint-inner alignment: strategy + all factors
        pieces = [self._spine.rename("strat")]
        for name in avail:
            pieces.append(rd.returns(name))
        joint = pd.concat(pieces, axis=1, join="inner").dropna()

        n = len(joint)
        n_strat = len(self._spine)
        if n_strat > 0 and n / n_strat < _JOINT_COVERAGE_WARN:
            logger.warning(
                f"[Conditioner.factor_regression] Joint inner join retains "
                f"only {n}/{n_strat} ({n / n_strat:.1%}) strategy observations"
                f" — possible calendar mismatch between series."
            )

        if n < _MIN_REG_OBS:
            logger.warning(
                f"[Conditioner.factor_regression] Only {n} jointly aligned "
                f"observations (need ≥ {_MIN_REG_OBS}) — returning {{}}."
            )
            return {}

        y = joint["strat"].values
        X = sm.add_constant(joint[avail].values, has_constant="add")
        L = hac_maxlags if hac_maxlags is not None else int(math.ceil(n ** 0.25))

        try:
            res = sm.OLS(y, X).fit()
            res_hac = sm.OLS(y, X).fit(
                cov_type="HAC", cov_kwds={"maxlags": L}
            )
        except Exception as exc:
            logger.warning(
                f"[Conditioner.factor_regression] OLS failed: {exc} "
                f"— returning {{}}."
            )
            return {}

        index_labels = ["const"] + avail
        table = pd.DataFrame(
            {
                "coef": res.params,
                "t_stat": res.tvalues,
                "t_stat_hac": res_hac.tvalues,
            },
            index=index_labels,
        )

        return {
            "table": table,
            "ann_alpha": float(res.params[0]) * self._ppy,
            "r_squared": float(res.rsquared),
            "n_obs": int(n),
            "hac_maxlags": L,
        }

    def rolling_correlation(
        self,
        window: int = 252,
        factor: str = "equities",
    ) -> pd.Series:
        """Trailing rolling Pearson correlation of the strategy to a factor.

        Uses pairwise alignment.  Returns NaN for the first ``window - 1``
        points (standard rolling behaviour with min_periods=window).

        Args:
            window: Rolling window length in periods.
            factor: Factor name as loaded by ReferenceData.

        Returns:
            pd.Series indexed by date.  Empty Series (with warning logged) if
            the factor is not available.
        """
        rd = self._get_reference()
        if factor not in rd.available():
            logger.warning(
                f"[Conditioner.rolling_correlation] Factor '{factor}' is not "
                f"available — returning empty Series."
            )
            return pd.Series(dtype=float)

        factor_ret = rd.returns(factor)
        aligned = pd.concat(
            [self._spine.rename("strat"), factor_ret.rename(factor)],
            axis=1, join="inner",
        ).dropna()

        return aligned["strat"].rolling(window).corr(aligned[factor])

    def conditional_performance(self) -> pd.DataFrame:
        """Strategy performance conditional on each factor's same-day sign.

        For each factor, splits strategy returns into two groups:
        dates where the factor return is positive (up) and dates where it is
        negative (down).  Days where the factor return is exactly zero are
        dropped from both groups.

        Returns:
            DataFrame indexed by factor name, columns:
            ann_return_up, ann_return_down, hit_rate_up, hit_rate_down,
            n_up, n_down.
            Empty DataFrame (with column headers) if no factors are available.
        """
        rd = self._get_reference()
        avail = rd.available()
        cols = [
            "ann_return_up", "ann_return_down",
            "hit_rate_up", "hit_rate_down",
            "n_up", "n_down",
        ]
        if not avail:
            return pd.DataFrame(columns=cols)

        def _ann_ret(s: pd.Series) -> float:
            return annualized_return(s, self._ppy) if len(s) >= 2 else float("nan")

        def _hit(s: pd.Series) -> float:
            return hit_rate(s) if len(s) >= 2 else float("nan")

        rows: dict[str, dict] = {}
        for name in avail:
            factor_ret = rd.returns(name)
            aligned = pd.concat(
                [self._spine.rename("strat"), factor_ret.rename(name)],
                axis=1, join="inner",
            ).dropna()

            up = aligned.loc[aligned[name] > 0, "strat"]
            down = aligned.loc[aligned[name] < 0, "strat"]

            rows[name] = {
                "ann_return_up": _ann_ret(up),
                "ann_return_down": _ann_ret(down),
                "hit_rate_up": _hit(up),
                "hit_rate_down": _hit(down),
                "n_up": float(len(up)),
                "n_down": float(len(down)),
            }

        return pd.DataFrame.from_dict(rows, orient="index")[cols]

    def report(self) -> dict[str, Any]:
        """Bundle all analytics into a single dict.

        Returns:
            Dict with keys: yearly, by_regime, correlations, betas,
            factor_regression, conditional_performance, rolling_correlation
            (rolling_correlation uses default window=252, factor="equities").
        """
        return {
            "yearly": self.yearly(),
            "by_regime": self.by_regime(),
            "correlations": self.correlations(),
            "betas": self.betas(),
            "factor_regression": self.factor_regression(),
            "conditional_performance": self.conditional_performance(),
            "rolling_correlation": self.rolling_correlation(),
        }

    def __repr__(self) -> str:
        """Compact text overview of the conditioned strategy.

        Shows full-sample Sharpe, annualised alpha with its HAC t-stat,
        equities beta (from factor regression), and the Sharpe spread across
        the volatility regime's labels.
        """
        rpt = PerformanceReport(
            self._spine, rf=self._rf, periods_per_year=self._ppy
        )
        full_sharpe = rpt.metrics()["sharpe"]

        reg = self.factor_regression()
        if reg and "table" in reg:
            ann_alpha = reg["ann_alpha"]
            tbl = reg["table"]
            hac_t = (
                float(tbl.loc["const", "t_stat_hac"])
                if "const" in tbl.index else float("nan")
            )
            eq_beta = (
                float(tbl.loc["equities", "coef"])
                if "equities" in tbl.index else float("nan")
            )
        else:
            ann_alpha = float("nan")
            hac_t = float("nan")
            eq_beta = float("nan")

        vol_spread_str = "n/a"
        by_reg = self.by_regime()
        if "volatility" in by_reg:
            sh = by_reg["volatility"]["sharpe"].dropna()
            if not sh.empty:
                vol_spread_str = f"{sh.min():.2f} → {sh.max():.2f}"

        def _f(v: float, pct: bool = False) -> str:
            if math.isnan(v):
                return "nan"
            return f"{v * 100:.2f}%" if pct else f"{v:.3f}"

        n = len(self._spine)
        lines = [
            f"Conditioner — {n} obs",
            "─" * 40,
            f"  Full-sample Sharpe    {_f(full_sharpe)}",
            f"  Ann. alpha            {_f(ann_alpha, pct=True)}  (HAC t={_f(hac_t)})",
            f"  Equities beta         {_f(eq_beta)}",
            f"  Sharpe (vol regime)   {vol_spread_str}",
        ]
        return "\n".join(lines)
