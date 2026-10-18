"""
src/backtest/stats.py

Statistical-significance helpers for robustness testing: stateless functions
on a per-period return series, same style as metrics.py.

    block_bootstrap_sharpe_ci   -- CI on the annualised Sharpe that respects autocorrelation
    deflated_sharpe_ratio       -- Bailey & Lopez de Prado (2014) multiple-testing haircut
    sharpe_excluding_top        -- Sharpe after dropping the best fraction of periods
    sharpe_excluding_tails      -- Sharpe after dropping the best AND worst fraction of periods
    gaussian_top_trim_shift     -- Sharpe lost to one-sided trimming even with no outliers at all
    sharpe_excluding_best_year  -- Sharpe after dropping the best calendar year
"""

from __future__ import annotations

import math
from statistics import NormalDist

import numpy as np
import pandas as pd

from src.backtest.metrics import sharpe_ratio

_EULER_MASCHERONI = 0.5772156649015329
_NORMAL = NormalDist()


def block_bootstrap_sharpe_ci(
    returns: pd.Series,
    periods_per_year: int = 252,
    n_boot: int = 2000,
    block_len: int | None = None,
    alpha: float = 0.05,
    seed: int = 0,
) -> tuple[float, float]:
    """Circular block-bootstrap (1 - alpha) confidence interval for the annualised Sharpe.

    Blocks of consecutive returns are resampled so serial dependence in the
    series survives. block_len defaults to round(sqrt(n)). Returns (nan, nan)
    for fewer than 10 observations.
    """
    r = np.asarray(returns, dtype=float)
    r = r[~np.isnan(r)]
    n = len(r)
    if n < 10:
        return (float("nan"), float("nan"))

    b = block_len or max(1, int(round(math.sqrt(n))))
    n_blocks = math.ceil(n / b)
    rng = np.random.default_rng(seed)
    offsets = np.arange(b)

    sharpes = np.empty(n_boot)
    chunk = 250
    for lo in range(0, n_boot, chunk):
        hi = min(lo + chunk, n_boot)
        starts = rng.integers(0, n, size=(hi - lo, n_blocks))
        idx = (starts[:, :, None] + offsets[None, None, :]) % n
        sample = r[idx.reshape(hi - lo, -1)[:, :n]]
        mean = sample.mean(axis=1)
        std = sample.std(axis=1, ddof=1)
        with np.errstate(divide="ignore", invalid="ignore"):
            sharpes[lo:hi] = np.where(std > 0, mean / std * math.sqrt(periods_per_year), np.nan)

    sharpes = sharpes[~np.isnan(sharpes)]
    if len(sharpes) == 0:
        return (float("nan"), float("nan"))
    return (
        float(np.quantile(sharpes, alpha / 2)),
        float(np.quantile(sharpes, 1 - alpha / 2)),
    )


def deflated_sharpe_ratio(
    returns: pd.Series,
    n_trials: int = 1,
    sharpe_variance: float | None = None,
) -> float:
    """Deflated Sharpe ratio: P(true Sharpe > 0) after correcting for selection among n_trials.

    Bailey & Lopez de Prado (2014). All Sharpes here are per-period (not
    annualised). The benchmark SR0 is the expected maximum Sharpe of n_trials
    skill-less strategies. With n_trials=1 this reduces to the probabilistic
    Sharpe ratio against zero.

    sharpe_variance is the cross-trial variance of per-period Sharpe estimates;
    we rarely have the other trials' returns, so it defaults to 1/(T-1), the
    sampling variance of a single zero-skill estimate. Pass an explicit value
    if you have a better one.

    Returns nan for fewer than 3 observations or zero volatility.
    """
    r = np.asarray(returns, dtype=float)
    r = r[~np.isnan(r)]
    t = len(r)
    if t < 3:
        return float("nan")
    std = float(np.std(r, ddof=1))
    if std == 0.0:
        return float("nan")

    sr = float(np.mean(r)) / std
    centred = (r - np.mean(r)) / std
    skew = float(np.mean(centred**3))
    kurt = float(np.mean(centred**4))  # Pearson (non-excess)

    if n_trials <= 1:
        sr0 = 0.0
    else:
        var = sharpe_variance if sharpe_variance is not None else 1.0 / (t - 1)
        sr0 = math.sqrt(var) * (
            (1 - _EULER_MASCHERONI) * _NORMAL.inv_cdf(1 - 1 / n_trials)
            + _EULER_MASCHERONI * _NORMAL.inv_cdf(1 - 1 / (n_trials * math.e))
        )

    denom_sq = 1 - skew * sr + (kurt - 1) / 4 * sr**2
    if denom_sq <= 0:
        return float("nan")
    z = (sr - sr0) * math.sqrt(t - 1) / math.sqrt(denom_sq)
    return float(_NORMAL.cdf(z))


def sharpe_excluding_top(returns: pd.Series, frac: float, periods_per_year: int = 252) -> float:
    """Annualised Sharpe after removing the best `frac` of periods (at least one)."""
    r = returns.dropna()
    n_drop = max(1, int(round(frac * len(r))))
    if len(r) - n_drop < 2:
        return float("nan")
    kept = r.sort_values().iloc[:-n_drop]
    return sharpe_ratio(kept.to_numpy(), periods_per_year=periods_per_year)


def sharpe_excluding_tails(returns: pd.Series, frac: float, periods_per_year: int = 252) -> float:
    """Annualised Sharpe after removing the best and the worst `frac` of periods (at least one each).

    Unlike sharpe_excluding_top, this leaves the Sharpe of a symmetric return
    distribution essentially unchanged, so a drop is evidence of tail dependence
    rather than an artifact of trimming.
    """
    r = returns.dropna()
    k = max(1, int(round(frac * len(r))))
    if len(r) - 2 * k < 2:
        return float("nan")
    kept = r.sort_values().iloc[k:-k]
    return sharpe_ratio(kept.to_numpy(), periods_per_year=periods_per_year)


def gaussian_top_trim_shift(frac: float, periods_per_year: int = 252) -> float:
    """Annualised Sharpe that dropping the best `frac` of periods removes from pure Gaussian returns.

    One-sided trimming lowers the mean even when nothing unusual is going on:
    the shift is phi(z)/(1 - frac) per-period standard deviations, with
    z = Phi^-1(1 - frac). Subtract it from the base Sharpe to get the
    no-outliers expectation for sharpe_excluding_top. Ignores the small
    change in volatility.
    """
    z = _NORMAL.inv_cdf(1 - frac)
    pdf = math.exp(-z * z / 2) / math.sqrt(2 * math.pi)
    return pdf / (1 - frac) * math.sqrt(periods_per_year)


def sharpe_excluding_best_year(
    returns: pd.Series, periods_per_year: int = 252
) -> tuple[float, int | None]:
    """Annualised Sharpe after removing the calendar year with the highest compounded return.

    Returns (sharpe, year_removed); (nan, None) if the series spans fewer than two years.
    """
    r = returns.dropna()
    years = r.index.year
    if len(set(years)) < 2:
        return (float("nan"), None)
    yearly = (1 + r).groupby(years).prod() - 1
    best = int(yearly.idxmax())
    kept = r[years != best]
    return (sharpe_ratio(kept.to_numpy(), periods_per_year=periods_per_year), best)
