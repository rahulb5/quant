"""
src/backtest/metrics.py

Pure, stateless functions for computing performance metrics on daily return series.
"""

from __future__ import annotations

import numpy as np
import pandas as pd


# ── Internal helper ───────────────────────────────────────────────────────────

def _to_arr(returns) -> np.ndarray:
    return np.asarray(returns, dtype=float)


# ── Return metrics ────────────────────────────────────────────────────────────

def total_return(returns) -> float:
    """Compound total return over the full period.

    Args:
        returns: Array-like of per-period returns.

    Returns:
        prod(1 + r) - 1, or nan if fewer than 2 observations.
    """
    r = _to_arr(returns)
    if len(r) < 2:
        return float("nan")
    return float(np.prod(1 + r) - 1)


def cagr(returns, periods_per_year: int = 252) -> float:
    """Compound annual growth rate.

    Args:
        returns: Array-like of per-period returns.
        periods_per_year: Number of periods in a year.

    Returns:
        (1 + total_return) ** (periods_per_year / n) - 1, or nan if fewer than 2 observations.
    """
    r = _to_arr(returns)
    if len(r) < 2:
        return float("nan")
    tr = total_return(r)
    return float((1 + tr) ** (periods_per_year / len(r)) - 1)


def annualized_return(returns, periods_per_year: int = 252) -> float:
    """Arithmetic annualized return.

    Args:
        returns: Array-like of per-period returns.
        periods_per_year: Number of periods in a year.

    Returns:
        mean(r) * periods_per_year, or nan if fewer than 2 observations.
    """
    r = _to_arr(returns)
    if len(r) < 2:
        return float("nan")
    return float(np.mean(r) * periods_per_year)


def annualized_volatility(returns, periods_per_year: int = 252) -> float:
    """Annualized standard deviation of returns.

    Args:
        returns: Array-like of per-period returns.
        periods_per_year: Number of periods in a year.

    Returns:
        std(r, ddof=1) * sqrt(periods_per_year), or nan if fewer than 2 observations.
    """
    r = _to_arr(returns)
    if len(r) < 2:
        return float("nan")
    return float(np.std(r, ddof=1) * np.sqrt(periods_per_year))


# ── Risk-adjusted metrics ─────────────────────────────────────────────────────

def sharpe_ratio(
    returns,
    rf: float | np.ndarray = 0.0,
    periods_per_year: int = 252,
) -> float:
    """Annualized Sharpe ratio.

    Args:
        returns: Array-like of per-period returns.
        rf: Per-period risk-free rate (scalar or array-like).
        periods_per_year: Number of periods in a year.

    Returns:
        (mean(excess) / std(excess, ddof=1)) * sqrt(periods_per_year),
        or nan if fewer than 2 observations or zero excess volatility.
    """
    r = _to_arr(returns)
    if len(r) < 2:
        return float("nan")
    excess = r - rf
    if float(np.ptp(excess)) == 0.0:
        return float("nan")
    std = float(np.std(excess, ddof=1))
    if std == 0.0:
        return float("nan")
    return float(np.mean(excess) / std * np.sqrt(periods_per_year))


def sortino_ratio(
    returns,
    rf: float | np.ndarray = 0.0,
    mar: float = 0.0,
    periods_per_year: int = 252,
) -> float:
    """Annualized Sortino ratio.

    Downside deviation is computed over all N periods (not just negative ones),
    using the minimum acceptable return (mar) as the threshold.

    Args:
        returns: Array-like of per-period returns.
        rf: Per-period risk-free rate (scalar or array-like).
        mar: Minimum acceptable return per period.
        periods_per_year: Number of periods in a year.

    Returns:
        (mean(excess) / downside_dev) * sqrt(periods_per_year),
        or nan if fewer than 2 observations or zero downside deviation.
    """
    r = _to_arr(returns)
    if len(r) < 2:
        return float("nan")
    excess = r - rf
    downside = np.minimum(excess - mar, 0.0)
    downside_dev = float(np.sqrt(np.mean(downside ** 2)))
    if downside_dev == 0.0:
        return float("nan")
    return float(np.mean(excess) / downside_dev * np.sqrt(periods_per_year))


def calmar_ratio(returns, periods_per_year: int = 252) -> float:
    """Calmar ratio: CAGR divided by absolute maximum drawdown.

    Args:
        returns: Array-like of per-period returns.
        periods_per_year: Number of periods in a year.

    Returns:
        cagr / abs(max_drawdown), or nan if max_drawdown == 0 or fewer than 2 observations.
    """
    r = _to_arr(returns)
    if len(r) < 2:
        return float("nan")
    c = cagr(r, periods_per_year)
    mdd = max_drawdown(r)
    if mdd == 0.0 or (mdd != mdd):  # 0 or nan
        return float("nan")
    return float(c / abs(mdd))


def sharpe_tstat(returns, rf: float | np.ndarray = 0.0) -> float:
    """T-statistic of the mean excess return.

    Note: equals the periodic Sharpe × √n.

    Args:
        returns: Array-like of per-period returns.
        rf: Per-period risk-free rate (scalar or array-like).

    Returns:
        mean(excess) / (std(excess, ddof=1) / sqrt(n)),
        or nan if fewer than 2 observations or zero excess volatility.
    """
    r = _to_arr(returns)
    if len(r) < 2:
        return float("nan")
    excess = r - rf
    if float(np.ptp(excess)) == 0.0:
        return float("nan")
    std = float(np.std(excess, ddof=1))
    if std == 0.0:
        return float("nan")
    return float(np.mean(excess) / (std / np.sqrt(len(r))))


# ── Drawdown ──────────────────────────────────────────────────────────────────

def drawdown_series(returns):
    """Per-period drawdown from the running equity peak.

    Args:
        returns: Array-like of per-period returns. If a pd.Series is passed
            the output preserves the index.

    Returns:
        equity / running_max - 1 (values ≤ 0). Returns a pd.Series if input
        is a pd.Series, otherwise a numpy array.
    """
    r = _to_arr(returns)
    equity = np.cumprod(1 + r)
    running_max = np.maximum.accumulate(equity)
    dd = equity / running_max - 1
    if isinstance(returns, pd.Series):
        return pd.Series(dd, index=returns.index)
    return dd


def max_drawdown(returns) -> float:
    """Maximum (peak-to-trough) drawdown.

    Args:
        returns: Array-like of per-period returns.

    Returns:
        min(drawdown_series), a negative float, or 0.0 if never underwater,
        or nan if fewer than 2 observations.
    """
    r = _to_arr(returns)
    if len(r) < 2:
        return float("nan")
    return float(np.min(drawdown_series(r)))


def max_drawdown_duration(returns) -> int:
    """Longest consecutive run of periods where the equity is below its running peak.

    Args:
        returns: Array-like of per-period returns.

    Returns:
        Maximum underwater stretch in periods (int). Returns 0 if never underwater
        or fewer than 2 observations.
    """
    r = _to_arr(returns)
    if len(r) < 2:
        return 0
    equity = np.cumprod(1 + r)
    running_max = np.maximum.accumulate(equity)
    underwater = equity < running_max
    max_dur = 0
    current = 0
    for u in underwater:
        if u:
            current += 1
            if current > max_dur:
                max_dur = current
        else:
            current = 0
    return max_dur


# ── Distribution metrics ──────────────────────────────────────────────────────

def skewness(returns) -> float:
    """Sample skewness of the return distribution.

    Args:
        returns: Array-like of per-period returns.

    Returns:
        pd.Series(r).skew(), or nan if fewer than 2 observations.
    """
    r = _to_arr(returns)
    if len(r) < 2:
        return float("nan")
    return float(pd.Series(r).skew())


def kurtosis(returns) -> float:
    """Excess (Fisher) kurtosis of the return distribution.

    Args:
        returns: Array-like of per-period returns.

    Returns:
        pd.Series(r).kurt() (excess kurtosis, normal = 0),
        or nan if fewer than 2 observations.
    """
    r = _to_arr(returns)
    if len(r) < 2:
        return float("nan")
    return float(pd.Series(r).kurt())


# ── Trade statistics ──────────────────────────────────────────────────────────

def hit_rate(returns) -> float:
    """Fraction of periods with strictly positive returns.

    Zeros are not counted as wins.

    Args:
        returns: Array-like of per-period returns.

    Returns:
        mean(r > 0), or nan if fewer than 2 observations.
    """
    r = _to_arr(returns)
    if len(r) < 2:
        return float("nan")
    return float(np.mean(r > 0))


def win_loss_ratio(returns) -> float:
    """Average winning return divided by the absolute average losing return.

    Args:
        returns: Array-like of per-period returns.

    Returns:
        mean(r[r > 0]) / abs(mean(r[r < 0])), or nan if no losing periods
        or fewer than 2 observations.
    """
    r = _to_arr(returns)
    if len(r) < 2:
        return float("nan")
    wins = r[r > 0]
    losses = r[r < 0]
    if len(losses) == 0:
        return float("nan")
    if len(wins) == 0:
        return float("nan")
    return float(np.mean(wins) / abs(np.mean(losses)))


# ── Tail-risk metrics ─────────────────────────────────────────────────────────

def value_at_risk(returns, level: float = 0.95) -> float:
    """Historical Value at Risk at the given confidence level.

    Args:
        returns: Array-like of per-period returns.
        level: Confidence level (e.g. 0.95 for 95% VaR).

    Returns:
        np.quantile(r, 1 - level) — the loss-side return (negative),
        or nan if fewer than 2 observations.
    """
    r = _to_arr(returns)
    if len(r) < 2:
        return float("nan")
    return float(np.quantile(r, 1.0 - level))


def conditional_value_at_risk(returns, level: float = 0.95) -> float:
    """Expected Shortfall (CVaR) at the given confidence level.

    Args:
        returns: Array-like of per-period returns.
        level: Confidence level (e.g. 0.95 for 95% CVaR).

    Returns:
        mean(r[r <= VaR]), or nan if no observations fall at or below VaR
        or fewer than 2 observations.
    """
    r = _to_arr(returns)
    if len(r) < 2:
        return float("nan")
    var = value_at_risk(r, level)
    tail = r[r <= var]
    if len(tail) == 0:
        return float("nan")
    return float(np.mean(tail))


# ── Period extremes ───────────────────────────────────────────────────────────

def best_period(returns) -> float:
    """Maximum single-period return.

    Args:
        returns: Array-like of per-period returns.

    Returns:
        max(r), or nan if fewer than 2 observations.
    """
    r = _to_arr(returns)
    if len(r) < 2:
        return float("nan")
    return float(np.max(r))


def worst_period(returns) -> float:
    """Minimum single-period return.

    Args:
        returns: Array-like of per-period returns.

    Returns:
        min(r), or nan if fewer than 2 observations.
    """
    r = _to_arr(returns)
    if len(r) < 2:
        return float("nan")
    return float(np.min(r))
