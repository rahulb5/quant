"""
src/returns/equity.py

Daily excess returns of the equity indices in local currency: total return
(dividends.load_index_total_returns) minus the index currency's cash rate,
accrued over the same interval. That's approximately what a local index
futures position earns, before converting its P&L to USD.

One-day spike-and-revert bad prints in the total-return series are repaired
(geometric midpoint, uses the next day's price) before subtracting cash (quality.remove_return_spikes, 10% threshold); found:
^RUTTR on 2015-10-20. Listed in .attrs["spikes_removed"].

HKD has no BIS series in the catalogue; it has been pegged to USD since 1983,
so HSI uses USD cash (CASH_PROXY).

Earliest dates follow the total-return sources: S&P 500 1988, Russell 2000
1995, Nasdaq 100 2000, DAX 1988, the other ETF-based indices ~2009 (earlier
with pre_coverage="hold_first", approximate and marked in .attrs).

Usage:
    from src.returns.equity import equity_excess_returns
    xr = equity_excess_returns(db)       # columns = price index ids
"""

from __future__ import annotations

import pandas as pd

from src.db.client import Database
from src.returns.cash import DAY_COUNT, accrue, cash_rate
from src.returns.dividends import INDEX_TOTAL_RETURN, PreCoverage, load_index_total_returns
from src.returns.quality import remove_return_spikes

SPIKE_THRESHOLD = 0.10   # equity one-day spike-and-revert; see quality.remove_return_spikes

INDEX_CURRENCY: dict[int, str] = {
    100001: "USD", 100002: "USD", 100003: "USD", 100004: "EUR", 100005: "GBP",
    100006: "EUR", 100009: "JPY", 100010: "HKD", 100013: "CAD", 100014: "AUD",
}
CASH_PROXY: dict[str, str] = {"HKD": "USD"}


def equity_excess_returns(
    db: Database,
    index_ids: list[int] | None = None,
    pre_coverage: PreCoverage = "drop",
) -> pd.DataFrame:
    """Local-currency excess returns, columns = price index ids.

    .attrs carries the total-return attrs ("method", "approx_until") plus
    "cash_currency": id -> currency whose cash rate was subtracted.
    """
    index_ids = list(INDEX_TOTAL_RETURN) if index_ids is None else index_ids
    missing = [i for i in index_ids if i not in INDEX_CURRENCY]
    if missing:
        raise ValueError(f"No currency mapping for index ids {missing}")

    tr = load_index_total_returns(db, index_ids, pre_coverage=pre_coverage)
    rates: dict[str, pd.Series] = {}
    out, cash_ccy, spikes = {}, {}, {}
    for i in index_ids:
        repaired, removed = remove_return_spikes(tr[i], SPIKE_THRESHOLD)
        tr[i] = repaired
        spikes[i] = list(removed)
        ccy = CASH_PROXY.get(INDEX_CURRENCY[i], INDEX_CURRENCY[i])
        if ccy not in rates:
            rates[ccy] = cash_rate(db, ccy)
        # Accrue over the index's own return intervals (its trading dates); the
        # first one has no prior date, so its excess return is NaN.
        cash = accrue(rates[ccy], tr[i].dropna().index, DAY_COUNT[ccy])
        out[i] = tr[i] - cash.reindex(tr.index)
        cash_ccy[i] = ccy

    result = pd.DataFrame(out)
    result.attrs = {**tr.attrs, "cash_currency": cash_ccy, "spikes_removed": spikes}
    return result
