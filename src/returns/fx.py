"""
src/returns/fx.py

Daily excess returns of being long each G10 currency against USD, funded in
USD, the way an FX forward or futures position earns:

    r_t = S_t / S_{t-1} - 1 + i_f(t-1) * days / DC_f - i_usd(t-1) * days / 360

S is USD per unit of foreign currency (USDxxx quotes are inverted). Rates are
BIS policy rates on both legs (cash.policy_rate), known at t-1.

Cleaning, applied to spot before returns:
- Start dates: the later of FLOAT_START (AUD floated Dec 1983, NZD Mar 1985,
  the rest after Bretton Woods, Mar 1973) and both legs' rate history. This
  also drops GBPUSD's quarterly pre-1971 rows.
- Weekend rows (Twelve Data has Saturday/Sunday prints from 2024) are dropped,
  so Monday's return runs from Friday.
- Spike-and-revert prints: a price that jumps more than SPIKE_THRESHOLD and
  comes back the next day (net two-day move under half the threshold) is
  replaced by the geometric midpoint of its neighbours. Found in the 2008
  Twelve Data history (EURUSD +17.3% / -13.4% on 2008-12-08/09; EURUSD's 2008
  bad prints all fall on the 8th and its 2008-08-09..25 rows are missing,
  which looks like a day/month swap at the source). Detection
  looks one day ahead, which is fine for repairing bad prints in a stored
  history but means this filter is not a live-trading rule.
- A return whose interval skips more than MAX_GAP_BDAYS weekdays is NaN
  (missing data, e.g. EURUSD 2008-08-09..25), not one multi-day return.

.attrs: "spikes_removed" and "gaps_nulled" (currency -> dates), and "carry_approx" (currency ->
cash.RATE_CAVEATS entries): periods where the policy rate isn't a
money-market rate, so carry is biased. Trend can use those years; a carry
strategy should start after them.

Usage:
    from src.returns.fx import fx_excess_returns
    xr = fx_excess_returns(db)           # columns = EUR, JPY, GBP, ...
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from src.db.client import Database
from src.returns.cash import DAY_COUNT, accrue, caveats_for, policy_rate

# currency -> (asset id, quoted as USDxxx so spot must be inverted)
FX_PAIRS: dict[str, tuple[int, bool]] = {
    "EUR": (200001, False), "JPY": (200002, True), "GBP": (200003, False),
    "AUD": (200004, False), "CAD": (200005, True), "CHF": (200006, True),
    "NZD": (200007, False), "SEK": (200027, True), "NOK": (200028, True),
}
FLOAT_START: dict[str, str] = {"AUD": "1983-12-12", "NZD": "1985-03-04"}
DEFAULT_FLOAT_START = "1973-03-19"
SPIKE_THRESHOLD = 0.04
MAX_GAP_BDAYS = 3        # missing weekdays tolerated inside one return interval


def remove_spikes(price: pd.Series, threshold: float = SPIKE_THRESHOLD) -> tuple[pd.Series, pd.DatetimeIndex]:
    """Repair one-day spike-and-revert prints with the geometric midpoint of the
    neighbouring prices (see quality.remove_return_spikes for why not drop).
    Returns (repaired price, repaired dates)."""
    lr = np.log(price).diff()
    nxt = lr.shift(-1)
    spike = (lr.abs() > threshold) & (nxt.abs() > threshold) & (np.sign(lr) != np.sign(nxt)) \
        & ((lr + nxt).abs() < threshold / 2)
    removed = price.index[spike.fillna(False).to_numpy()]
    fixed = price.copy()
    mid = np.sqrt(price.shift(1) * price.shift(-1))
    fixed[removed] = mid[removed]
    return fixed, removed


def null_long_gaps(r: pd.Series, max_gap_bdays: int = MAX_GAP_BDAYS) -> tuple[pd.Series, pd.DatetimeIndex]:
    """NaN any return whose interval skips more than max_gap_bdays weekdays.

    FX trades every weekday, so such a gap is missing data, not a holiday; a
    return spanning it would be earned entirely by the weight held before it.
    """
    obs = r.dropna()
    prev = obs.index[:-1].values.astype("datetime64[D]")
    cur = obs.index[1:].values.astype("datetime64[D]")
    skipped = np.busday_count(prev, cur) - 1
    long_gap = obs.index[1:][skipped > max_gap_bdays]
    out = r.copy()
    out[long_gap] = np.nan
    return out, long_gap


def _load_spot(db: Database, asset_id: int) -> pd.Series:
    df = db.query_df(
        """
        SELECT timezone('UTC', timestamp)::DATE AS d, close
        FROM prices WHERE asset_id = ? AND interval = '1d' ORDER BY d
        """,
        [asset_id],
    )
    return pd.Series(df["close"].to_numpy(dtype=float), index=pd.DatetimeIndex(pd.to_datetime(df["d"])))


def fx_excess_returns(db: Database, currencies: list[str] | None = None) -> pd.DataFrame:
    """Long-foreign, USD-funded excess returns; columns = currency codes. See module docstring."""
    currencies = list(FX_PAIRS) if currencies is None else currencies
    unknown = [c for c in currencies if c not in FX_PAIRS]
    if unknown:
        raise ValueError(f"No FX pair for {unknown}; expected some of {list(FX_PAIRS)}")

    usd = policy_rate(db, "USD")
    out: dict[str, pd.Series] = {}
    spikes: dict[str, list[pd.Timestamp]] = {}
    gaps: dict[str, list[pd.Timestamp]] = {}
    for ccy in currencies:
        asset_id, inverted = FX_PAIRS[ccy]
        spot = _load_spot(db, asset_id)
        spot = 1.0 / spot if inverted else spot
        foreign = policy_rate(db, ccy)
        start = max(pd.Timestamp(FLOAT_START.get(ccy, DEFAULT_FLOAT_START)),
                    foreign.index[0], usd.index[0])
        spot = spot[(spot.index >= start) & (spot > 0) & (spot.index.dayofweek < 5)]
        spot, removed = remove_spikes(spot)
        spikes[ccy] = list(removed)

        dates = spot.index
        carry = accrue(foreign, dates, DAY_COUNT[ccy]) - accrue(usd, dates, DAY_COUNT["USD"])
        out[ccy], nulled = null_long_gaps(spot.pct_change() + carry)
        gaps[ccy] = list(nulled)

    result = pd.DataFrame(out).sort_index()
    result.attrs = {
        "spikes_removed": spikes,
        "gaps_nulled": gaps,
        "carry_approx": {c: caveats_for(c) for c in currencies if caveats_for(c)},
    }
    return result
