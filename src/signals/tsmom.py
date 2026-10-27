"""
src/signals/tsmom.py

Time-series momentum (trend), the cross-asset baseline of
docs/plans/cross_asset_strategies.md, Phase 1. Definitions:
docs/research/tsmom_definitions_v1.md (sections 2-3).

Per instrument, on its own trading days:
- signal = mean over lookbacks L of sign(cumulative excess return over the last L observations);
- vol    = annualised EWMA standard deviation of daily excess returns.

Both at close t use returns up to and including t. Sizing (vol scaling,
sleeve allocation, portfolio vol target, weekly rebalance, holiday hold) is a
portfolio-layer concern: src.experiments.portfolio.vol_target_weights().

Input is the returns layer (src.returns.layer.excess_returns), so this signal
inherits its construction and caveats (see that module).

Usage:
    from src.db.client import db
    from src.signals.tsmom import SignalConfig, load_data, calculate_signal

    db.open()
    (returns,) = load_data()
    signal_df = calculate_signal(returns, config=SignalConfig())
    db.close()
"""

from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np
import pandas as pd

from src.db.client import db
from src.returns.layer import ALL_KEYS, excess_returns

ANNUALISATION = 252

DEFAULT_INSTRUMENTS: tuple[str, ...] = (
    "EQ.SPX", "EQ.NDX", "EQ.RUT", "EQ.SX5E", "EQ.DAX", "EQ.NKY",
    "RT.UST2", "RT.UST5", "RT.UST10", "RT.UST30",
    "FX.EUR", "FX.JPY", "FX.GBP", "FX.AUD", "FX.CAD", "FX.CHF",
)


@dataclass(frozen=True)
class SignalConfig:
    """Parameters from tsmom_definitions_v1.md section 2 (standard values, not tuned)."""

    instruments: tuple[str, ...] = DEFAULT_INSTRUMENTS
    lookbacks: tuple[int, ...] = (21, 63, 252)
    vol_com: int = 60
    vol_min_obs: int = 60

    def __post_init__(self) -> None:
        # YAML gives lists; keep the config hashable/frozen.
        object.__setattr__(self, "instruments", tuple(self.instruments))
        object.__setattr__(self, "lookbacks", tuple(int(x) for x in self.lookbacks))
        unknown = [k for k in self.instruments if k not in ALL_KEYS]
        if unknown:
            raise ValueError(f"Unknown instruments {unknown}; see src.returns.layer.ALL_KEYS")
        if not self.lookbacks or min(self.lookbacks) < 1:
            raise ValueError("lookbacks must be positive integers")


# ── Data loading ──────────────────────────────────────────────────────────────

def load_data() -> tuple[pd.DataFrame]:
    """Every returns-layer instrument (the signal selects its universe). Caller manages db.open()."""
    return (excess_returns(db),)


# ── Pure signal construction ──────────────────────────────────────────────────

def trend_signal(r: pd.Series, lookbacks: tuple[int, ...]) -> pd.Series:
    """Mean of sign(cumulative return over the last L observations) across lookbacks.

    `r` holds one instrument's returns on its own trading days (no NaN). NaN
    until the longest lookback is filled.
    """
    log_growth = np.log1p(r)
    signs = [np.sign(log_growth.rolling(L, min_periods=L).sum()) for L in lookbacks]
    return pd.concat(signs, axis=1).mean(axis=1, skipna=False)


def ewma_vol(r: pd.Series, com: int, min_obs: int) -> pd.Series:
    """Annualised EWMA standard deviation (centre of mass `com`), NaN before `min_obs`."""
    return r.ewm(com=com, min_periods=min_obs).std() * np.sqrt(ANNUALISATION)


def sleeve_of(key: str) -> str:
    return key.split(".")[0]


def calculate_signal(returns: pd.DataFrame, config: SignalConfig = SignalConfig()) -> pd.DataFrame:
    """Long-format signal panel on the union calendar.

    Columns: date, instrument, sleeve, signal, vol, ret, trading.
    `trading` is False on an instrument's non-trading days (ret, signal and
    vol are NaN there); the portfolio rule holds weights across them.
    """
    missing = [k for k in config.instruments if k not in returns.columns]
    if missing:
        raise ValueError(f"returns panel lacks instruments {missing}")

    frames = []
    for key in config.instruments:
        r = returns[key].dropna()
        own = pd.DataFrame({
            "signal": trend_signal(r, config.lookbacks),
            "vol": ewma_vol(r, config.vol_com, config.vol_min_obs),
            "ret": r,
        }).reindex(returns.index)
        own["trading"] = own["ret"].notna()
        own["instrument"] = key
        own["sleeve"] = sleeve_of(key)
        own.index.name = "date"
        frames.append(own.reset_index())

    out = pd.concat(frames, ignore_index=True)
    return out[["date", "instrument", "sleeve", "signal", "vol", "ret", "trading"]]
