"""
src/returns/layer.py

The one entry point strategies use for daily excess returns (Phase 0 of
docs/plans/cross_asset_strategies.md). Columns are instrument keys:

    EQ.*  equity index, local-currency excess return   (src/returns/equity.py)
    RT.*  constant-maturity US Treasury excess return  (src/returns/bonds.py)
    FX.*  long foreign currency vs USD, incl. carry    (src/returns/fx.py)

Every column is a daily simple return in excess of cash, NaN on days the
instrument has no observation (each market keeps its own calendar; the next
observation's return covers a holiday). Commodities aren't included yet.

Look-ahead note: a few bad prints are repaired using the next day's price
(attrs "equity.spikes_removed", "fx.spikes_removed"). Returns on those dates
are a repair of history; don't treat them as what a live system saw.

.attrs: "sleeve" (key -> EQ/RT/FX) plus the sleeve modules' attrs, prefixed:
"equity.method", "equity.approx_until", "equity.cash_currency",
"equity.spikes_removed", "fx.spikes_removed", "fx.gaps_nulled", "fx.carry_approx".

Usage:
    from src.db.client import db
    from src.returns.layer import excess_returns

    db.open()
    xr = excess_returns(db)                               # everything
    xr = excess_returns(db, ["EQ.SPX", "RT.UST10", "FX.EUR"], start="1990-01-01")
    db.close()
"""

from __future__ import annotations

import pandas as pd

from src.db.client import Database
from src.returns.bonds import ust_excess_returns
from src.returns.dividends import PreCoverage
from src.returns.equity import equity_excess_returns
from src.returns.fx import FX_PAIRS, fx_excess_returns

EQUITY_KEYS: dict[str, int] = {
    "EQ.SPX": 100001, "EQ.NDX": 100002, "EQ.RUT": 100003, "EQ.SX5E": 100004,
    "EQ.UKX": 100005, "EQ.DAX": 100006, "EQ.NKY": 100009, "EQ.HSI": 100010,
    "EQ.TSX": 100013, "EQ.AS51": 100014,
}
RATES_KEYS: dict[str, int] = {"RT.UST2": 2, "RT.UST5": 5, "RT.UST10": 10, "RT.UST30": 30}
FX_KEYS: dict[str, str] = {f"FX.{c}": c for c in FX_PAIRS}
ALL_KEYS: list[str] = [*EQUITY_KEYS, *RATES_KEYS, *FX_KEYS]


def excess_returns(
    db: Database,
    keys: list[str] | None = None,
    start: str | None = None,
    end: str | None = None,
    pre_coverage: PreCoverage = "drop",
) -> pd.DataFrame:
    """Daily excess returns for `keys` (default ALL_KEYS), optionally clipped to [start, end].

    pre_coverage applies to equity dividends (see dividends.py).
    """
    keys = ALL_KEYS if keys is None else keys
    unknown = [k for k in keys if k not in ALL_KEYS]
    if unknown:
        raise ValueError(f"Unknown instrument keys {unknown}; see layer.ALL_KEYS")

    parts: list[pd.DataFrame] = []
    attrs: dict = {"sleeve": {k: k.split(".")[0] for k in keys}}

    eq = [k for k in keys if k in EQUITY_KEYS]
    if eq:
        df = equity_excess_returns(db, [EQUITY_KEYS[k] for k in eq], pre_coverage=pre_coverage)
        back = {v: k for k, v in EQUITY_KEYS.items()}
        for name, mapping in df.attrs.items():
            attrs[f"equity.{name}"] = {back[i]: v for i, v in mapping.items()}
        parts.append(df.rename(columns=back))

    rt = [k for k in keys if k in RATES_KEYS]
    if rt:
        df = ust_excess_returns(db, tuple(RATES_KEYS[k] for k in rt))
        parts.append(df.rename(columns={v: k for k, v in RATES_KEYS.items()}))

    fx = [k for k in keys if k in FX_KEYS]
    if fx:
        df = fx_excess_returns(db, [FX_KEYS[k] for k in fx])
        for name, mapping in df.attrs.items():
            attrs[f"fx.{name}"] = {f"FX.{c}": v for c, v in mapping.items()}
        parts.append(df.rename(columns=lambda c: f"FX.{c}"))

    out = pd.concat(parts, axis=1, sort=True)[keys]
    out = out.loc[start:end] if (start or end) else out
    out = out.dropna(how="all")
    out.attrs = attrs
    return out
