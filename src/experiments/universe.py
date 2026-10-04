"""
src/experiments/universe.py

AssetUniverse: resolves "which assets, at what native frequency" for one
asset class, and exposes a shared returns_panel() built on top of that.

Adding a new asset class (equities today, crypto once a collector exists)
means writing one subclass that implements asset_ids() and registering it
in UNIVERSE_REGISTRY — load_prices()/returns_panel() are inherited as-is.

Usage:
    from src.db.client import db
    from src.experiments.universe import UNIVERSE_REGISTRY

    db.open()
    universe = UNIVERSE_REGISTRY["commodity"]()
    returns = universe.returns_panel(resample="W-MON")
    db.close()
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import ClassVar

import pandas as pd

from src.db.client import db
from src.signals.cot_commercial_positioning import COMMODITY_MAP


class AssetUniverse(ABC):
    """Base class for one asset class's universe of tradable assets.

    Subclasses only need to implement asset_ids(); load_prices() and
    returns_panel() are shared so frequency is a returns_panel() argument,
    not something each new asset class has to reimplement.
    """

    asset_class: ClassVar[str]
    native_interval: ClassVar[str] = "1d"

    @abstractmethod
    def asset_ids(self) -> dict[str, int]:
        """Human-readable key (ticker/commodity name) -> asset_id."""
        raise NotImplementedError

    def load_prices(self, start: str | None = None, end: str | None = None) -> pd.DataFrame:
        """Query `prices` for this universe's asset_ids at native_interval.

        Returns columns: asset_id, timestamp, close.
        """
        ids = list(self.asset_ids().values())
        if not ids:
            return pd.DataFrame(columns=["asset_id", "timestamp", "close"])

        placeholders = ",".join("?" * len(ids))
        clauses = [f"asset_id IN ({placeholders})", "interval = ?"]
        params: list = [*ids, self.native_interval]
        if start is not None:
            clauses.append("timestamp >= ?")
            params.append(start)
        if end is not None:
            clauses.append("timestamp <= ?")
            params.append(end)

        rows = db.query(
            f"""
            SELECT asset_id, CAST(timestamp AS DATE) AS timestamp, close
            FROM prices
            WHERE {' AND '.join(clauses)}
            ORDER BY asset_id, timestamp
            """,
            params,
        )
        prices = pd.DataFrame(rows, columns=["asset_id", "timestamp", "close"])
        if not prices.empty:
            prices["timestamp"] = pd.to_datetime(prices["timestamp"])
        return prices

    def returns_panel(
        self,
        resample: str | None = None,
        start: str | None = None,
        end: str | None = None,
    ) -> pd.DataFrame:
        """Wide panel: index=date, columns=asset_ids() keys, values=simple returns.

        resample=None uses native frequency; resample='W-MON' (or any pandas
        offset alias) resamples to that frequency's last close first. For
        the commodity universe, resample='W-MON' reproduces the same weekly
        dates src.signals.cot_commercial_positioning.weekly_prices_with_trend()
        produces, since both use plain pandas .resample(...).last().
        """
        prices = self.load_prices(start=start, end=end)
        if prices.empty:
            return pd.DataFrame()

        id_to_key = {v: k for k, v in self.asset_ids().items()}
        wide = prices.pivot(index="timestamp", columns="asset_id", values="close")
        wide = wide.rename(columns=id_to_key)

        if resample is not None:
            wide = wide.resample(resample).last()

        return wide.pct_change().dropna(how="all")


class CommodityUniverse(AssetUniverse):
    """Futures tracked by the COT commercial-positioning signal (COMMODITY_MAP)."""

    asset_class = "commodity"
    native_interval = "1d"

    def asset_ids(self) -> dict[str, int]:
        tickers = [cfg["price_ticker"] for cfg in COMMODITY_MAP.values()]
        placeholders = ",".join("?" * len(tickers))
        rows = db.query(
            f"SELECT id, ticker FROM assets WHERE ticker IN ({placeholders})",
            tickers,
        )
        ticker_to_id = {r["ticker"]: r["id"] for r in rows}
        return {
            name: ticker_to_id[cfg["price_ticker"]]
            for name, cfg in COMMODITY_MAP.items()
            if cfg["price_ticker"] in ticker_to_id
        }


class EquityUniverse(AssetUniverse):
    """Current S&P 500 constituents — asset_class='equity' with id < 100000.

    id < 100000 excludes the fixed equity-index catalog (^GSPC, ^NDX, ...)
    at ids 100001-100015, which share asset_class='equity' but aren't
    constituents.
    """

    asset_class = "equity"
    native_interval = "1d"

    def asset_ids(self) -> dict[str, int]:
        rows = db.query(
            "SELECT id, ticker FROM assets WHERE asset_class = 'equity' AND id < 100000"
        )
        return {r["ticker"]: r["id"] for r in rows}


UNIVERSE_REGISTRY: dict[str, type[AssetUniverse]] = {
    "commodity": CommodityUniverse,
    "equity": EquityUniverse,
}
