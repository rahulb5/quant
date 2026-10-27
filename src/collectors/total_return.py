"""
src/collectors/total_return.py

Total-return sources for the equity indices, from Yahoo Finance:

- Total-return indices (^SP500TR, ^RUTTR) in the equity_index block.
- Distributing ETFs in local currency (block "etf", 500xxx), whose dividends
  give each price index a dividend yield (src/returns/dividends.py).

Prices are fetched with auto_adjust=False (`close` is split-adjusted only),
and each ex-date's cash dividend goes to the `dividends` table. Yahoo's
Adj Close is stored as given but is not used: it's wrong for some non-US
listings (ISF.L applies pence dividends as pounds; 1321.T mixes split bases
and goes negative), while the raw dividend amounts are clean.

On a split, Yahoo restates past closes and dividends, so rows fetched at
different times can sit on different bases. Every fetch therefore pulls the
full history and overwrites (ON CONFLICT DO UPDATE); there's no incremental
mode. The catalogue is ten tickers, so this stays fast.

Dates follow the equity-index convention: the exchange's local trading date
stored as midnight America/New_York.

Usage:
    from src.db.client import db
    from src.collectors.total_return import TotalReturnCollector

    db.open()
    TotalReturnCollector().fetch_all()
    db.close()
"""

from __future__ import annotations

from datetime import date

import pandas as pd
import yfinance as yf

from src.collectors.base import BaseCollector
from src.db.client import db

STORAGE_TZ = "America/New_York"
FULL_HISTORY_START = "1980-01-01"

# Fixed ids. Equity-index block for TR indices, "etf" block for ETFs.
# 500005 (1321.T) is retired: its Yahoo dividends aren't split-adjusted.
TOTAL_RETURN_SOURCES: list[dict] = [
    {"id": 100016, "ticker": "^SP500TR", "name": "S&P 500 Total Return",      "currency": "USD"},
    {"id": 100017, "ticker": "^RUTTR",   "name": "Russell 2000 Total Return", "currency": "USD"},
    {"id": 500001, "ticker": "SPY",      "name": "SPDR S&P 500 ETF (validation vs ^SP500TR)", "currency": "USD"},
    {"id": 500002, "ticker": "QQQ",      "name": "Invesco QQQ (Nasdaq 100)",                  "currency": "USD"},
    {"id": 500003, "ticker": "EXW1.DE",  "name": "iShares Core EURO STOXX 50 (DE), distributing", "currency": "EUR"},
    {"id": 500004, "ticker": "ISF.L",    "name": "iShares Core FTSE 100, distributing",       "currency": "GBp"},
    {"id": 500006, "ticker": "XIU.TO",   "name": "iShares S&P/TSX 60 (TSX Composite proxy)",  "currency": "CAD"},
    {"id": 500007, "ticker": "STW.AX",   "name": "SPDR S&P/ASX 200 Fund",                     "currency": "AUD"},
    {"id": 500008, "ticker": "2800.HK",  "name": "Tracker Fund of Hong Kong (Hang Seng)",     "currency": "HKD"},
    {"id": 500009, "ticker": "1329.T",   "name": "iShares Core Nikkei 225 ETF",               "currency": "JPY"},
]


class TotalReturnCollector(BaseCollector):
    """Collects total-return indices and distributing ETFs (prices + dividends)."""

    def __init__(self) -> None:
        super().__init__(source="yfinance")

    # ── Public interface ──────────────────────────────────────────────────────

    def ensure_assets(self) -> None:
        """Register every catalogue entry at its fixed id (no-op if present)."""
        for a in TOTAL_RETURN_SOURCES:
            if db.query("SELECT 1 FROM assets WHERE id = ?", [a["id"]]):
                continue
            db.run(
                """
                INSERT INTO assets (id, ticker, name, asset_class, currency)
                VALUES (?, ?, ?, 'equity', ?)
                """,
                [a["id"], a["ticker"], a["name"], a["currency"]],
            )

    def fetch_all(self, tickers: list[str] | None = None) -> dict[str, int]:
        """Full-history refresh of the catalogue, tolerating per-ticker failures.

        Returns:
            Dict mapping ticker -> price rows upserted for every ticker that succeeded.
        """
        self.ensure_assets()
        wanted = [a for a in TOTAL_RETURN_SOURCES if tickers is None or a["ticker"] in tickers]
        today = date.today().isoformat()
        results: dict[str, int] = {}
        for a in wanted:
            try:
                results[a["ticker"]] = self.run(
                    from_date=FULL_HISTORY_START, to_date=today, asset_id=a["id"], interval="1d"
                )
            except Exception:
                continue  # run() already logged + audited the failure
        return results

    # ── BaseCollector implementation ──────────────────────────────────────────

    def collect(
        self,
        from_date: str,
        to_date: str,
        asset_id: int | None = None,
        series_id: int | None = None,
        interval: str | None = None,
    ) -> int:
        if asset_id is None:
            raise ValueError("asset_id is required for TotalReturnCollector.collect()")
        rows = db.query("SELECT ticker FROM assets WHERE id = ?", [asset_id])
        if not rows:
            raise ValueError(f"No asset found with id={asset_id}")
        raw = yf.Ticker(str(rows[0]["ticker"])).history(
            start=from_date, interval="1d", auto_adjust=False, actions=True
        )
        px = to_price_rows(raw)
        if px.empty:
            return 0
        px.insert(0, "asset_id", asset_id)
        px["source"] = self.source
        divs = to_dividend_rows(raw)
        divs.insert(0, "asset_id", asset_id)
        divs["source"] = self.source

        def steps(q) -> None:
            q("DELETE FROM dividends WHERE asset_id = ?", [asset_id])   # full refresh, see module docstring
            for rec in divs.itertuples(index=False):
                q("INSERT INTO dividends (asset_id, ex_date, amount, source) VALUES (?, ?, ?, ?)",
                  [rec.asset_id, rec.ex_date, rec.amount, rec.source])

        db.run_df(
            """
            INSERT INTO prices
              (asset_id, interval, timestamp, open, high, low, close, volume, adj_close, source)
            SELECT asset_id, '1d', timestamp, open, high, low, close, volume, adj_close, source
            FROM px
            ON CONFLICT (asset_id, interval, timestamp) DO UPDATE SET
                open = excluded.open, high = excluded.high, low = excluded.low,
                close = excluded.close, volume = excluded.volume,
                adj_close = excluded.adj_close, source = excluded.source
            """,
            px,
            name="px",
        )
        db.transaction(steps)
        return len(px)


def _local_dates(idx: pd.DatetimeIndex) -> pd.DatetimeIndex:
    return (idx.tz_localize(None) if idx.tz is not None else idx).normalize()


def to_price_rows(raw: pd.DataFrame) -> pd.DataFrame:
    """yfinance history (auto_adjust=False) -> prices-table columns.

    Indices have no 'Adj Close'; close is used. Rows with a missing close are dropped.
    """
    if raw.empty:
        return pd.DataFrame()
    adj = raw["Adj Close"] if "Adj Close" in raw else raw["Close"]
    df = pd.DataFrame({
        "timestamp": _local_dates(raw.index).tz_localize(STORAGE_TZ),
        "open": raw["Open"].to_numpy(dtype=float),
        "high": raw["High"].to_numpy(dtype=float),
        "low": raw["Low"].to_numpy(dtype=float),
        "close": raw["Close"].to_numpy(dtype=float),
        "volume": raw["Volume"].fillna(0).to_numpy(dtype=float),
        "adj_close": adj.to_numpy(dtype=float),
    })
    df = df.dropna(subset=["close"])
    df["adj_close"] = df["adj_close"].fillna(df["close"])
    for col in ("open", "high", "low"):
        df[col] = df[col].fillna(df["close"])
    return df.drop_duplicates("timestamp", keep="last").reset_index(drop=True)


def to_dividend_rows(raw: pd.DataFrame) -> pd.DataFrame:
    """yfinance history (actions=True) -> columns ex_date (date), amount; dividends > 0 only."""
    if raw.empty or "Dividends" not in raw:
        return pd.DataFrame(columns=["ex_date", "amount"])
    d = raw["Dividends"]
    d = d[d > 0]
    df = pd.DataFrame({"ex_date": _local_dates(d.index).date, "amount": d.to_numpy(dtype=float)})
    return df.groupby("ex_date", as_index=False)["amount"].sum()
