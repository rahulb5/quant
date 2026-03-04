"""
src/collectors/equity.py

Equity price collector using Yahoo Finance (via yfinance).

Fetches daily OHLCV data for a single asset and inserts it into the
prices table. Also provides helpers for reading S&P 500 constituents
from a GitHub-hosted CSV and ensuring assets exist in the assets table.

Typical workflow:
    from src.db.client import db
    from src.collectors.equity import EquityCollector

    db.open()

    collector = EquityCollector()

    # 1. Resolve (or create) the asset record
    asset_id = collector.ensure_asset("AAPL", "Apple Inc.")

    # 2. Collect daily prices — error handling + fetch logging handled by run()
    rows = collector.run(
        asset_id=asset_id,
        from_date="2024-01-01",
        to_date="2024-12-31",
    )

    db.close()
"""

from __future__ import annotations

from datetime import date, timedelta

import pandas as pd
import yfinance as yf

from src.collectors.base import BaseCollector
from src.db.client import db


class EquityCollector(BaseCollector):
    """Collects daily OHLCV equity prices from Yahoo Finance."""

    def __init__(self) -> None:
        super().__init__(source="yfinance")

    # ── Public helpers ────────────────────────────────────────────────────────

    @staticmethod
    def get_sp500_tickers() -> list[dict[str, str]]:
        """Read the S&P 500 constituent list from a GitHub-hosted CSV.

        Returns:
            List of dicts with 'ticker' and 'name' keys.
            Ticker dots are normalised to hyphens (e.g. BRK.B → BRK-B)
            to match the format yfinance expects.
        """
        url = "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/main/data/constituents.csv"
        df = pd.read_csv(url)
        return [
            {
                "ticker": str(row["Symbol"]).replace(".", "-"),
                "name": str(row["Security"]),
            }
            for _, row in df.iterrows()
        ]

    def ensure_asset(self, ticker: str, name: str) -> int:
        """Return the asset id for ticker, inserting a new row if needed.

        Args:
            ticker: Ticker symbol (e.g. 'AAPL', 'BRK-B').
            name:   Human-readable company name.

        Returns:
            The id of the existing or newly created assets row.
        """
        rows = db.query("SELECT id FROM assets WHERE ticker = ?", [ticker])
        if rows:
            return int(rows[0]["id"])

        next_id = self._next_asset_id()
        db.run(
            """
            INSERT INTO assets (id, ticker, name, asset_class, currency)
            VALUES (?, ?, ?, 'equity', 'USD')
            """,
            [next_id, ticker, name],
        )
        self.logger.debug(f"[{self.source}] Registered asset {ticker} (id={next_id})")
        return next_id

    def collect_batch(
        self,
        assets: list[dict[str, str | int]],
        from_date: str,
        to_date: str,
        chunk_size: int = 100,
    ) -> dict[str, int]:
        """Download prices for multiple tickers using yf.download() in chunks.

        More efficient than calling run() per ticker because yf.download()
        batches the HTTP requests.

        Args:
            assets:     List of dicts with 'ticker' (str) and 'asset_id' (int) keys.
            from_date:  Start date (inclusive), e.g. '2024-01-01'.
            to_date:    End date (inclusive), e.g. '2024-12-31'.
            chunk_size: Max tickers per yf.download() call (default 100).

        Returns:
            Dict mapping ticker -> rows_inserted for every ticker that had data.
            Tickers with no data are omitted from the result.
        """
        ticker_to_id: dict[str, int] = {
            str(a["ticker"]): int(a["asset_id"]) for a in assets
        }
        tickers = list(ticker_to_id.keys())
        results: dict[str, int] = {}

        for chunk_start in range(0, len(tickers), chunk_size):
            chunk = tickers[chunk_start : chunk_start + chunk_size]
            chunk_num = chunk_start // chunk_size + 1
            self.logger.info(
                f"[{self.source}] Batch {chunk_num}: downloading {len(chunk)} tickers "
                f"({from_date} → {to_date})"
            )

            batch_df = self._fetch_ohlcv_batch(chunk, from_date, to_date)
            ticker_dfs = self._split_batch_df(batch_df, chunk)

            for ticker in chunk:
                asset_id = ticker_to_id[ticker]

                if ticker not in ticker_dfs:
                    self.logger.warning(
                        f"[{self.source}] No data for {ticker} ({from_date} → {to_date})"
                    )
                    self._log_fetch(
                        from_date=from_date,
                        to_date=to_date,
                        asset_id=asset_id,
                        interval="1d",
                        rows_inserted=0,
                        status="partial",
                        error_msg="No data returned by yfinance",
                    )
                    continue

                try:
                    rows = self._insert_prices(asset_id, ticker_dfs[ticker])
                    self.logger.debug(
                        f"[{self.source}] {ticker}: inserted {rows} row(s)"
                    )
                    self._log_fetch(
                        from_date=from_date,
                        to_date=to_date,
                        asset_id=asset_id,
                        interval="1d",
                        rows_inserted=rows,
                        status="success",
                    )
                    results[ticker] = rows
                except Exception as e:
                    self.logger.error(
                        f"[{self.source}] {ticker}: insert failed: {e}"
                    )
                    self._log_fetch(
                        from_date=from_date,
                        to_date=to_date,
                        asset_id=asset_id,
                        interval="1d",
                        rows_inserted=0,
                        status="failed",
                        error_msg=str(e),
                    )

        return results

    def update(self, asset_id: int) -> int:
        """Fetch prices from the day after the last stored date up to today.

        Args:
            asset_id: Row id in the assets table.

        Returns:
            Number of new rows inserted, or 0 if already up to date.
        """
        last_date = self._get_last_price_date(asset_id)
        from_date = (date.fromisoformat(last_date) + timedelta(days=1)).isoformat()
        to_date = date.today().isoformat()

        if from_date >= to_date:
            self.logger.info(
                f"[{self.source}] asset_id={asset_id} already up to date"
            )
            return 0

        return self.run(asset_id=asset_id, from_date=from_date, to_date=to_date)

    def update_batch(
        self, assets: list[dict[str, str | int]]
    ) -> dict[str, int]:
        """Call update() for each asset, tolerating per-ticker failures.

        Args:
            assets: List of dicts with 'ticker' (str) and 'asset_id' (int) keys.

        Returns:
            Dict mapping ticker -> rows_inserted for every ticker that succeeded.
            Tickers that raised an exception are omitted from the result.
        """
        results: dict[str, int] = {}

        for asset in assets:
            ticker = str(asset["ticker"])
            asset_id = int(asset["asset_id"])
            try:
                results[ticker] = self.update(asset_id=asset_id)
            except Exception as e:
                self.logger.error(
                    f"[{self.source}] {ticker}: update failed: {e}"
                )

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
        """Fetch daily OHLCV for asset_id and insert into the prices table.

        Args:
            from_date: Start of the fetch window (inclusive), e.g. '2024-01-01'.
            to_date:   End of the fetch window (inclusive), e.g. '2024-12-31'.
            asset_id:  Row id in the assets table. Required.

        Returns:
            Number of new rows inserted (duplicates are silently skipped).

        Raises:
            ValueError: If asset_id is not provided or not found in the DB.
        """
        if asset_id is None:
            raise ValueError("asset_id is required for EquityCollector.collect()")

        ticker = self._get_ticker(asset_id)
        df = self._fetch_ohlcv(ticker, from_date, to_date)

        if df.empty:
            self.logger.warning(
                f"[{self.source}] No data returned for {ticker} "
                f"({from_date} → {to_date})"
            )
            return 0

        return self._insert_prices(asset_id, df)

    # ── Private helpers ───────────────────────────────────────────────────────

    def _get_ticker(self, asset_id: int) -> str:
        """Look up the ticker symbol for an asset_id."""
        rows = db.query("SELECT ticker FROM assets WHERE id = ?", [asset_id])
        if not rows:
            raise ValueError(f"No asset found with id={asset_id}")
        return str(rows[0]["ticker"])

    def _fetch_ohlcv(self, ticker: str, from_date: str, to_date: str) -> pd.DataFrame:
        """Download daily OHLCV from Yahoo Finance.

        yfinance's end parameter is exclusive, so we add one day to to_date
        to ensure the requested end date is included in the results.

        Returns an empty DataFrame if yfinance returns no data.
        """
        end = (date.fromisoformat(to_date) + timedelta(days=1)).isoformat()
        self.logger.debug(
            f"[{self.source}] Downloading {ticker} {from_date} → {to_date}"
        )
        df = yf.Ticker(ticker).history(
            start=from_date,
            end=end,
            interval="1d",
            auto_adjust=True,   # Close is adjusted for splits and dividends
            actions=False,      # Drop Dividends / Stock Splits columns
        )
        return df

    def _fetch_ohlcv_batch(
        self, tickers: list[str], from_date: str, to_date: str
    ) -> pd.DataFrame:
        """Download daily OHLCV for multiple tickers in one yf.download() call.

        Returns a DataFrame with MultiIndex columns (ticker, field), or an
        empty DataFrame if yfinance returns nothing.
        """
        end = (date.fromisoformat(to_date) + timedelta(days=1)).isoformat()
        return yf.download(
            tickers=tickers,
            start=from_date,
            end=end,
            interval="1d",
            auto_adjust=True,
            actions=False,
            group_by="ticker",
            progress=False,
        )

    def _split_batch_df(
        self, df: pd.DataFrame, tickers: list[str]
    ) -> dict[str, pd.DataFrame]:
        """Extract per-ticker DataFrames from a yf.download() batch result.

        Handles two shapes yfinance may return:
        - MultiIndex columns (ticker, field) — the normal multi-ticker case.
        - Flat columns (field) — when only one ticker was requested.

        Tickers with entirely empty data are excluded from the result.
        """
        if df.empty:
            return {}

        if isinstance(df.columns, pd.MultiIndex):
            available = set(df.columns.get_level_values(0))
            result: dict[str, pd.DataFrame] = {}
            for ticker in tickers:
                if ticker not in available:
                    continue
                ticker_df = df[ticker].dropna(how="all")
                if not ticker_df.empty:
                    result[ticker] = ticker_df
            return result

        # Single-ticker fallback: flat DataFrame
        if len(tickers) == 1:
            ticker_df = df.dropna(how="all")
            return {tickers[0]: ticker_df} if not ticker_df.empty else {}

        return {}

    def _insert_prices(self, asset_id: int, df: pd.DataFrame) -> int:
        """Insert OHLCV rows into the prices table inside a single transaction.

        Rows that already exist (matching the composite PK: asset_id, interval,
        timestamp) are silently skipped via ON CONFLICT DO NOTHING.

        Returns the number of new rows inserted.
        """
        inserted = 0

        def steps(q) -> None:
            nonlocal inserted
            for ts, row in df.iterrows():
                # ts is a timezone-aware Timestamp; convert to ISO string for storage
                timestamp = ts.isoformat()
                q(
                    """
                    INSERT INTO prices
                      (asset_id, interval, timestamp,
                       open, high, low, close, volume,
                       adj_close, source)
                    VALUES (?, '1d', ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT DO NOTHING
                    """,
                    [
                        asset_id,
                        timestamp,
                        float(row["Open"]),
                        float(row["High"]),
                        float(row["Low"]),
                        float(row["Close"]),
                        float(row["Volume"]),
                        float(row["Close"]),  # auto_adjust=True: Close == Adj Close
                        self.source,
                    ],
                )
                inserted += 1

        db.transaction(steps)
        return inserted

    def _next_asset_id(self) -> int:
        """Return the next available id for the assets table."""
        rows = db.query("SELECT COALESCE(MAX(id), 0) + 1 AS next_id FROM assets")
        return int(rows[0]["next_id"])

    def _get_last_price_date(self, asset_id: int) -> str:
        """Return the most recent price date for asset_id at the '1d' interval.

        Returns '2000-01-01' if no rows exist yet, so that a subsequent
        update() call fetches the full history from that fallback date.
        """
        rows = db.query(
            """
            SELECT MAX(timestamp)::DATE AS last_date
            FROM prices
            WHERE asset_id = ? AND interval = '1d'
            """,
            [asset_id],
        )
        last = rows[0]["last_date"] if rows else None
        if last is None:
            return "2000-01-01"
        # DuckDB may return a date object or a string
        return last.isoformat() if hasattr(last, "isoformat") else str(last)[:10]
