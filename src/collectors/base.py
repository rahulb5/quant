"""
src/collectors/base.py

Abstract base class for all data collectors.

Subclasses must implement collect(), which fetches data from an external
source and inserts it into the database. Use run() to execute collect()
with automatic error handling and data_fetch_log auditing.

Usage:
    class YahooCollector(BaseCollector):
        def __init__(self):
            super().__init__(source="yahoo")

        def collect(self, from_date, to_date, asset_id=None, **kwargs) -> int:
            # fetch & insert data here
            return rows_inserted

    collector = YahooCollector()
    collector.run(asset_id=1, interval="1d", from_date="2024-01-01", to_date="2024-01-31")
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Literal

from src.db.client import db
from src.shared.utils import logger

FetchStatus = Literal["success", "partial", "failed"]


class BaseCollector(ABC):
    """Abstract base for all data collectors.

    Provides:
    - shared logger
    - run()        — executes collect() with error handling and fetch logging
    - _log_fetch() — writes an audit row to data_fetch_log
    """

    def __init__(self, source: str) -> None:
        self.source = source
        self.logger = logger

    # ── Public interface ──────────────────────────────────────────────────────

    def run(
        self,
        from_date: str,
        to_date: str,
        asset_id: int | None = None,
        series_id: int | None = None,
        interval: str | None = None,
    ) -> int:
        """Execute collect() with error handling and automatic fetch logging.

        Args:
            from_date:  Start of the fetch range (DATE string, e.g. '2024-01-01').
            to_date:    End of the fetch range (DATE string).
            asset_id:   ID of the asset being collected (prices).
            series_id:  ID of the macro series being collected.
            interval:   Price interval ('1d', '1h', etc.) if applicable.

        Returns:
            Number of rows inserted.

        Raises:
            Re-raises any exception from collect() after logging and auditing it.
        """
        self.logger.info(
            f"[{self.source}] Starting collection {from_date} → {to_date}"
        )
        try:
            rows = self.collect(
                from_date=from_date,
                to_date=to_date,
                asset_id=asset_id,
                series_id=series_id,
                interval=interval,
            )
            self.logger.info(f"[{self.source}] Inserted {rows} row(s)")
            self._log_fetch(
                from_date=from_date,
                to_date=to_date,
                asset_id=asset_id,
                series_id=series_id,
                interval=interval,
                rows_inserted=rows,
                status="success",
            )
            return rows

        except Exception as e:
            self.logger.error(f"[{self.source}] Collection failed: {e}")
            self._log_fetch(
                from_date=from_date,
                to_date=to_date,
                asset_id=asset_id,
                series_id=series_id,
                interval=interval,
                rows_inserted=0,
                status="failed",
                error_msg=str(e),
            )
            raise

    @abstractmethod
    def collect(
        self,
        from_date: str,
        to_date: str,
        asset_id: int | None = None,
        series_id: int | None = None,
        interval: str | None = None,
    ) -> int:
        """Fetch data from the source and insert it into the database.

        Subclasses implement this. Do not call directly — use run() so that
        error handling and fetch logging are applied automatically.

        Returns:
            Number of rows inserted.
        """
        ...

    # ── Private helpers ───────────────────────────────────────────────────────

    def _log_fetch(
        self,
        from_date: str,
        to_date: str,
        rows_inserted: int,
        status: FetchStatus,
        asset_id: int | None = None,
        series_id: int | None = None,
        interval: str | None = None,
        error_msg: str | None = None,
    ) -> None:
        """Write an audit row to data_fetch_log.

        Errors here are logged as warnings so they never mask the original
        collection error.
        """
        try:
            next_id_rows = db.query(
                "SELECT COALESCE(MAX(id), 0) + 1 AS next_id FROM data_fetch_log"
            )
            next_id = int(next_id_rows[0]["next_id"])
            db.run(
                """
                INSERT INTO data_fetch_log
                  (id, asset_id, series_id, interval, fetched_from, fetched_to,
                   source, rows_inserted, status, error_msg)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    next_id,
                    asset_id,
                    series_id,
                    interval,
                    from_date,
                    to_date,
                    self.source,
                    rows_inserted,
                    status,
                    error_msg,
                ],
            )
        except Exception as log_err:
            self.logger.warning(
                f"[{self.source}] Failed to write fetch log: {log_err}"
            )
