"""
src/db/client.py

DuckDB connection manager.
- Opens (or creates) the database file
- Runs pending migrations on startup
- Exports a singleton `db` for use across the app

Usage:
    from src.db.client import db

    db.open()
    rows = db.query("SELECT * FROM assets WHERE is_active = true")
    db.close()
"""

from __future__ import annotations

import duckdb
from pathlib import Path
from typing import Any, Callable

from src.db.migrations import MIGRATIONS
from src.shared.config import config
from src.shared.utils import logger


class Database:
    def __init__(self, path: str) -> None:
        self.path = path
        self._con: duckdb.DuckDBPyConnection | None = None

    # ── Connect & migrate ────────────────────────────────────────────────────

    def open(self) -> None:
        if self._con is not None:
            return

        if self.path != ":memory:":
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)

        logger.info(f"Opening DuckDB at {self.path}")
        self._con = duckdb.connect(self.path)
        self._run_migrations()

    # ── Query helpers ────────────────────────────────────────────────────────

    def query(self, sql: str, params: list[Any] | None = None) -> list[dict[str, Any]]:
        """Run a SELECT and return all rows as plain dicts."""
        self._assert_open()
        result = self._con.execute(sql, params or [])  # type: ignore[union-attr]
        cols = [d[0] for d in result.description]
        return [dict(zip(cols, row)) for row in result.fetchall()]

    def run(self, sql: str, params: list[Any] | None = None) -> int:
        """Run an INSERT/UPDATE/DELETE and return rows changed."""
        self._assert_open()
        result = self._con.execute(sql, params or [])  # type: ignore[union-attr]
        return result.rowcount

    def transaction(self, fn: Callable[[Callable[..., None]], None]) -> None:
        """Run multiple statements in a single transaction.

        Example:
            db.transaction(lambda q: (
                q("INSERT INTO assets ..."),
                q("INSERT INTO prices ..."),
            ))
        """
        self._assert_open()
        self._con.execute("BEGIN")  # type: ignore[union-attr]
        try:
            def q(sql: str, params: list[Any] | None = None) -> None:
                self._con.execute(sql, params or [])  # type: ignore[union-attr]

            fn(q)
            self._con.execute("COMMIT")  # type: ignore[union-attr]
        except Exception:
            self._con.execute("ROLLBACK")  # type: ignore[union-attr]
            raise

    def close(self) -> None:
        if self._con is not None:
            self._con.close()
            self._con = None
            logger.info("DuckDB connection closed")

    # ── Migrations ───────────────────────────────────────────────────────────

    def _run_migrations(self) -> None:
        # Bootstrap: apply migration 1 directly (no version table yet)
        self._con.execute(MIGRATIONS[0].sql)  # type: ignore[union-attr]

        current = self._get_current_version()
        pending = [m for m in MIGRATIONS if m.version > current]

        if not pending:
            logger.info(f"DB schema up to date (version {current})")
            return

        logger.info(f"Applying {len(pending)} migration(s) from version {current}")
        for migration in pending:
            self._apply_migration(migration)

        logger.info(f"DB schema migrated to version {MIGRATIONS[-1].version}")

    def _get_current_version(self) -> int:
        try:
            rows = self.query("SELECT MAX(version) AS version FROM schema_version")
            return rows[0]["version"] or 0
        except Exception:
            return 0

    def _apply_migration(self, migration: Any) -> None:
        logger.debug(f"Applying migration {migration.version}: {migration.description}")
        try:
            def run_steps(q: Callable[..., None]) -> None:
                q(migration.sql)
                q(
                    "INSERT INTO schema_version (version, description) VALUES (?, ?)",
                    [migration.version, migration.description],
                )

            self.transaction(run_steps)
            logger.info(f"Migration {migration.version} applied: {migration.description}")
        except Exception as e:
            logger.error(f"Migration {migration.version} failed: {e}")
            raise

    # ── Helpers ──────────────────────────────────────────────────────────────

    def _assert_open(self) -> None:
        if self._con is None:
            raise RuntimeError("Database not open. Call db.open() first.")


# ── Singleton ────────────────────────────────────────────────────────────────

db = Database(config.db_path)
