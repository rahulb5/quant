"""
src/collectors/asset_id_cleanup.py

One-off fix for equities registered outside their id block. EquityCollector
used to assign MAX(id) + 1 over the whole assets table, so constituents added
after the GSCI indices landed at 400012+ — inside the GSCI block, and outside
EquityUniverse's `id < 100000` filter, so they were silently left out of the
equity universe. src/db/asset_ids.py now allocates per block; this moves the
existing rows into the equity block.

Each misplaced asset is moved in this order: insert the asset under its new
id, copy its prices, repoint data_fetch_log, delete the old prices, then
delete the old asset. Statements run separately: DuckDB rejects a parent
delete that is in the same transaction as the child delete when a foreign key
points at it (see cot_cleanup.py). The steps are idempotent, so an
interrupted run can simply be re-run.
"""

from __future__ import annotations

from typing import Any

from src.db.asset_ids import ID_BLOCKS, next_asset_id
from src.db.client import Database

_EQUITY_LO = ID_BLOCKS["equity"][0]
_EQUITY_HI = ID_BLOCKS["equity_index"][1]  # constituents + the fixed index catalog

_PRICE_COLS = "interval, timestamp, open, high, low, close, volume, adj_close, vwap, source"


def misplaced_equities(database: Database) -> list[dict[str, Any]]:
    """Equity assets whose id is outside the equity and equity-index blocks."""
    return database.query(
        "SELECT id, ticker, name, exchange, currency, is_active, created_at FROM assets "
        "WHERE asset_class = 'equity' AND id NOT BETWEEN ? AND ? ORDER BY id",
        [_EQUITY_LO, _EQUITY_HI],
    )


def renumber_misplaced_equities(database: Database, dry_run: bool = True) -> list[dict[str, Any]]:
    """Move misplaced equities into the equity block, carrying their prices and fetch log.

    Args:
        database: An open, writable Database (writable even for dry_run=True,
                  which only issues SELECTs).
        dry_run:  If True (default), plan only and change nothing.

    Returns:
        One dict per misplaced asset: ticker, old_id, new_id, prices,
        data_fetch_log (rows moved or, for a dry run, that would be).

    Raises:
        ValueError: a misplaced asset has a non-NULL exchange. The assets
                    table is UNIQUE (ticker, exchange), so the new row could
                    not coexist with the old one during the move.
    """
    plan = []
    next_id = next_asset_id(database, "equity")
    for asset in misplaced_equities(database):
        if asset["exchange"] is not None:
            raise ValueError(f"{asset['ticker']} (id={asset['id']}) has exchange set; move it by hand")
        # A previous, interrupted run may already have created the new row.
        existing = database.query(
            "SELECT id FROM assets WHERE ticker = ? AND id BETWEEN ? AND ?",
            [asset["ticker"], *ID_BLOCKS["equity"]],
        )
        if existing:
            new_id = int(existing[0]["id"])
        else:
            new_id, next_id = next_id, next_id + 1
        plan.append({
            "ticker": asset["ticker"],
            "old_id": int(asset["id"]),
            "new_id": new_id,
            "prices": database.query(
                "SELECT COUNT(*) AS n FROM prices WHERE asset_id = ?", [asset["id"]]
            )[0]["n"],
            "data_fetch_log": database.query(
                "SELECT COUNT(*) AS n FROM data_fetch_log WHERE asset_id = ?", [asset["id"]]
            )[0]["n"],
            "_asset": asset,
            "_exists": bool(existing),
        })

    if not dry_run:
        for p in plan:
            a, old, new = p["_asset"], p["old_id"], p["new_id"]
            if not p["_exists"]:
                database.run(
                    "INSERT INTO assets (id, ticker, name, asset_class, exchange, currency, is_active, created_at) "
                    "VALUES (?, ?, ?, 'equity', NULL, ?, ?, ?)",
                    [new, a["ticker"], a["name"], a["currency"], a["is_active"], a["created_at"]],
                )
            database.run(
                f"INSERT INTO prices (asset_id, {_PRICE_COLS}) "
                f"SELECT ?, {_PRICE_COLS} FROM prices WHERE asset_id = ? ON CONFLICT DO NOTHING",
                [new, old],
            )
            database.run("UPDATE data_fetch_log SET asset_id = ? WHERE asset_id = ?", [new, old])
            database.run("DELETE FROM prices WHERE asset_id = ?", [old])
            database.run("DELETE FROM assets WHERE id = ?", [old])

    return [{k: v for k, v in p.items() if not k.startswith("_")} for p in plan]
