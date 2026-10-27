"""
src/db/asset_ids.py

Asset-id blocks. Each asset class owns a fixed id range, so a collector that
registers a new asset takes the next free id inside its own block rather than
MAX(id) + 1 over the whole table (which put new equities after the GSCI
indices, at 400012+).

Equity indices share asset_class='equity' with constituents but have their
own block, so the key is a block name; for every other block it is the
asset_class. Forex, equity-index and GSCI collectors use fixed catalog ids
that sit inside their blocks.

Usage:
    from src.db.asset_ids import next_asset_id

    new_id = next_asset_id(db, "equity")
"""

from __future__ import annotations

from src.db.client import Database

# block -> (first id, last id), inclusive.
ID_BLOCKS: dict[str, tuple[int, int]] = {
    "equity":       (1, 99_999),          # S&P 500 constituents
    "equity_index": (100_001, 199_999),   # scripts/fetch_equity.py --indices (fixed ids)
    "forex":        (200_001, 299_999),   # CURRENCY_PAIRS, incl. crypto pairs (fixed ids)
    "futures":      (300_001, 399_999),   # commodity futures
    "index":        (400_001, 499_999),   # S&P GSCI excess-return indices (fixed ids)
    "etf":          (500_001, 599_999),   # collectors/total_return.py ETFs (fixed ids, asset_class='equity')
}


def next_asset_id(database: Database, block: str) -> int:
    """Next free id in a block: one past the block's current max.

    Raises:
        ValueError: unknown block, or the block is full.
    """
    if block not in ID_BLOCKS:
        raise ValueError(f"Unknown asset id block {block!r}; expected one of {list(ID_BLOCKS)}")
    lo, hi = ID_BLOCKS[block]
    rows = database.query(
        "SELECT COALESCE(MAX(id), ?) AS max_id FROM assets WHERE id BETWEEN ? AND ?",
        [lo - 1, lo, hi],
    )
    next_id = int(rows[0]["max_id"]) + 1
    if next_id > hi:
        raise ValueError(f"Asset id block {block!r} ({lo}-{hi}) is full")
    return next_id
