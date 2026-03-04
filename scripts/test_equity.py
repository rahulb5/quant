"""
scripts/test_equity.py

Temporary smoke test for EquityCollector.
Run from the project root with the venv activated:
    python scripts/test_equity.py
"""

from src.db.client import db
from src.collectors.equity import EquityCollector

db.open()

collector = EquityCollector()

asset_id = collector.ensure_asset("AAPL", "Apple Inc.")
print(f"asset_id: {asset_id}")

rows = collector.run(asset_id=asset_id, from_date="2024-01-01", to_date="2024-01-31")
print(f"rows inserted: {rows}")

prices = db.query(
    "SELECT timestamp, open, high, low, close, volume FROM prices WHERE asset_id = ? ORDER BY timestamp LIMIT 5",
    [asset_id],
)
print(f"\nFirst 5 rows for asset_id={asset_id}:")
for row in prices:
    print(row)

db.close()
