# quant

A Python DuckDB database layer for storing financial and macroeconomic data locally.

## Overview

This project provides a persistent local database using [DuckDB](https://duckdb.org/) via the `duckdb` Python package. It includes a connection manager, versioned schema migrations, typed insert helpers, and a full test suite.

## Project Structure

```
quant/
├── src/
│   ├── db/
│   │   ├── client.py       # Database class, query helpers & db singleton
│   │   └── migrations.py   # Versioned schema definitions
│   └── shared/
│       ├── config.py       # Config loader (env vars / .env)
│       └── utils.py        # Logger (console + file handlers)
├── data/
│   ├── quant.db            # DuckDB database file
│   └── repository.py       # Typed insert functions for each table
├── scripts/
│   └── init_db.py          # Creates the DB and applies migrations
└── tests/
    └── db/
        └── test_client.py  # Full test suite (pytest)
```

## Database Schema

The schema is applied via sequential migrations. Each migration runs exactly once.

| Version | Table                | Description                                      |
|---------|----------------------|--------------------------------------------------|
| 1       | `schema_version`     | Tracks applied migrations                        |
| 2       | `assets`             | Financial instruments (equities, crypto, etc.)   |
| 3       | `prices`             | OHLCV price data across multiple intervals       |
| 4       | `macro_series`       | Macroeconomic data series metadata               |
| 5       | `macro_observations` | Point-in-time macro values with revision history |
| 6       | `data_fetch_log`     | Audit log of data fetching operations            |

## Configuration

The database path defaults to `data/quant.db` and can be overridden via environment variables:

```bash
DB_PATH=data/mydb.db        # Path to the DuckDB file
LOG_LEVEL=debug             # debug | info | warn | error
APP_ENV=development         # development | production | test
```

Or create a `.env` file in the project root.

## Usage

```python
from src.db.client import db

# Open the database (runs migrations automatically)
db.open()

# SELECT — returns a list of dicts
assets = db.query("SELECT * FROM assets WHERE is_active = true")

# INSERT / UPDATE / DELETE — returns rows changed
changed = db.run(
    "INSERT INTO assets (id, ticker, name, asset_class, currency) VALUES (?, ?, ?, ?, ?)",
    [1, "AAPL", "Apple Inc.", "equity", "USD"],
)

# Transaction
def steps(q):
    q("INSERT INTO assets (id, ticker, name, asset_class, currency) VALUES (?, ?, ?, ?, ?)",
      [2, "BTC", "Bitcoin", "crypto", "USD"])
    q("INSERT INTO prices (asset_id, interval, timestamp, open, high, low, close, volume, source) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
      [2, "1d", "2024-01-01", 42000, 43000, 41000, 42500, 1000, "binance"])

db.transaction(steps)

# Close when done
db.close()
```

### Using the repository insert helpers

```python
from src.db.client import db
from data.repository import Asset, Price, insert_asset, insert_prices, log_fetch, DataFetchLog

db.open()

insert_asset(Asset(id=1, ticker="AAPL", name="Apple Inc.", asset_class="equity"))

insert_prices([
    Price(asset_id=1, interval="1d", timestamp="2024-01-01T00:00:00Z",
          open=185, high=188, low=184, close=187, volume=50_000_000, source="yahoo")
])

log_fetch(DataFetchLog(
    asset_id=1, fetched_from="2024-01-01", fetched_to="2024-01-31",
    source="yahoo", rows_inserted=1, status="success",
))

db.close()
```

## Getting Started

```bash
# Activate the virtual environment
source ../bin/activate

# Init the database (creates quant.db and applies all migrations)
python scripts/init_db.py

# Run the test suite
pytest
```

## Scripts

| Command                      | Description                        |
|------------------------------|------------------------------------|
| `pytest`                     | Run the test suite                 |
| `pytest -v`                  | Run tests with verbose output      |
| `python scripts/init_db.py`  | Initialise the database            |

## Dependencies

- [`duckdb`](https://duckdb.org/docs/api/python/overview) — DuckDB Python bindings
- [`python-dotenv`](https://github.com/theskumar/python-dotenv) — Environment variable loading
- [`pytest`](https://docs.pytest.org/) — Test framework
