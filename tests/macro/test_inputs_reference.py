"""tests/macro/test_inputs_reference.py — load_reference_returns on a temp DuckDB."""

from __future__ import annotations

import pandas as pd
import pytest

from data.repository import Asset, Price, insert_asset, insert_price
from src.db.client import Database
from src.macro.inputs import REFERENCE_LABELS, load_reference_returns


@pytest.fixture
def ref_db(monkeypatch, tmp_path):
    import data.repository as repository_module

    database = Database(str(tmp_path / "ref.db"))
    database.open()
    monkeypatch.setattr(repository_module, "db", database)
    yield database
    database.close()


def _prices(asset_id, ticker, closes, start="2024-01-01"):
    insert_asset(Asset(id=asset_id, ticker=ticker, name=ticker, asset_class="futures"))
    for i, close in enumerate(closes):
        ts = (pd.Timestamp(start) + pd.Timedelta(days=i)).isoformat()
        insert_price(Price(asset_id=asset_id, interval="1d", timestamp=ts, open=close, high=close,
                           low=close, close=close, volume=0, source="test"))


def test_price_assets_become_simple_returns_and_missing_sources_are_omitted(ref_db):
    _prices(100001, "^GSPC", [100.0, 110.0, 99.0])
    _prices(300009, "GC1", [10.0, 10.0, 12.0])
    out = load_reference_returns(ref_db)

    assert list(out.columns) == ["sp500", "gold"]                    # no crude, dollar or rates data
    assert out["sp500"].dropna().tolist() == pytest.approx([0.10, -0.10])
    assert out["gold"].dropna().tolist() == pytest.approx([0.0, 0.20])
    assert set(out.columns) <= set(REFERENCE_LABELS)
    assert out.index.is_monotonic_increasing


def test_empty_database_gives_empty_frame(ref_db):
    assert load_reference_returns(ref_db).empty


def test_dollar_and_treasury_proxy_columns_end_at_the_last_observation(ref_db):
    dates = pd.bdate_range("2024-01-02", periods=300)
    ref_db.run("""INSERT INTO macro_series (id, code, name, source, frequency, units, description) VALUES
        (1, 'DGS10', 'ten year', 'FRED', 'daily', 'Percent', NULL),
        (2, 'DTWEXBGS', 'dollar', 'FRED', 'daily', 'Index', NULL)""")
    for i, d in enumerate(dates):
        ref_db.run("INSERT INTO macro_observations VALUES (1, ?, ?, ?, false)", [d.date(), d.date(), 4.0 + 0.001 * i])
        ref_db.run("INSERT INTO macro_observations VALUES (2, ?, ?, ?, false)", [d.date(), d.date(), 100.0 + 0.1 * i])
    out = load_reference_returns(ref_db)

    assert {"dollar", "treasury_10y_proxy"} <= set(out.columns)
    assert out["treasury_10y_proxy"].dropna().index.max() == dates[-1]        # not extended to today
    assert out["dollar"].dropna().iloc[0] == pytest.approx(0.1 / 100.0)
    assert out["treasury_10y_proxy"].dropna().iloc[5] < 0.01                  # carry minus a small duration hit
