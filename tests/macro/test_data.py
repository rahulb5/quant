"""Tests for src/macro/data.py against a seeded temp DuckDB."""

import pytest

from src.db.client import Database
from src.macro.data import cot_measures, list_series, load_cot, load_fred


@pytest.fixture
def ro(macro_db_path):
    return Database(macro_db_path, read_only=True)


def test_list_series_catalog(ro):
    cat = list_series(ro).set_index("code")
    assert set(cat.index) == {"CPI", "CFTC.DISAGG_FUT.067651", "CFTC.DISAGG_FUT.NYME"}
    assert cat.at["CPI", "n_obs"] == 120
    assert cat.at["CFTC.DISAGG_FUT.067651", "report_type"] == "disagg_fut"
    assert bool(cat.at["CPI", "contract_keyed"])
    assert bool(cat.at["CFTC.DISAGG_FUT.067651", "contract_keyed"])
    assert not bool(cat.at["CFTC.DISAGG_FUT.NYME", "contract_keyed"])
    assert cat.at["CFTC.DISAGG_FUT.067651", "asset_group"] == "energy"
    assert cat["asset_group"].isna()[["CPI", "CFTC.DISAGG_FUT.NYME"]].all()


def test_load_fred_vintages(ro):
    latest = load_fred(ro, "CPI", "latest")
    first = load_fred(ro, "CPI", "first")
    assert len(latest) == len(first) == 120
    assert (latest - first).round(9).eq(1.0).all()
    assert latest.attrs["release_date"].iloc[0].day == 15  # 2015-01-01 + 45d
    assert first.attrs["release_date"].iloc[0].day == 15 and first.attrs["release_date"].iloc[0].month == 1


def test_load_fred_rejects_bad_vintage(ro):
    with pytest.raises(ValueError):
        load_fred(ro, "CPI", "median")


def test_load_cot_net_and_pct_of_oi(ro):
    net = load_cot(ro, "CFTC.DISAGG_FUT.067651", "mm_net")
    assert net.iloc[0] == 0 and net.iloc[-1] == 149
    pct = load_cot(ro, "CFTC.DISAGG_FUT.067651", "mm_net", pct_of_oi=True)
    assert pct.iloc[-1] == pytest.approx(14.9)
    oi = load_cot(ro, "CFTC.DISAGG_FUT.067651", "open_interest")
    assert (oi == 1000).all()


def test_load_cot_unknown_measure_and_series(ro):
    with pytest.raises(ValueError):
        load_cot(ro, "CFTC.DISAGG_FUT.067651", "lf_net")
    assert load_cot(ro, "NOPE", "mm_net").empty


def test_cot_measures_puts_open_interest_last():
    assert list(cot_measures("tff_fut"))[-1] == "open_interest"
    assert cot_measures("legacy_comb") == cot_measures("legacy_fut")
