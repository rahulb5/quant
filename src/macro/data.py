"""
src/macro/data.py

Read-side loaders for macro_series / macro_observations (FRED) and
cot_positions (CFTC). Each function takes a Database and manages nothing
else: query_df() opens and closes the connection if the caller has not.

FRED series carry revision history, so loaders pick a vintage:
  latest — most recent release per period (what FRED shows today)
  first  — first release per period (what was known in real time)

COT rows are reduced to one positioning measure (e.g. managed-money net),
optionally as % of open interest.

Usage:
    from src.db.client import Database
    from src.macro.data import list_series, load_fred, load_cot

    ro = Database("data/quant.db", read_only=True)
    catalog = list_series(ro)
    cpi = load_fred(ro, "CPIAUCSL", vintage="first")
    mm_net = load_cot(ro, "CFTC.DISAGG_FUT.067651", "mm_net", pct_of_oi=True)
"""

from __future__ import annotations

from typing import Literal

import pandas as pd

from src.db.client import Database

Vintage = Literal["latest", "first"]

# measure key → (label, long column, short column); open interest is special-cased.
COT_MEASURES: dict[str, dict[str, tuple[str, str, str]]] = {
    "legacy_fut": {
        "nc_net":   ("Non-commercial net", "leg_nc_long", "leg_nc_short"),
        "comm_net": ("Commercial net", "leg_comm_long", "leg_comm_short"),
        "nr_net":   ("Non-reportable net", "leg_nr_long", "leg_nr_short"),
    },
    "disagg_fut": {
        "mm_net":   ("Managed money net", "dis_mm_long", "dis_mm_short"),
        "pmpu_net": ("Producer/merchant net", "dis_pmpu_long", "dis_pmpu_short"),
        "sd_net":   ("Swap dealer net", "dis_sd_long", "dis_sd_short"),
        "or_net":   ("Other reportable net", "dis_or_long", "dis_or_short"),
        "nr_net":   ("Non-reportable net", "dis_nr_long", "dis_nr_short"),
    },
    "tff_fut": {
        "lf_net":     ("Leveraged funds net", "tff_lf_long", "tff_lf_short"),
        "am_net":     ("Asset manager net", "tff_am_long", "tff_am_short"),
        "dealer_net": ("Dealer net", "tff_dealer_long", "tff_dealer_short"),
        "or_net":     ("Other reportable net", "tff_or_long", "tff_or_short"),
        "nr_net":     ("Non-reportable net", "tff_nr_long", "tff_nr_short"),
    },
}
COT_MEASURES["legacy_comb"] = COT_MEASURES["legacy_fut"]
OPEN_INTEREST = "open_interest"


def cot_measures(report_type: str) -> dict[str, str]:
    """measure key → label for a report type, open interest last."""
    out = {k: v[0] for k, v in COT_MEASURES[report_type].items()}
    out[OPEN_INTEREST] = "Open interest"
    return out


def list_series(db: Database) -> pd.DataFrame:
    """Catalog of every series that has data.

    Columns: code, name, source ('FRED' | 'CFTC'), frequency, units,
    report_type (CFTC only), n_obs, first_date, last_date, and
    contract_keyed — False for CFTC series whose code ends in an exchange
    code (e.g. CFTC.LEGACY_FUT.NYME) rather than a 6-character contract code.
    Those rows mix every contract on the exchange, one per report date.
    The collector no longer writes them (and scripts/migrate_cot_contract_keys.py
    removed the old ones), so this is a safety net. asset_group is the COT
    catalog group (energy, rates, fx, ...) for CFTC series, else NULL.
    """
    return db.query_df("""
        WITH fred AS (
            SELECT series_id, COUNT(DISTINCT period_date) AS n_obs,
                   MIN(period_date) AS first_date, MAX(period_date) AS last_date,
                   NULL AS report_type
            FROM macro_observations GROUP BY series_id
        ), cot AS (
            SELECT series_id, COUNT(*) AS n_obs,
                   MIN(report_date) AS first_date, MAX(report_date) AS last_date,
                   ANY_VALUE(report_type) AS report_type
            FROM cot_positions GROUP BY series_id
        ), stats AS (SELECT * FROM fred UNION ALL SELECT * FROM cot)
        SELECT ms.code, ms.name, ms.source, ms.frequency, ms.units,
               st.report_type, st.n_obs, st.first_date, st.last_date,
               ms.source <> 'CFTC'
                 OR LENGTH(TRIM(SPLIT_PART(ms.code, '.', 3))) = 6 AS contract_keyed,
               CASE WHEN ms.source = 'CFTC' AND ms.description LIKE 'COT %'
                    THEN SUBSTR(ms.description, 5) END AS asset_group
        FROM macro_series ms JOIN stats st ON st.series_id = ms.id
        ORDER BY ms.source DESC, ms.code
    """)


def load_fred(db: Database, code: str, vintage: Vintage = "latest") -> pd.Series:
    """One value per period_date for a FRED series, at the requested vintage.

    The returned Series is indexed by period_date (DatetimeIndex) and has a
    `release_date` attribute in .attrs holding the matching release dates.
    """
    if vintage not in ("latest", "first"):
        raise ValueError(f"vintage must be 'latest' or 'first', got {vintage!r}")
    order = "DESC" if vintage == "latest" else "ASC"
    df = db.query_df(f"""
        SELECT period_date, release_date, value FROM (
            SELECT mo.period_date, mo.release_date, mo.value,
                   ROW_NUMBER() OVER (
                       PARTITION BY mo.period_date ORDER BY mo.release_date {order}
                   ) AS rn
            FROM macro_observations mo
            JOIN macro_series ms ON ms.id = mo.series_id
            WHERE ms.code = ?
        ) WHERE rn = 1
        ORDER BY period_date
    """, [code])
    s = pd.Series(df["value"].to_numpy(), index=pd.DatetimeIndex(df["period_date"]), name=code)
    s.attrs["release_date"] = pd.Series(pd.DatetimeIndex(df["release_date"]), index=s.index)
    return s


def load_cot(db: Database, code: str, measure: str, pct_of_oi: bool = False) -> pd.Series:
    """One positioning measure per report_date for a CFTC series.

    `measure` is a key from cot_measures(report_type). With pct_of_oi, net
    measures are divided by open interest and expressed in percent.
    """
    rows = db.query_df(
        "SELECT DISTINCT p.report_type FROM cot_positions p "
        "JOIN macro_series ms ON ms.id = p.series_id WHERE ms.code = ?",
        [code],
    )
    if rows.empty:
        return pd.Series(dtype=float, name=code, index=pd.DatetimeIndex([]))
    report_type = rows["report_type"].iloc[0]

    if measure == OPEN_INTEREST:
        expr = "p.open_interest"
    elif measure in COT_MEASURES[report_type]:
        _, long_col, short_col = COT_MEASURES[report_type][measure]
        expr = f"(p.{long_col} - p.{short_col})"
        if pct_of_oi:
            expr = f"{expr} / NULLIF(p.open_interest, 0) * 100"
    else:
        raise ValueError(f"Unknown measure {measure!r} for {report_type}; expected {list(cot_measures(report_type))}")

    df = db.query_df(f"""
        SELECT p.report_date, {expr} AS value
        FROM cot_positions p JOIN macro_series ms ON ms.id = p.series_id
        WHERE ms.code = ?
        ORDER BY p.report_date
    """, [code])
    df = df.dropna(subset=["value"])
    return pd.Series(df["value"].to_numpy(dtype=float), index=pd.DatetimeIndex(df["report_date"]), name=code)
