"""
scripts/fetch_cot.py

Fetches CFTC Commitments of Traders (COT) data from the Socrata open-data
API and stores it in quant.db (cot_positions table).

Four report types are fetched:
  legacy_fut  — Legacy Futures Only
  legacy_comb — Legacy Combined (futures + options)
  disagg_fut  — Disaggregated Futures Only
  tff_fut     — Traders in Financial Futures

Usage:
    python scripts/fetch_cot.py                              # incremental (last 2 weeks)
    python scripts/fetch_cot.py --mode backfill              # full history
    python scripts/fetch_cot.py --report-type legacy_fut     # single report type
    python scripts/fetch_cot.py --mode backfill --report-type disagg_fut
"""

from __future__ import annotations

import argparse
from datetime import date, timedelta

import requests

from src.db.client import db

# ── Endpoint catalogue ────────────────────────────────────────────────────────

ENDPOINTS: list[dict] = [
    {
        "report_type":  "legacy_fut",
        "url":          "https://publicreporting.cftc.gov/resource/6dca-aqww.json",
        "code_prefix":  "CFTC.LEGACY_FUT",
        "oi_field":     "open_interest_fut",
    },
    {
        "report_type":  "legacy_comb",
        "url":          "https://publicreporting.cftc.gov/resource/jun7-fc8e.json",
        "code_prefix":  "CFTC.LEGACY_COMB",
        "oi_field":     "open_interest_all",
    },
    {
        "report_type":  "disagg_fut",
        "url":          "https://publicreporting.cftc.gov/resource/72hh-3qpy.json",
        "code_prefix":  "CFTC.DISAGG_FUT",
        "oi_field":     "open_interest_fut",
    },
    {
        "report_type":  "tff_fut",
        "url":          "https://publicreporting.cftc.gov/resource/gpe5-46if.json",
        "code_prefix":  "CFTC.TFF_FUT",
        "oi_field":     "open_interest_fut",
    },
]

# ── Field mappings: CFTC API field → cot_positions column ────────────────────

LEGACY_MAP: dict[str, str] = {
    "noncomm_positions_long_all":   "leg_nc_long",
    "noncomm_positions_short_all":  "leg_nc_short",
    "noncomm_postions_spread_all":  "leg_nc_spread",    # CFTC typo: "postions"
    "comm_positions_long_all":      "leg_comm_long",
    "comm_positions_short_all":     "leg_comm_short",
    "nonrept_positions_long_all":   "leg_nr_long",
    "nonrept_positions_short_all":  "leg_nr_short",
    "traders_noncomm_long_all":     "leg_nc_traders_long",
    "traders_noncomm_short_all":    "leg_nc_traders_short",
    "traders_noncomm_spread_all":   "leg_nc_traders_spread",
    "traders_comm_long_all":        "leg_comm_traders_long",
    "traders_comm_short_all":       "leg_comm_traders_short",
}

DISAGG_MAP: dict[str, str] = {
    "prod_merc_positions_long_all":     "dis_pmpu_long",
    "prod_merc_positions_short_all":    "dis_pmpu_short",
    "swap_positions_long_all":          "dis_sd_long",
    "swap__positions_short_all":        "dis_sd_short",     # CFTC double underscore
    "swap__positions_spread_all":       "dis_sd_spread",
    "m_money_positions_long_all":       "dis_mm_long",
    "m_money_positions_short_all":      "dis_mm_short",
    "m_money_positions_spread_all":     "dis_mm_spread",
    "other_rept_positions_long_all":    "dis_or_long",
    "other_rept_positions_short_all":   "dis_or_short",
    "other_rept_positions_spread_all":  "dis_or_spread",
    "nonrept_positions_long_all":       "dis_nr_long",
    "nonrept_positions_short_all":      "dis_nr_short",
    # Trader counts
    "traders_prod_merc_long_all":       "dis_pmpu_traders_long",
    "traders_prod_merc_short_all":      "dis_pmpu_traders_short",
    "traders_swap_long_all":            "dis_sd_traders_long",
    "traders_swap_short_all":           "dis_sd_traders_short",
    "traders_swap_spread_all":          "dis_sd_traders_spread",
    "traders_m_money_long_all":         "dis_mm_traders_long",
    "traders_m_money_short_all":        "dis_mm_traders_short",
    "traders_m_money_spread_all":       "dis_mm_traders_spread",
    "traders_other_rept_long_all":      "dis_or_traders_long",
    "traders_other_rept_short_all":     "dis_or_traders_short",
    "traders_other_rept_spread_all":    "dis_or_traders_spread",
}

TFF_MAP: dict[str, str] = {
    "dealer_positions_long_all":        "tff_dealer_long",
    "dealer_positions_short_all":       "tff_dealer_short",
    "dealer_positions_spread_all":      "tff_dealer_spread",
    "asset_mgr_positions_long_all":     "tff_am_long",
    "asset_mgr_positions_short_all":    "tff_am_short",
    "asset_mgr_positions_spread_all":   "tff_am_spread",
    "lev_money_positions_long_all":     "tff_lf_long",
    "lev_money_positions_short_all":    "tff_lf_short",
    "lev_money_positions_spread_all":   "tff_lf_spread",
    "other_rept_positions_long_all":    "tff_or_long",
    "other_rept_positions_short_all":   "tff_or_short",
    "other_rept_positions_spread_all":  "tff_or_spread",
    "nonrept_positions_long_all":       "tff_nr_long",
    "nonrept_positions_short_all":      "tff_nr_short",
    # Trader counts
    "traders_dealer_long_all":          "tff_dealer_traders_long",
    "traders_dealer_short_all":         "tff_dealer_traders_short",
    "traders_dealer_spread_all":        "tff_dealer_traders_spread",
    "traders_asset_mgr_long_all":       "tff_am_traders_long",
    "traders_asset_mgr_short_all":      "tff_am_traders_short",
    "traders_asset_mgr_spread_all":     "tff_am_traders_spread",
    "traders_lev_money_long_all":       "tff_lf_traders_long",
    "traders_lev_money_short_all":      "tff_lf_traders_short",
    "traders_lev_money_spread_all":     "tff_lf_traders_spread",
    "traders_other_rept_long_all":      "tff_or_traders_long",
    "traders_other_rept_short_all":     "tff_or_traders_short",
    "traders_other_rept_spread_all":    "tff_or_traders_spread",
}

REPORT_TYPE_FIELD_MAP: dict[str, dict[str, str]] = {
    "legacy_fut":  LEGACY_MAP,
    "legacy_comb": LEGACY_MAP,
    "disagg_fut":  DISAGG_MAP,
    "tff_fut":     TFF_MAP,
}

# ── CLI args ──────────────────────────────────────────────────────────────────

parser = argparse.ArgumentParser(description="Fetch CFTC COT data into quant.db")
parser.add_argument(
    "--mode",
    choices=["incremental", "backfill"],
    default="incremental",
    help="incremental: last 2 weeks (default). backfill: full history.",
)
parser.add_argument(
    "--report-type",
    choices=["legacy_fut", "legacy_comb", "disagg_fut", "tff_fut"],
    default=None,
    metavar="TYPE",
    help="Fetch a single report type (default: all four).",
)
args = parser.parse_args()

# ── Setup ─────────────────────────────────────────────────────────────────────

db.open()

today = date.today().isoformat()
endpoints_to_run = (
    [e for e in ENDPOINTS if e["report_type"] == args.report_type]
    if args.report_type else ENDPOINTS
)
PAGE_SIZE = 10_000

print(
    f"CFTC COT fetch  mode={args.mode}  "
    f"reports={[e['report_type'] for e in endpoints_to_run]}\n"
)

# ── Helpers ───────────────────────────────────────────────────────────────────

def _fetch_page(url: str, limit: int, offset: int, where: str | None = None) -> list[dict]:
    """Fetch one page from a Socrata endpoint."""
    params: dict = {"$limit": limit, "$offset": offset, "$order": "report_date_as_yyyy_mm_dd"}
    if where:
        params["$where"] = where
    resp = requests.get(url, params=params, timeout=60)
    resp.raise_for_status()
    return resp.json()


def _fetch_all_rows(url: str) -> list[dict]:
    """Fetch all rows for an endpoint, paginating as needed."""
    if args.mode == "incremental":
        cutoff = (date.today() - timedelta(weeks=2)).isoformat()
        where = f"report_date_as_yyyy_mm_dd >= '{cutoff}'"
        rows = _fetch_page(url, PAGE_SIZE, 0, where=where)
        print(f"    fetched {len(rows)} rows (incremental since {cutoff})")
        return rows

    # backfill: paginate until empty
    all_rows: list[dict] = []
    offset = 0
    while True:
        page = _fetch_page(url, PAGE_SIZE, offset)
        if not page:
            break
        all_rows.extend(page)
        print(f"    page offset={offset:>7,}  →  {len(all_rows):>7,} rows so far", flush=True)
        if len(page) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
    return all_rows


def _ensure_series(code: str, name: str) -> int:
    """Return series_id for the given CFTC series code, inserting if missing."""
    rows = db.query("SELECT id FROM macro_series WHERE code = ?", [code])
    if rows:
        return int(rows[0]["id"])

    next_id = int(
        db.query("SELECT COALESCE(MAX(id), 0) + 1 AS n FROM macro_series")[0]["n"]
    )
    db.run(
        """
        INSERT INTO macro_series (id, code, name, source, frequency, units, seasonal_adj)
        VALUES (?, ?, ?, 'CFTC', 'weekly', 'contracts', false)
        """,
        [next_id, code, name],
    )
    return next_id


def _log_fetch(series_id: int, from_date: str, rows_inserted: int, status: str,
               error_msg: str | None = None) -> None:
    """Write an audit row to data_fetch_log."""
    try:
        next_id = int(
            db.query("SELECT COALESCE(MAX(id), 0) + 1 AS n FROM data_fetch_log")[0]["n"]
        )
        db.run(
            """
            INSERT INTO data_fetch_log
              (id, series_id, interval, fetched_from, fetched_to,
               source, rows_inserted, status, error_msg)
            VALUES (?, ?, NULL, ?, ?, 'CFTC', ?, ?, ?)
            """,
            [next_id, series_id, from_date, today, rows_inserted, status, error_msg],
        )
    except Exception as e:
        print(f"  [WARN] Failed to write fetch log: {e}")


def _parse_float(val) -> float | None:
    """Convert a CFTC API string value to float, or None if missing/invalid."""
    if val is None or val == "":
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _build_upsert_sql(db_columns: list[str]) -> str:
    """Generate INSERT ... ON CONFLICT DO UPDATE SET SQL for the given columns."""
    col_list = ", ".join(db_columns)
    placeholders = ", ".join(["?"] * len(db_columns))
    pk = {"series_id", "report_date", "report_type"}
    updates = ",\n                    ".join(
        f"{c} = excluded.{c}" for c in db_columns if c not in pk
    )
    return f"""
        INSERT INTO cot_positions ({col_list})
        VALUES ({placeholders})
        ON CONFLICT (series_id, report_date, report_type) DO UPDATE SET
            {updates}
    """


# ── Main loop ─────────────────────────────────────────────────────────────────

total_rows = 0
failures: list[tuple[str, str]] = []

for endpoint in endpoints_to_run:
    report_type = endpoint["report_type"]
    url         = endpoint["url"]
    code_prefix = endpoint["code_prefix"]
    oi_field    = endpoint["oi_field"]
    field_map   = REPORT_TYPE_FIELD_MAP[report_type]

    print(f"── {report_type} ({'backfill' if args.mode == 'backfill' else 'incremental'}) ──")

    try:
        raw_rows = _fetch_all_rows(url)
        if not raw_rows:
            print(f"  no rows returned\n")
            continue

        # Build upsert SQL once per endpoint (column list is fixed per report type)
        shared_cols = [
            "series_id", "report_date", "release_date", "report_type",
            "market_name", "cftc_market_code", "cftc_commodity_code",
            "exchange_name", "commodity", "open_interest",
        ]
        type_cols = list(field_map.values())
        all_cols = shared_cols + type_cols
        upsert_sql = _build_upsert_sql(all_cols)

        series_cache: dict[str, int] = {}
        rows_upserted = 0
        first_series_id: int | None = None

        # Process in batches of 500 rows per transaction
        BATCH = 500
        for batch_start in range(0, len(raw_rows), BATCH):
            batch = raw_rows[batch_start: batch_start + BATCH]

            def steps(q, _batch=batch) -> None:
                nonlocal rows_upserted, first_series_id, series_cache

                for row in _batch:
                    # --- Resolve market code and series ---
                    mkt_code = (
                        row.get("cftc_market_code")
                        or row.get("cftc_contract_market_code", "UNKNOWN")
                    )
                    series_code = f"{code_prefix}.{mkt_code}"
                    market_name = row.get("market_and_exchange_names", "")

                    if series_code not in series_cache:
                        series_cache[series_code] = _ensure_series(series_code, market_name)
                    series_id = series_cache[series_code]
                    if first_series_id is None:
                        first_series_id = series_id

                    # --- Dates ---
                    report_date_str = row.get("report_date_as_yyyy_mm_dd", "")[:10]
                    if not report_date_str:
                        return
                    try:
                        release_date_str = (
                            date.fromisoformat(report_date_str) + timedelta(days=3)
                        ).isoformat()
                    except ValueError:
                        return

                    # --- Shared values ---
                    shared_vals = [
                        series_id,
                        report_date_str,
                        release_date_str,
                        report_type,
                        market_name,
                        mkt_code,
                        row.get("commodity_code"),
                        row.get("exchange_name"),
                        row.get("commodity"),
                        _parse_float(row.get(oi_field)),
                    ]

                    # --- Type-specific values ---
                    type_vals = [_parse_float(row.get(api_field)) for api_field in field_map]

                    q(upsert_sql, shared_vals + type_vals)
                    rows_upserted += 1

            db.transaction(steps)

        first_date = raw_rows[0].get("report_date_as_yyyy_mm_dd", today)[:10]
        if first_series_id is not None:
            _log_fetch(first_series_id, first_date, rows_upserted, "success")

        total_rows += rows_upserted
        print(f"  {rows_upserted:>7,} rows upserted  ✓\n")

    except Exception as e:
        print(f"  FAILED: {e}\n")
        failures.append((report_type, str(e)))

# ── Summary ───────────────────────────────────────────────────────────────────

print("── Summary ──────────────────────────────────────────")
print(f"  Reports processed : {len(endpoints_to_run)}")
print(f"  Total rows upserted : {total_rows:,}")
print(f"  Failed              : {len(failures)}")

if failures:
    print(f"\n  Failed report types:")
    for rtype, err in failures:
        print(f"    {rtype}: {err}")

# ── Teardown ──────────────────────────────────────────────────────────────────

db.close()
