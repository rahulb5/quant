"""
src/collectors/cot.py

CFTC Commitments of Traders (COT) collector using the Socrata open-data API.

Four report types, one Socrata endpoint each:
  legacy_fut  — Legacy Futures Only
  legacy_comb — Legacy Combined (futures + options)
  disagg_fut  — Disaggregated Futures Only
  tff_fut     — Traders in Financial Futures

Each endpoint call returns rows for many CFTC market codes at once, so the
natural unit of work is a (report_type, date range) pair rather than a
single series. Like CommodityCollector/GsciCollector, collect() is not used
for this; fetch_report_type() does the fetch/upsert and calls _log_fetch()
directly, using the first series touched as the audit row's representative
series_id.

Usage:
    from src.db.client import db
    from src.collectors.cot import CotCollector

    db.open()
    collector = CotCollector()
    collector.fetch_all(mode="update")
    db.close()
"""

from __future__ import annotations

from datetime import date, timedelta

import requests

from src.collectors.base import BaseCollector
from src.db.client import db

PAGE_SIZE = 10_000
TWO_WEEKS = timedelta(weeks=2)

# ── Endpoint catalogue ────────────────────────────────────────────────────────

ENDPOINTS: list[dict] = [
    {
        "report_type": "legacy_fut",
        "url":         "https://publicreporting.cftc.gov/resource/6dca-aqww.json",
        "code_prefix": "CFTC.LEGACY_FUT",
        "oi_field":    "open_interest_fut",
    },
    {
        "report_type": "legacy_comb",
        "url":         "https://publicreporting.cftc.gov/resource/jun7-fc8e.json",
        "code_prefix": "CFTC.LEGACY_COMB",
        "oi_field":    "open_interest_all",
    },
    {
        "report_type": "disagg_fut",
        "url":         "https://publicreporting.cftc.gov/resource/72hh-3qpy.json",
        "code_prefix": "CFTC.DISAGG_FUT",
        "oi_field":    "open_interest_all",
    },
    {
        "report_type": "tff_fut",
        "url":         "https://publicreporting.cftc.gov/resource/gpe5-46if.json",
        "code_prefix": "CFTC.TFF_FUT",
        "oi_field":    "open_interest_fut",
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
    "prod_merc_positions_long":          "dis_pmpu_long",
    "prod_merc_positions_short":         "dis_pmpu_short",
    "swap_positions_long_all":           "dis_sd_long",
    "swap__positions_short_all":         "dis_sd_short",      # CFTC double underscore
    "swap__positions_spread_all":        "dis_sd_spread",
    "m_money_positions_long_all":        "dis_mm_long",
    "m_money_positions_short_all":       "dis_mm_short",
    "m_money_positions_spread":          "dis_mm_spread",
    "other_rept_positions_long":         "dis_or_long",
    "other_rept_positions_short":        "dis_or_short",
    "other_rept_positions_spread":       "dis_or_spread",
    "nonrept_positions_long_all":        "dis_nr_long",
    "nonrept_positions_short_all":       "dis_nr_short",
    # Trader counts
    "traders_prod_merc_long_all":        "dis_pmpu_traders_long",
    "traders_prod_merc_short_all":       "dis_pmpu_traders_short",
    "traders_swap_long_all":             "dis_sd_traders_long",
    "traders_swap_short_all":            "dis_sd_traders_short",
    "traders_swap_spread_all":           "dis_sd_traders_spread",
    "traders_m_money_long_all":          "dis_mm_traders_long",
    "traders_m_money_short_all":         "dis_mm_traders_short",
    "traders_m_money_spread_all":        "dis_mm_traders_spread",
    "traders_other_rept_long_all":       "dis_or_traders_long",
    "traders_other_rept_short":          "dis_or_traders_short",
    "traders_other_rept_spread":         "dis_or_traders_spread",
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

# ── Targeted commodity contracts (CFTC 6-digit market codes) ─────────────────

TARGET_MARKET_CODES: dict[str, str] = {
    "067651": "Crude Oil WTI",
    "023651": "Natural Gas",
    "088691": "Gold",
    "084691": "Silver",
    "085692": "Copper",
    "002602": "Corn",
    "001602": "Wheat SRW",
    "005602": "Soybeans",
    "083731": "Coffee C",
    "080732": "Sugar No 11",
    "073732": "Cocoa",
}


class CotCollector(BaseCollector):
    """Collects CFTC Commitments of Traders data from the Socrata API."""

    def __init__(self) -> None:
        super().__init__(source="CFTC")

    # ── Public interface ──────────────────────────────────────────────────────

    @staticmethod
    def get_all_report_types() -> list[str]:
        return [e["report_type"] for e in ENDPOINTS]

    def fetch_all(
        self,
        mode: str = "update",
        report_types: list[str] | None = None,
        targeted: bool = False,
    ) -> dict[str, int]:
        """Fetch every catalogued report type, tolerating per-report failures.

        Args:
            mode:         'update' (default) — from last DB date, falling back
                          to 2 weeks ago if empty. 'incremental' — last 2 weeks.
                          'backfill' — full history.
            report_types: Subset of report types to fetch. Defaults to all four.
            targeted:     If True, restrict to the 11 physical commodities in
                          TARGET_MARKET_CODES (disagg_fut only).

        Returns:
            Dict mapping report_type -> rows_upserted for every report that succeeded.
        """
        if targeted:
            endpoints = [e for e in ENDPOINTS if e["report_type"] == "disagg_fut"]
            market_codes = list(TARGET_MARKET_CODES.keys())
        else:
            endpoints = [
                e for e in ENDPOINTS
                if report_types is None or e["report_type"] in report_types
            ]
            market_codes = None

        results: dict[str, int] = {}
        for endpoint in endpoints:
            report_type = endpoint["report_type"]
            try:
                rows = self.fetch_report_type(
                    report_type, mode=mode, market_codes=market_codes, targeted=targeted,
                )
                results[report_type] = rows
                self.logger.info(f"[{self.source}] {report_type}: upserted {rows} row(s)")
            except Exception as e:
                self.logger.error(f"[{self.source}] {report_type} failed: {e}")

        return results

    def fetch_report_type(
        self,
        report_type: str,
        mode: str = "update",
        market_codes: list[str] | None = None,
        targeted: bool = False,
    ) -> int:
        """Fetch and upsert one report type's rows for the given mode/filters.

        Logs via _log_fetch() directly (see module docstring) rather than run(),
        since one call can touch many series at once.
        """
        endpoint = next(e for e in ENDPOINTS if e["report_type"] == report_type)
        cutoff = self._resolve_cutoff(report_type, mode)
        today = date.today().isoformat()

        raw_rows = self._fetch_all_rows(endpoint["url"], cutoff, market_codes)
        if not raw_rows:
            return 0

        rows_upserted, first_series_id = self._upsert_rows(endpoint, raw_rows, report_type, targeted)

        first_date = raw_rows[0].get("report_date_as_yyyy_mm_dd", today)[:10]
        if first_series_id is not None:
            self._log_fetch(
                from_date=first_date,
                to_date=today,
                series_id=first_series_id,
                rows_inserted=rows_upserted,
                status="success",
            )
        return rows_upserted

    # ── BaseCollector implementation ──────────────────────────────────────────

    def collect(
        self,
        from_date: str,
        to_date: str,
        asset_id: int | None = None,
        series_id: int | None = None,
        interval: str | None = None,
    ) -> int:
        """Not used directly — use fetch_report_type() / fetch_all() instead."""
        del from_date, to_date, asset_id, series_id, interval
        raise NotImplementedError("Use fetch_report_type() for COT data.")

    # ── Private helpers ───────────────────────────────────────────────────────

    def _resolve_cutoff(self, report_type: str, mode: str) -> str | None:
        """Return the report_date cutoff for a fetch, or None for a full backfill."""
        two_weeks_ago = (date.today() - TWO_WEEKS).isoformat()

        if mode == "backfill":
            return None
        if mode == "incremental":
            return two_weeks_ago

        last = self._last_db_date(report_type)
        return last if last else two_weeks_ago

    def _last_db_date(self, report_type: str) -> str | None:
        """Return the latest report_date in cot_positions for this report_type, or None."""
        rows = db.query(
            "SELECT MAX(report_date) AS d FROM cot_positions WHERE report_type = ?",
            [report_type],
        )
        val = rows[0]["d"] if rows else None
        return str(val)[:10] if val else None

    def _fetch_page(self, url: str, limit: int, offset: int, where: str | None = None) -> list[dict]:
        """Fetch one page from a Socrata endpoint."""
        params: dict = {"$limit": limit, "$offset": offset, "$order": "report_date_as_yyyy_mm_dd"}
        if where:
            params["$where"] = where
        resp = requests.get(url, params=params, timeout=60)
        resp.raise_for_status()
        return resp.json()

    def _fetch_all_rows(
        self,
        url: str,
        cutoff: str | None,
        market_codes: list[str] | None = None,
    ) -> list[dict]:
        """Fetch all rows for an endpoint, paginating as needed.

        cutoff:       ISO date string — fetch rows >= this date. None means full backfill.
        market_codes: Optional list of cftc_contract_market_code values to restrict to.
        """

        def build_where(cutoff: str | None, market_codes: list[str] | None) -> str | None:
            parts: list[str] = []
            if cutoff is not None:
                parts.append(f"report_date_as_yyyy_mm_dd >= '{cutoff}'")
            if market_codes:
                quoted = ",".join(f"'{c}'" for c in market_codes)
                parts.append(f"cftc_contract_market_code in ({quoted})")
            return " AND ".join(parts) if parts else None

        where = build_where(cutoff, market_codes)

        # Incremental (cutoff only, no backfill): single page is sufficient
        if cutoff is not None and market_codes is None:
            rows = self._fetch_page(url, PAGE_SIZE, 0, where=where)
            self.logger.debug(f"[{self.source}] fetched {len(rows)} rows (since {cutoff})")
            return rows

        # Backfill or targeted: paginate until the API returns fewer rows than PAGE_SIZE
        all_rows: list[dict] = []
        offset = 0
        while True:
            page = self._fetch_page(url, PAGE_SIZE, offset, where=where)
            if not page:
                break
            all_rows.extend(page)
            self.logger.debug(
                f"[{self.source}] page offset={offset:,} -> {len(all_rows):,} rows so far"
            )
            if len(page) < PAGE_SIZE:
                break
            offset += PAGE_SIZE
        return all_rows

    def _ensure_series(self, code: str, name: str) -> int:
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

    def _parse_float(self, val) -> float | None:
        """Convert a CFTC API string value to float, or None if missing/invalid."""
        if val is None or val == "":
            return None
        try:
            return float(val)
        except (ValueError, TypeError):
            return None

    def _build_upsert_sql(self, db_columns: list[str]) -> str:
        """Generate INSERT ... ON CONFLICT DO UPDATE SET SQL for the given columns."""
        col_list = ", ".join(db_columns)
        placeholders = ", ".join(["?"] * len(db_columns))
        pk = {"series_id", "report_date", "report_type"}
        updates = ",\n                ".join(
            f"{c} = excluded.{c}" for c in db_columns if c not in pk
        )
        return f"""
            INSERT INTO cot_positions ({col_list})
            VALUES ({placeholders})
            ON CONFLICT (series_id, report_date, report_type) DO UPDATE SET
                {updates}
        """

    def _upsert_rows(
        self,
        endpoint: dict,
        raw_rows: list[dict],
        report_type: str,
        targeted: bool,
    ) -> tuple[int, int | None]:
        """Parse and upsert raw CFTC rows for one report type.

        Returns (rows_upserted, first_series_id) — first_series_id is the
        representative series used for the audit log row, or None if no
        row carried a usable report_date.
        """
        code_prefix = endpoint["code_prefix"]
        oi_field = endpoint["oi_field"]
        field_map = REPORT_TYPE_FIELD_MAP[report_type]

        shared_cols = [
            "series_id", "report_date", "release_date", "report_type",
            "market_name", "cftc_market_code", "cftc_commodity_code",
            "exchange_name", "commodity", "open_interest",
        ]
        type_cols = list(field_map.values())
        all_cols = shared_cols + type_cols
        upsert_sql = self._build_upsert_sql(all_cols)

        series_cache: dict[str, int] = {}
        param_rows: list[list] = []
        first_series_id: int | None = None

        for row in raw_rows:
            if targeted:
                mkt_code = row.get("cftc_contract_market_code") or row.get("cftc_market_code", "UNKNOWN")
            else:
                mkt_code = row.get("cftc_market_code") or row.get("cftc_contract_market_code", "UNKNOWN")

            series_code = f"{code_prefix}.{mkt_code}"
            market_name = row.get("market_and_exchange_names", "")

            if series_code not in series_cache:
                series_cache[series_code] = self._ensure_series(series_code, market_name)
            series_id = series_cache[series_code]
            if first_series_id is None:
                first_series_id = series_id

            report_date_str = row.get("report_date_as_yyyy_mm_dd", "")[:10]
            if not report_date_str:
                continue
            try:
                release_date_str = (date.fromisoformat(report_date_str) + timedelta(days=3)).isoformat()
            except ValueError:
                continue

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
                self._parse_float(row.get(oi_field)),
            ]
            type_vals = [self._parse_float(row.get(api_field)) for api_field in field_map]
            param_rows.append(shared_vals + type_vals)

        BATCH = 500
        rows_upserted = 0
        for batch_start in range(0, len(param_rows), BATCH):
            batch = param_rows[batch_start: batch_start + BATCH]

            def steps(q, _batch=batch) -> None:
                for params in _batch:
                    q(upsert_sql, params)

            db.transaction(steps)
            rows_upserted += len(batch)

        return rows_upserted, first_series_id
