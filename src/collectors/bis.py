"""
src/collectors/bis.py

Central bank policy rates from the BIS (dataflow WS_CBPOL, daily, end of
period), via the public SDMX API. No key needed.

Stored like FRED's unrevised daily market series: one row per
(series, period_date) in macro_observations with release_date = period_date,
so load_fred() and the macro explorer read them unchanged. A policy rate is
public on the day it applies, so there is no release lag to model.

Codes are "BIS.CBPOL.<area>", e.g. BIS.CBPOL.XM (euro area, from 1999). The
euro area series starts in 1999; BIS.CBPOL.DE (Bundesbank) covers 1948-1998.
Splicing the two is left to the consumer (the returns layer), not stored.

BIS notes rate-definition changes per area (e.g. the ECB steering rate moved
from MRO to the deposit facility rate on 2024-09-18); see the COMPILATION
field on data.bis.org.

Usage:
    from src.db.client import db
    from src.collectors.bis import BisPolicyRateCollector

    db.open()
    BisPolicyRateCollector().fetch_all()              # incremental (last 60 days)
    BisPolicyRateCollector().fetch_all(full_history=True)
    db.close()
"""

from __future__ import annotations

import io
from datetime import date, timedelta

import pandas as pd
import requests

from src.collectors.base import BaseCollector
from src.db.client import db

BASE_URL = "https://stats.bis.org/api/v1/data/WS_CBPOL"
FULL_HISTORY_SENTINEL = "1900-01-01"
INCREMENTAL_DAYS = 60       # re-fetch window; BIS publishes weekly
REQUEST_TIMEOUT = 120

# BIS REF_AREA -> name. G10 currencies plus DKK; DE is the pre-euro DEM rate.
POLICY_RATE_AREAS: dict[str, str] = {
    "US": "United States",
    "XM": "Euro area",
    "DE": "Germany (pre-euro, to 1998)",
    "JP": "Japan",
    "GB": "United Kingdom",
    "CH": "Switzerland",
    "CA": "Canada",
    "AU": "Australia",
    "NZ": "New Zealand",
    "SE": "Sweden",
    "NO": "Norway",
    "DK": "Denmark",
}


def series_code(area: str) -> str:
    return f"BIS.CBPOL.{area}"


class BisPolicyRateCollector(BaseCollector):
    """Collects daily central bank policy rates from the BIS."""

    def __init__(self) -> None:
        super().__init__(source="BIS")

    # ── Public interface ──────────────────────────────────────────────────────

    def ensure_series(self, area: str) -> int:
        """Return the series_id for an area, registering it if needed."""
        if area not in POLICY_RATE_AREAS:
            raise ValueError(f"Unknown BIS area {area!r}; expected one of {list(POLICY_RATE_AREAS)}")
        code = series_code(area)
        rows = db.query("SELECT id FROM macro_series WHERE code = ?", [code])
        if rows:
            return int(rows[0]["id"])

        next_id = int(
            db.query("SELECT COALESCE(MAX(id), 0) + 1 AS next_id FROM macro_series")[0]["next_id"]
        )
        db.run(
            """
            INSERT INTO macro_series
              (id, code, name, source, frequency, units, seasonal_adj, description)
            VALUES (?, ?, ?, 'BIS', 'daily', 'Percent', false, ?)
            """,
            [
                next_id,
                code,
                f"Central bank policy rate: {POLICY_RATE_AREAS[area]}",
                "BIS WS_CBPOL, daily, end of period. Rate definitions change over time; "
                "see the series' compilation notes on data.bis.org.",
            ],
        )
        self.logger.debug(f"[{self.source}] Registered series {code} (id={next_id})")
        return next_id

    def fetch_all(
        self,
        areas: list[str] | None = None,
        full_history: bool = False,
    ) -> dict[str, int]:
        """Fetch every area via run(), tolerating per-area failures.

        One HTTP request per area keeps failures isolated and the fetch log
        per series; there are only a dozen areas.

        Returns:
            Dict mapping area -> rows upserted for every area that succeeded.
        """
        areas = areas if areas is not None else list(POLICY_RATE_AREAS)
        today = date.today().isoformat()
        start = (
            FULL_HISTORY_SENTINEL if full_history
            else (date.today() - timedelta(days=INCREMENTAL_DAYS)).isoformat()
        )
        results: dict[str, int] = {}
        for area in areas:
            series_id = self.ensure_series(area)
            try:
                results[area] = self.run(from_date=start, to_date=today, series_id=series_id)
            except Exception:
                continue  # run() already logged + audited the failure
        return results

    # ── BaseCollector implementation ──────────────────────────────────────────

    def collect(
        self,
        from_date: str,
        to_date: str,
        asset_id: int | None = None,
        series_id: int | None = None,
        interval: str | None = None,
    ) -> int:
        if series_id is None:
            raise ValueError("series_id is required for BisPolicyRateCollector.collect()")
        area = self._get_area(series_id)
        start = None if from_date == FULL_HISTORY_SENTINEL else from_date
        df = parse_csv(self._fetch_csv(area, start, to_date))
        if df.empty:
            return 0
        return self._upsert_observations(series_id, df)

    # ── Private helpers ───────────────────────────────────────────────────────

    def _get_area(self, series_id: int) -> str:
        rows = db.query("SELECT code FROM macro_series WHERE id = ?", [series_id])
        if not rows:
            raise ValueError(f"No macro_series found with id={series_id}")
        return str(rows[0]["code"]).rsplit(".", 1)[-1]

    def _fetch_csv(self, area: str, start: str | None, end: str) -> str:
        params = {"format": "csv", "detail": "dataonly", "endPeriod": end}
        if start:
            params["startPeriod"] = start
        resp = requests.get(f"{BASE_URL}/D.{area}/all", params=params, timeout=REQUEST_TIMEOUT)
        if resp.status_code == 404:   # SDMX: no observations in the window
            return ""
        resp.raise_for_status()
        return resp.text

    def _upsert_observations(self, series_id: int, df: pd.DataFrame) -> int:
        df = df.copy()
        df.insert(0, "series_id", series_id)
        db.run_df(
            """
            INSERT INTO macro_observations
              (series_id, period_date, release_date, value, is_final)
            SELECT series_id, period_date, period_date, value, true FROM obs
            ON CONFLICT (series_id, period_date, release_date) DO UPDATE SET
                value = excluded.value
            """,
            df,
            name="obs",
        )
        return len(df)


def parse_csv(text: str) -> pd.DataFrame:
    """BIS SDMX CSV -> columns period_date (date), value (float); missing days dropped."""
    if not text.strip():
        return pd.DataFrame(columns=["period_date", "value"])
    raw = pd.read_csv(io.StringIO(text))
    out = pd.DataFrame({
        "period_date": pd.to_datetime(raw["TIME_PERIOD"]).dt.date,
        "value": pd.to_numeric(raw["OBS_VALUE"], errors="coerce"),
    })
    return out.dropna(subset=["value"]).drop_duplicates("period_date", keep="last").reset_index(drop=True)
