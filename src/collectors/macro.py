"""
src/collectors/macro.py

Macroeconomic data collector using the FRED API.

Fetches all vintage releases for non-daily series (so revision history is
preserved — see macro_observations.is_final) and a plain observation series
for daily series, which have no meaningful revisions and would otherwise hit
FRED's 2000-vintage limit via get_series_all_releases().

Usage:
    from src.db.client import db
    from src.collectors.macro import MacroCollector

    db.open()
    collector = MacroCollector()
    collector.fetch_all()                      # all catalogued series, 30y lookback
    collector.fetch_all(full_history=True)
    db.close()
"""

from __future__ import annotations

import os
from datetime import date, timedelta

import pandas as pd
from fredapi import Fred

from src.collectors.base import BaseCollector
from src.db.client import db

FULL_HISTORY_SENTINEL = "1900-01-01"

# ── Series catalogue ──────────────────────────────────────────────────────────

SERIES_CODES: list[str] = [
    # ── Daily financial ──
    "DFF", "T10Y2Y", "T10YIE", "DTWEXBGS", "BAMLH0A0HYM2",
    # Treasury yield curve (daily)
    "DGS2", "DGS5", "DGS10", "DGS30",
    # Credit spreads (daily)
    "BAMLC0A0CM", "BAMLC0A4CBBB",
    # Volatility (daily)
    "VIXCLS",

    # ── Weekly ──
    "ICSA", "WM2NS",
    # Fed balance sheet (weekly)
    "WALCL",

    # ── Monthly ──
    "CPIAUCSL", "CPILFESL", "UNRATE", "PAYEMS", "INDPRO",
    "RETAILSMNSA", "HOUST", "PCE",
    # Additional monthly macro
    "CSUSHPINSA", "DGORDER", "JTSJOL", "RSXFS",
    # Money/credit (monthly)
    "TOTRESNS",
    # Survey / sentiment (monthly)
    "UMCSENT", "MICH",
    # International (monthly)
    "USALOLITONOSTSAM",   # US Leading Indicator (OECD CLI)
    "OECDLOLITONOSTSAM",  # OECD Composite Leading Indicator (OECD - Total)
    "CPALTT01CNM657N",    # China CPI
    "CP0000EZ19M086NEST", # Eurozone CPI (HICP, index level)
    "CPALTT01JPM659N",    # Japan CPI
    "CPALTT01GBM659N",    # UK CPI

    # ── Quarterly ──
    "GDP", "GDPCTPI",
]

# FRED periodically retires/renames series codes (see e.g. the OECD MEI and
# international CPI series below). Map each SERIES_CODES entry to known prior
# codes for the same underlying series, newest-first, so ensure_series() can
# fall back automatically instead of aborting the whole run. When you hit a
# "series does not exist" error for a code below, find its replacement on
# FRED, move the old code here as a fallback, and put the new one in
# SERIES_CODES.
SERIES_ALIASES: dict[str, list[str]] = {
    "OECDLOLITONOSTSAM": ["OABORAGNOSTSAM"],
    "CP0000EZ19M086NEST": ["CPALTT01EZM659N"],
}

# Maps FRED's free-text frequency to our schema's allowed values
FREQ_MAP: dict[str, str] = {
    "Daily":      "daily",
    "Weekly":     "weekly",
    "Biweekly":   "weekly",
    "Monthly":    "monthly",
    "Quarterly":  "quarterly",
    "Semiannual": "annual",
    "Annual":     "annual",
}


class MacroCollector(BaseCollector):
    """Collects macroeconomic series (with revision history) from FRED."""

    def __init__(self) -> None:
        super().__init__(source="FRED")
        api_key = os.environ.get("FRED_API_KEY")
        if not api_key:
            raise RuntimeError("FRED_API_KEY is not set. Add it to your .env file.")
        self.fred = Fred(api_key=api_key)

    # ── Public interface ──────────────────────────────────────────────────────

    @staticmethod
    def get_all_series_codes() -> list[str]:
        return list(SERIES_CODES)

    def ensure_series(self, code: str) -> int:
        """Return the series_id for code, inserting FRED metadata if not present.

        Falls back through SERIES_ALIASES[code] if `code` itself is no longer
        recognized by FRED (retired/renamed series), and registers the row
        under whichever code actually resolved.
        """
        candidates = [code] + SERIES_ALIASES.get(code, [])

        for candidate in candidates:
            rows = db.query("SELECT id FROM macro_series WHERE code = ?", [candidate])
            if rows:
                return int(rows[0]["id"])

        info = None
        resolved_code = None
        last_error: Exception | None = None
        for candidate in candidates:
            try:
                info = self.fred.get_series_info(candidate)
                resolved_code = candidate
                break
            except ValueError as e:
                last_error = e
                self.logger.warning(f"[{self.source}] {candidate}: not found on FRED ({e})")
                continue

        if info is None:
            raise last_error  # all known codes/aliases are dead

        if resolved_code != code:
            self.logger.warning(
                f"[{self.source}] {code} no longer resolves on FRED; "
                f"falling back to alias {resolved_code}"
            )

        frequency_raw = str(info.get("frequency", "Daily"))
        frequency = FREQ_MAP.get(frequency_raw, "daily")

        seasonal_short = str(info.get("seasonal_adjustment_short", "NSA"))
        seasonal_adj = seasonal_short in ("SA", "SAAR", "SAAQ")

        notes = info.get("notes", None)
        description = str(notes) if pd.notna(notes) else None

        next_id = int(
            db.query("SELECT COALESCE(MAX(id), 0) + 1 AS next_id FROM macro_series")[0]["next_id"]
        )
        db.run(
            """
            INSERT INTO macro_series
              (id, code, name, source, frequency, units, seasonal_adj, description)
            VALUES (?, ?, ?, 'FRED', ?, ?, ?, ?)
            """,
            [
                next_id,
                resolved_code,
                str(info.get("title", resolved_code)),
                frequency,
                str(info.get("units_short", "")),
                seasonal_adj,
                description,
            ],
        )
        self.logger.debug(f"[{self.source}] Registered series {resolved_code} (id={next_id})")
        return next_id

    def fetch_all(
        self,
        codes: list[str] | None = None,
        full_history: bool = False,
    ) -> dict[str, int]:
        """Fetch every catalogued series via run(), tolerating per-series failures.

        Args:
            codes:        FRED codes to fetch. Defaults to all of SERIES_CODES.
            full_history: If True, fetch all available history. Otherwise a
                           30-year lookback window is applied.

        Returns:
            Dict mapping FRED code -> rows_upserted for every series that succeeded.
        """
        codes = codes if codes is not None else SERIES_CODES
        today = date.today().isoformat()
        lookback = (
            FULL_HISTORY_SENTINEL if full_history
            else (date.today() - timedelta(days=365 * 30)).isoformat()
        )
        results: dict[str, int] = {}

        for code in codes:
            try:
                series_id = self.ensure_series(code)
            except Exception as e:
                self.logger.error(f"[{self.source}] {code}: no live series/alias on FRED ({e}); skipping")
                continue

            try:
                rows = self.run(from_date=lookback, to_date=today, series_id=series_id)
                results[code] = rows
                self.logger.info(f"[{self.source}] {code}: upserted {rows} row(s)")
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
        """Fetch observations for series_id and upsert into macro_observations.

        from_date doubles as the lookback cutoff (FULL_HISTORY_SENTINEL for no
        cutoff). Daily series skip vintage fetching (see module docstring).
        """
        if series_id is None:
            raise ValueError("series_id is required for MacroCollector.collect()")

        cutoff = None if from_date == FULL_HISTORY_SENTINEL else from_date

        if self._is_daily(series_id):
            raw_df = self._fetch_daily(series_id, cutoff)
        else:
            raw_df = self._fetch_vintages(series_id, cutoff)

        if raw_df.empty:
            return 0

        return self._upsert_observations(series_id, raw_df)

    # ── Private helpers ───────────────────────────────────────────────────────

    def _get_code(self, series_id: int) -> str:
        rows = db.query("SELECT code FROM macro_series WHERE id = ?", [series_id])
        if not rows:
            raise ValueError(f"No macro_series found with id={series_id}")
        return str(rows[0]["code"])

    def _is_daily(self, series_id: int) -> bool:
        rows = db.query("SELECT frequency FROM macro_series WHERE id = ?", [series_id])
        return bool(rows) and rows[0]["frequency"] == "daily"

    def _fetch_daily(self, series_id: int, cutoff: str | None) -> pd.DataFrame:
        """Fetch a daily series without vintage data.

        Daily series do not have meaningful revisions, so we use get_series()
        instead of get_series_all_releases() (which hits FRED's 2000-vintage
        limit). release_date is set equal to period_date, making is_final
        True for all rows.
        """
        code = self._get_code(series_id)
        s = self.fred.get_series(code, observation_start=cutoff)
        s = s.dropna()
        df = s.reset_index()
        df.columns = pd.Index(["period_date", "value"])
        df["release_date"] = df["period_date"]
        return df

    def _fetch_vintages(self, series_id: int, cutoff: str | None) -> pd.DataFrame:
        """Fetch all vintage releases for a non-daily series."""
        code = self._get_code(series_id)
        raw_df = self.fred.get_series_all_releases(code)
        raw_df = raw_df.rename(columns={"date": "period_date", "realtime_start": "release_date"})
        raw_df = raw_df.dropna(subset=["value"])

        if cutoff:
            cutoff_ts = pd.Timestamp(cutoff)
            raw_df = raw_df[raw_df["period_date"] >= cutoff_ts]

        return raw_df

    def _to_date_str(self, val) -> str:
        return val.isoformat()[:10] if hasattr(val, "isoformat") else str(val)[:10]

    def _upsert_observations(self, series_id: int, df: pd.DataFrame) -> int:
        """Upsert rows into macro_observations and return the row count.

        is_final is set to True only for the row with the MAX release_date
        per period_date; all other vintages are marked False.
        """
        if df.empty:
            return 0

        max_release = df.groupby("period_date")["release_date"].transform("max")
        df = df.copy()
        df["is_final"] = df["release_date"] == max_release

        count = 0

        def steps(q) -> None:
            nonlocal count
            for _, row in df.iterrows():
                q(
                    """
                    INSERT INTO macro_observations
                      (series_id, period_date, release_date, value, is_final)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT (series_id, period_date, release_date) DO UPDATE SET
                        value    = excluded.value,
                        is_final = excluded.is_final
                    """,
                    [
                        series_id,
                        self._to_date_str(row["period_date"]),
                        self._to_date_str(row["release_date"]),
                        float(row["value"]),
                        bool(row["is_final"]),
                    ],
                )
                count += 1

        db.transaction(steps)
        return count
