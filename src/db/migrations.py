"""
src/db/migrations.py

Versioned schema migrations. Each entry is applied exactly once,
in order, and never modified after being committed.

To make a schema change: add a NEW entry at the bottom.
Never edit an existing migration — always append.
"""

from dataclasses import dataclass


@dataclass
class Migration:
    version: int
    description: str
    sql: str


MIGRATIONS: list[Migration] = [
    Migration(
        version=1,
        description="Create schema_version table",
        sql="""
        CREATE TABLE IF NOT EXISTS schema_version (
            version     INTEGER NOT NULL,
            description TEXT    NOT NULL,
            applied_at  TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """,
    ),
    Migration(
        version=2,
        description="Create assets table",
        sql="""
        CREATE TABLE IF NOT EXISTS assets (
            id          INTEGER PRIMARY KEY,
            ticker      TEXT    NOT NULL,
            name        TEXT    NOT NULL,
            asset_class TEXT    NOT NULL CHECK (asset_class IN (
                          'equity', 'crypto', 'forex', 'commodity', 'futures', 'index'
                        )),
            exchange    TEXT,
            currency    TEXT    NOT NULL DEFAULT 'USD',
            is_active   BOOLEAN NOT NULL DEFAULT true,
            created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),

            UNIQUE (ticker, exchange)
        );
        """,
    ),
    Migration(
        version=3,
        description="Create prices table",
        sql="""
        CREATE TABLE IF NOT EXISTS prices (
            asset_id    INTEGER     NOT NULL REFERENCES assets(id),
            interval    TEXT        NOT NULL CHECK (interval IN (
                          '1m', '5m', '15m', '30m', '1h', '4h', '1d', '1w', '1M'
                        )),
            timestamp   TIMESTAMPTZ NOT NULL,
            open        DOUBLE      NOT NULL,
            high        DOUBLE      NOT NULL,
            low         DOUBLE      NOT NULL,
            close       DOUBLE      NOT NULL,
            volume      DOUBLE      NOT NULL DEFAULT 0,
            adj_close   DOUBLE,
            vwap        DOUBLE,
            source      TEXT        NOT NULL,

            PRIMARY KEY (asset_id, interval, timestamp)
        );
        """,
    ),
    Migration(
        version=4,
        description="Create macro_series table",
        sql="""
        CREATE TABLE IF NOT EXISTS macro_series (
            id              INTEGER PRIMARY KEY,
            code            TEXT    NOT NULL UNIQUE,
            name            TEXT    NOT NULL,
            source          TEXT    NOT NULL,
            frequency       TEXT    NOT NULL CHECK (frequency IN (
                              'daily', 'weekly', 'monthly', 'quarterly', 'annual'
                            )),
            units           TEXT    NOT NULL,
            seasonal_adj    BOOLEAN NOT NULL DEFAULT false,
            description     TEXT,
            created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """,
    ),
    Migration(
        version=5,
        description="Create macro_observations table",
        sql="""
        CREATE TABLE IF NOT EXISTS macro_observations (
            series_id       INTEGER NOT NULL REFERENCES macro_series(id),
            period_date     DATE    NOT NULL,
            release_date    DATE    NOT NULL,
            value           DOUBLE  NOT NULL,
            is_final        BOOLEAN NOT NULL DEFAULT false,

            PRIMARY KEY (series_id, period_date, release_date)
        );
        """,
    ),
    Migration(
        version=6,
        description="Create data_fetch_log table",
        sql="""
        CREATE TABLE IF NOT EXISTS data_fetch_log (
            id              INTEGER PRIMARY KEY,
            asset_id        INTEGER REFERENCES assets(id),
            series_id       INTEGER REFERENCES macro_series(id),
            interval        TEXT,
            fetched_from    DATE    NOT NULL,
            fetched_to      DATE    NOT NULL,
            fetched_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
            source          TEXT    NOT NULL,
            rows_inserted   INTEGER NOT NULL DEFAULT 0,
            status          TEXT    NOT NULL CHECK (status IN ('success', 'partial', 'failed')),
            error_msg       TEXT,

            CHECK (asset_id IS NOT NULL OR series_id IS NOT NULL)
        );
        """,
    ),
    Migration(
        version=7,
        description="Create cot_positions table",
        sql="""
        CREATE TABLE IF NOT EXISTS cot_positions (
            series_id           INTEGER NOT NULL REFERENCES macro_series(id),
            report_date         DATE    NOT NULL,
            release_date        DATE    NOT NULL,
            report_type         TEXT    NOT NULL CHECK (report_type IN (
                                  'legacy_fut', 'legacy_comb', 'disagg_fut', 'tff_fut'
                                )),
            market_name         TEXT,
            cftc_market_code    TEXT,
            cftc_commodity_code TEXT,
            exchange_name       TEXT,
            commodity           TEXT,
            open_interest       DOUBLE,

            -- Legacy Futures / Combined
            leg_nc_long             DOUBLE, leg_nc_short            DOUBLE, leg_nc_spread           DOUBLE,
            leg_comm_long           DOUBLE, leg_comm_short          DOUBLE,
            leg_nr_long             DOUBLE, leg_nr_short            DOUBLE,
            leg_nc_traders_long     DOUBLE, leg_nc_traders_short    DOUBLE, leg_nc_traders_spread   DOUBLE,
            leg_comm_traders_long   DOUBLE, leg_comm_traders_short  DOUBLE,

            -- Disaggregated Futures
            dis_pmpu_long   DOUBLE, dis_pmpu_short  DOUBLE,
            dis_sd_long     DOUBLE, dis_sd_short    DOUBLE, dis_sd_spread   DOUBLE,
            dis_mm_long     DOUBLE, dis_mm_short    DOUBLE, dis_mm_spread   DOUBLE,
            dis_or_long     DOUBLE, dis_or_short    DOUBLE, dis_or_spread   DOUBLE,
            dis_nr_long     DOUBLE, dis_nr_short    DOUBLE,
            dis_pmpu_traders_long   DOUBLE, dis_pmpu_traders_short  DOUBLE,
            dis_sd_traders_long     DOUBLE, dis_sd_traders_short    DOUBLE, dis_sd_traders_spread   DOUBLE,
            dis_mm_traders_long     DOUBLE, dis_mm_traders_short    DOUBLE, dis_mm_traders_spread   DOUBLE,
            dis_or_traders_long     DOUBLE, dis_or_traders_short    DOUBLE, dis_or_traders_spread   DOUBLE,

            -- Traders Financial Futures
            tff_dealer_long     DOUBLE, tff_dealer_short    DOUBLE, tff_dealer_spread    DOUBLE,
            tff_am_long         DOUBLE, tff_am_short        DOUBLE, tff_am_spread        DOUBLE,
            tff_lf_long         DOUBLE, tff_lf_short        DOUBLE, tff_lf_spread        DOUBLE,
            tff_or_long         DOUBLE, tff_or_short        DOUBLE, tff_or_spread        DOUBLE,
            tff_nr_long         DOUBLE, tff_nr_short        DOUBLE,
            tff_dealer_traders_long     DOUBLE, tff_dealer_traders_short    DOUBLE, tff_dealer_traders_spread    DOUBLE,
            tff_am_traders_long         DOUBLE, tff_am_traders_short        DOUBLE, tff_am_traders_spread        DOUBLE,
            tff_lf_traders_long         DOUBLE, tff_lf_traders_short        DOUBLE, tff_lf_traders_spread        DOUBLE,
            tff_or_traders_long         DOUBLE, tff_or_traders_short        DOUBLE, tff_or_traders_spread        DOUBLE,

            PRIMARY KEY (series_id, report_date, report_type)
        );
        """,
    ),
]
