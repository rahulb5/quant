  // Add this entry to the end of the MIGRATIONS array in src/db/migrations.ts

  {
    version: 7,
    description: "Create cot_positions table for CFTC COT, Disaggregated, and TFF reports",
    sql: `
      CREATE TABLE IF NOT EXISTS cot_positions (

        -- ── Identity ─────────────────────────────────────────────────────────
        -- series_id references macro_series.id
        -- Code convention:
        --   CFTC.LEGACY_FUT.<cftc_market_code>
        --   CFTC.LEGACY_COMB.<cftc_market_code>
        --   CFTC.DISAGG_FUT.<cftc_market_code>
        --   CFTC.TFF_FUT.<cftc_market_code>
        series_id               INTEGER NOT NULL REFERENCES macro_series(id),
        report_type             TEXT    NOT NULL CHECK (report_type IN (
                                  'legacy_futures', 'legacy_combined',
                                  'disaggregated_futures', 'tff_futures'
                                )),

        -- ── Dates ─────────────────────────────────────────────────────────────
        report_date             DATE    NOT NULL,  -- Tuesday (as-of date)
        release_date            DATE    NOT NULL,  -- Friday (CFTC publication date)

        -- ── Contract metadata ─────────────────────────────────────────────────
        market_name             TEXT    NOT NULL,
        cftc_market_code        TEXT    NOT NULL,
        cftc_commodity_code     TEXT,
        exchange_name           TEXT,
        commodity_group         TEXT,
        commodity_subgroup      TEXT,
        commodity               TEXT,
        fut_only_or_combined    TEXT    NOT NULL CHECK (fut_only_or_combined IN ('F', 'FO')),

        -- ── Open interest ─────────────────────────────────────────────────────
        open_interest           DOUBLE,

        -- ── LEGACY columns (null for disaggregated / tff rows) ────────────────
        -- Non-commercial
        leg_nc_long             DOUBLE,
        leg_nc_short            DOUBLE,
        leg_nc_spread           DOUBLE,
        leg_nc_traders_long     INTEGER,
        leg_nc_traders_short    INTEGER,
        leg_nc_traders_spread   INTEGER,
        -- Commercial
        leg_comm_long           DOUBLE,
        leg_comm_short          DOUBLE,
        leg_comm_traders_long   INTEGER,
        leg_comm_traders_short  INTEGER,
        -- Non-reportable
        leg_nr_long             DOUBLE,
        leg_nr_short            DOUBLE,

        -- ── DISAGGREGATED columns (null for legacy / tff rows) ────────────────
        -- Producer / Merchant / Processor / User
        dis_pmpu_long           DOUBLE,
        dis_pmpu_short          DOUBLE,
        dis_pmpu_spread         DOUBLE,
        dis_pmpu_traders_long   INTEGER,
        dis_pmpu_traders_short  INTEGER,
        dis_pmpu_traders_spread INTEGER,
        -- Swap Dealers
        dis_sd_long             DOUBLE,
        dis_sd_short            DOUBLE,
        dis_sd_spread           DOUBLE,
        dis_sd_traders_long     INTEGER,
        dis_sd_traders_short    INTEGER,
        dis_sd_traders_spread   INTEGER,
        -- Managed Money
        dis_mm_long             DOUBLE,
        dis_mm_short            DOUBLE,
        dis_mm_spread           DOUBLE,
        dis_mm_traders_long     INTEGER,
        dis_mm_traders_short    INTEGER,
        dis_mm_traders_spread   INTEGER,
        -- Other Reportable
        dis_or_long             DOUBLE,
        dis_or_short            DOUBLE,
        dis_or_spread           DOUBLE,
        dis_or_traders_long     INTEGER,
        dis_or_traders_short    INTEGER,
        dis_or_traders_spread   INTEGER,
        -- Non-reportable
        dis_nr_long             DOUBLE,
        dis_nr_short            DOUBLE,

        -- ── TFF columns (null for legacy / disaggregated rows) ────────────────
        -- Dealer / Intermediary
        tff_dealer_long           DOUBLE,
        tff_dealer_short          DOUBLE,
        tff_dealer_spread         DOUBLE,
        tff_dealer_traders_long   INTEGER,
        tff_dealer_traders_short  INTEGER,
        tff_dealer_traders_spread INTEGER,
        -- Asset Manager / Institutional
        tff_am_long               DOUBLE,
        tff_am_short              DOUBLE,
        tff_am_spread             DOUBLE,
        tff_am_traders_long       INTEGER,
        tff_am_traders_short      INTEGER,
        tff_am_traders_spread     INTEGER,
        -- Leveraged Funds
        tff_lf_long               DOUBLE,
        tff_lf_short              DOUBLE,
        tff_lf_spread             DOUBLE,
        tff_lf_traders_long       INTEGER,
        tff_lf_traders_short      INTEGER,
        tff_lf_traders_spread     INTEGER,
        -- Other Reportable
        tff_or_long               DOUBLE,
        tff_or_short              DOUBLE,
        tff_or_spread             DOUBLE,
        tff_or_traders_long       INTEGER,
        tff_or_traders_short      INTEGER,
        tff_or_traders_spread     INTEGER,
        -- Non-reportable
        tff_nr_long               DOUBLE,
        tff_nr_short              DOUBLE,

        -- ── Audit ─────────────────────────────────────────────────────────────
        fetched_at              TIMESTAMPTZ NOT NULL DEFAULT now(),

        PRIMARY KEY (series_id, report_type, report_date)
      );
    `,
  },
