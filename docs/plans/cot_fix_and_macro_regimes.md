# COT collector fix + macro regime layer — implementation plan

**Status:** approved by the researcher on 2026-10-22 (decisions below). Part A complete (A1–A5 done, A6 docs updated); Part B not started and **superseded (proposed) by `docs/plans/macro_research_layer.md`**. See "Part A outcome" below.
**Read first:** `CLAUDE.md` (Python env + DB conventions), `docs/architecture.md` (repo map),
`docs/plans/dashboard_v1.md` (dashboard conventions), `quant_research_os_build_plan.md` Sections 1.2
(research first) and 6 (research data integrity).

## Goal

Build out macro analysis using only free data, aimed at research rather than charts. Two parts,
done in order:

- **Part A — fix the COT collector**, so positioning data for rates, FX, equity-index and the
  wider commodity set is usable. Today only 11 series are valid.
- **Part B — a macro regime layer** in `src/macro/`. It produces regime labels that use only data
  available on each date, plugs them into the existing `Conditioner` so every experiment gets
  macro-regime diagnostics, and adds a regime page to the dashboard's Macro section.

Commodity futures curves (carry, roll-adjusted prices) are out of scope. They wait on a paid data
source; see Section 6.

## Decisions already made (don't re-litigate)

1. **Free data only:** FRED (already collected, with vintages), CFTC COT, and Yahoo prices.
2. **Research first.** Each dashboard view answers a written question. Regime definitions are
   fixed *before* looking at how assets or strategies perform in them (see B7).
3. **Regimes reuse `src/backtest/regimes.py`'s `Regime` base class**, so `Conditioner`,
   `run_backtest()` and the Diagnostics tab need no new plumbing.
4. **Regimes that feed backtests use first-release data, aligned on release dates.** Latest-vintage
   data stays available for the explorer only.
5. **The dashboard stays read-only**, with short-lived read-only DB handles cached on the DB
   file's modification time, as in `dashboard/components.py`.

## Decisions (answered by the researcher on 2026-10-22)

| # | Decision |
|---|---|
| D1 | **Curated catalog of ~35 contracts** for now. The catalog is the extension point: adding a contract later is one catalog entry plus a backfill for that code. |
| D2 | **Approved:** delete the exchange-keyed rows (99 series, ~50k rows) after a file backup. |
| D3 | **Opt-in flag, default on.** `diagnostics.regimes` in the config, defaulting to every registered regime; a config can narrow or empty it. |
| D4 | **Yield-based Treasury proxy** for bond returns. ETFs are deferred. |

## Current state (verified 2026-10-22, commit `cb1c0c2`)

**COT (`cot_positions`, 110 CFTC series in `macro_series`):**

| report_type | series | rows | history | contract-keyed rows |
|---|---|---|---|---|
| legacy_fut | 34 | 19,356 | 1986– | 0 |
| legacy_comb | 32 | 16,194 | 1995– | 0 |
| disagg_fut | 32 | 21,166 | 2006– | 11,429 (the 11 targeted commodities) |
| tff_fut | 12 | 4,608 | 2006– | 0 |

- **The bug** is in `src/collectors/cot.py` `_upsert_rows()`. When `targeted=False`, the series
  code is built from `cftc_market_code`, which is the **exchange** code (`NYME`, `CBT`, ...), not
  `cftc_contract_market_code` (the 6-character contract code). Every contract on an exchange
  collapses into one `series_id`. Because the primary key is `(series_id, report_date,
  report_type)`, only the last-written contract per week survives.
- **Second defect:** some codes carry trailing whitespace from the API (`CFTC.DISAGG_FUT.CBT ` and
  `CFTC.DISAGG_FUT.CBT` both exist). Codes must be trimmed.
- **EXP-0001 is unaffected.** `src/signals/cot_commercial_positioning.py` reads only the 11
  contract-keyed `disagg_fut` series. Re-running it after the fix is the regression check.
- `release_date = report_date + 3 days` (Tuesday → Friday). In holiday weeks the CFTC publishes on
  Monday instead. For weekly Monday rebalancing (EXP-0001 uses `W-MON`) the data is still
  available in time, so no change is needed. Note it in the docstring.

**FRED (`macro_observations`, 48 series):**

- Non-daily series store every vintage, so `load_fred(vintage="first")` gives real first-release
  dates. Median lag from period to release is ~35–50 days for CPI, payrolls, unemployment and
  industrial production.
- **Pitfall 1: history before the vintage era.** For periods before FRED/ALFRED kept vintages, the
  "first release" is the earliest vintage. Example: CPI for 1947 shows as released in 1972.
  Aligning on `release_date` handles this correctly: that history simply becomes available in
  1972. But it means first-release regimes can't start before roughly the 1960s–70s.
- **Pitfall 2: weekly claims.** `ICSA` is stored as `daily` with `release_date == period_date`,
  but claims for the week ending Saturday are published the following Thursday. Shift it by +5
  days in the alignment layer (a per-series override), or the regime looks ahead.
- Daily market series (`VIXCLS`, `BAMLH0A0HYM2`, `DGS*`, `T10Y2Y`, `DFF`) have no revisions and a
  lag of 0, which is correct.

---

## Part A — COT collector fix

### A1. Contract catalog (`src/collectors/cot.py`)

Replace `TARGET_MARKET_CODES` with a `COT_CONTRACTS` catalog: one entry per contract, giving the
CFTC contract code, display name, asset group (`rates`, `equity_index`, `fx`, `vol`, `energy`,
`metals`, `ags`, `softs`) and the report types it belongs to. Starting set:

- **Existing 11 commodities:** codes unchanged, `disagg_fut` + `legacy_fut` + `legacy_comb`.
- **Rates** (`tff_fut` + legacy): UST 2Y, 5Y, 10Y, Ultra 10Y, Bond, Ultra Bond, 3M SOFR,
  30-day Fed funds.
- **Equity index** (`tff_fut` + legacy): E-mini S&P 500, E-mini Nasdaq-100, E-mini Russell 2000,
  mini Dow.
- **FX** (`tff_fut` + legacy): EUR, JPY, GBP, CHF, CAD, AUD, MXN, USD index.
- **Vol** (`tff_fut` + legacy): VIX futures.

Before hardcoding, **confirm each code with a discovery query** (see the sketch below): query the
Socrata endpoints for `cftc_contract_market_code` and `market_and_exchange_names` over recent
weeks and record the exact codes. Some codes contain letters, e.g. `13874A` for the E-mini S&P
500 and `1170E1` for VIX. Contracts are sometimes relisted under new codes, e.g. the Eurodollar
to SOFR transition. Where history matters, note the predecessor code in the catalog entry, but
don't splice series in this plan.

Discovery-query sketch (TFF endpoint shown; run it for each endpoint):

```sql
-- Socrata SoQL against https://publicreporting.cftc.gov/resource/gpe5-46if.json
SELECT cftc_contract_market_code, market_and_exchange_names, MAX(report_date_as_yyyy_mm_dd)
WHERE report_date_as_yyyy_mm_dd >= '2026-09-01'
GROUP BY cftc_contract_market_code, market_and_exchange_names
```

### A2. Collector changes

- **Always key by contract.** `mkt_code = row["cftc_contract_market_code"].strip()`. Drop the
  `targeted` branch in `_upsert_rows()`. Store the contract code in the `cftc_market_code` column,
  and document in the migration comment and the module docstring that the column now holds the
  contract code. The exchange is already in `exchange_name`.
- **Filter to the catalog.** `fetch_all()` always passes the catalog's codes for each report type
  as `market_codes`. The `targeted` flag goes away (`scripts/fetch_cot.py` loses `--targeted`).
  Keep a `contracts=` argument so a subset can be fetched.
- **Skip rows with no contract code**, with a warning, rather than writing a series coded
  `UNKNOWN`.
- `_ensure_series()` stores the catalog display name rather than the raw CFTC string.
  Optionally, add the asset group to `macro_series.description`, to avoid a migration.
- **Fix pagination.** The "single page is sufficient" shortcut in `_fetch_all_rows()` no longer
  applies once `market_codes` is always set, because the code paginates whenever filters are
  present. Check that `update` mode still works after a long gap.

### A3. Clean up existing data (D2 approved)

A one-off script, `scripts/migrate_cot_contract_keys.py`. It is idempotent and has a `--dry-run`
flag that prints counts only.

1. Copy `data/quant.db` to `data/quant.db.bak-20261022` (refuse to run if the backup already exists).
2. Find the bad series: CFTC series in `macro_series` whose trimmed third code segment isn't
   6 characters long (the same rule as `list_series()`'s `contract_keyed`).
3. In one transaction, delete their `cot_positions` rows, then the `data_fetch_log` rows that
   reference them (`data_fetch_log.series_id` is a foreign key to `macro_series`), then the
   `macro_series` rows.
4. Print before and after counts for each report type.

This is not a schema migration, so there's no new migration entry. The schema doesn't change.

### A4. Backfill and validate

1. Run `scripts/fetch_cot.py --mode backfill` (catalog only). Estimated volume: ~35 contracts ×
   ~1,050 weeks × up to 3 report types ≈ 70–100k rows, about 10 paginated requests per endpoint.
2. Validation queries, all of which must pass:
   - no duplicate `(series_id, report_date, report_type)` (guaranteed by the primary key, but
     check the counts);
   - each series has ≤ 1 row per week, with gaps only where the CFTC skipped a week, e.g. the
     2019 government shutdown;
   - spot-check 3 contracts (E-mini S&P 500, 10Y note, EUR) for one date against the CFTC's
     published report;
   - `list_series()` reports `contract_keyed = True` for every CFTC row.
3. **Regression check:** re-run EXP-0001 (`scripts/run_experiment.py`) and confirm its headline
   metrics are unchanged. Its series were already contract-keyed.
4. Update `src/macro/data.py`. The `contract_keyed` column and the explorer's "hide
   exchange-keyed" default become no-ops. Keep the column for one release as a safety net, and
   drop the default filter from `dashboard/macro/explorer.py`. Group the explorer's COT picker by
   asset group.

### A5. Tests (`tests/collectors/test_cot.py`)

- Two contracts on the same exchange in one non-targeted payload → two series, both kept.
- A code with trailing whitespace → one trimmed series.
- A row missing a contract code → skipped and a warning logged.
- Only catalog codes are requested: assert the `$where` clause built by `_fetch_all_rows`.
- The migration script on an in-memory DB: exchange-keyed series and their dependent rows are
  removed, contract-keyed ones are left alone, and the dry run changes nothing.

### A6. Docs

Update `docs/architecture.md` with `/update-architecture`: remove the keying bug from Section 3
and note the catalog. Update the `cot-exchange-keying-bug` memory to say it's resolved.

---

## Part B — macro regime layer

> **Superseded (proposed 2026-10-22):** see `docs/plans/macro_research_layer.md`, which blends this
> Part B with `docs/Macro Research Layer — Design Review.md`. Kept here for reference.

### Design

```
FRED (first-release) ─┐
COT (release_date)  ──┼─► src/macro/pit.py ──► src/macro/indicators.py ──► src/macro/regimes.py
Yahoo prices        ──┘   point-in-time        pure derived series          Regime subclasses
                          daily alignment      (growth, inflation, ...)            │
                                                                ┌──────────────────┼────────────────────┐
                                                                ▼                  ▼                    ▼
                                                    Conditioner.by_regime()  src/macro/analysis.py  dashboard/macro/regimes.py
                                                    (every experiment)       asset-by-regime stats   regime page
```

All of `pit.py`, `indicators.py` and `analysis.py` are pure pandas functions with no DB access
and no Streamlit, matching `src/macro/transforms.py`. Only the `Regime` subclasses touch the DB,
through `src/macro/data.py`.

### B1. Point-in-time alignment (`src/macro/pit.py`)

This is the core building block. Every later step depends on it being right.

- `as_of_daily(values, release_dates, index=None, release_lag_days=0) -> pd.Series`: places each
  observation on its release date (plus an optional per-series lag), keeps the latest *period*
  known as of each day, and forward-fills onto a business-day index.
- `load_pit(db, code, extra_lag_days=None)`: `load_fred(..., vintage="first")` followed by
  `as_of_daily`, using a `RELEASE_LAG_OVERRIDES = {"ICSA": 5}` table for series whose stored
  release dates are known to be wrong.
- `staleness(values, release_dates)`: days since the last release, shown in the dashboard so a
  regime built on a 45-day-old print is visible.
- Edge case: when several periods are released on the same date (bulk pre-vintage history), keep
  the latest period.

Tests: truncation invariance, i.e. labels on a prefix of the data are identical to the prefix of
the full labels (mirrors the existing regimes test); the ICSA override shifts the series; a
release on a weekend lands on the next business day.

### B2. New free FRED series (`src/collectors/macro.py` `SERIES_CODES`)

| Code | Why |
|---|---|
| `USREC` | NBER recession indicator. Used only to sanity-check regimes, **never** as an input (it's published with a long lag). |
| `T10Y3M` | 10Y–3M curve, the Fed's preferred recession spread. |
| `NFCI` | Chicago Fed National Financial Conditions Index (weekly, revised, so use first release). |
| `DGS3MO` | 3M constant-maturity bill for the curve and the bond-return proxy. (`DTB3`, `M2SL`, `PPIACO` and a few others stop updating around 2026-02/03 in our DB; check whether the collector is failing on them while adding these.) |
| `SAHMREALTIME` | Real-time Sahm rule, to cross-check our own first-release calculation. |

Backfill with `scripts/fetch_macro.py` for these codes only.

### B3. Indicators (`src/macro/indicators.py`)

Pure functions that take point-in-time daily series. Each returns a daily series, and each
composite uses an expanding z-score (no full-sample normalisation):

- **Growth momentum:** mean z-score of the 6-month change in industrial production growth
  (year on year), 3-month annualised payroll growth, the inverted unemployment gap (3-month
  average minus its 12-month low, i.e. Sahm-style) and inverted 4-week-average claims
  (year on year).
- **Inflation momentum:** mean z-score of 6-month changes in core CPI (3-month annualised) and
  the 10Y breakeven `T10YIE` (from 2003; core CPI alone before that).
- **Financial stress:** level of HY OAS `BAMLH0A0HYM2` (from 1997), cross-checked against `NFCI`.
- **Policy stance:** 6-month change in `DFF`.
- **Curve:** level of `T10Y2Y` / `T10Y3M`, plus 3-month changes in the 10Y yield and the 2s10s
  spread (for bull/bear steepener and flattener states).
- **Dollar:** `DTWEXBGS` against its 200-day moving average (from 2006).
- **Positioning (after Part A):** leveraged-fund net % of open interest, as an expanding z-score,
  for the 10Y note, E-mini S&P 500 and EUR, aligned on COT `release_date`. These are "crowding"
  indicators, not regimes yet.

Each function's docstring states its earliest valid date.

### B4. Regime classifiers (`src/macro/regimes.py`)

These are subclasses of `src.backtest.regimes.Regime`. They reuse `expanding_bucket_labels()` /
`threshold_labels()` where possible.

| Regime `name` | Labels | Rule | Valid from (approx.) |
|---|---|---|---|
| `growth_inflation` | `goldilocks`, `reflation`, `stagflation`, `deflation` | sign of growth momentum × sign of inflation momentum | ~1970s (first-release era) |
| `financial_stress` | `calm`, `normal`, `stressed` | expanding terciles of HY OAS | ~1998 (1-year burn-in) |
| `policy` | `easing`, `on_hold`, `tightening` | 6-month change in `DFF` ≤ −25 bp / between / ≥ +25 bp | 1955 |
| `curve_dynamics` | `bull_steepener`, `bear_steepener`, `bull_flattener`, `bear_flattener` | sign of the 3-month change in the 10Y yield × sign of the 3-month change in 2s10s | 1977 |
| `dollar_trend` | `strong_usd`, `weak_usd` | `DTWEXBGS` above/below its 200-day moving average | 2006 |
| `recession_risk` | `normal`, `sahm_triggered` | first-release unemployment 3-month average − 12-month low ≥ 0.5 pp | ~1960s |

- **Optional hysteresis:** a `min_duration` argument holds a label until the new state has
  persisted for N days, to stop month-to-month flipping. Default 0 for the market-based regimes
  and 21 business days for `growth_inflation`. Report the flip rate either way.
- The existing `VolRegime`, `RatesRegime` and `TrendRegime` stay as they are.
- Add a `REGIME_REGISTRY: dict[str, type[Regime]]` covering all nine regimes.

### B5. Experiment integration

- `ExperimentConfig` gains `diagnostics.regimes: list[str]` (default: every `REGIME_REGISTRY`
  key; see D3). `runner.run_experiment()` builds the list and passes `regimes=` to
  `run_backtest()`. No change is needed inside `Conditioner`.
- The Research Diagnostics tab's "Performance by regime" table grows from 3 regimes to 9
  (~30 columns). Change it to one row per `regime: label` with metrics as columns, grouped by
  regime, so it reads top to bottom. Update the dashboard smoke tests.
- Re-run EXP-0001 and EXP-0002 so their `diagnostics.json` includes the new regimes.
  Re-running doesn't change their headline metrics.

### B6. Asset-by-regime analysis (`src/macro/analysis.py`)

Pure functions over (labels, returns):

- `performance_by_regime(labels, returns)`: annualised return, vol, Sharpe, hit rate, % of time
  and observation count for each label. Same columns as `Conditioner.by_regime()`, so share the
  helper.
- `forward_returns_by_regime(labels, prices, horizons=(21, 63))`: average forward return after
  entering each regime. This answers "what happens next", which is different from "what happens
  during".
- `transition_matrix(labels)`, `regime_durations(labels)`, `current_regime(labels)` (label plus
  days in regime).
- **Reference assets** come from what's already in the DB: S&P 500 (100001), Nasdaq-100, gold
  (400003), crude (400001), the dollar (`DTWEXBGS`), plus a constant-duration Treasury proxy:
  `r_t ≈ y_{t-1}/252 − D·Δy_t`, with D ≈ 8.5 for the 10Y. The proxy is labelled as such
  everywhere (see D4).

### B7. Validate the regimes before using them

This is research discipline, done once and written up in `docs/research/macro_regimes_v1.md`:

1. **Freeze definitions first.** Commit B3/B4 before running any asset-performance numbers.
   Parameter choices are taken from standard practice, not from the results. Record any later
   change with a reason.
2. **Sanity checks:** `growth_inflation` / `recession_risk` against `USREC`; `financial_stress`
   against known episodes (1998, 2008, 2011, 2020, 2022). Check the flip rate and average
   duration of each regime.
3. **Lookahead check:** compare first-release labels with latest-vintage labels. Large
   disagreement means the latest-vintage version would have flattered any backtest. Report the
   percentage of days that differ.
4. **Overlap check:** the label-agreement matrix across regimes, e.g. how much `financial_stress`
   duplicates `VolRegime`. If two regimes are nearly identical, drop one.

### B8. Dashboard page (`dashboard/macro/regimes.py`)

Add one file and one navigation line in `dashboard/app.py`. Loaders go in
`dashboard/components.py`, cached on the DB file's modification time like `load_series()`. Every
panel answers one question:

| Panel | Question it answers |
|---|---|
| Current state cards (label, days in regime, input staleness) | "What regime are we in right now, and how fresh is that call?" |
| Regime timeline (coloured bands under the S&P 500 or a chosen asset) | "When did each regime occur, and how did markets behave through them?" |
| Asset performance by regime (table + forward-return bars) | "Which assets have historically done well or badly in this regime?" |
| Transition matrix + durations | "How long do these regimes usually last, and what typically follows?" |
| Indicator drilldown (composite + components, with z-scores) | "Which inputs are driving the current classification?" |

Smoke tests go in `tests/dashboard/test_pages.py` against the temp DuckDB fixture.

### B9. Tests

- `tests/macro/test_pit.py`, `test_indicators.py`, `test_regimes.py`, `test_analysis.py`:
  synthetic series with known answers; a truncation-invariance check on every regime; empty or
  short history returns an empty Series, not an error.
- Integration: one `run_experiment()` with `diagnostics.regimes` set, against the in-memory DB
  fixtures in `tests/experiments/`.

---

## 4. Order of work and checkpoints

| Step | Delivers | Checkpoint (stop for researcher review) |
|---|---|---|
| A1–A2, A5 | catalog, collector fix, tests | contract codes confirmed by the discovery query |
| A3–A4 | backup, delete, backfill, validation, EXP-0001 regression | validation output shared (D2 approved) |
| A6 | architecture doc, memory | — |
| B1–B2 | point-in-time alignment, new FRED series | — |
| B3–B4 | indicators, regimes (definitions frozen) | definitions reviewed **before** B6 numbers are produced |
| B7 | validation write-up | researcher reads the write-up |
| B5–B6 | experiment integration, analysis functions, EXP-0001/0002 re-run | — |
| B8 | dashboard page | page reviewed in the running app |
| — | `/update-architecture` | — |

Natural follow-on (not in this plan): extract the yield-curve signal from
`notebooks/signals/yield_curve_regime.ipynb`, using `curve_dynamics` and the point-in-time layer,
as EXP-0003.

## 5. Risks

- **Too few regime episodes.** Four-state regimes over ~50 years give only a handful of episodes
  each, so asset-by-regime Sharpes are noisy. Show `n_obs` and the number of distinct episodes
  next to every statistic.
- **Mining regime definitions.** Iterating on thresholds until a strategy looks good in some
  regime is a hidden degree of freedom. B7.1 (freeze first) and recording changes are the guard.
- **Revisions to weekly series.** `NFCI` is revised. Use first release, and accept a shorter
  usable history.
- **Contract code changes** (e.g. Eurodollar → SOFR) break long positioning histories. They're
  documented in the catalog, not spliced.

## 6. Out of scope

- Commodity futures curves, carry, and roll-adjusted prices (waiting on Databento or Norgate; see
  the 2026-10-22 discussion).
- Splicing predecessor COT contracts.
- Regime-switching models (HMM, Markov switching). Rule-based regimes come first, because
  they're auditable and free of lookahead.
- Trading regimes directly as a strategy. Regimes are diagnostics and conditioning inputs here.
  A regime-timed allocation would be its own experiment with its own hypothesis.

---

## Part A outcome (2026-10-22)

Done as planned, with these differences and findings:

- **Catalog:** 35 contracts, codes confirmed against the live API. Differences from the sketch: RBOB
  gasoline, ULSD and platinum were added to reach 35 (the plan listed 11 commodities + ~26 financial);
  the mini DJIA is `124603`; SOFR uses `134741` (3M). Known history breaks are noted in the catalog.
- **Cleanup:** `src/collectors/cot_cleanup.py` + `scripts/migrate_cot_contract_keys.py`. 99 series /
  49,895 `cot_positions` rows / 28 `data_fetch_log` rows deleted; backup at
  `data/quant.db.bak-20261022` (git-ignored).
- **Backfill:** 142,921 rows. Series per report: legacy_fut 35, legacy_comb 35, disagg_fut 14, tff_fut 21.
- **Extra bug found in validation (not in the plan):** `TFF_MAP` used wrong API field names and
  `open_interest_fut` doesn't exist for legacy_fut/tff_fut, so every TFF position column and
  legacy_fut open interest was NULL. Fixed, re-backfilled, regression tests added. Leveraged-fund
  and asset-manager positioning (needed for B3's positioning indicators) is now real.
- **EXP-0001 regression:** checked at the input level rather than by re-running (a re-run rewrites the
  experiment's artifacts). All 11,429 shared rows of its 11 disagg series are identical before and
  after; the only difference is 23 new weeks (2026-05-12 → 2026-10-13), as the old data stopped at
  2026-05-05. Re-run the experiment when you want its results to include them.
- **Explorer:** the exchange-keyed checkbox/warning was replaced by an asset-group filter;
  `list_series()` gained `asset_group` and keeps `contract_keyed` as a safety net.
- **Tests:** 464 pass (was 450).
- **Open housekeeping:** `data/quant.db.bak-20261022` can be deleted once you're comfortable.
