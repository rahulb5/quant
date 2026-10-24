# Macro research layer — implementation plan (blended)

**Status:** approved by the researcher on 2026-10-22 (E1–E4 below; E2 revised for CPI). **C0 done 2026-10-22** (see "C0 outcome"); C0–C3 done; C4 (rates) next.
Replaces Part B of `docs/plans/cot_fix_and_macro_regimes.md`. Part A there (the COT fix) is complete
and is a prerequisite.
**Inputs:** the original Part B, and `docs/Macro Research Layer — Design Review.md`. Where they
disagree, this plan says which way it goes and why.
**Read first:** `CLAUDE.md`, `docs/architecture.md`, `docs/plans/dashboard_v1.md`.

## Goal

Build a macro research layer on free data. Its main object is a **continuous macro state**: where
each dimension is, which way it is moving, and what markets are pricing. Discrete regimes are a
small reporting layer derived from that state. Every value must use only data available on its
date, including the revisions published by then.

The research loop this supports:

```
macro state → macro impulse → market pricing → positioning → asset implications
```

rather than `macro data → regime label → asset returns`.

## What changed from the original Part B

| Area | Original Part B | This plan | Why |
|---|---|---|---|
| PIT method | First release of each period, stitched together | **As-of vintage reconstruction**: each value is computed on the whole dataset as published on that date | YoY, rolling means and acceleration mix vintages under stitching (review §6). All vintages are already stored, so this is cheap |
| Data hygiene | ICSA +5-day release-lag override | **Fix the stored rows first** (C0) | The override shifts the date but keeps the revised value; see C0 |
| Core object | 9 regime classifiers | Continuous state vector (level + impulse per dimension); 4 macro regimes on top | Regimes have 5–15 independent episodes (review §1, §7) |
| Policy | easing / hold / tightening from ΔDFF | **Stance** (real policy rate) × **impulse** (ΔDFF) | Holding at 5.5% while inflation falls is tightening (review §3) |
| Rates | `curve_dynamics` regime | Rates decomposition module: real yield, breakeven, term premium, curve, policy expectations | Review §4 |
| Financial stress | HY OAS terciles | Financial conditions **and** credit supply, kept separate | Review §9 |
| Sahm rule | One of nine regimes | Event flag, plus a labor deterioration composite | Review §8 |
| Dollar trend, COT | `dollar_trend` regime; COT crowding indicators | Market-state and positioning layers, not macro regimes | Review §5 |
| Analysis | Regime tables + forward returns | Continuous regressions first, with HAC errors for overlapping horizons; regime tables with episode bootstrap | Review §7 |
| Pricing vs reality | — | Macro state vs market-implied state, per dimension | Review §2, limited to what free data supports |
| Research hygiene | `n_trials` declared by hand | Trial ledger + walk-forward check | Review, "roadmap priority" |
| Global layer | — | **Deferred**; needs a new collector (see Out of scope) | FRED's international series are stale; see Section 6 |

**Where the review isn't followed:**

- **Nominal = real + breakeven + term premium double-counts.** Breakevens contain an inflation risk
  premium and real yields contain a real term premium. Use `nominal = real + breakeven` as the main
  split, and show the term premium (a model estimate) separately.
- **"Reality minus expectations" uses market-implied expectations only.** Consensus-forecast
  surprises need paid data, apart from the quarterly Philly Fed SPF. V1 compares macro state with
  market pricing, not data with consensus.
- **r\* is left out of V1.** Laubach-Williams / HLW estimates aren't available as of past dates. Stance
  uses the real policy rate measured against its own expanding median.

## Decisions carried over (don't re-litigate)

From the original plan: free data only (FRED, CFTC, Yahoo); research first, with definitions frozen
before results are seen; regimes reuse the `src.backtest.regimes.Regime` base class; the dashboard
stays read-only; D3 (`diagnostics.regimes` opt-in flag, default on); D4 (yield-based Treasury proxy).

## Decisions (answered by the researcher on 2026-10-22)

| # | Decision |
|---|---|
| E1 | **As-of vintage reconstruction** replaces original decision 4 ("first-release data, aligned on release dates"). Latest-vintage data stays explorer-only, as before |
| E2 | **Before a series' vintage history starts, backtests drop the data by default**, with one exception: series documented as unrevised (`UNREVISED_SERIES`, C1) use latest values with a fixed, conservative release lag. This gives CPI its full history (see "CPI history" below). `pre_vintage="latest_lagged"` stays available for exploration and is labelled everywhere it's used |
| E3 | **4 macro + 2 market regimes** (C6). The other old regimes become continuous indicators |
| E4 | **Robustness-grid variants are recorded in the trial ledger but don't count** towards `n_trials`; experiment runs and logged notebook variants do |

### CPI history (E2 exception)

The ALFRED vintages for seasonally adjusted CPI start in 1972 (headline `CPIAUCSL`) and 1996 (core
`CPILFESL`). **Not-seasonally-adjusted CPI is essentially never revised:** BLS only revises the
seasonal factors, which leave NSA untouched. Checked against ALFRED on 2026-10-22, for periods from 1998:

| Series | First-print vs latest YoY, mean / max | First-print vs latest 3m annualised, mean / max |
|---|---|---|
| `CPILFENS` (core, NSA) | 0.004 / 0.11 pp | 0.007 / 0.46 pp |
| `CPIAUCNS` (headline, NSA) | 0.004 / 0.12 pp | 0.007 / 0.48 pp |
| `CPILFESL` (core, SA) | 0.041 / 0.18 pp | 0.31 / 1.18 pp |
| `CPIAUCSL` (headline, SA) | 0.069 / 0.35 pp | 0.54 / 2.22 pp |

Only 12 of 836 core NSA periods were ever changed. So YoY inflation built on NSA CPI, with a
fixed release lag, is point-in-time back to 1958 (core) and 1914 (headline), not 1996/1972.
Release lag from the period start: median 45 days, max 59 (1998 onward); max 62 for 1973–1995 in
the SA vintages. Use **62 days**. Older headline NSA vintages show index rebasing (1967=100 to
1982-84=100), not revisions, so latest values are the right ones to use.

What this doesn't cover: 3-month annualised inflation needs SA data, and SA revisions are large
(0.3–0.5 pp mean). Those measures stay real-time from the SA vintages (1996 for core).

---

## Design

```
                  ┌──────────── src/macro/pit.py ─────────────┐
FRED vintages ───►│ as-of reconstruction, release-date        │
COT (release) ───►│ alignment, staleness                      │
Yahoo prices ────►└───────────────────────────────────────────┘
                                     │  daily point-in-time series
         ┌───────────────────────────┼──────────────────────────────┐
         ▼                           ▼                              ▼
 src/macro/state.py          src/macro/rates.py           src/macro/market.py
 macro state vector          rates decomposition          market state + positioning
 (growth, inflation,         (real, BE, TP, curve,        (credit, USD, vol, cyc/def,
  labor, policy, FC,          policy expectations)         copper/gold, COT crowding)
  credit supply)                     │                              │
         └───────────────┬───────────┴──────────────┬───────────────┘
                         ▼                          ▼
              src/macro/regimes.py          src/macro/analysis.py
              small Regime layer            continuous regressions, regime
              (+ event flags)               tables, episode bootstrap,
                         │                  pricing divergence
         ┌───────────────┼─────────────────────┐
         ▼               ▼                     ▼
 Conditioner      dashboard/macro/state.py   docs/research/macro_state_v1.md
 (every experiment)
```

`pit.py`, `state.py`, `rates.py`, `market.py` and `analysis.py` are pure pandas: no DB access, no
Streamlit, like `src/macro/transforms.py`. Thin loader functions in `src/macro/data.py` and the
`Regime` subclasses are the only code that touches the DB.

---

## C0. Fix FRED rows stamped on their own period date (prerequisite)

**The bug.** `FREQ_MAP` matched FRED's whole frequency string, so "Weekly, Ending Saturday" (and
"Weekly, As of Wednesday", "Monthly, End of Period") fell through to the default `daily`. That
turned off vintage fetching for ICSA, WM2NS, WALCL and JPNASSETS. DTWEXBGS is genuinely daily but
is revised. Each of the affected series contains two kinds of rows:
real ALFRED vintages, and rows written by the daily-mode `_fetch_daily()`, which stamps
`release_date = period_date` on a later, revised value. `load_fred(vintage="first")` picks the
stamped row because it's the earliest, so "first release" returns a revised value, dated before it
was published. Verified 2026-10-22:

| Series | Periods with a stamped row and later vintages |
|---|---|
| `ICSA` | 1,564 / 3,117 (no real vintage rows after 2026-02-21) |
| `DTWEXBGS` | 5,047 / 5,328 (it is revised; the original plan assumed it isn't) |
| `WM2NS` | 1,561 / 2,383 |

Example: ICSA for the week of 2024-03-02 shows 213k "released" 2024-03-02. The first print was 217k,
published on 2024-03-07.

**Fix.**

1. `frequency_from_fred()` matches FRED's leading word and raises on an unknown one (no silent
   default). `MacroCollector._is_daily()` is true only for unrevised daily market series:
   label `daily` and code not in `REVISED_DAILY` (`DTWEXBGS`).
2. One-off script `scripts/purge_stamped_fred_rows.py`, built like `migrate_cot_contract_keys.py`:
   backup, then **fix labels, re-fetch in vintage mode, delete stamped rows, recompute
   `is_final`**, with `--dry-run`. The order matters: the label selects the fetch mode, so a
   re-fetch before the label fix writes the stamped rows again (the first rehearsal did exactly
   that). A stamped row is deleted only where a real release exists for the same period.
4. **Archive the ICE credit series before anything else touches the DB.** FRED now serves only the
   last 3 years of `BAMLH0A0HYM2`, `BAMLC0A0CM`, `BAMLC0A4CBBB` and `BAMLEMCBPIOAS` (ICE licensing:
   a fresh fetch of HY OAS starts 2023-10-23). Our DB holds 1996/1997–2026 for all four and it can't
   be downloaded again. Export them to `data/archive/ice_credit_spreads.parquet` (git-ignored, plus
   a copy alongside the dated DB backup), and have the purge script skip these codes. Routine
   fetches are safe (the upsert never deletes), but a rebuild or a lost DB file would lose the
   history for good. Add a note to `SERIES_CODES`, and a test that the purge skips them.
5. **Commodity prices stopped updating on 2026-05-08** (GC1, CL1, HG1 and the rest of the 300xxx
   block). Check `data_fetch_log`; if the collector simply hasn't been run, run
   `scripts/fetch_commodity.py` in update mode. GC1/CL1 feed the reference factors and the
   copper/gold ratio (C5).
6. Stale series. `DTB3`, `M2SL` and `PPIACO` stop around 2026-02/03; find out why, using
   `data_fetch_log`. The international CPIs and OECD CLIs are also stale. List them in
   `SERIES_ALIASES` or remove them, and don't build on them (Section 6).

**Tests:** `tests/collectors/test_macro.py` (frequency mapping, qualified weekly label → vintage
mode, `REVISED_DAILY`) and `tests/collectors/test_fred_cleanup.py` (dry run, only stamped rows with
a real release deleted, idempotent, `load_fred(first)` returns the true first print). The purge is
restricted to `AFFECTED`, so it never touches the ICE codes.

### C0 outcome (2026-10-22)

- **ICE credit spreads archived** to `data/archive/ice_credit_spreads.parquet` (git-ignored): HY OAS
  7,770 rows from 1996-12-31, IG 7,769, BBB 7,770, EM 7,114 (from 1998-12-31).
- **Stale series explained:** `DTB3`, `M2SL` and `PPIACO` aren't failing; they left `SERIES_CODES`
  after their last fetch on 2026-03-26. Nothing to fix.
- **Commodity prices refreshed** (stale since 2026-05-08); GC1, CL1 and HG1 now run to 2026-10-22.
- **Rehearsed on a copy of the DB**, with the live FRED re-fetch: stamped rows 3,117 / 2,383 / 1,242
  / 335 / 5,198 (ICSA / WM2NS / WALCL / JPNASSETS / DTWEXBGS) all replaced by real releases, one
  `is_final` per period, none left over. Spot checks match the published figures: ICSA week ending
  2020-03-21 = 3,283,000 released 2020-03-26; 2020-03-28 = 6,648,000 released 2020-04-02;
  2024-03-02 = 217,000 released 2024-03-07.
- **A finding for C1:** ALFRED's earliest real vintage for old ICSA weeks (1967 onward) is
  2009-05-28, so ICSA is real-time from 2009 only. SA claims are revised about 3.8% of their level
  each year by seasonal factors, so they aren't treated as unrevised. Use the **NSA** series
  `ICNSA` (and a YoY, which needs no seasonal adjustment) as `UNREVISED_SERIES` with a 5-day lag;
  verify its revisions when C2 adds it. ICSA stays as a real-time cross-check from 2009.
- **Applied to the live DB 2026-10-22** and verified: no stamped rows in the five series, one
  `is_final` per period, ICE rows and all other tables unchanged, spot checks as above.

## C1. Point-in-time layer (`src/macro/pit.py`)

The core building block. Every later step depends on it being right.

- `load_vintages(db, code) -> pd.DataFrame` (in `src/macro/data.py`): every row,
  `(period_date, release_date, value)`.
- `as_of(vintages, v) -> pd.Series`: the series as published on date `v`, meaning the latest
  `release_date ≤ v` for each period. ALFRED stores a row only when a value changes, so this is
  correct.
- `realtime(vintages, fn, release_lag_days=0) -> pd.Series`: **the main function.** For each
  distinct release date `v`, compute `fn(as_of(vintages, v))` and keep its value at the latest
  period. The result is indexed by `v + lag`. `fn` is any period-indexed transform (YoY, 3-month
  annualised, gap to a trailing low, ...). About 800 vintages × a short series is cheap. Cache by
  `(code, fn name, max release_date)`.
- `to_daily(s, index) -> pd.Series`: forward-fill onto a business-day index. A release on a
  weekend lands on the next business day.
- `staleness(s) -> pd.Series`: days since the last release, shown on the dashboard.
- `UNREVISED_SERIES = {"CPILFENS": 62, "CPIAUCNS": 62, "DRTSCILM": 60, "CCSA": 12, "SAHMREALTIME": 40}`
  (code → release lag in days from the period start for monthly/quarterly series, or from the week
  end for weekly series; refine the lags from the vintage dates when building). Revision checks,
  2026-10-22: `DRTSCILM` and `SAHMREALTIME` have no revisions in their vintage era (SAHMREALTIME
  is real-time by construction); `CCSA` revisions are about 2% of the series' spread. `ICNSA`
  (NSA initial claims, lag 5 days) is added once C2 has verified its revisions. For these, `realtime()` uses latest values shifted by the lag over the whole history
  instead of reconstructing vintages. A series is added only with a measured revision check like
  the CPI one in E2.
- `first_vintage_date(vintages)` and the `pre_vintage` policy (E2). With `"drop"`, nothing is
  emitted before the first real vintage date plus `fn`'s own window. With `"latest_lagged"`, the
  latest values are used with the series' median release lag and flagged in `.attrs`.
- Composites are built **after** each component is computed in real time and moved to the daily
  index. Each component is lookahead-clean, so their combination is too.
- `load_fred(vintage="first")` stays for the explorer and B7-style comparisons. Backtests use
  `realtime()`.

**Release dates for daily series.** Treasury, fed funds and VIX are published the same evening.
With `lag=1` execution, a lag of 0 is fine. `BAML*` OAS appears the next morning: use a +1
business-day override (`RELEASE_LAG_OVERRIDES`). After C0, `DTWEXBGS` (published weekly on Mondays
in H.10) takes its timing from real vintages.

**COT.** `release_date = report_date + 3` is correct except in holiday weeks, when the CFTC
publishes on Monday and +3 lands a business day early. Add a CFTC holiday-week adjustment if a
positioning signal is sensitive to it; otherwise note it in the docstring.

### C1 outcome (2026-10-22)

Built as `src/macro/pit.py` (`as_of`, `latest`, `realtime`, `unrevised`, `pit_series`, `to_daily`,
`staleness`, `first_vintage_date`, `UNREVISED_SERIES`, `RELEASE_LAG_OVERRIDES`) plus
`load_vintages()` in `src/macro/data.py`; 19 tests in `tests/macro/test_pit.py`. Differences from
the sketch above:

- **No cache.** `realtime()` rebuilds PAYEMS (860 vintages), INDPRO (1,223) and UNRATE (800) in
  0.2–0.4 s each, so the cache is left out until something needs it.
- **`pit_series(vintages, fn, code)`** dispatches to `unrevised()` for `UNREVISED_SERIES` codes
  and applies `RELEASE_LAG_OVERRIDES` otherwise.
- **`fn` must be causal** (value at period p uses only periods <= p). `realtime()` keeps only the
  newest period's value; `unrevised()` applies `fn` once to the whole series.
- **Worked example on the real DB (since 1990):**

  | Series / transform | Real-time vs first-release stitched (mean / max) | Real-time vs latest vintage (mean / max) |
  |---|---|---|
  | PAYEMS YoY % | 0.30 / 1.02 pp | 0.25 / 0.97 pp |
  | PAYEMS 3m annualised % | 0.45 / 4.00 pp | 0.44 / 2.65 pp |
  | INDPRO YoY % | 3.10 / **24.5** pp | 1.24 / 3.92 pp |
  | UNRATE gap to 12-month low | 0.01 / 0.10 pp | 0.05 / 0.30 pp |

  Sept 2008 payrolls YoY read -0.21% in real time, -0.41% stitched and -0.58% in the latest
  vintage. INDPRO's 24.5 pp gap is an index rebase between vintages, which stitching mixes and
  as-of snapshots don't.

**Tests:**
- Truncation invariance: `realtime()` on vintages cut at date `k` equals the full output's prefix
  up to `k`.
- A revision to period `t−1`, published with period `t`, changes the YoY at `t` but not the stored
  YoY at `t−1`.
- The pre-vintage policies behave as documented.
- The stamped-row case from C0 fails loudly: `realtime()` warns when it sees
  `release_date == period_date` on a non-daily series.

## C2. New free FRED series (`src/collectors/macro.py`)

The original B2 list, plus what the state vector and the rates module need. Backfill these codes
only.

| Code | Use | Vintages | From |
|---|---|---|---|
| `USREC` | Sanity check only, **never an input** | — | 1854 |
| `T10Y3M`, `DGS3MO`, `DGS1` | Curve; front end | daily | 1982 / 1981 / 1962 |
| `NFCI` | Financial conditions. **Real-time only from 2011** (vintages start 2011; revisions 0.22 on a spread of 1.36); before that, use the HY OAS / VIX / curve composite (C3), with NFCI as a cross-check | yes | 1971 |
| `SAHMREALTIME` | Cross-check of our Sahm flag (`UNREVISED_SERIES`) | treated as unrevised | 1959 |
| `DFII5`, `DFII10` | Real yields (TIPS) | daily | 2003 |
| `T5YIE`, `T5YIFR` | 5Y breakeven, 5y5y forward | daily | 2003 |
| `THREEFYTP10` | 10Y term premium (Kim-Wright, a model estimate) | daily | 1990 |
| `CPILFENS`, `CPIAUCNS` | Core / headline CPI, NSA: long point-in-time inflation history (E2) | unrevised | 1957 / 1913 |
| `PCEPILFE` | Core PCE (inflation level vs the 2% target) | yes | 1959 |
| `CCSA` | Continuing claims (`UNREVISED_SERIES`) | treated as unrevised | 1967 |
| `ICNSA` | Initial claims, NSA: YoY of the 4-week average replaces SA `ICSA` in the growth impulse (`UNREVISED_SERIES`, lag 5 days; verify revisions) | treated as unrevised | 1967 |
| `TEMPHELPS` | Temporary help employment. Revisions are large (about a third of its spread), so real-time from 2011 only | yes | 1990 |
| `AWHAETP` | Average weekly hours, private | yes | 2006 |
| `DRTSCILM` | SLOOS: net % of banks tightening C&I standards (credit supply; `UNREVISED_SERIES`) | treated as unrevised | 1990 |
| `TOTBKCR` | Bank credit (credit supply). Large revisions, real-time from 1996 | yes | 1973 |

Check each code with `get_series_info()` before adding it; aliases go in `SERIES_ALIASES`.

### C2 outcome (2026-10-22)

All 20 series are in `SERIES_CODES` (58 codes in total) and backfilled with full history. Findings:

- **FRED caps a request at 100,000 rows and `get_series_all_releases()` silently truncates.**
  NFCI (restated in full every release) came back as 100,000 rows covering 1971–1976, and TOTBKCR
  stopped at 1999. `MacroCollector._fetch_all_releases()` now pages with `offset`, and
  `_upsert_observations()` writes one bulk statement (`Database.run_df`) instead of one per row.
  Re-fetched: NFCI 931,539 rows (801 vintages from 2011-05-25), TOTBKCR 206,251 rows to 2026-10-07.
- **`ICNSA` joins `UNREVISED_SERIES` with a 19-day lag.** Its advance estimate is revised the next
  week: value known 5 days after the week end differs from the latest by 1.10% of the level,
  after 12 days 0.57%, after 19 days 0.48% (the rest is annual re-benchmarking, which stays).
  That's small against claims' cyclical range, but it is not zero.
- **`THREEFYTP10` is in `REVISED_DAILY`.** The Kim-Wright term premium is re-estimated: mean
  first-vs-latest revision 0.24 pp on a spread of 0.73, and vintages only exist from 2016, so it
  is real-time from 2016 only. It is shown beside the rates decomposition (C4), never used as a
  regime input.
- **Daily series with no vintages** (`DFII5`, `DFII10`, `T5YIE`, `T5YIFR`, `T10Y3M`, `DGS1`,
  `DGS3MO`) are stored with `release_date = period_date`, as for the other market series.
- `scripts/fetch_macro.py --series` takes a comma-separated list.

## C3. Macro state vector (`src/macro/state.py`)

Pure functions over real-time daily series from C1. For each dimension: a **level** (where are we?)
and an **impulse** (which way are we moving?). Every composite is the mean of expanding z-scores of
its components; never normalise on the full sample. Each function's docstring states its
earliest valid date and its parameter choices, which come from standard practice and are fixed
before any results are seen (C8).

| Dimension | Level | Impulse | Valid from (approx., with E2 = drop) |
|---|---|---|---|
| **Growth** `G` | z-mean of: IP YoY, payrolls YoY, −(UNRATE − its expanding median) | z-mean of 6-month changes in: IP YoY, 3m-annualised payrolls, −(4-week-average NSA claims YoY, `ICNSA`) | ~1967 (claims; payrolls 1955, UNRATE 1960) |
| **Inflation** `π` | core CPI **NSA** YoY − 2 (core PCE YoY − 2 as a cross-check) | 6-month change in core NSA YoY (full history); plus, as supplements, the 6-month change in SA 3m-annualised core CPI (from 1996) and Δ`T10YIE` (from 2003) | Core 1958, headline 1914 (via `UNREVISED_SERIES`) |
| **Labor** `L` (for the composite and the flag) | UNRATE − its expanding median (not CBO `NROU`: its revisions are larger than its own variation, and it contains projections to 2036) | z-mean of: 3-month Δ UNRATE, claims and continuing claims YoY, temp help 6m % (from 2011), hours 6m Δ (from 2010) | ~1967 (fewer components before 2011) |
| **Policy** `P` | Stance = DFF − core CPI NSA YoY (real policy rate), minus its expanding median | Impulse = 6-month Δ DFF | Stance 1958 (DFF 1954, core NSA YoY 1958) |
| **Financial conditions** `FC` | Before 2011: z-mean of HY OAS, VIX and −(T10Y3M). From 2011: the same composite plus real-time `NFCI`, with the composite alone kept as the series that is comparable across the break | 3-month Δ | 1997 (HY OAS archive, C0.4) |
| **Credit supply** `CS` | z-mean of: SLOOS tightening, −(bank credit YoY), HY OAS from 1997 | 2-quarter Δ SLOOS | 1990 (HY OAS component from 1997) |

**C3 outcome (2026-10-24).** Definitions frozen in `docs/research/macro_state_definitions_v1.md`
(researcher kept every default, J1–J9). Built as `src/macro/inputs.py` (DB loading + per-series causal
transforms through `pit.py`, 20 inputs in 4.9 s) and `src/macro/state.py` (pure: `expanding_z`,
`macro_state`, 0.1 s); 13 tests. Also: a daily fast path in `pit.realtime()`, and
`USE_FIRST_RELEASE_DATE` so unrevised series with a real first print use it instead of the
conservative lag. Clarifications are in the spec's change log. Live values on 2026-10-23: G_level
0.14, G_impulse 0.10, core CPI 0.56 pp above the 2% target (impulse -0.01), P_stance +0.27 (restrictive,
barely), FC -0.05, CS -0.20.

Output: `macro_state(db_or_inputs) -> pd.DataFrame` with columns `G_level, G_impulse, pi_level,
pi_impulse, L_level, L_impulse, P_stance, P_impulse, FC, FC_impulse, CS`, plus a staleness column
per input.

## C4. Rates decomposition (`src/macro/rates.py`)

A first-class module, not part of "curve dynamics".

- `nominal_decomposition()`: `DGS10 = DFII10 + T10YIE` (and the same for 5Y), with daily and 3-month
  changes attributed to the real and breakeven parts. From 2003. Report the TIPS-liquidity caveat
  (e.g. late 2008).
- `term_premium()`: `THREEFYTP10` level and changes, shown **next to** the decomposition, labelled
  as a model estimate. Never added to it (see "Where the review isn't followed").
- `curve()`: `T10Y2Y`, `T10Y3M` levels; 3-month changes in DGS10 and 2s10s; bull/bear
  steepener/flattener state.
- `policy_expectations()`: `DGS2 − DFF` (and `DGS1 − DFF`) as the free stand-in for OIS. It
  measures expected policy over the next 1–2 years, with term premium mixed in. Label it as a proxy.
- `treasury_proxy_returns()`: the constant-duration proxy from the original B6 (D4), moved here.

**C4 outcome (2026-10-24).** Built `src/macro/rates.py` (pure: `nominal_decomposition`, `term_premium`,
`curve`, `policy_expectations`, `treasury_proxy_returns`) and `load_rates_inputs()` in
`src/macro/inputs.py` (13 daily FRED codes); 10 tests. Definitions follow spec section 2 unchanged.
Notes: `THREEFYTP10` is vintaged from 2016-05 only, so under E2 "drop" the term premium starts
there (`latest_lagged` for exploration). 10Y decomposition residual max 2 bp (rounding). Live values on
2026-10-23: 10Y 4.96 = real 2.64 + BE 2.34 (3m move +35 bp, two-thirds real), curve `bear_flattener`,
T10Y2Y +0.31, DGS2-DFF +0.77.

## C5. Market state and positioning (`src/macro/market.py`)

Not macro state. Used for the pricing comparison (C7) and as conditioning variables.

| Variable | Source | From |
|---|---|---|
| Credit: HY OAS level and 3m Δ; IG − BBB | `BAMLH0A0HYM2`, `BAMLC0A0CM`, `BAMLC0A4CBBB` | 1997 (history held only in our DB; see C0.4) |
| Dollar: `DTWEXBGS` vs 200-day MA; 3m % change | FRED (after C0) | 2006 |
| Volatility: VIX level, expanding tercile | `VIXCLS` | 1990 |
| Cyclicals vs defensives (equity growth pricing) | Russell 2000 / S&P 500 (100003/100001); copper / gold (`HG1` 300017 / `GC1` 300009) | Russell 1987; copper/gold 2000 (after the C0.5 refresh) |
| Equity trend | S&P 500 vs 200-day MA (existing `TrendRegime`) | — |
| **Positioning:** leveraged-fund and asset-manager net % of OI, expanding z-score, aligned on COT `release_date` | TFF (fixed in Part A): 10Y note, E-mini S&P, EUR, JPY; managed money for crude, gold, copper | 2006 |

Sector ETFs (XLY/XLP/XLI/XLU) would make a better cyclicals/defensives measure, but they aren't in
the DB. Add them through the equity collector only if the ratios above turn out too noisy. Yahoo
history for them starts in 1998.

**C5 outcome (2026-10-24).** Built `src/macro/market.py` (pure: `credit`, `dollar`, `volatility`,
`equity_trend`, `growth_pricing`, `inflation_pricing`, `policy_pricing`, `positioning`, `market_state`)
and `load_market_inputs()` / `load_prices()` in `src/macro/inputs.py`; 9 tests (547 in the suite pass).
Definitions follow spec section 3, with the clarifications in the spec's change log. Findings for review:

- **Dollar starts in late 2019, not 2006.** `DTWEXBGS` vintages only begin 2019-02; under E2 "drop" the
  `usd_state` (and C6 `dollar_trend`) is valid from 2019-11. The index *is* revised (first print vs latest:
  mean 0.96% in level, max 1.9%; 63-day % change differs by 0.12 pp mean, 1.8 pp max), so it doesn't
  qualify for `UNREVISED_SERIES`. Researcher decision needed (see the spec change log).
- `growth_pricing` has 2 components from ~1993, 3 from 1997 (HY OAS), 4 from 2003 (copper/gold); `_n` is stored.
- Positioning z-scores start 2009 (2006 data + 156-week burn-in); `inflation_pricing` 2006.
- S&P 500 and Russell prices in the DB end 2026-10-02 (stale; refresh via `scripts/fetch_equity.py`).
- Live 2026-10-23: HY OAS 3.40 (+0.59 in 3m), `strong_usd` (+1.6% vs 200d), `mid_vol` (VIX 19), `risk_on`,
  growth pricing -0.47, inflation pricing +0.46, policy pricing +0.27; 10Y-note and E-mini leveraged
  funds are net short at about -1.7 and -1.6 sigma.

## C6. Regime layer (`src/macro/regimes.py`)

`Regime` subclasses that **read the C3/C5 state** and don't compute their own inputs. Each regime
has a `layer` class attribute (`"macro"` or `"market"`) so the Diagnostics tab and the dashboard
can group them.

| `name` | layer | Labels | Rule |
|---|---|---|---|
| `growth_inflation` | macro | `goldilocks`, `reflation`, `stagflation`, `deflation` | sign of `G_impulse` × sign of `pi_impulse`; hysteresis `min_duration=21` business days |
| `growth_level` | macro | `below_trend`, `above_trend` | sign of `G_level`. Lets every growth/inflation table be split by level (review §1: "↑ from 0% ≠ ↑ from 4%") |
| `policy` | macro | `restrictive_tightening`, `restrictive_easing`, `accommodative_tightening`, `accommodative_easing` | sign of `P_stance` × sign of `P_impulse` (±25 bp dead-band on impulse → `on_hold` folds into the nearer side, reported separately) |
| `financial_conditions` | macro | `loose`, `normal`, `tight` | expanding terciles of the `FC` composite (C3) |
| `curve_dynamics` | market | bull/bear × steepener/flattener | as in the original B4 |
| `dollar_trend` | market | `strong_usd`, `weak_usd` | as in the original B4 |

**Event flags** (`src/macro/flags.py`, plain boolean Series, not regimes and not in
`Conditioner.by_regime()`): `sahm_triggered` (real-time, cross-checked against `SAHMREALTIME`);
`labor_deteriorating` (`L_impulse` above its expanding 80th percentile); `curve_inverted`.

`REGIME_REGISTRY: dict[str, type[Regime]]` covers the six above plus the existing `VolRegime`,
`RatesRegime` and `TrendRegime`. Run C8.4 (overlap) before deciding whether `RatesRegime` and
`curve_dynamics` both stay.

## C7. Analysis (`src/macro/analysis.py`)

Continuous analysis comes first. Regime tables summarise it.

1. **Forward-return regressions.** `forward_regression(state, returns, horizons=(21, 63, 126, 252))`:
   `R_{t→t+h} = α + β·X_t + ε` on the daily overlapping sample, with Newey-West HAC errors at lag
   `L ≥ h` (not the `ceil(n^0.25)` default that `Conditioner` uses, which is too short for
   overlapping returns). Also report the same regression on non-overlapping samples as a
   cross-check. Reuse the statsmodels pattern in `Conditioner.factor_regression()`.
2. **Regime tables.** `performance_by_regime(labels, returns)`, using the same columns as
   `Conditioner.by_regime()` (share the helper), plus `n_obs`, **`n_episodes`** and mean episode
   length. An episode is a maximal run of one label.
3. **Episode bootstrap.** `episode_bootstrap_ci(labels, returns, stat, n_boot=2000)`: resample whole
   episodes within each label, not days. Give a CI for the mean return and the Sharpe. Show a
   warning when `n_episodes < 5`.
4. **Forward returns by regime entry.** As in the original B6, with HAC errors at lag `h` and the
   episode count shown.
5. **Pricing divergence.** `divergence(macro_z, market_z)` per dimension, e.g. growth state
   (`G_level`, `G_impulse`) vs market-implied growth (z-mean of Δ2s10s, −ΔHY OAS, ΔRussell/SPX,
   Δcopper/gold); inflation impulse vs Δbreakevens; policy impulse vs Δ(2Y − DFF). The output is
   the gap, its expanding z-score, and forward returns of the relevant asset conditioned on the
   gap. This answers "growth is deteriorating, but has the bond market already priced it?"
6. `transition_matrix`, `regime_durations`, `current_regime`: as in the original plan.

Reference assets: as in the original B6 (S&P 500, Nasdaq-100, gold, crude, dollar), with the
Treasury proxy from C4.

## C8. Validation write-up (`docs/research/macro_state_v1.md`)

Done once, before C9.

1. **Freeze definitions first.** Commit C3–C6 before computing any asset-return numbers. Record any
   later change with a reason and a new trial-ledger family version.
2. **Sanity checks:** `G_level`/`G_impulse` and `sahm_triggered` against `USREC`; `FC` and credit
   supply against 1998, 2001, 2008, 2011, 2020, 2022, 2023; how the policy quadrants map onto known
   Fed cycles.
3. **Vintage check:** real-time state vs a latest-vintage state. Report the correlation per
   component and the share of days on which each regime label differs. Large gaps mean a
   latest-vintage version would have flattered a backtest.
4. **Overlap check:** label-agreement matrix across all registered regimes; correlation matrix of
   the state vector. Drop near-duplicates.
5. **Episode count** for every regime label. Labels with fewer than 5 episodes are reported but not
   used in experiment verdicts.

## C9. Experiment integration

As in the original B5, with the C6 registry:

- `ExperimentConfig.diagnostics.regimes: list[str]` defaults to every `REGIME_REGISTRY` key (D3).
  `runner.run_experiment()` passes them to `run_backtest()`.
- The Diagnostics "Performance by regime" table becomes one row per `regime: label`, grouped by
  `layer`, with `n_episodes` shown.
- New, optional: a "state exposure" table with the strategy's daily returns regressed on the state
  vector (C7.1 at `h = 1`).
- Re-run EXP-0001 and EXP-0002 for the new diagnostics; headline metrics must not change.

## C10. Trial ledger

Stops `n_trials` from depending on memory. This gets worse with AI-assisted research, where trying
twenty specifications costs almost nothing.

- **Migration v11:** a `trials` table with `id`, `family`, `family_version`, `kind` (`experiment` |
  `notebook` | `robustness`), `experiment_id` (nullable), `config_hash`, `params` (JSON), `sharpe`,
  `n_obs`, `git_sha`, `created_at`.
- `ExperimentConfig.validation.family: str` (e.g. `growth_regime_001`), required for new
  experiments. `scripts/new_experiment.py` asks for it.
- `run_experiment()` and `run_robustness()` write rows automatically: robustness-grid variants as
  `kind="robustness"`.
- `src/experiments/trials.py`: `log_trial(family, params, returns)` for notebooks, plus
  `n_trials(family)` = distinct `config_hash` over kinds `experiment` + `notebook` (E4).
- `check_statistical()` uses `max(config.validation.n_trials, n_trials(family))`, so a hand-set
  value can only raise the count. The research note prints both numbers.
- **Limitation:** notebook variants are counted only if `log_trial()` is called. The research note
  says so.

## C11. Walk-forward check (`src/experiments/robustness.py`)

A new check, `walk_forward`, built on the existing parameter-grid machinery:

- Expanding train windows (default: at least 5 years of training, 1-year test steps). On each train
  window, pick the grid point with the best Sharpe, then score it on the next test window. Stitch
  the test windows into one out-of-sample return series.
- Report out-of-sample Sharpe vs full-sample in-sample Sharpe, the decay ratio, and how often the
  chosen parameter changes between windows.
- `FLAG` if the out-of-sample Sharpe is below half the in-sample Sharpe; `KILL` if it is ≤ 0.
  Thresholds go in `RobustnessConfig`.
- `SKIPPED` when the config has no parameter grid.

## C12. Dashboard (`dashboard/macro/state.py`)

One page, one navigation line in `dashboard/app.py`. Loaders go in `dashboard/components.py`, cached
on the DB file's modification time. Each panel answers one question:

| Panel | Question |
|---|---|
| State vector cards (level, impulse, z, staleness) | "Where is each macro dimension, which way is it moving, and how fresh is the data?" |
| Rates decomposition (stacked real / BE change; TP alongside) | "What is driving the move in 10Y yields?" |
| Macro vs market-implied (divergence per dimension) | "Has the market already priced what the data shows?" |
| Positioning (COT z-scores) | "Is the market crowded in the direction of the macro move?" |
| Regime timeline + current labels and event flags | "Which regime are we in, and when did it start?" |
| Asset response (regression betas with HAC CIs; regime table with `n_episodes`) | "How have assets responded to this state historically, and how much evidence is there?" |

Smoke tests go in `tests/dashboard/test_pages.py` against the temp DuckDB fixture.

## C13. Tests

- `tests/macro/test_pit.py` (C1 list), `test_state.py`, `test_rates.py`, `test_market.py`,
  `test_regimes.py`, `test_analysis.py`: synthetic data with known answers; truncation invariance
  on every state column and every regime; empty or short history returns an empty Series, not an
  error.
- `test_analysis.py`: the episode bootstrap on a two-episode series gives a wide CI; HAC lag ≥ h is
  enforced.
- `tests/collectors/test_macro.py`: the C0 cases.
- `tests/experiments/test_trials.py`: counting rules (E4); a hand-set `n_trials` can only raise the
  count.
- `tests/experiments/test_robustness.py`: walk-forward on a known-overfit grid → `FLAG`/`KILL`.

---

## 4. Order of work and checkpoints

| Step | Delivers | Checkpoint (stop for researcher review) |
|---|---|---|
| C0 | Stamped-row purge, collector fix, stale-series triage | Before/after row counts; ICSA first prints match the published releases for 3 spot dates |
| C1 | `pit.py` | Truncation-invariance tests pass; one worked example (payrolls YoY: real-time vs stitched vs latest) |
| C2 | New FRED series | — |
| C3–C5 | State vector, rates, market and positioning | **Definitions reviewed and committed before any C7 numbers** |
| C6 | Regime layer + flags | — |
| C8 | Validation write-up | Researcher reads it |
| C10 | Trial ledger | — (done before C9 so the re-runs are logged) |
| C7, C9 | Analysis functions; experiment integration; EXP-0001/0002 re-run | — |
| C11 | Walk-forward check; run it on EXP-0001/0002 | Results shared |
| C12 | Dashboard page | Page reviewed in the running app |
| — | `/update-architecture` | — |

The trial ledger (C10) can run in parallel with C1–C6; it doesn't depend on them.

Natural follow-on (not in this plan): the yield-curve signal from
`notebooks/signals/yield_curve_regime.ipynb` as EXP-0003, built on C4 and registered under a ledger
family from its first run.

## 5. Risks

- **Few independent episodes.** This is the reason state comes before regimes. Show `n_episodes` and
  episode-bootstrap CIs next to every regime statistic.
- **Short real-time history.** NSA CPI gives inflation and policy stance history back to 1958, but
  TIPS start in 2003, HY OAS in 1997 and SA 3m-annualised core CPI in 1996, so some dimensions only
  have 20–30 years. Every output states its valid-from date.
- **The unrevised-series assumption.** C8.3 re-checks it: across the vintage era, compare
  `UNREVISED_SERIES` latest-plus-lag values with the true first prints.
- **Mining definitions.** C8.1 (freeze first) and the trial ledger are the guards. A change to any
  C3–C6 definition starts a new `family_version`.
- **Model-based inputs.** The Kim-Wright term premium is a model estimate. It is available as of
  past dates, but carries model risk. Label it as such. CBO `NROU` is not used.
- **ICE credit spread history is irreplaceable.** FRED serves only 3 years of it now; C0.4 archives
  our copy.
- **Thin real-time history for some labor and credit inputs.** `TEMPHELPS` and `NFCI` are real-time
  from 2011 only. The composites use fewer components before then, and C8.3 compares the two
  eras.
- **Proxies.** 2Y − DFF stands in for OIS, and the yield-based Treasury series for bond returns.
  Both are labelled everywhere.
- **Vintage-reconstruction cost.** `realtime()` runs once per release date. Cache it; if it gets
  slow, compute only the latest few periods per vintage.

## 6. Out of scope

- **Global / relative macro (review §10).** It's the right next step, but FRED's international series
  can't support it. China/Japan/UK CPI and the OECD CLIs in the DB stopped updating (2022–2025), and
  their vintages only start in 2013–2018. It needs a new collector against the OECD / ECB / BIS SDMX
  APIs. Plan that separately once C3 has fixed the per-country state definition
  `X_c = [G, ΔG, π, Δπ, r_real, FC]`.
- Consensus-forecast surprises (paid data). The Philly Fed SPF is a possible later free addition.
- r\* estimates (not available as of past dates).
- Commodity futures curves and carry (waiting on a paid source).
- Regime-switching models (HMM / Markov switching).
- Trading regimes or the state vector directly. That would be its own experiment with its own
  hypothesis and ledger family.
