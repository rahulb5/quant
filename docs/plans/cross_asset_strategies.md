# Cross-asset strategies — implementation plan

**Status:** DRAFT, 2026-10-25. Researcher chose 2026-10-25: build trend on equity, rates and FX first, with
existing data only (no commodities, no new fetches), then macro-plan C11 (S6 = yes). **Done:** R0 partial
(BIS rates, equity dividends), R3–R6 for equity/rates/FX. **Open:** Checkpoint 0 (R3 ETF validation
deferred), S1, S2, S5. C11 done. Phase 1 run: EXP-0003 complete, robustness `NEEDS_REVIEW` with all checks passing; holdout still locked. **Next:** F0 forward tracking (S5 open), researcher review of EXP-0003, then Phase 2.
**Goal, in one line:** an interview-facing research portfolio (one deep flagship plus a clean baseline)
that is also the start of a forward-tracked record I could later trade with micro futures.
**Read first:** `CLAUDE.md`, `docs/architecture.md`, `docs/plans/macro_research_layer.md` (its frozen
definitions and trial ledger are reused here).

## Scope

Liquid cross-asset strategies at daily frequency on free data, where the point-in-time macro layer is an
advantage. **Out:** single-stock equities (survivorship bias, see EXP-0002) and commodity curve carry
(needs 2nd-month data, which we don't have).

| # | Strategy | Role | Phase |
|---|---|---|---|
| 1 | Time-series momentum, vol-scaled, 1–12m (Moskowitz, Ooi, Pedersen 2012) | Baseline every other stream is regressed on | 1 |
| 3 | Macro-driven duration timing (growth/inflation impulse, macro state vs market pricing) | **Flagship**, uses the vintages | 2 |
| 2 | G10 FX carry | Third stream; risk premium with crash risk, conditioned on FC regimes | 3 |
| 4–6 | Cross-sectional commodities, sector/country rotation, VRP timing | Backlog, not planned here | — |

Order: returns layer → trend → duration → FX carry → portfolio/risk layer → paper engine → live sleeve.

## What the data actually supports (checked 2026-10-25)

The pasted proposal assumed some data we don't have. What's in `data/quant.db`:

| Need | In the DB | Gap |
|---|---|---|
| Commodity excess returns | Front-month continuous series only (CL1, NG1, GC1, SI1, HG1, KC1, SB1, CC1, ZC1, ZW1, ZS1; 2000–), **not roll-adjusted**. 2nd–4th month rows are empty. GSCI ER indices 400001–400011 hold **3 rows each**; Yahoo's `^SPGS*P` symbols return nothing | **Biggest gap.** No free roll-adjusted series yet. See S2 |
| Bond returns | `DGS3MO, DGS1, DGS2, DGS5, DGS10, DGS30`, `DFII5/10`, `DFF`, daily, decades | None for building returns; no ETF prices to validate against yet (IEF/TLT/SHY not collected) |
| Equity index returns | 15 indices (100001–100015), **price only, local currency** | No dividends. Futures excess return ≈ price return + dividend yield − cash; see S3 |
| FX spot | 29 pairs from Twelve Data, majors from 1971 | Fine |
| FX carry (foreign short rates) | Only 10y yields for DE/FR/GB/JP, monthly. The macro plan already flags FRED's international series as stale | **Blocks FX carry** until a source is found (R5 data gate) |
| Cash rate | `DGS3MO`, `DFF` (`DTB3` was dropped from the collector) | Pick one (S4) |

**Likely cause of EXP-0001's tails (to confirm in R2):** CL1 printed −$37.63 on 2020-04-20. Percent returns
around a negative price are meaningless, and every roll gap shows up as a fake return. This fits "best day
+119%, excess kurtosis 275".

## Decisions needed before R0

| # | Question | Recommendation |
|---|---|---|
| S1 | Universe: about 18 instruments, each mapped to a micro future or liquid ETF (draft below)? | Yes. Edit the draft list |
| S2 | Commodity returns source, if R0 confirms GSCI ER is unavailable for free | **ETF-based excess returns** (adjusted close return − cash): GLD 2004, SLV 2006, DBO/DBB/DBA/UNG 2007, CPER 2011, CORN/WEAT/SOYB 2010. Gives 15–19 years, with no roll artefacts. Before 2007, history comes only from front-month series with a known roll calendar, used for exploration only |
| S3 | Equity index excess returns | US indices: ETF total return (SPY/QQQ/IWM) − cash. Non-US: price return − cash + a fixed dividend-yield assumption per index, labelled approximate. Local-currency returns (≈ how a local futures position earns) |
| S4 | Cash rate for excess returns | `DGS3MO` (daily, long history), with `DFF` where `DGS3MO` is missing |
| S5 | Forward-tracking venue | Push the daily position log to a **public** GitHub repo. A local commit timestamp can be backdated; GitHub's push record can't. Optionally stamp each day with OpenTimestamps |
| S6 | Finish macro-plan C11 (walk-forward check) before Phase 1 freezes? | **Yes.** It's small, and trend is the first strategy that should pass it |

### Draft universe (S1)

Micro availability is to be verified in R1. Don't treat it as confirmed.

| Sleeve | Instruments | Backtest proxy | Trade vehicle |
|---|---|---|---|
| Equity | S&P 500, Nasdaq 100, Russell 2000, Euro Stoxx 50, DAX, Nikkei 225 | indices + ETFs | MES, MNQ, M2K; Eurex/OSE micros (verify) |
| Rates | UST 2y, 5y, 10y, 30y | returns built from FRED CMT yields | ZT/ZF/ZN/UB or micro yield futures (verify sizing) |
| FX | EUR, JPY, GBP, AUD, CAD, CHF vs USD | Twelve Data spot + carry | CME micro FX (M6E, M6A, M6B, …; verify JPY/CAD/CHF) |
| Commodities | WTI, nat gas, gold, silver, copper, corn | ETFs (S2) | MCL, MGC, SIL, MHG; NG and grains (verify) |

## Phase 0 — Returns layer (`src/returns/`)

Output: one function, `excess_returns(ids, start, end) -> pd.DataFrame` (daily, columns = universe ids),
plus a per-instrument quality report. Every strategy reads this function and nothing else.

### R0. Data gate (no code kept)
Spend a few hours confirming sources before building anything:
1. Retry GSCI ER on Yahoo with alternative symbols. Record the result either way.
2. Fetch adjusted-close history for the S2/S3 ETFs plus IEF, TLT, SHY, IEI. Record start dates and gaps.
3. Look for free foreign 3m rates: FRED `IR3TIB01*M156N` freshness, ECB/BoE/BoJ/RBA/BoC/SNB public APIs.
   Record freshness per currency.
4. Write the findings into this plan as the R0 outcome. S2 and S3 are confirmed or revised here.

### R0 progress (2026-10-25): foreign rates and equity dividends done
- **Foreign rates:** `src/collectors/bis.py` + `scripts/fetch_bis.py`. Daily BIS policy rates for US, XM, DE
  (to 1998), JP, GB, CH, CA, AU, NZ, SE, NO, DK, 1946–1986 onward, current to last week. Stored in
  `macro_observations` as `BIS.CBPOL.<area>`, readable with `load_fred()`. **R5 is no longer blocked.**
  Caveat: BIS splices definition changes. The euro area moves from MRO to the deposit rate on 2024-09-18,
  which reads as a 75 bp drop when the actual cut was 25 bp. R5 must handle this (e.g. use €STR from 2019).
- **Equity dividends (R4):** `src/collectors/total_return.py` (`^SP500TR`, `^RUTTR`, 8 distributing ETFs,
  new `dividends` table, migration 12) + `src/returns/dividends.py` (`load_index_total_returns`). Methods:
  exact TR index (S&P 500, Russell 2000), DAX is already total return, the other 7 indices get ETF trailing
  yield. Check: SPY's dividends reproduce `^SP500TR` to −0.07 pp/yr (TE 0.23%). Not covered: CAC, IBEX,
  Shanghai, KOSPI, Bovespa.
- Yahoo's Adj Close is wrong for ISF.L (pence dividends treated as pounds) and 1321.T (split mix-up), so
  dividend amounts are stored and used instead. Asset 500005 (1321.T) is retired, with rows left in the DB.
- Commodities (S2) and Databento/Norgate are still open.

### R1. Universe catalogue
`src/returns/universe.py`: one record per instrument with id, sleeve, backtest proxy, trade vehicle,
contract multiplier, and history start. Fixed IDs in a new 500xxx block (`src/db/asset_ids.py`) for the ETFs.

### R2. Commodity returns + EXP-0001 diagnosis
1. ETF collector (reuse `equity.py` patterns; adjusted close).
2. Excess return = ETF total return − cash.
3. **Diagnose EXP-0001:** compare CL1 front-month returns with DBO/USO and list the days where they diverge
   by more than 5%. Confirm or reject the roll-gap and negative-price explanation. Write it up as a short
   section in `experiments/EXP-0001/` notes. This doesn't need a re-run; the C9 overwrite-vs-new-id decision
   still stands.

### R3. Bond returns from yields
For maturities 2/5/10/30: price each day's par bond at yesterday's yield, then reprice at today's yield with
one day of aging. Return = coupon carry + rolldown (interpolated curve) + price change. Excess return = that −
cash. **Validation gate:** correlation > 0.98 and annual tracking error < 1% against SHY/IEI/IEF/TLT from 2002
(or each ETF's start), after matching duration. Fail → fix before Phase 1.

### R4. Equity index excess returns
As per S3. Validation: SPY-derived vs `^GSPC` + assumed dividend yield; tracking error is reported, not gated.

### R5. FX returns with carry
Spot return of foreign currency vs USD + (foreign rate − USD rate)/252. If R0 finds no fresh free foreign
rates, ship spot-only FX returns flagged `carry_missing` (enough for trend) and **block Phase 3** on data.

### R6. Quality report + tests
Per instrument: start date, % missing, max |daily return|, kurtosis, worst 5 days with dates. Fail flags:
|return| > 25%, non-positive prices, gaps > 5 business days. Tests: synthetic yield paths for R3 (flat curve
⇒ return = yield/252), a sign convention test for FX, and no NaN propagation across sleeves.

**Checkpoint 0:** quality report reviewed by the researcher; R3 validation passed.

### R3–R6 outcome (2026-10-25): returns layer for equity, rates, FX (no commodities)

Built with existing data only, as the researcher asked. `src/returns/{cash,bonds,equity,fx,quality,layer}.py`,
`scripts/returns_quality.py`. Entry point: `layer.excess_returns(db, keys, start, end)`, 23 keys:
`EQ.*` (10 indices), `RT.UST2/5/10/30`, `FX.*` (9 G10 currencies vs USD). Tests: 48 new in `tests/returns/`;
full suite green apart from the scope-related guard tests (see final report).

**Checkpoint 0 is not met:** R3's ETF validation gate (IEF/TLT/SHY/IEI) needs new data and was deferred.
Instead, yearly 10y total returns were checked against known years: 2022 −16.4%, 2020 +11.2%, 2008 +22.2%,
2023 +3.7%, 2024 −1.4%. Steep-curve years (2013 −6.6%) run about 2–3 pp above simple annual approximations
because daily rolldown is included. The researcher decides whether to fetch the four ETFs or accept this.

Choices made where the plan was open or said otherwise (spec review listed them as deviations):
- **Accrual convention:** cash and carry = rate × calendar days / day count (act/360 USD, EUR, CHF, SEK,
  NOK; act/365 GBP, JPY, AUD, NZD, CAD), not "/252". Bonds use dt = days/365.25. R6's "flat curve ⇒ yield/252"
  test is therefore "flat curve ⇒ yield × dt".
- **FX carry uses BIS policy rates on both legs**, including USD (BIS US = Fed funds target). Bonds and USD
  equity use DGS3MO/DFF (S4), so USD cash differs slightly between sleeves.
- **Equity** = local total return − local cash. HKD uses USD cash (peg). The R4 "SPY vs ^GSPC + assumed
  yield" check is replaced by the R0 SPY-vs-^SP500TR check (−0.07 pp/yr).
- **Universe:** all instruments with data (23 keys), a superset of the S1 draft; strategies pick from it.
- **Keys, not ids:** columns are string keys because R1 (the catalogue with ids) isn't built.
- **Bonds:** returns NaN after gaps over 10 days; no rolldown on days the shorter tenor is missing; FRED's
  DGS30 has values through the 2002–06 suspension, so that window is not NaN (source construction not checked).
- **Data repairs (not in the plan):**
  - Spike-and-revert bad prints are replaced by the neighbours' geometric midpoint: FX at 4%, equity TR at 10%.
    Found: EQ.RUT 2015-10-20 (`^RUTTR`); FX.EUR on the 8th of five months in 2008 (Twelve Data day/month swap);
    AUD 2008-10-15; NOK 2008-10-22; CHF 1982-12-30. The repair uses the next day's price (documented; tested as
    a known non-truncation-invariant behaviour).
  - FX: weekend rows (Twelve Data, 2024+) are dropped. A return skipping more than 3 weekdays is NaN (EUR
    2008-08-08 and 08-26). Start = later of float date and rate history.
- **Quality report:** returns-based, so there is no non-positive-price flag. FX drops spot ≤ 0 (none exist).
  Extra columns: ann_mean/vol, n>10%, max gap.
- **Carry caveats** (`cash.RATE_CAVEATS`), reported per currency, not corrected: CHF to 1999 (discount
  rate), JPY to 1998 (discount rate) plus 1999–2006 and 2013–16 (stale BIS values), SEK to 1987, EUR/DE to
  1998 (definition unchecked), EUR 2008-10-15 to 2024-09-17 (MRO 50–75 bp above overnight). Fine for trend;
  a carry strategy should start after them.

Review (one round): spec-conformance and pit-reviewer. Fixed in-diff:
- spike repair credited day t's move to day t's weight under `apply_costs` (now the midpoint repair);
- multi-day FX gaps attributed to one weight (now NaN);
- JPY stale-rate caveats added;
- bond and accrual timing tests strengthened, plus a mixed-calendar test;
- DGS30 docstring corrected.

Quality report (`scripts/returns_quality.py`): no `abs>25%` flags. `gap>5bd` on EQ.NKY (7 bdays) and FX.EUR
(Aug 2008).

**Look at first:** `execution.apply_costs` sums NaN returns as 0 (pre-existing; see final report). With
per-market calendars this is right for holidays, but Phase 1 must confirm it's the intended behaviour.

## Phase 1 — Trend baseline (`src/signals/tsmom.py`)

A deliberately standard implementation. Its value is being clean, not clever.

- **Definitions, frozen before any backtest** (`docs/research/tsmom_definitions_v1.md`): lookbacks 1/3/12m
  equal-blended, sign signal, ex-ante vol from EWMA (60-day center of mass), 40% annual vol target per
  instrument, sleeve risk parity, portfolio target 10%, daily signal with weekly rebalance, lag 1, costs per
  sleeve (not a flat 1 bp).
- **Experiment infra:** a new portfolio rule `vol_scaled_weights` in `src/experiments/portfolio.py` and a data
  source `returns_layer` in `universe.py`. Trial-ledger family `tsmom`, v1.
- **Validation:** correlation with the SG Trend Index (or the BTOP50 if SG isn't freely downloadable) over
  overlapping years; crisis alpha in 2008, 2014, 2022; C11 walk-forward; macro-regime table (C9).
- **Output:** EXP-0003 + a short research note. **Starts forward tracking (F0) the day it's frozen.**

### Phase 1 outcome so far (2026-10-25): built, not run

Ready to run once the researcher freezes the definitions. No P&L has been computed.

**Built:**
- `docs/research/tsmom_definitions_v1.md` (DRAFT, with the proposed EXP-0003 config and questions T1–T4).
- `src/signals/tsmom.py` (blended-sign trend + EWMA vol per instrument).
- `portfolio.vol_target_weights`, registered as rule `vol_target`; the plan's name `vol_scaled_weights` was
  renamed. It does equal risk per sleeve/instrument, a realised-vol portfolio scaler, the gross cap, a
  calendar-Friday rebalance on each instrument's own trading days, and a stale-data exit.
- The `returns_layer` universe, `DataConfig.instruments`, and the new `PortfolioConfig` fields.
- The runner refuses a signal universe that doesn't match `data.instruments`.

**Tests:** 25 new across `tests/signals/test_tsmom.py`, `tests/experiments/test_portfolio.py`,
`test_universe.py` and `test_runner.py`. They include truncation invariance with random inputs; a
mutation check confirmed it catches tomorrow's signal, vol or scaler, and a centred window.

**Settled from the plan's "look at first":** `apply_costs` summing NaN returns as 0 is right under
per-market calendars, provided an instrument's weight only changes on its own trading days. The rule enforces
that, and an end-to-end test through `apply_costs` checks it.

**Differences from the plan text** (all in the definitions doc):
- no separate 40% per-instrument target; risk units plus a portfolio scaler instead;
- flat 2 bp costs (T3);
- λ bounds [0.5, 5.0], set from a positions-only check;
- crisis-alpha years listed as things to report, not gates.

**Review (one round):**
- *pit-reviewer:* no leak found. Fixed: weak truncation test; rebalance rule depended on later dates;
  stale series held forever; λ estimated on the 23-key union calendar. Async closes added to limitations.
- *spec-conformance:* fixed the two-universe mismatch risk (runner check); `hold_limit` is now configurable;
  "gross" now means held gross. Documented: the cap applies per trade, the zero-return sign, the 1990 start
  reasoning, and the costless first entry (a pre-existing `apply_costs` behaviour).

**Run (2026-10-26):** the researcher froze the definitions (T1–T4 defaults) and EXP-0003 was created from the
doc's config. In-sample results (1990-01 to 2025-10, holdout locked):

| Metric | Value |
|---|---|
| Sharpe | 0.57 (t-stat 3.5) |
| Vol | 10.4% |
| CAGR | 5.5% |
| Max drawdown | −24% |
| Cost breakeven | 21 bp |
| Robustness | `NEEDS_REVIEW`: every check PASS, holdout SKIPPED (locked) |
| Walk-forward | OOS Sharpe 0.56 vs 0.67 |

Sleeve contributions (Sharpe): RT 0.57, FX 0.32, EQ 0.29. Sleeve correlations are 0.08–0.15. The strategy
earned +17.5% in 2008 and +18.3% in 2022, and averaged +5.7% in the S&P 500's 10 worst months.
By decade, Sharpe was 1.26 in the 1990s, 0.53 in the 2000s, 0.16 in the 2010s and 0.17 in the 2020s.

Fixed during the run (weights unchanged):
- `vol_target_weights` no longer stores Series in `.attrs`, which broke `pd.concat`; use `diagnostics=True`.
- Robustness charts now handle list-valued grid parameters.

Earlier status line: the researcher answers T1–T4 and freezes the doc. Then `scripts/new_experiment.py` creates EXP-0003
with the doc's config, followed by `run_experiment`, `run_robustness`, and F0.

## F0. Forward-tracking log (runs in parallel from Phase 1)
`scripts/track_positions.py`: after the US close, refresh data, compute target weights for every frozen
strategy, and append them to `tracking/<strategy>.csv` with date, weights, the data timestamp and the config
hash. Commit and push it (S5). The script must never rewrite past rows (append-only, check before writing).
Scheduled with launchd on the laptop; a missed day is backfilled by the next run and flagged `late`.
This grows into the paper-trading engine in Phase 3.

## Phase 2 — Macro duration flagship

The interview centerpiece. It will be planned in detail in its own plan file once Phase 1 is done. Skeleton:

- Target: 10y (and 2s10s) **excess returns from R3**, horizons 1m/3m.
- Signals only from frozen macro state v2 (growth impulse, inflation impulse, policy stance/impulse) and
  the rates decomposition (C4), using as-of vintages. Pricing divergence = macro state vs market-implied
  (breakeven, curve, policy expectations).
- Method: continuous regressions with HAC first (C7), then a simple, pre-registered sizing rule. Definitions
  are frozen before looking at any returns, as in C8.1.
- Must show value **beyond trend**: report alpha after regressing on Phase 1's return stream and on the 10y
  carry + rolldown alone (the naive benchmark every duration timer is compared with).
- Deep research note: data vintages, what a first-release-only version would have done (the honest-backtest
  argument), failure periods.

## Phase 3 — FX carry, combination, paper engine
FX carry (blocked by the R5 data gate) → risk layer (vol targeting, correlation-aware sleeve weights,
drawdown rules) → paper engine on F0's log (fills at next open, costs, P&L attribution).

## Phase 4 — Live micro sleeve
Minimum account size from the frozen universe: for each instrument, the smallest position (1 micro contract)
as a fraction of target risk. If rounding error is above about 30% of target, drop or proxy that instrument.
Not before 6+ months of F0 record.

## Risks
- **Commodity history is short** (ETFs from 2007), so trend's commodity sleeve has fewer crises in-sample.
  Accept it and say so in the note; don't splice unadjusted front-month data in to lengthen it.
- **Yield-built bond returns ignore futures CTD/basis**, so a few bp/yr of tracking difference versus ZN is
  expected. The R3 ETF gate bounds it.
- **Non-US equity dividends are assumed**, which biases the level of returns, not trend signals.
- **Scope creep into strategies 4–6** before the flagship is done. One deep flagship beats six shallow ones.
- **Day-job overlap (FX carry):** keep the personal version built only from public data and on personal time,
  and don't port anything from work.
