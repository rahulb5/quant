# Project overview: a personal Quant Research OS

**Purpose of this file:** a self-contained description of what is being built, why, and where it stands,
written for a reviewer who has no access to the repository. Nothing here requires reading the code.
Facts are as of 2026-10-26.

---

## 1. What this is

A **local-first, single-researcher research platform** for systematic trading ideas. The target workflow is:

> hypothesis → data → experiment → backtest → robustness → (later) portfolio → (later) monitoring

It is explicitly **not** a production trading system and not an auto-discovery engine for profitable
strategies. The goal is to give one person the research leverage of a small team: AI coding agents
(Claude Code) handle plumbing (collectors, tests, refactors, experiment execution, diagnostics, docs), while
the researcher owns hypotheses, economic reasoning, interpretation and any capital decision.

### Guiding principles (from the project's build plan)

1. **Refine before rebuilding.** Audit and improve what exists; no rewrites.
2. **Research first, infrastructure second.** No microservices, cloud, or orchestration. Everything runs on
   a laptop.
3. **Reproducibility over convenience.** Every result should trace to a data version, code version, config,
   experiment ID and timestamp.
4. **AI amplifies judgment, it is not an oracle.** Verdicts from the system never say "approved". The best
   outcome of the robustness suite is `NEEDS_REVIEW`.
5. **Free data only for now.** FRED/ALFRED, CFTC, Yahoo Finance. Paid sources (Databento, Norgate) are
   deferred.

## 2. Tech stack

- Python 3.12, pandas, statsmodels, pytest.
- **DuckDB** single file (`data/quant.db`) as the only database. Append-only versioned migrations (currently v12).
- **Streamlit** read-only dashboard.
- Jupyter notebooks for exploratory research; mature logic is then promoted into modules.
- Experiment artifacts as plain files (YAML, JSON, Parquet, PNG, Markdown) in `experiments/EXP-000N/`.
- About 820 tests, all passing.

## 3. Architecture

```
 Collectors ──► DuckDB ──► Signals ──► Experiment runner ──► Backtest stack ──► Artifacts + registry
 (Yahoo, FRED,   (single   (modules     (config → weights     (costs, perf,      (files per experiment
  CFTC COT,      file)      in src/      → returns)            conditioning,      + queryable table
  BIS)              │       signals)        ▲                   regimes)          + trial ledger)
                    │                       │                      ▲                     │
                    ├──► Returns layer ─────┘ (daily excess        │                     ▼
                    │    returns: equity, US duration, FX)         │     Robustness suite  Streamlit dashboard
                    ▼                                              │     (try to kill it)  (read-only)
              Macro layer ─────────── macro regimes, state ────────┘
              (point-in-time state,   exposure
               rates, market, regimes)
```

### 3.1 Data layer

Eight collectors share a `BaseCollector` (error handling plus an audit log table `data_fetch_log`):

| Source | Content | Asset-ID scheme |
|---|---|---|
| Yahoo Finance | Current S&P 500 constituents (530 names), 15 equity indices | ids 1–530 constituents, 100001–100015 indices |
| Yahoo Finance | Total-return sources: `^SP500TR`, `^RUTTR` and 8 local-currency distributing ETFs, with their cash dividends in a `dividends` table | 100016–100017, 500xxx |
| Yahoo Finance | Commodity futures (front-month continuous) | 300xxx |
| Yahoo Finance | S&P GSCI excess-return indices (Yahoo serves almost no history for these) | 400xxx |
| Twelve Data | Currencies (29 pairs, majors from 1971) | 200xxx |
| BIS | Daily central bank policy rates for 12 developed markets, from 1946–1986 | `macro_series` (`BIS.CBPOL.<area>`) |
| FRED/ALFRED | 58 macro series, **all vintages stored for revisable series** (so the value as published on any past date, and the true release dates, are available) | `macro_series` / `macro_observations` |
| CFTC | Commitments of Traders for a curated catalog of 35 contracts (energy, metals, ags, softs, rates, equity index, FX, VIX); legacy, disaggregated and TFF reports, weekly | `cot_positions` |

Each asset class owns a fixed id block; new assets take the next free id inside their own block.

Data repairs done in the last week:
- **COT keying bug (fixed).** Most COT series had been keyed by *exchange* code instead of *contract* code, so
  every contract on an exchange collapsed into one series. Now keyed by contract code; the corrupt series were
  purged after a backup and the catalog re-backfilled. A second bug had left the leveraged-fund / asset-manager
  columns of the TFF report empty; fixed and backfilled.
- **FRED release dates (fixed).** A frequency-mapping bug had stored five series (weekly claims, M2, the Fed
  and Bank of Japan balance sheets, the broad dollar index) with the release date equal to the period date, i.e. as if
  known on the day they measure. They were re-fetched with their real vintages. Weekly initial claims only have
  real vintages from 2009, so the state uses the not-seasonally-adjusted series, which is effectively unrevised,
  with a fixed publication lag.
- **ICE credit spreads archived.** FRED now serves only three years of the ICE high-yield / investment-grade
  spread series, so the full history (from 1996) is kept in a local archive file.

Known limitations: no raw/immutable snapshot layer (a re-fetch overwrites in place; each experiment records
`data_as_of` but the data is not frozen); equity universe is **current** S&P 500 constituents only, so there is
survivorship bias; commodity "continuous" futures have no roll methodology, so a contract roll shows up as a
one-day return jump, and carry/curve research is blocked on a paid data source. The researcher has deferred new
data sources (2026-10-25) in favour of building strategies on what exists.

### 3.1b Returns layer (`src/returns/`)

One function, `excess_returns(db, keys)`, gives strategies daily **excess returns over cash** for 23 instruments:
- 10 equity indices: total return minus local cash, in local currency;
- 4 constant-maturity US Treasuries (2/5/10/30y): built from FRED yields as coupon carry + rolldown + repricing;
- 9 G10 currencies vs USD: spot plus the BIS policy-rate differential.

Each market keeps its own trading calendar.

Construction details a reviewer should know:
- **Dividends.** S&P 500 and Russell 2000 use published total-return indices; DAX is already total return. The
  other indices take a trailing 12-month yield from a local ETF's cash dividends; on the S&P 500 this reproduces
  the official TR index to 0.07 pp/yr.
- **Data repairs.** Yahoo's adjusted close was wrong for two non-US ETFs. Twelve Data's 2008 EURUSD has
  day/month-swapped rows. Nine one-day bad prints are repaired with the neighbours' midpoint; this uses the next
  day's price, so it repairs history and isn't a live rule.
- **Carry caveats.** BIS policy rates aren't money-market rates in some periods: Swiss and Japanese discount rates
  before ~2000, and the ECB's main refinancing rate in 2008–24, which sat 50–75 bp above overnight rates. These are
  flagged, not corrected.
- **Validation.** The bond returns match known yearly returns, e.g. 10y −16.4% in 2022, +22.2% in 2008. The planned
  check against IEF/TLT is deferred (it needs new data).

### 3.2 Signals

A signal is a module under `src/signals/` exposing `load_data()`, `calculate_signal(*data, config)` and a
`SignalConfig`. Three exist:

- `cot_commercial_positioning`: 52-week rolling percentile of commercial net positioning in commodity futures.
- `formulaic_alpha_19`: one of the Kakushadze "101 Formulaic Alphas", the best of a 10-alpha screen.
- `tsmom`: time-series momentum (trend) across equity, US duration and FX. It is the mean of the 1-, 3- and
  12-month return signs, with EWMA vol (Moskowitz, Ooi and Pedersen 2012). Definitions were frozen before any
  backtest (`docs/research/tsmom_definitions_v1.md`).

A third idea (yield-curve regime) is still notebook-only because the researcher hasn't settled on a variant.
The workflow is: explore in a notebook → once a concrete signal spec is resolved, promote it to a module.

### 3.3 Backtest stack (`src/backtest/`, the most mature part)

- `execution.apply_costs(weights, returns, cost_bps, lag)`: weights decided at close *t* earn the return at
  *t+1* (lag=1). Turnover is 0.5·Σ|Δw|; cost = bps × turnover.
- `PerformanceReport`: CAGR, Sharpe, Sortino, Calmar, drawdown, VaR/CVaR, skew/kurtosis, hit rate.
- `Conditioner`: performance by year and by regime, correlations/betas to reference factors (equities, dollar,
  gold, crude), factor regression with Newey-West HAC standard errors, rolling correlations. The regime table
  reports the **number of episodes** (maximal runs of one label) beside the day count, because episodes, not
  days, are the independent observations.
- `regimes.py`: three simple market regimes: VIX terciles (expanding), 10Y-2Y sign, S&P vs 200-day MA. The
  macro regimes (3.7) plug into the same interface.
- `stats.py`: block-bootstrap Sharpe confidence interval, Deflated Sharpe Ratio (Bailey and López de Prado),
  trimmed-Sharpe helpers.
- `run_backtest()`: one-call wrapper that chains all of the above.

### 3.4 Experiment system (`src/experiments/`)

Each experiment is a YAML config (hypothesis, data, signal module, portfolio rule, cost, lag, validation,
classification, diagnostics) plus a runner that produces: `results.json`, `diagnostics.json`,
`performance.parquet`, charts, and a `research_note.md`. A DuckDB `experiments` table holds queryable headline
metrics. The runner stamps the git SHA and `data_as_of`, and supports a **holdout period** that is dropped before
backtesting and scored only once via an explicit `--unlock-holdout` flag, then frozen.

- Universes (`AssetUniverse` subclasses): commodity, equity, and `returns_layer` (the cross-asset excess returns
  of 3.1b). Adding crypto = one subclass.
- Portfolio rules (deliberately narrow, config-driven):
  - equal-weight long basket above a threshold;
  - cross-sectional decile long/short, dollar-neutral;
  - **vol-targeted time-series positions**: equal risk per sleeve and instrument, a realised-vol portfolio scaler
    to 10%, gross cap 8×, weekly calendar rebalance on each instrument's own trading days, exit on stale data.

  There is no general optimizer or risk layer.
- **Macro diagnostics.** By default every experiment is now conditioned on all nine regimes (six macro and market
  regimes from 3.7 plus the three in 3.3), reported by layer with episode counts. Optionally it also regresses the
  strategy's next-period return on the 12-variable macro state vector ("state exposure").
- **Trial ledger.** Every specification tried for a research idea is recorded in a `trials` table under a
  *family* name (e.g. `cot_commercial_positioning`). Experiment runs and notebook variants count as trials;
  robustness-grid variants are recorded but not counted. A trial's identity is a hash of its data, signal,
  portfolio and backtest settings, so re-running or re-wording doesn't inflate the count. Limitation: a notebook
  variant is counted only if the notebook calls `log_trial()`.

### 3.5 Robustness suite

Run per experiment (`run_robustness.py`). Eight checks, each `PASS`/`FLAG`/`KILL`/`SKIPPED`: cost stress (and
breakeven bps), lag sensitivity, outlier trimming, subperiods (halves, decades, crisis windows), parameter grid,
**walk-forward** (expanding ≥5-year windows pick the best grid point, scored on the next year; FLAG if
out-of-sample Sharpe < half in-sample, KILL if ≤ 0), statistical (bootstrap CI and deflated Sharpe), and
holdout. Walk-forward only tests selection *within* a config's grid, not choices made across signals. The deflated Sharpe uses the larger of the declared
trial count and the ledger's count, so a hand-set number can only raise it. The overall verdict is `KILLED` or
`NEEDS_REVIEW`, never "approved".

### 3.6 Dashboard

Read-only Streamlit, two sections:

- **Research**: experiment list, detail with Performance / Robustness / Diagnostics / Note / Config tabs. It
  reads experiment folders only, never the DB.
- **Macro**:
  - *Macro explorer*: FRED (latest or first-release vintage) and CFTC COT series, with level/change transforms
    and rolling z-scores.
  - *Macro state*: where each macro dimension is and which way it is moving (with data age), what is driving
    10-year yields, whether the market has priced what the data shows, how stretched COT positioning is, which
    regimes are active and since when, and how assets have historically responded (betas with Newey-West
    intervals, regime tables with episode counts). The state and regimes are point-in-time; the asset returns
    are ex-post, and the page says so.

  Both use short-lived read-only DuckDB handles so collectors can still write.

No run, approve or edit controls anywhere.

### 3.7 Macro research layer (`src/macro/`)

Built over the past week to a written plan. Its main object is a **continuous macro state**, not regime labels:
for each dimension, a *level* (where are we?) and an *impulse* (which way are we moving?). Regimes are a small
reporting layer on top. The research loop it supports is
"macro state → impulse → market pricing → positioning → asset implications".

- **Point-in-time construction.** Each value on date *t* is computed from the whole dataset *as published on
  t*, revisions included: the series is rebuilt at every release date and the transform (YoY, rolling mean,
  etc.) is applied to that snapshot. Stitching first releases together is not enough, because a YoY change mixes
  this month's first print with a since-revised base. History that was loaded into ALFRED in bulk (and so was
  never "released" in real time) is dropped by default.
- **State vector.** Growth, inflation, labor, policy (stance = real policy rate vs its own expanding median;
  impulse = change in the fed funds rate), financial conditions, and credit supply. Composites are means of
  expanding z-scores (no full-sample normalization), clipped at ±3, with component counts and staleness.
- **Rates decomposition.** 10Y and 5Y nominal yields split into real yield + breakeven, with a model term premium
  shown separately (it double-counts if added), and curve level and direction over 63 business days.
- **Market state and positioning**, kept separate from the macro state: credit spreads, dollar, volatility, curve,
  breakevens, a growth-pricing composite, and COT net positioning as % of open interest.
- **Regimes** (definitions frozen before any asset return was computed):

  | Regime | Labels |
  |---|---|
  | `growth_inflation` | goldilocks / reflation / stagflation / deflation (sign of growth impulse × inflation impulse) |
  | `growth_level` | above / below trend |
  | `policy` | restrictive or accommodative stance × tightening or easing direction (±25 bp dead band) |
  | `financial_conditions` | loose / normal / tight (expanding terciles) |
  | `curve_dynamics` | bull/bear steepener/flattener |
  | `dollar_trend` | strong / weak USD |

  All but `growth_level` use a causal persistence rule: a new label takes effect only after 21 consecutive
  business days.
- **Event flags**, not regimes: Sahm rule (real-time), labor deteriorating, 10Y-3M curve inverted.
- **Analysis functions**: forward-return regressions on the state (HAC lag ≥ horizon, plus a non-overlapping
  cross-check), performance by regime with an episode bootstrap, forward returns after entering a regime,
  transition matrices and durations, and a macro-vs-market "pricing divergence" (data minus market-implied, per
  dimension) with forward returns by divergence tercile. Bonds are proxied from yields (constant 8.5 duration)
  because bond ETFs are deferred.

**Validation (done before looking at any asset returns).** The state and regimes were checked against NBER
recessions, known stress episodes, other vintages of the same data, and each other:

- Growth impulse has an AUC of 0.90 against NBER recessions; the real-time Sahm flag fired in all 9 recessions
  (3–4 months after the start in five of them, earlier in two) and twice outside one.
- Financial conditions peak at the 91st–99th percentile of their history in each known stress episode since 2001.
- A latest-vintage build differs from the real-time one on 7% of days for `growth_inflation` and ~0–5% for the
  other macro regimes, so a backtest on today's revised data would modestly flatter growth-based results.
- Lookahead test: cutting the database at 2012 and at 2019 and rebuilding gives bit-identical values for all 15
  real-time inputs.
- No two regimes are near-duplicates (highest Cramér's V 0.56).
- Without the persistence rule, four regimes flipped every 3–10 days. With it, every label has at least 7
  independent episodes. This was the one definition change made after validation (frozen as "v2"); it was
  motivated by label stability, not by returns.
- One assumption failed and was replaced: continuing claims had been treated as unrevised, but they are revised
  for weeks after release, so they now use real vintages from 2009 and a lagged approximation before.

**Not done yet:** re-running EXP-0001/0002 with the new diagnostics (both are locked as complete; the researcher
has to choose between overwriting them and giving them new IDs). EXP-0003 already has them.

**Explicitly out of scope:** a global / cross-country macro layer (FRED's international series are stale; it needs
a new collector), consensus-forecast surprises (paid data), r\* estimates (not available as of past dates),
commodity curves and carry, HMM/Markov-switching models, and trading the regimes or state vector directly (that
would be its own experiment with its own hypothesis and trial family).

### 3.8 How the AI agent is constrained

The rules that keep the agent on the plumbing side are enforced by hooks that run on every file edit and shell
command, not left to instructions:

- frozen definitions documents can't be edited (except their change log);
- a completed experiment's results, diagnostics and config can't be edited;
- the agent can't write a conclusion or economic interpretation into an experiment (those stay placeholders until
  the researcher writes them);
- destructive operations (dropping or deleting data tables, deleting the DB, force-push, one-off purge scripts
  without a dry run) prompt for the researcher's approval;
- "approved", "profitable" or "ready to trade" can't appear in experiment files;
- during a scoped task, edits outside the declared file list are blocked, and only the researcher can widen it.

Each step of a plan is built by a `/plan-step` routine: declare scope, implement, test, then an independent review
by two read-only reviewer agents (one checks the code against the plan and frozen definitions, the other hunts
for lookahead), record the outcome, stop.

## 4. Current research results

| ID | Hypothesis | Headline | Robustness |
|---|---|---|---|
| EXP-0001 | Extreme commercial net-long positioning (52-week percentile ≥ 90) in 11 commodity futures predicts positive forward weekly returns | Sharpe 0.27, CAGR 4.7%, **max drawdown −77%**, t-stat 1.2, 2006–2026, weekly | `NEEDS_REVIEW` |
| EXP-0002 | Formulaic alpha #19 as a daily decile long/short on S&P 500 constituents | Sharpe 0.66, CAGR 8.6%, t-stat 3.4, 2000–2026, 1 bp costs; **cost breakeven only about 7 bps** | `NEEDS_REVIEW` |
| EXP-0003 | Standard trend following (time-series momentum) across 6 equity indices, 4 US Treasury maturities and 6 G10 currencies earns a positive risk-adjusted excess return | Sharpe 0.57, t-stat 3.5, CAGR 5.5%, vol 10.4%, max drawdown −24%, 1990–2025, 2 bp costs (breakeven 21 bp); holdout from 2025-10-25 still locked | `NEEDS_REVIEW`, all 7 scored checks PASS |

**EXP-0003 (trend baseline)** is the first experiment built end-to-end on frozen definitions. Points to weigh:
- **Robustness.** Every grid variant has a Sharpe of 0.56–0.67; the standard setting is the weakest of them, so the
  result wasn't selected. Walk-forward out-of-sample Sharpe is 0.56 vs 0.67.
- **Diversification and crisis behaviour.** Daily correlation with the S&P 500 is −0.16. It returned +17.5% in 2008
  and +18.3% in 2022, and averaged +5.7% in the S&P 500's 10 worst months (index −11.3%).
- **Sleeves.** Duration contributes most (Sharpe 0.57 vs FX 0.32 and equity 0.29), and the sleeves are nearly
  uncorrelated (0.08–0.15).
- **Decay.** Sharpe was 1.26 in the 1990s, 0.53 in the 2000s, 0.16 in the 2010s and 0.17 in the 2020s, matching
  the industry's weak post-2010 trend decade. Most of the full-sample Sharpe is pre-2010.
- **Bond bull market.** Part of the duration sleeve's contribution is the 35-year bond bull market ("long bonds")
  rather than trend skill. Separating the two is open.
- **Missing pieces.** No commodities, so it's not a full CTA replica, and the SG Trend comparison is deferred (no data).

**Walk-forward on the older experiments** (computed read-only, not stored in their artifacts): EXP-0001 PASS
(OOS 0.39 vs 0.47), EXP-0002 PASS (OOS 0.52 vs 0.55). It can't see that EXP-0002 was the best of 10 alphas.

Honest read on EXP-0001 and EXP-0002: neither is a validated edge. EXP-0001's return distribution is extreme (best day +119%,
excess kurtosis 275). That looks like a data or return-construction artifact in the continuous futures series
and **has not been investigated**. The likely cause is WTI's negative settlement on 2020-04-20 plus roll gaps. EXP-0002 carries survivorship bias and is cost-fragile. Neither has a
holdout (both predate the mechanism); data arriving after the freeze date is meant to serve as their
out-of-sample test. EXP-0002's stored results cover 503 names; a re-run would cover the current 530. Neither has
been re-run with the macro diagnostics, so there are **no macro-conditioned results yet**. The value so far is
the *pipeline* closing the loop, not the strategies.

## 5. What's next

The cross-asset plan (`docs/plans/cross_asset_strategies.md`) now drives the work. Its goal is an interview-facing
research portfolio plus a forward-tracked record that could later be traded with micro futures. Its order is:
returns layer (done for equity/rates/FX) → trend baseline (done, EXP-0003) → **macro-driven duration timing**
(the flagship, using the point-in-time macro layer) → G10 FX carry → a portfolio/risk layer and paper trading.

1. **Forward tracking for EXP-0003:** a daily, append-only log of target positions, ideally pushed to a public repo
   so the timestamps are credible. Open decision: public or local.
2. **The researcher's review of EXP-0003,** then the one-time holdout unlock.
3. **Phase 2, macro duration timing:** its own plan, with definitions frozen before any returns are seen, and
   reported as alpha beyond the trend stream and beyond plain carry + rolldown.
4. **Carried over:** the EXP-0001/0002 re-run decision; tests for the three untested collectors; a realistic-cost
   re-run of EXP-0002; investigating EXP-0001's tails.

## 6. Longer-term roadmap (from the build plan)

Completed: repository audit, experiment registry, robustness engine (now with walk-forward), read-only dashboard,
point-in-time macro layer, trial ledger, agent guardrails, cross-asset returns layer (equity/rates/FX), trend
baseline.
Not started: a general portfolio/risk layer (CVXPY), a paper-trading engine, an AI-agent tool surface
(`search_experiments()`, `create_experiment()` etc.), a raw/processed data split with dataset metadata, a
futures roll methodology, a global macro layer, and equity style factors in robustness.
The build plan's original flagship, **commodity curve carry**, is blocked on paid data. Cheap options were
identified (Databento's free credit for CME contracts from 2010; Norgate at about $270/year for deeper history),
but new data is deferred for now.

## 7. Known weaknesses and risks (please challenge these)

1. **Multiple-testing exposure.** EXP-0001 declares `n_trials=96` (grid over windows and thresholds); EXP-0002
   was picked as the best of 10 alphas. Both counts are still self-reported, because both experiments predate the
   trial ledger. EXP-0003 is the first logged in the ledger (family `tsmom`, 1 counted trial, its grid recorded but
   not counted). Notebook exploration is counted only if logged.
2. **Survivorship bias** in the equity universe.
3. **Return-series quality for futures** (see EXP-0001's extreme tails; no roll adjustment).
4. **Reproducibility gaps:** no frozen data snapshots; `git_sha` records HEAD, not uncommitted changes.
5. **Cost realism:** flat 1 bp one-way for EXP-0001/0002, flat 2 bp for EXP-0003; no per-instrument costs.
5b. **Returns-layer approximations:** bond returns come from yields, not futures. FX carry uses policy rates. Non-US
   equity dividends come from ETFs (net of fees and withholding). Markets close at different times. The bond-ETF
   validation is still deferred.
6. **Regime researcher-degrees-of-freedom:** thresholds (±25 bp policy, 0.5 pp Sahm, terciles, 21-day
   persistence) come from convention and were frozen before returns were looked at, but the 21-day rule was
   added after a validation pass. Any further change is supposed to need a written reason and a new trial-family
   version.
7. **Few independent observations.** Regimes have 25–159 episodes each, and as few as 7 for a single label;
   financial conditions cover only 2002 onward (33 episodes). Real-time history is thin for some inputs (initial
   and continuing claims only from 2009; credit-supply history was restated in 2019).
8. **Point-in-time approximations:** some series use "latest value + fixed publication lag" because they are
   (nearly) unrevised; the 2025 government shutdown delayed releases past those lags, so on a few dates a value
   is used up to a few weeks early.
9. **Single-researcher, single-machine:** DuckDB file with one writer, no CI.

## 8. Questions I'd like a reviewer to weigh in on

- Is a continuous state vector with a thin regime layer on top the right structure, given how few independent
  episodes the regimes have?
- Is as-of vintage reconstruction sufficient to rule out lookahead, or are there other leaks (e.g. seasonal-factor
  revisions, series re-based by the source, expanding z-scores on forward-filled days)?
- Was adding the 21-day persistence rule after validation (without looking at returns) acceptable, or is it a
  hidden parameter?
- Is a flat cost assumption acceptable at this stage, or should cost modeling be a priority before more signals?
- Which should come next: macro-conditioned research on the existing strategies, investigating EXP-0001's tails,
  building the raw/processed data split, or adding new signals?
- Is the robustness suite missing a check that commonly kills strategies in practice?
- For the trend baseline: how should the duration sleeve's contribution be split between trend skill and the
  1990–2020 bond bull market? And is a realised-vol portfolio scaler (rather than a covariance model) acceptable
  for a baseline?
- Given trend's weak 2010s, is the right flagship macro-driven duration timing, or something less correlated
  with trend?
