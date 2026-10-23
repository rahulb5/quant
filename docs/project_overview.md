# Project overview: a personal Quant Research OS

**Purpose of this file:** a self-contained description of what is being built, why, and where it stands,
written for a reviewer who has no access to the repository. Nothing here requires reading the code.
Facts are as of 2026-10-22 (commit `cb1c0c2` plus uncommitted COT-fix work).

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
- **DuckDB** single file (`data/quant.db`) as the only database. Append-only versioned migrations (currently v10).
- **Streamlit** read-only dashboard.
- Jupyter notebooks for exploratory research; mature logic is then promoted into modules.
- Experiment artifacts as plain files (YAML, JSON, Parquet, PNG, Markdown) in `experiments/EXP-000N/`.
- ~450 tests.

## 3. Architecture

```
 Collectors ──► DuckDB ──► Signals ──► Experiment runner ──► Backtest stack ──► Artifacts + registry
 (Yahoo, FRED,   (single   (modules     (config → weights     (costs, perf,      (files per experiment
  CFTC COT)      file)      in src/      → returns)            conditioning,      + queryable table)
                            signals)                            regimes)                 │
                                                                                         ▼
                                                              Robustness suite    Streamlit dashboard
                                                              (try to kill it)    (read-only)
```

### 3.1 Data layer

Six collectors share a `BaseCollector` (error handling plus an audit log table `data_fetch_log`):

| Source | Content | Asset-ID scheme |
|---|---|---|
| Yahoo Finance | S&P 500 constituents (~503 stocks), 15 equity indices | ids 1..~503 constituents, 100001–100015 indices |
| Yahoo Finance | Commodity futures (front-month continuous) | 300xxx |
| Yahoo Finance | S&P GSCI excess-return indices | 400xxx |
| Yahoo Finance | Currencies | — |
| FRED/ALFRED | 48 macro series, **all vintages stored for non-daily series** (so first-release values and true release dates are available) | `macro_series` / `macro_observations` |
| CFTC | Commitments of Traders (legacy, disaggregated, TFF report types), weekly | `cot_positions` |

Known limitations: no raw/immutable snapshot layer (a re-fetch overwrites in place; each experiment records
`data_as_of` but the data is not frozen); equity universe is **current** S&P 500 constituents only, so there is
survivorship bias; commodity "continuous" futures have no roll methodology, so carry/curve research is blocked
on a paid data source.

### 3.2 Signals

A signal is a module under `src/signals/` exposing `load_data()`, `calculate_signal(*data, config)` and a
`SignalConfig`. Two exist:

- `cot_commercial_positioning`: 52-week rolling percentile of commercial net positioning in commodity futures.
- `formulaic_alpha_19`: one of the Kakushadze "101 Formulaic Alphas", the best of a 10-alpha screen.

A third idea (yield-curve regime) is still notebook-only because the researcher hasn't settled on a variant.
The workflow is: explore in a notebook → once a concrete signal spec is resolved, promote it to a module.

### 3.3 Backtest stack (`src/backtest/`, the most mature part)

- `execution.apply_costs(weights, returns, cost_bps, lag)`: weights decided at close *t* earn the return at
  *t+1* (lag=1). Turnover is 0.5·Σ|Δw|; cost = bps × turnover.
- `PerformanceReport`: CAGR, Sharpe, Sortino, Calmar, drawdown, VaR/CVaR, skew/kurtosis, hit rate.
- `Conditioner`: performance by year and by regime, correlations/betas to reference factors (equities, dollar,
  gold, crude), factor regression with Newey-West HAC standard errors, rolling correlations.
- `regimes.py`: lookahead-clean classifiers: VIX terciles (expanding), 10Y-2Y sign, S&P vs 200-day MA.
- `stats.py`: block-bootstrap Sharpe confidence interval, Deflated Sharpe Ratio (Bailey and López de Prado),
  trimmed-Sharpe helpers.
- `run_backtest()`: one-call wrapper that chains all of the above.

### 3.4 Experiment system (`src/experiments/`)

Each experiment is a YAML config (hypothesis, data, signal module, portfolio rule, cost, lag, validation,
classification) plus a runner that produces: `results.json`, `diagnostics.json`, `performance.parquet`,
charts, and a `research_note.md`. A DuckDB `experiments` table holds queryable headline metrics. The runner
stamps the git SHA and `data_as_of`, and supports a **holdout period** that is dropped before backtesting and
scored only once via an explicit `--unlock-holdout` flag, then frozen.

- Universes (`AssetUniverse` subclasses): commodity, equity. Adding crypto = one subclass.
- Portfolio rules (deliberately narrow, config-driven): equal-weight long basket above a threshold; cross-sectional
  decile long/short, dollar-neutral. There is no general optimizer or risk layer.

### 3.5 Robustness suite

Run per experiment (`run_robustness.py`). Seven checks, each `PASS`/`FLAG`/`KILL`/`SKIPPED`: cost stress (and
breakeven bps), lag sensitivity, outlier trimming, subperiods (halves, decades, crisis windows), parameter grid,
statistical (bootstrap CI and deflated Sharpe using the declared number of trials), and holdout. The overall
verdict is `KILLED` or `NEEDS_REVIEW`, never "approved".

### 3.6 Dashboard

Read-only Streamlit, two sections:

- **Research**: experiment list, detail with Performance / Robustness / Diagnostics / Note / Config tabs. It
  reads experiment folders only, never the DB.
- **Macro**: explorer for FRED (latest or first-release vintage) and CFTC COT series, with level/change
  transforms and rolling z-scores. It uses short-lived read-only DuckDB handles so collectors can still write.

No run, approve or edit controls anywhere.

## 4. Current research results

| ID | Hypothesis | Headline | Robustness |
|---|---|---|---|
| EXP-0001 | Extreme commercial net-long positioning (52-week percentile ≥ 90) in 11 commodity futures predicts positive forward weekly returns | Sharpe 0.27, CAGR 4.7%, **max drawdown −77%**, t-stat 1.2, 2006–2026, weekly | `NEEDS_REVIEW` |
| EXP-0002 | Formulaic alpha #19 as a daily decile long/short on S&P 500 constituents | Sharpe 0.66, CAGR 8.6%, t-stat 3.4, 2000–2026, 1 bp costs; **cost breakeven only about 7 bps** | `NEEDS_REVIEW` |

Honest read: neither is a validated edge. EXP-0001's return distribution is extreme (best day +119%,
excess kurtosis 275). That looks like a data or return-construction artifact in the continuous futures series
and **has not been investigated**. EXP-0002 carries survivorship bias and is cost-fragile. Neither has a
holdout (both predate the mechanism); data arriving after the freeze date is meant to serve as their
out-of-sample test. The value so far is the *pipeline* closing the loop, not the strategies.

## 5. In flight: COT fix and macro regime layer

Approved plan (`docs/plans/cot_fix_and_macro_regimes.md`), done in order.

**Part A: COT collector fix (in progress, uncommitted).**
A bug keyed most COT series by *exchange* code instead of *contract* code, so every contract on an exchange
collapsed into one series and only the last-written contract per week survived. Only the 11 targeted
commodity series were valid. Fix: key by trimmed 6-character contract code, restrict collection to a curated
catalog of ~35 contracts (rates, equity index, FX, VIX, plus the existing 11 commodities), delete the 99
corrupt series (~50k rows) after a file backup, backfill, and validate (spot-check against CFTC's published
reports; re-run EXP-0001 as a regression check, since its inputs were never affected).

**Part B: macro regime layer (`src/macro/`).** Rule-based regimes computed strictly from information
available on each date:

- `pit.py`: point-in-time alignment. Observations are placed on their *release* date using first-release
  vintages (with per-series overrides, e.g. weekly claims are published 5 days after the stored date).
- `indicators.py`: growth momentum, inflation momentum, financial stress, policy stance, curve dynamics,
  dollar trend, and COT crowding. Composites use expanding z-scores (no full-sample normalization).
- `regimes.py`: six new classifiers (growth×inflation four-state, financial stress terciles, policy
  easing/hold/tightening, curve bull/bear steepener/flattener, dollar trend, Sahm-rule recession risk), added to
  the existing three for nine in total. They plug into the existing `Conditioner`, so **every experiment
  automatically gets macro-regime diagnostics**.
- `analysis.py`: asset performance by regime, forward returns after entering a regime, transition matrices,
  durations. Bonds are proxied from yields (`r ≈ y/252 − D·Δy`, D≈8.5) because ETFs are deferred.
- A dashboard page with current regime, timeline, asset-by-regime table, transitions and indicator drilldown.

**Research discipline built into the plan:** regime definitions are frozen and committed *before* any
asset-performance numbers are produced; first-release vs latest-vintage labels are compared to quantify
lookahead; regimes that largely duplicate each other get dropped; and `n_obs` plus episode count are shown
beside every statistic because four-state regimes over about 50 years yield only a handful of episodes.

**Explicitly out of scope:** commodity curves and carry, splicing predecessor COT contracts, HMM/Markov-switching
models, and trading regimes directly as a strategy (regimes are diagnostics and conditioning inputs).

## 6. Longer-term roadmap (from the build plan)

Completed: repository audit, experiment registry, robustness engine, read-only dashboard.
Not started: a general portfolio/risk layer (CVXPY), a paper-trading engine, an AI-agent tool surface
(`search_experiments()`, `create_experiment()` etc.), a raw/processed data split with dataset metadata, a
futures roll methodology, and equity style factors / walk-forward validation in robustness.
The build plan's intended first flagship project is **commodity curve carry**, which is blocked on paid data.

## 7. Known weaknesses and risks (please challenge these)

1. **Multiple-testing exposure.** EXP-0001 declares `n_trials=96` (grid over windows and thresholds); EXP-0002
   was picked as the best of 10 alphas. Deflated Sharpe uses these declared counts, which depend on honest
   self-reporting.
2. **Survivorship bias** in the equity universe.
3. **Return-series quality for futures** (see EXP-0001's extreme tails).
4. **Reproducibility gaps:** no frozen data snapshots; `git_sha` records HEAD, not uncommitted changes.
5. **Cost realism:** flat 1 bp one-way for everything.
6. **Regime researcher-degrees-of-freedom:** thresholds (±25 bp policy, 0.5 pp Sahm, terciles, 21-day hysteresis)
   come from convention, but any iteration after seeing performance would be a hidden parameter.
7. **Short history for first-release regimes:** usable from roughly the 1970s, and several indicators start
   only in 1997–2006.
8. **Single-researcher, single-machine:** DuckDB file with one writer, no CI.

## 8. Questions I'd like a reviewer to weigh in on

- Is the regime set sensible as *conditioning diagnostics*, or is it too much structure for the data available
  (few independent episodes)?
- Is first-release / release-date alignment sufficient to rule out lookahead, or are there other leaks
  (e.g. vintage revisions of seasonal factors, FRED series re-based)?
- Is a flat cost assumption acceptable at this stage, or should cost modeling be a priority before more signals?
- Which should come next: investigating EXP-0001's tails, building the raw/processed data split, or adding
  new signals?
- Is the robustness suite missing a check that commonly kills strategies in practice?
