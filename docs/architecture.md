# Architecture

**Last audited:** 2026-10-19 (after adding the Streamlit dashboard shell + Research section, the
experiment classification block, migration v10, and the read-only DB mode)
**Audited by:** Claude Code (`/update-architecture` skill maintains this file — see bottom section)

This document is the Phase 1 deliverable from `quant_research_os_build_plan.md`: an inventory and
classification of what exists in this repository, so future work (by the researcher or an AI agent)
starts from an accurate map rather than re-deriving it from scratch.

---

## 1. How the system works today

### Where does data enter?
A single consistent path: `src/collectors/{commodity,equity,gsci,currency,macro,cot}.py` all
subclass `BaseCollector` (`src/collectors/base.py`), which provides `run()` (error handling) and
`_log_fetch()` (audit logging to `data_fetch_log`). COT/macro/currency were refactored out of
standalone scripts into this pattern — COT and GSCI/Commodity still bypass `run()`/`collect()` for
their bulk multi-series fetches (`fetch_report_type()`, `fetch_single()`) and call `_log_fetch()`
directly instead, which is an intentional, established escape valve for one-call-fetches-many-series
shapes, not an inconsistency.

`scripts/fetch_*.py` for all six sources are now thin CLI wrappers around the collector classes —
argument parsing and summary printing only, no fetch/parse/upsert logic.

### Where is raw data stored?
There is no raw/processed separation. Everything is written directly into DuckDB tables
(`data/quant.db`) via typed insert helpers in `data/repository.py`. There is no Parquet layer and
no immutable raw snapshot — a re-fetch overwrites/upserts in place.

### How is data transformed?
Ad hoc, inline in notebooks (`notebooks/signals/*.ipynb`). There is no `src/features/` or
transformation-versioning layer; a notebook reads straight from DuckDB and computes whatever it
needs in-cell.

### How are signals represented?
As modules under `src/signals/`, following the `load_data()` / `calculate_signal(*data, config)` /
`SignalConfig` contract from the build plan's Section 7:
- `cot_commercial_positioning.py` — the first extraction; `notebooks/signals/cot_commercial_positioning.ipynb`
  imports from it instead of duplicating the logic inline. The notebook's IC analysis,
  regime-dependence, and in-sample/out-of-sample variant-selection cells are unchanged (that's
  research process, not signal plumbing, so it stays in the notebook on purpose — see the module's
  docstring).
- `formulaic_alpha_19.py` — the second extraction, ported from the best-performing alpha (highest
  Sharpe, lowest turnover) of the 10-alpha screen in `notebooks/signals/formulaic_alphas_batch1.ipynb`
  (see `memory/formulaic_alphas_screening.md`). The notebook screens the other 9 candidates and
  stays as-is; only alpha_19 was promoted into a module. Carries the same survivorship-bias caveat
  as the notebook (current S&P 500 constituents only, via `EquityUniverse`).

Yield-curve regime (`yield_curve_regime.ipynb`) is not yet extracted — it hasn't settled on a
final signal variant (level vs. zscore vs. momentum vs. percentile) or asset weighting, so there's
nothing resolved to promote yet. Still inline-only.

### How are backtests run?
This is the most mature part of the system — `src/backtest/`:
- `metrics.py` — stateless functions (CAGR, Sharpe, Sortino, Calmar, drawdown, hit rate, ...)
- `execution.py` — `apply_costs(weights, asset_returns, cost_bps, lag)` → `ExecutionResult`
  (turnover, costs, net returns)
- `performance.py` — `PerformanceReport` wrapping a return series (equity curve, drawdown curve,
  summary)
- `reference.py` — `ReferenceData` (factor returns/levels: equities, dollar, gold, crude)
- `regimes.py` — lookahead-clean regime classifiers (vol terciles, rates, trend)
- `conditioning.py` — `Conditioner`: yearly slices, regime slices, correlations, betas,
  Newey-West-corrected factor regression, rolling correlation
- `backtest.py` — `run_backtest(weights, asset_returns, cost_bps, lag, name, ...)` →
  `BacktestReport`, a one-call wrapper over the above
- `stats.py` — significance helpers for robustness testing: `block_bootstrap_sharpe_ci()`,
  `deflated_sharpe_ratio()` (Bailey & Lopez de Prado), `sharpe_excluding_top()`,
  `sharpe_excluding_tails()`, `sharpe_excluding_best_year()`, `gaussian_top_trim_shift()`

`run_backtest()` itself still takes **weights directly** — the signal → weight translation now
exists, but one level up, in `src/experiments/` (below), not inside `src/backtest/`.

### How are experiments identified?
`src/experiments/` is a hybrid registry: a DuckDB `experiments` table (migration version 8, now at v10 —
`id`, `hypothesis`, `status`, `sharpe`, `cagr`, `max_drawdown`, `ann_turnover`, ...) for queryable
metadata, plus a filesystem `experiments/EXP-000N/` directory per experiment holding
`config.yaml`, `results.json`, `diagnostics.json`, `performance.parquet`, `charts/`, and
`research_note.md`. `scripts/new_experiment.py` allocates the next ID and scaffolds the config;
`scripts/run_experiment.py` (-> `src/experiments/runner.run_experiment()`) executes it end to end:
config -> signal module -> weights/returns panels -> `run_backtest()` -> every artifact above,
updating the registry row to `complete` (with headline metrics) or `failed` (with the error) on
exit. The runner also writes `status`/`conclusion` back into `config.yaml` so it cannot drift from
the registry, and stamps `git_sha` (HEAD at run time, which does not capture uncommitted changes)
and `data_as_of` (last date in the returns panel actually used). Re-fetching into DuckDB still
changes inputs silently — there is no raw snapshot — so `data_as_of` records but does not freeze
the data. If `validation.holdout_start` is set, the runner drops every date on/after it before
backtesting. Two experiments are live and `complete`:
- **EXP-0001** (COT commercial positioning) — the first to close the full
  hypothesis -> experiment -> backtest -> report loop.
- **EXP-0002** (formulaic alpha_19) — the first to exercise `EquityUniverse` and the
  `decile_long_short` portfolio rule; Sharpe 0.66, CAGR 8.6%, 2000–2026.

Supporting pieces make this reusable across asset classes and portfolio shapes rather than
COT-specific:
- `src/experiments/universe.py` — an `AssetUniverse` ABC (`asset_ids()` abstract;
  `load_prices()`/`returns_panel()` concrete and shared) with `CommodityUniverse` (EXP-0001) and
  `EquityUniverse` (EXP-0002) as sibling subclasses. Adding crypto later is one more subclass plus
  a `UNIVERSE_REGISTRY` entry — no changes elsewhere.
- `src/experiments/portfolio.py` — two deliberately narrow, config-driven rules (column names all
  come from `config.portfolio`, not hardcoded), not a general portfolio layer:
  `equal_weight_long_basket()` (threshold-based long basket, EXP-0001) and `decile_long_short()`
  (cross-sectional decile long/short, dollar-neutral, EXP-0002 — ported from
  `formulaic_alphas_batch1.ipynb`'s `decile_weights()`).
- `src/experiments/runner.py`'s `PORTFOLIO_REGISTRY` dispatches `config.portfolio.rule` to one of
  the two functions above; adding a new portfolio construction shape means writing one function
  and registering it there.

A signal module plugs into this by exposing `load_data()`, `calculate_signal(*data, config)`, and
a `SignalConfig` class — both `cot_commercial_positioning.py` and `formulaic_alpha_19.py` do this.

### How is robustness tested?
`src/experiments/robustness.py` (`scripts/run_robustness.py <config.yaml> [--unlock-holdout]`) is
the standard "try to kill it" suite, run per experiment rather than inside `run_experiment()`. It
loads signal data once, then sweeps cheap variants (`apply_costs()` on cached weights/returns)
for seven checks, each `PASS`/`FLAG`/`KILL`/`SKIPPED`: `cost_stress` (multipliers + breakeven bps),
`lag_sensitivity`, `outliers` (symmetric best/worst trim — one-sided trimming is biased, see the
module), `subperiods` (halves, decades, yearly, crisis windows), `parameters` (grid over dotted
config paths such as `portfolio.threshold` or `signal.parameters.trend_window`, re-computing the
signal per setting), `statistical` (block-bootstrap Sharpe CI + deflated Sharpe using
`validation.n_trials`), and `holdout`. Thresholds live in `config.robustness`
(`RobustnessConfig`) so a verdict is traceable to its rules. Overall result is `KILLED` or
`NEEDS_REVIEW`; the suite never emits "approved". The holdout is scored only with
`--unlock-holdout`, once, and frozen in `holdout.json`. Outputs: `robustness.json`,
`charts/robustness_*.png`, the research note's Robustness section, and four registry columns
(migration v9: `robustness_verdict`, `sharpe_2x_cost`, `sharpe_ci_low`, `deflated_sharpe`).
Both live experiments have been run through it (both `NEEDS_REVIEW`; neither has a holdout, since
they predate the mechanism). `scripts/new_experiment.py` defaults `holdout_start` to one year
before the scaffold date and takes `--n-trials`.

**Classification.** Each config carries an optional `classification` block
(`ClassificationConfig`: `asset_class` [market], `universe`, `signal_family`, `frequency`,
`direction`; allowed values are module-level tuples in `config.py`), separate from the technical
`data.asset_class` key (`commodity`/`equity`). Migration v10 adds `market`, `universe`,
`signal_family`, `frequency`, `direction` to the `experiments` table; `scripts/new_experiment.py`
requires them. Both live experiments are backfilled.

### How are results viewed?
`dashboard/` is a read-only Streamlit app (`st.navigation` sections; only "Research" exists).
Run: `PYTHONPATH=<repo> <venv>/bin/python -m streamlit run dashboard/app.py`. The Research pages
read experiment folders through `src/experiments/artifacts.py` (no DuckDB, no Streamlit imports),
so a running dashboard never holds the DB write lock. `Database(path, read_only=True)` exists
(skips migrations; `run`/`transaction` raise) as groundwork for future Macro/Markets pages; no page
uses it yet. No run/approve/edit controls; the verdicts never say "approved".

### Where are results stored?
Both places now, depending on what's being asked: structured/queryable headline metrics live in
the `experiments` DuckDB table (including the robustness summary columns); the full diagnostic
suite (yearly/regime/correlation/beta/factor regression), `robustness.json`, `holdout.json` (once
unlocked), the return series, and charts live under `experiments/EXP-000N/`. Pre-registry
research (formulaic alphas, yield-curve regime) is still notebook-only, as described above.

### How is portfolio construction separated from signal research?
Still no general portfolio/risk layer (no `src/portfolio/`, no CVXPY, no optimizer) — correctly
deferred per the build plan. `src/experiments/portfolio.py`'s `equal_weight_long_basket()` is the
narrow, intentional exception: a single inline rule needed to turn a signal into weights for
`run_backtest()`, not a step toward a general optimizer.

### How are tests performed?
`pytest`, coverage is improving but still uneven:
- `tests/backtest/` — full coverage of `metrics`, `performance`, `execution`, `conditioning`,
  `regimes`, `reference`, `backtest`. Solid.
- `tests/db/` — covers `src/db/client.py`, including read-only mode.
- `tests/dashboard/` — `streamlit.testing.v1.AppTest` smoke tests of every page against a tmp
  experiments tree (`QUANT_EXPERIMENTS_DIR`), including an experiment with no `robustness.json`.
- `tests/experiments/test_artifacts.py` — the artifacts loader (bare drafts, holdout states, env override).
- `tests/collectors/` — covers the three newly-refactored collectors (`currency`, `macro`, `cot`):
  asset/series creation, upsert idempotency, audit logging, cutoff/mode resolution. Uses an
  in-memory DuckDB monkeypatched into both the collector module and `src/collectors/base.py`
  (`_log_fetch` imports its own `db` reference, so both must be patched) and stubs the network
  boundary (`_fetch_ohlcv`, `fred.get_series*`, `_fetch_all_rows`).
- `src/collectors/{commodity,equity,gsci}.py` — still **zero** pytest coverage.
  `scripts/test_equity.py` and `scripts/test_macro.py` exist but are manual smoke scripts outside
  the `tests/` tree, not part of the suite.
- `tests/signals/` — covers `src/signals/cot_commercial_positioning.py` (pure-pandas transforms,
  plus `load_data()` against an in-memory DB).
- `tests/backtest/test_stats.py` — the significance helpers, including a simulation check of the
  Gaussian trim shift.
- `tests/experiments/test_robustness.py` — per-check verdict logic on synthetic series, dotted-path
  overrides, and an integration run (artifacts, registry row, note-section replacement, holdout
  stays locked until unlocked then frozen). 429 tests total.
- `tests/experiments/` — covers `universe` (asset-id resolution, returns-panel pivoting, and a
  direct check that `resample='W-MON'` reproduces `weekly_prices_with_trend()`'s own dates),
  `portfolio`, `registry` (CRUD against an in-memory DB), `config` (yaml round-trip), and one
  happy-path + one failure-path integration test for `runner.run_experiment()` against a
  monkeypatched signal.

### How does an AI agent interact with the repository?
Via `CLAUDE.md` (Python env + DB conventions) and the persistent memory file at
`memory/MEMORY.md` (gitignored, mirrored into Claude Code's auto-memory). No repo-exposed tool
surface yet (`search_experiments()`, `create_experiment()`, etc. from the build plan's Phase 5
don't exist — reasonable, since the experiment system they'd operate on doesn't exist yet either).

---

## 2. Inventory & classification

| Component | Path | Classification | Notes |
|---|---|---|---|
| DuckDB client + migrations | `src/db/client.py`, `src/db/migrations.py` | **KEEP** | Versioned, append-only migrations; singleton `db.open()`/`db.close()` pattern |
| Typed insert helpers | `data/repository.py` | **KEEP** | |
| `BaseCollector` | `src/collectors/base.py` | **KEEP** | Good abstraction; now used by all six collectors |
| Commodity/Equity/GSCI collectors | `src/collectors/{commodity,equity,gsci}.py` | **KEEP** | Follow the `BaseCollector` pattern correctly; still untested |
| Currency/Macro/COT collectors | `src/collectors/{currency,macro,cot}.py` | **KEEP** | Refactored from standalone scripts into `BaseCollector` subclasses; tested |
| Backtest engine | `src/backtest/*.py` | **KEEP** | Most mature layer in the repo; ahead of where the build plan assumes this stage would be |
| COT commercial positioning signal | `src/signals/cot_commercial_positioning.py` | **KEEP** | First signal extracted into a reusable `calculate_signal(data, config)`-style module; tested |
| Formulaic alpha_19 signal | `src/signals/formulaic_alpha_19.py` | **KEEP** | Second extraction; best-of-batch alpha from the formulaic-alphas screen; tested |
| Yield-curve regime signal | `notebooks/signals/yield_curve_regime.ipynb` | **REFACTOR** | Still inline-only — signal variant/asset-weighting never resolved in-notebook, so not yet extraction-ready |
| Formulaic alphas screen (remaining 9) | `notebooks/signals/formulaic_alphas_batch1.ipynb` | **KEEP** | Intentionally stays a screening notebook; alpha_19 was the only candidate promoted to a module |
| Experiment registry | `src/experiments/{config,registry,runner,report}.py`, `experiments/` table + dirs | **KEEP** | Hybrid DuckDB-metadata + filesystem-artifacts design; `EXP-0001`/`EXP-0002` live and `complete`; tested |
| Asset-universe abstraction | `src/experiments/universe.py` | **KEEP** | `AssetUniverse` ABC + `CommodityUniverse`/`EquityUniverse`; both now exercised by a real experiment |
| Portfolio rules | `src/experiments/portfolio.py` | **KEEP** | `equal_weight_long_basket()` + `decile_long_short()`, dispatched via `runner.py`'s `PORTFOLIO_REGISTRY`; still deliberately narrow, not a general portfolio layer |
| Experiment CLI scripts | `scripts/{new_experiment,run_experiment}.py` | **KEEP** | Thin wrappers, same convention as the `fetch_*.py` collector scripts |
| Significance helpers | `src/backtest/stats.py` | **KEEP** | Bootstrap CI, deflated Sharpe, trimmed Sharpes; tested |
| Robustness engine | `src/experiments/robustness.py`, `scripts/run_robustness.py` | **KEEP** | Seven checks, config-driven thresholds, holdout lock; run on EXP-0001/0002. Missing: equity style factors, walk-forward, regime-conditioned verdicts |
| Collector tests | `tests/collectors/` | **PARTIAL** | Covers `currency`/`macro`/`cot`; `commodity`/`equity`/`gsci` still have zero pytest coverage |
| Raw/processed data split | — | **MISSING** | Plan's Section 5.1; everything goes straight into DuckDB |
| Futures roll methodology | — | **MISSING** | Needed before any commodity curve work; no continuous-contract/roll logic exists for the 300xxx asset block |
| Portfolio/risk layer (general) | — | **MISSING** | Phase 7 — correctly deferred; see the narrow exception above |
| Dashboard | `dashboard/`, `src/experiments/artifacts.py` | **KEEP** | Read-only Streamlit shell + Research section (experiment list, detail with Performance/Robustness/Diagnostics/Note/Config tabs). Macro/Markets sections, Compare page, approval state not built |

---

## 3. Known gaps / technical debt

- `commodity`/`equity`/`gsci` collectors still have zero pytest coverage (only `currency`/`macro`/
  `cot` were tested).
- Yield-curve regime signal is still inline-only — unlike the other two signals, its research
  hasn't settled on a final variant/weighting, so there's nothing concrete to extract yet.
- Neither live experiment has a held-out period (they predate `holdout_start`); the plan is to treat
  data arriving after the freeze date as their out-of-sample test. `git_sha`/`data_as_of` are `null`
  on both for the same reason — their original inputs can't be reconstructed.
- Robustness gaps: no equity style factors (momentum/value/size) in the factor regression, no
  walk-forward validation, and `git_sha` records HEAD, so uncommitted code isn't captured.
- Cost realism: both experiments assume 1 bp one-way. EXP-0002's breakeven is only ~7 bps, so its
  cost assumption is the load-bearing one.
- No raw/processed data separation or futures roll methodology — both prerequisites for any
  commodity curve work.
- `scripts/update_commodities.py` is broken — calls `collector.fetch_single(stooq_ticker=...)` and
  reads `entry["stooq"]`, neither of which exist on the current Yahoo-Finance-based
  `CommodityCollector` (a leftover from a Stooq -> yfinance migration). `scripts/update_all.py`
  skips commodities for now rather than fixing this.
- Collectors (`commodity`, `equity`, `gsci`, `currency`, `macro`, `cot`) import the global `db`
  singleton directly rather than accepting it as a constructor/method parameter, unlike
  `src/backtest/reference.py`'s `ReferenceData`, which takes an injectable `database=` arg. This
  makes collector tests rely on monkeypatching the singleton (works, but `src/backtest/` shows a
  cleaner pattern) — worth reconciling if collectors get more test coverage.

## 4. Suggested next steps (unordered priority, smallest-step first)

1. Add `tests/collectors/` coverage for `commodity`/`equity`/`gsci` (same monkeypatch pattern as
   `currency`/`macro`/`cot` — see `tests/collectors/test_cot.py` for the `_log_fetch` gotcha).
2. Dashboard follow-ups: market/macro read API (`src/data_access/`) with Macro/Markets sections, a
   Compare page once there are >2 experiments, then an approval state and signal-agnostic
   paper-trading engine (Phase 8) once something survives the gate.
3. Close the open research questions in `yield_curve_regime.ipynb` (pick a signal variant via its
   existing IC analysis) before extracting it into `src/signals/`.
4. Fix `scripts/update_commodities.py`'s stale Stooq-era calls.

---

## 5. Maintaining this document

This file is kept current by the `update-architecture` skill (`.claude/skills/update-architecture/`).
Run it after any change that adds, removes, or reclassifies a component — new signals, new
collectors, a new `tests/` directory, the experiment registry going live, etc. Manual edits to
Sections 3–4 are expected between audits; the skill updates Sections 1–2 and the "Last audited"
line, and folds its findings into Sections 3–4 without discarding notes you've added.
