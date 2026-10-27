# Architecture

**Last audited:** 2026-10-26 (after cross-asset Phase 1: the `tsmom` trend signal, the `vol_target`
portfolio rule, the `returns_layer` universe and EXP-0003. Before that, 2026-10-25: cross-asset Phase 0 (BIS and
total-return collectors, `dividends` table / migration v12, the `src/returns/` layer) and macro-plan C11
(walk-forward). Earlier, 2026-10-24: macro research layer C0–C10 and C12, asset-id blocks, agentic guardrails)
**Audited by:** Claude Code (`/update-architecture` skill maintains this file — see bottom section)

This document is the Phase 1 deliverable from `quant_research_os_build_plan.md`: an inventory and
classification of what exists in this repository, so future work (by the researcher or an AI agent)
starts from an accurate map rather than re-deriving it from scratch.

---

## 1. How the system works today

### Where does data enter?
A single consistent path: `src/collectors/{commodity,equity,gsci,currency,macro,cot,bis,total_return}.py` all
subclass `BaseCollector` (`src/collectors/base.py`), which provides `run()` (error handling) and
`_log_fetch()` (audit logging to `data_fetch_log`). COT/macro/currency were refactored out of
standalone scripts into this pattern — COT and GSCI/Commodity still bypass `run()`/`collect()` for
their bulk multi-series fetches (`fetch_report_type()`, `fetch_single()`) and call `_log_fetch()`
directly instead, which is an intentional, established escape valve for one-call-fetches-many-series
shapes, not an inconsistency.

COT only collects the `COT_CONTRACTS` catalog (35 contracts across energy, metals, ags, softs, rates,
equity index, FX and VIX; legacy/legacy-combined/disaggregated/TFF reports as applicable). Every series
is keyed by the 6-character CFTC **contract** code (`CFTC.<REPORT>.<contract>`), never the exchange
code; adding a contract is one catalog entry plus `fetch_cot.py --mode backfill --contract <code>`.
Known history breaks (Russell 2000 e-mini moved to ICE 2008–2018; SOFR from 2018) are documented in
the catalog, not spliced. `src/collectors/cot_cleanup.py` + `scripts/migrate_cot_contract_keys.py` is
the one-off purge of the old exchange-keyed series.

`scripts/fetch_*.py` for all eight sources are now thin CLI wrappers around the collector classes —
argument parsing and summary printing only, no fetch/parse/upsert logic.

Two collectors added 2026-10-25:
- `bis.py` (`scripts/fetch_bis.py`) — daily central bank policy rates from the public BIS SDMX API for 12
  developed markets (US, euro area, Germany to 1998, JP, GB, CH, CA, AU, NZ, SE, NO, DK). Stored in
  `macro_observations` as `BIS.CBPOL.<area>` with `release_date = period_date`, so `load_fred()` and the
  macro explorer read them unchanged.
- `total_return.py` (`scripts/fetch_total_return.py`) — `^SP500TR`, `^RUTTR` and 8 local-currency
  distributing ETFs. Prices with `auto_adjust=False`, plus cash dividends into the new `dividends` table
  (migration v12). Yahoo's Adj Close is wrong for some non-US listings, so it's stored but not used. The
  fetch is always full history, because Yahoo restates history on splits.

**Asset ids** are allocated per block by `src/db/asset_ids.py` (`next_asset_id(db, block)`: equities
1+, equity indices 100001+, futures 300xxx, GSCI 400xxx, ETFs 500xxx with `asset_class='equity'`),
replacing the old global `MAX(id)+1`. The total-return catalogue uses fixed ids: 100016–100017 and
500001–500009, with 500005 (1321.T) retired but its rows still stored.
`src/collectors/asset_id_cleanup.py` + `scripts/renumber_misplaced_assets.py` moved the 27
constituents that had landed at 400012–400038 to 504–530 (run 2026-10-22).

**FRED vintages.** The macro collector fetches ALFRED vintages, so `macro_observations` holds every
release of a period (`release_date`, `is_final`). A frequency-mapping bug had turned vintage fetching
off for ICSA, WM2NS, WALCL, JPNASSETS and DTWEXBGS, which were stored with `release_date ==
period_date`; `src/collectors/fred_cleanup.py` + `scripts/purge_stamped_fred_rows.py` re-fetched them
with real releases and deleted the stamped rows (applied 2026-10-22, backup
`data/quant.db.bak-20261022-fred-stamped`). FRED now serves only three years of the ICE credit series
(`BAML*`), so the full history is archived at `data/archive/ice_credit_spreads.parquet` and never
purged.

### Where is raw data stored?
There is no raw/processed separation. Everything is written directly into DuckDB tables
(`data/quant.db`) via typed insert helpers in `data/repository.py`. There is no Parquet layer and
no immutable raw snapshot — a re-fetch overwrites/upserts in place.

### How is data transformed?
Three paths now. Signal research is still ad hoc, inline in notebooks (`notebooks/signals/*.ipynb`)
or inside a signal module's `calculate_signal()`; there is no general `src/features/` layer.

Macro data has a proper, point-in-time transformation layer in `src/macro/` (built per
`docs/plans/macro_research_layer.md`; definitions frozen in `docs/research/macro_state_definitions_v1.md`,
amended as family v2 in `macro_state_definitions_v2.md`). Every module except the loaders is pure
pandas, with no DB access:
- `data.py` — loaders: `list_series`, `load_fred` (latest or first vintage), `load_vintages`,
  `load_cot`, `cot_measures`.
- `pit.py` — as-of reconstruction: `realtime(vintages, fn)` rebuilds the series at every release
  date and applies a causal transform to that snapshot (so YoY etc. never mix vintages);
  `unrevised()` for series documented as unrevised; `to_daily()` onto a business-day grid. Pre-vintage
  history is dropped by default.
- `inputs.py` — the only module combining DB loading with `pit`: `load_state_inputs`,
  `load_rates_inputs`, `load_market_inputs`, `load_prices`, and `load_reference_returns` (S&P 500,
  Nasdaq-100, GC1, CL1, broad dollar, a constant-duration 10Y Treasury proxy).
- `state.py` — `macro_state()`: level and impulse per dimension (growth, inflation, labor, policy,
  ...) as expanding, clipped z-score composites, with component counts and staleness.
- `rates.py` — 10Y/5Y nominal decomposition (real + breakeven, term premium) and curve state over
  `CHANGE_WINDOW` = 63 business days; `treasury_proxy_returns()`.
- `market.py` — market state and positioning (credit spreads, dollar, vol, curve, breakevens, COT
  net % OI), i.e. what markets are pricing, kept separate from the macro state.
- `regimes.py` — `Regime` subclasses over the state (`growth_inflation`, `growth_level`, `policy`,
  `financial_conditions` in the macro layer; `curve_dynamics`, `dollar_trend` in the market layer),
  a causal `min_duration` = 21 filter (all but `growth_level`), `REGIME_REGISTRY`, a shared
  `StatePanel`, and `build_regimes(names)`.
- `flags.py` — boolean event flags (Sahm rule, labor deteriorating, curve inverted), not regimes.
- `analysis.py` — returns vs state: HAC forward regressions, `performance_by_regime`, episode
  bootstrap, forward returns by regime, macro-vs-market pricing divergence, transition matrix.
- `validation.py` + `scripts/macro_state_validation.py` — the numbers behind
  `docs/research/macro_state_validation.md` (episode counts, recession checks, real-time vs latest).
- `transforms.py` — explorer transforms (inferred sampling rate, level/change, rolling z).

Cross-asset returns have their own layer, `src/returns/` (Phase 0 of `docs/plans/cross_asset_strategies.md`,
equity/rates/FX only). Strategies are meant to read `layer.excess_returns(db, keys, start, end)` and
nothing else: daily excess returns over cash, 23 string keys (`EQ.*` 10 indices, `RT.UST2/5/10/30`, `FX.*`
9 G10 currencies vs USD), and each market keeps its own calendar (NaN on its non-trading days).
- `cash.py` — `cash_rate()` (USD: DGS3MO, falling back to DFF; others: BIS policy rate, EUR spliced DE → XM
  in 1999), `policy_rate()`, `accrue()` (rate known at t−1 × calendar days / day count), and `RATE_CAVEATS`
  (periods where the BIS series isn't a money-market rate).
- `bonds.py` — constant-maturity par-bond returns from FRED CMT yields (coupon carry + rolldown + repricing).
- `dividends.py` — local total returns per equity index: exact TR index, DAX as a performance index, or
  price + trailing ETF dividend yield.
- `equity.py` — total return minus local cash.
- `fx.py` — spot + policy-rate carry. Repairs spike-and-revert prints, drops weekend rows, and nulls gaps
  of more than 3 weekdays.
- `quality.py` — `quality_report()` and `remove_return_spikes()`. `scripts/returns_quality.py` prints the
  report.

The bad-print repairs look one day ahead (a repair of stored history, listed in `.attrs`). Commodities are
not in the layer yet.

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

- `tsmom.py` — time-series momentum, the cross-asset trend baseline. It is the mean of the 21/63/252-day return
  signs, plus EWMA vol (centre of mass 60), per instrument on its own trading days. It reads the returns layer
  (`load_data()` = `layer.excess_returns(db)`), and `SignalConfig.instruments` selects the universe. It was written
  from frozen definitions rather than promoted from a notebook (`docs/research/tsmom_definitions_v1.md`, FROZEN v1
  2026-10-26).

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
  Newey-West-corrected factor regression, rolling correlation. The regime table is the shared
  `regime_performance()` (also used by `src/macro/analysis.py`) and reports `n_episodes` and
  `mean_episode_length`, since a regime's independent evidence is its episodes, not its days
- `backtest.py` — `run_backtest(weights, asset_returns, cost_bps, lag, name, ...)` →
  `BacktestReport`, a one-call wrapper over the above
- `stats.py` — significance helpers for robustness testing: `block_bootstrap_sharpe_ci()`,
  `deflated_sharpe_ratio()` (Bailey & Lopez de Prado), `sharpe_excluding_top()`,
  `sharpe_excluding_tails()`, `sharpe_excluding_best_year()`, `gaussian_top_trim_shift()`

`run_backtest()` itself still takes **weights directly** — the signal → weight translation now
exists, but one level up, in `src/experiments/` (below), not inside `src/backtest/`.

### How are experiments identified?
`src/experiments/` is a hybrid registry: a DuckDB `experiments` table (migration version 8; the schema is now at v12 —
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
backtesting. Three experiments are live and `complete`:
- **EXP-0001** (COT commercial positioning) — the first to close the full
  hypothesis -> experiment -> backtest -> report loop.
- **EXP-0002** (formulaic alpha_19) — the first to exercise `EquityUniverse` and the
  `decile_long_short` portfolio rule; Sharpe 0.66, CAGR 8.6%, 2000–2026.
- **EXP-0003** (trend baseline, family `tsmom`) — the first on the `returns_layer` universe and the `vol_target`
  rule, the first with a holdout (from 2025-10-25, still locked), and the first logged in the trial ledger.
  In-sample 1990–2025: Sharpe 0.57; robustness `NEEDS_REVIEW` with every scored check PASS.

Supporting pieces make this reusable across asset classes and portfolio shapes rather than
COT-specific:
- `src/experiments/universe.py` — an `AssetUniverse` ABC (`asset_ids()` abstract;
  `load_prices()`/`returns_panel()` concrete and shared) with `CommodityUniverse` (EXP-0001) and
  `EquityUniverse` (EXP-0002) as sibling subclasses. `ReturnsLayerUniverse` (`returns_layer`, EXP-0003)
  overrides `returns_panel()` to read `src/returns/layer.py` directly; its columns are layer keys, and
  `asset_ids()` is empty by design. `DataConfig.instruments` restricts the panel, and the runner refuses
  a signal universe that differs from it. Adding crypto later is one more subclass plus a
  `UNIVERSE_REGISTRY` entry — no changes elsewhere.
- `src/experiments/portfolio.py` — three deliberately narrow, config-driven rules (column names all
  come from `config.portfolio`, not hardcoded), not a general portfolio layer:
  `equal_weight_long_basket()` (threshold-based long basket, EXP-0001) and `decile_long_short()`
  (cross-sectional decile long/short, dollar-neutral, EXP-0002 — ported from
  `formulaic_alphas_batch1.ipynb`'s `decile_weights()`), and `vol_target_weights()` (registry key
  `vol_target`, EXP-0003). The vol-target rule does:
  - equal risk per sleeve and per live instrument;
  - a realised-vol portfolio scaler to `target_vol`, computed on the portfolio's own trading rows;
  - a gross cap;
  - a calendar-Friday rebalance on each instrument's own trading days, so a weight never changes on that
    instrument's holiday;
  - an exit after `hold_limit` rows of missing data.

  Its settings are extra `PortfolioConfig` fields, ignored by the other rules. `diagnostics=True` returns
  the scaler and gross; they are not kept in `.attrs`, because pandas can't concatenate frames whose attrs
  hold Series.
- `src/experiments/runner.py`'s `PORTFOLIO_REGISTRY` dispatches `config.portfolio.rule` to one of
  the functions above; adding a new portfolio construction shape means writing one function
  and registering it there.

A signal module plugs into this by exposing `load_data()`, `calculate_signal(*data, config)`, and
a `SignalConfig` class — `cot_commercial_positioning.py`, `formulaic_alpha_19.py` and `tsmom.py` do this.

**Macro diagnostics.** `config.diagnostics` (`DiagnosticsConfig`) selects `regimes` (default: every
`REGIME_REGISTRY` key, macro and market layers) and an optional `state_exposure` table (next-period
net return regressed on the 12-column macro state vector, `analysis.forward_regression` at h = 1).
`run_experiment()` passes the built regimes to `run_backtest(regimes=...)` and writes `regime_layers`
and `state_exposure` into `diagnostics.json`. Neither is part of a trial's identity. Because the
default includes the macro regimes, every run builds the full `StatePanel` (1954 to today).

**Trial ledger.** Migration v11 adds a `trials` table; `src/experiments/trials.py` (`log_trial`,
`n_trials`, `effective_n_trials`, `config_hash`, ...) and `scripts/trials.py count|list|families`.
`config.validation.family` (+ `family_version`) groups trials of one idea; `new_experiment.py
--family` is required. `run_experiment()` logs a `kind="experiment"` row, `run_robustness()` logs
its base config plus each grid point as `kind="robustness"` (recorded, not counted), notebooks call
`log_trial()`. A trial's identity is a hash of its data/signal/portfolio/backtest settings.
EXP-0001 (`cot_commercial_positioning`) and EXP-0002 (`formulaic_alphas`) have families set.

### How is robustness tested?
`src/experiments/robustness.py` (`scripts/run_robustness.py <config.yaml> [--unlock-holdout]`) is
the standard "try to kill it" suite, run per experiment rather than inside `run_experiment()`. It
loads signal data once, then sweeps cheap variants (`apply_costs()` on cached weights/returns)
for eight checks, each `PASS`/`FLAG`/`KILL`/`SKIPPED`: `cost_stress` (multipliers + breakeven bps),
`lag_sensitivity`, `outliers` (symmetric best/worst trim — one-sided trimming is biased, see the
module), `subperiods` (halves, decades, yearly, crisis windows), `parameters` (grid over dotted
config paths such as `portfolio.threshold` or `signal.parameters.trend_window`, re-computing the
signal per setting), `walk_forward` (C11, 2026-10-25: expanding train windows of ≥ 5y pick the best
grid point, scored on the next year; FLAG if out-of-sample Sharpe < 0.5 × in-sample, KILL if ≤ 0; shares
the grid backtests with `parameters` via `grid_net_returns()`), `statistical` (block-bootstrap Sharpe CI + deflated Sharpe using
`max(validation.n_trials, ledger n_trials(family))`, so a hand-set count can only raise it; both
numbers go into `robustness.json` and the research note), and `holdout`. Thresholds live in `config.robustness`
(`RobustnessConfig`) so a verdict is traceable to its rules. Overall result is `KILLED` or
`NEEDS_REVIEW`; the suite never emits "approved". The holdout is scored only with
`--unlock-holdout`, once, and frozen in `holdout.json`. Outputs: `robustness.json`,
`charts/robustness_*.png`, the research note's Robustness section, and four registry columns
(migration v9: `robustness_verdict`, `sharpe_2x_cost`, `sharpe_ci_low`, `deflated_sharpe`).
Both live experiments have been run through it (both `NEEDS_REVIEW`; neither has a holdout, since
they predate the mechanism). Their stored `robustness.json` predates `walk_forward`. It was computed
read-only on 2026-10-25 (both PASS; numbers in the C11 outcome of the macro plan). `scripts/new_experiment.py` defaults `holdout_start` to one year
before the scaffold date and takes `--n-trials`.

**Classification.** Each config carries an optional `classification` block
(`ClassificationConfig`: `asset_class` [market], `universe`, `signal_family`, `frequency`,
`direction`; allowed values are module-level tuples in `config.py`), separate from the technical
`data.asset_class` key (`commodity`/`equity`). Migration v10 adds `market`, `universe`,
`signal_family`, `frequency`, `direction` to the `experiments` table; `scripts/new_experiment.py`
requires them. Both live experiments are backfilled.

### How are results viewed?
`dashboard/` is a read-only Streamlit app with two `st.navigation` sections: "Research" and "Macro".
Run: `PYTHONPATH=<repo> <venv>/bin/python -m streamlit run dashboard/app.py`. The Research pages
read experiment folders through `src/experiments/artifacts.py` (no DuckDB, no Streamlit imports),
so a running dashboard never holds the DB write lock. The Research Diagnostics tab shows the
"Performance by regime" table one row per regime label, grouped by layer (macro, market), with
`n_episodes` (pre-C9 diagnostics load under "unassigned").

The **Macro** section (`dashboard/macro/explorer.py`) is the first DuckDB-backed page. It uses
`Database(path, read_only=True)` (skips migrations; `run`/`transaction` raise) through short-lived
handles in `dashboard/components.py` (`load_catalog()`, `load_series()`), cached with
`st.cache_data` keyed on the DB file's mtime, so a collector run invalidates the cache and the
dashboard never holds a lock. The page picks a FRED series (latest or first-release vintage), a BIS
policy rate, or a CFTC COT positioning measure (filterable by asset group; optionally % of open interest), applies a level/change transform,
and plots it with a rolling z-score. Data access and math live outside Streamlit in `src/macro/`:
`data.py` (`list_series`, `load_fred`, `load_cot`, `cot_measures`; each takes a `Database`) and
`transforms.py` (pure: `periods_per_year` inferred from dates since stored `frequency` labels are
unreliable, `transform`, `rolling_zscore`). No run/approve/edit controls; the verdicts never say
"approved".

The **Macro state** page (`dashboard/macro/state.py`, `url_path="macro-state"`) is built
from one bundle (`components.macro_state_bundle()` / `load_macro_state()`, same read-only, mtime-keyed cache; ~12 s uncached). Panels: state cards (level, impulse, z, staleness),
10Y rates decomposition, macro vs market-implied divergence with forward returns by gap tercile,
COT positioning z-scores, current regimes + event flags + timeline, and asset response (univariate
HAC betas per +1 s.d. and the regime table with `n_episodes`). State/regime panels are point-in-time;
the asset returns are ex-post, and the page says so. Checked running by the researcher 2026-10-24.

### Where are results stored?
Both places now, depending on what's being asked: structured/queryable headline metrics live in
the `experiments` DuckDB table (including the robustness summary columns); the full diagnostic
suite (yearly/regime/correlation/beta/factor regression), `robustness.json`, `holdout.json` (once
unlocked), the return series, and charts live under `experiments/EXP-000N/`. Pre-registry
research (formulaic alphas, yield-curve regime) is still notebook-only, as described above.

### How is portfolio construction separated from signal research?
Still no general portfolio/risk layer (no `src/portfolio/`, no CVXPY, no optimizer) — correctly
deferred per the build plan. `src/experiments/portfolio.py`'s `equal_weight_long_basket()`, `decile_long_short()` and
`vol_target_weights()` are the narrow, intentional exceptions: single config-driven rules needed to turn a signal into weights for
`run_backtest()`, not a step toward a general optimizer.

### How are tests performed?
`pytest`, coverage is improving but still uneven:
- `tests/backtest/` — full coverage of `metrics`, `performance`, `execution`, `conditioning`,
  `regimes`, `reference`, `backtest`. Solid.
- `tests/db/` — covers `src/db/client.py` (including read-only mode) and `asset_ids.py`.
- `tests/dashboard/` — `streamlit.testing.v1.AppTest` smoke tests of every page against a tmp
  experiments tree (`QUANT_EXPERIMENTS_DIR`), including an experiment with no `robustness.json`,
  and the Macro explorer and Macro state pages against a temp DuckDB (`DB_PATH`); the Macro state
  page is mostly tested with a synthetic bundle, the real loader only on the sparse fixture DB.
- `tests/macro/` — every `src/macro/` module: `data`, `pit`, `state`, `rates`, `market`, `regimes`
  (incl. truncation/look-ahead checks), `analysis`, `validation`, `transforms`, and
  `inputs.load_reference_returns` (`test_inputs_reference.py`), on a shared `conftest.py` fixture.
- `tests/hooks/` — `guard.py` rules and `scope.py`. These fail while a `/plan-step` scope is active
  (`.claude/scope.json` present), by design.
- `tests/experiments/test_artifacts.py` — the artifacts loader (bare drafts, holdout states, env override).
- `tests/returns/` — every `src/returns/` module on synthetic data in an in-memory DB (shared
  helpers in `test_cash.py`):
  - accrual timing;
  - bond carry, duration, rolldown and timing;
  - dividend events and loaders;
  - equity cash and spike repair;
  - FX inversion, carry, float dates, weekends and gaps;
  - the quality flags and a documented non-truncation-invariance test for the spike repair;
  - a mixed-calendar layer test.
- `tests/collectors/` — covers `currency`, `macro`, `cot`, `bis`, `total_return` and the one-off
  `cot_cleanup`, `fred_cleanup`, `asset_id_cleanup` modules. For the first three collectors:
  asset/series creation, upsert idempotency, audit logging, cutoff/mode resolution. COT also tests
  contract keying (two contracts on one exchange stay separate, whitespace trimmed, rows with no code
  skipped), the catalog, realistic TFF/legacy field names, and `cot_cleanup` (`test_cot_cleanup.py`).
  821 tests total across the suite (2026-10-26). Uses an
  in-memory DuckDB monkeypatched into both the collector module and `src/collectors/base.py`
  (`_log_fetch` imports its own `db` reference, so both must be patched) and stubs the network
  boundary (`_fetch_ohlcv`, `fred.get_series*`, `_fetch_all_rows`).
- `src/collectors/{commodity,equity,gsci}.py` — still **zero** pytest coverage.
  `scripts/test_equity.py` and `scripts/test_macro.py` exist but are manual smoke scripts outside
  the `tests/` tree, not part of the suite.
- `tests/signals/` — covers `src/signals/cot_commercial_positioning.py` (pure-pandas transforms,
  plus `load_data()` against an in-memory DB), `formulaic_alpha_19.py`, and `tsmom.py`, including signal
  truncation invariance.
- `tests/experiments/test_portfolio.py` covers `vol_target_weights`:
  - risk shares, scaler bounds and gross cap;
  - weekly calendar rebalance, holiday hold (including end-to-end through `apply_costs`) and stale-data exit;
  - truncation invariance on random inputs at Friday and mid-week cuts. A mutation check confirmed it catches
    tomorrow's signal, vol or scaler, and a centred window.
- `tests/backtest/test_stats.py` — the significance helpers, including a simulation check of the
  Gaussian trim shift.
- `tests/experiments/test_robustness.py` — per-check verdict logic on synthetic series (including
  walk-forward on a known-overfit grid → FLAG/KILL, train-only selection), dotted-path overrides, and an
  integration run (artifacts, registry row, note-section replacement, holdout
  stays locked until unlocked then frozen).
- Other `tests/experiments/` files — cover `universe` (asset-id resolution, returns-panel pivoting, and a
  direct check that `resample='W-MON'` reproduces `weekly_prices_with_trend()`'s own dates),
  `portfolio`, `registry` (CRUD against an in-memory DB), `config` (yaml round-trip, diagnostics
  defaults), `trials` (ledger hashing and counting), and integration tests for
  `runner.run_experiment()` against a monkeypatched signal (happy path, failure path, regime layers,
  state exposure). The runner fixture patches the DB singleton so macro regimes don't read
  `data/quant.db`.

### How does an AI agent interact with the repository?
Via `CLAUDE.md` (Python env + DB conventions) and the persistent memory file at
`memory/MEMORY.md` (gitignored, mirrored into Claude Code's auto-memory).

Phase A of `docs/plans/agentic_workflow.md` is built and enforces the build plan's boundaries in
code rather than memory:
- **Hooks** (`.claude/settings.json`): `.claude/hooks/guard.py` (PreToolUse) blocks edits to FROZEN
  definitions docs above their change log (G1), to completed experiments' artifacts (G2), and to
  conclusions / economic-rationale sections (G3); asks before destructive SQL, DB deletion,
  force-push or one-off purge/migrate/renumber scripts without `--dry-run` (G4); blocks
  "approved"/"profitable" language under `experiments/` (G5). `QUANT_ALLOW_RESULT_EDIT=1` lifts
  G2/G3/G5. `.claude/hooks/scope.py` (PostToolUse) enforces a per-task file scope
  (`.claude/scope.json`, S1/S2), which only the researcher widens.
- **Reviewer agents** (`.claude/agents/`): `pit-reviewer` (look-ahead hunting in macro/COT/vintage
  code) and `spec-conformance-reviewer` (implementation vs plan step and frozen definitions). Both
  read-only.
- **Skills** (`.claude/skills/`): `/plan-step` (one scoped plan step: implement, test, diff-scoped
  review, record outcome, stop), `notebook-to-signal`, `update-architecture`.

Phases B onward of the agentic plan are not built. No repo-exposed tool
surface yet (`search_experiments()`, `create_experiment()`, etc. from the build plan's Phase 5
don't exist). The experiment registry and artifacts loader they would wrap now exist, so this is no
longer blocked, just not started.

---

## 2. Inventory & classification

| Component | Path | Classification | Notes |
|---|---|---|---|
| DuckDB client + migrations | `src/db/client.py`, `src/db/migrations.py` | **KEEP** | Versioned, append-only migrations; singleton `db.open()`/`db.close()` pattern |
| Typed insert helpers | `data/repository.py` | **KEEP** | |
| `BaseCollector` | `src/collectors/base.py` | **KEEP** | Good abstraction; now used by all eight collectors |
| BIS policy-rate collector | `src/collectors/bis.py`, `scripts/fetch_bis.py` | **KEEP** | `BaseCollector` via `run()`, one request per area; reuses `macro_observations`; tested. Rate-definition breaks are documented, not adjusted |
| Total-return collector | `src/collectors/total_return.py`, `scripts/fetch_total_return.py`, `dividends` table (v12) | **KEEP** | `BaseCollector` via `run()`; fixed-id catalogue; full-refresh by design; tested |
| Returns layer | `src/returns/{cash,bonds,dividends,equity,fx,quality,layer}.py`, `scripts/returns_quality.py` | **REFINE** | One entry point for equity/rates/FX excess returns; pure functions plus thin loaders; tested and look-ahead reviewed. Missing commodities, the R1 catalogue (string keys, not ids) and the R3 ETF validation |
| Commodity/Equity/GSCI collectors | `src/collectors/{commodity,equity,gsci}.py` | **KEEP** | Follow the `BaseCollector` pattern correctly; still untested |
| Currency/Macro/COT collectors | `src/collectors/{currency,macro,cot}.py` | **KEEP** | Refactored from standalone scripts into `BaseCollector` subclasses; tested. COT now collects a 35-contract catalog keyed by contract code |
| COT cleanup | `src/collectors/cot_cleanup.py`, `scripts/migrate_cot_contract_keys.py` | **KEEP** | One-off purge of exchange-keyed series (dry-run, backup, FK-ordered deletes); idempotent, tested. Already run; safe to retire later |
| Backtest engine | `src/backtest/*.py` | **KEEP** | Most mature layer in the repo; ahead of where the build plan assumes this stage would be |
| COT commercial positioning signal | `src/signals/cot_commercial_positioning.py` | **KEEP** | First signal extracted into a reusable `calculate_signal(data, config)`-style module; tested |
| Formulaic alpha_19 signal | `src/signals/formulaic_alpha_19.py` | **KEEP** | Second extraction; best-of-batch alpha from the formulaic-alphas screen; tested |
| Trend (TSMOM) signal | `src/signals/tsmom.py`, `docs/research/tsmom_definitions_v1.md` | **KEEP** | Cross-asset trend baseline on the returns layer; definitions FROZEN v1 before any backtest; truncation-tested; EXP-0003 |
| Yield-curve regime signal | `notebooks/signals/yield_curve_regime.ipynb` | **REFACTOR** | Still inline-only — signal variant/asset-weighting never resolved in-notebook, so not yet extraction-ready |
| Formulaic alphas screen (remaining 9) | `notebooks/signals/formulaic_alphas_batch1.ipynb` | **KEEP** | Intentionally stays a screening notebook; alpha_19 was the only candidate promoted to a module |
| Experiment registry | `src/experiments/{config,registry,runner,report}.py`, `experiments/` table + dirs | **KEEP** | Hybrid DuckDB-metadata + filesystem-artifacts design; `EXP-0001`/`EXP-0002`/`EXP-0003` live and `complete`; tested |
| Asset-universe abstraction | `src/experiments/universe.py` | **KEEP** | `AssetUniverse` ABC + `CommodityUniverse`/`EquityUniverse`/`ReturnsLayerUniverse`; each exercised by a real experiment |
| Portfolio rules | `src/experiments/portfolio.py` | **KEEP** | `equal_weight_long_basket()` + `decile_long_short()` + `vol_target_weights()`, dispatched via `runner.py`'s `PORTFOLIO_REGISTRY`; still deliberately narrow, not a general portfolio layer (no covariance model) |
| Experiment CLI scripts | `scripts/{new_experiment,run_experiment}.py` | **KEEP** | Thin wrappers, same convention as the `fetch_*.py` collector scripts |
| Significance helpers | `src/backtest/stats.py` | **KEEP** | Bootstrap CI, deflated Sharpe, trimmed Sharpes; tested |
| Robustness engine | `src/experiments/robustness.py`, `scripts/run_robustness.py` | **KEEP** | Eight checks incl. `walk_forward` (C11), config-driven thresholds, holdout lock; run on EXP-0001/0002 (walk-forward read-only). Missing: equity style factors, regime-conditioned verdicts |
| Collector tests | `tests/collectors/` | **PARTIAL** | Covers `currency`/`macro`/`cot`/`bis`/`total_return`; `commodity`/`equity`/`gsci` still have zero pytest coverage |
| Raw/processed data split | — | **MISSING** | Plan's Section 5.1; everything goes straight into DuckDB |
| Futures roll methodology | — | **MISSING** | Needed before any commodity curve work; no continuous-contract/roll logic exists for the 300xxx asset block |
| Portfolio/risk layer (general) | — | **MISSING** | Phase 7 — correctly deferred; see the narrow exception above |
| Dashboard | `dashboard/`, `src/experiments/artifacts.py` | **KEEP** | Read-only Streamlit shell + Research section (experiment list, detail with Performance/Robustness/Diagnostics/Note/Config tabs). Markets section, Compare page, approval state not built |
| Macro explorer | `dashboard/macro/explorer.py`, `src/macro/{data,transforms}.py`, `tests/macro/` | **KEEP** | First read-only-DuckDB page; data access and transforms kept Streamlit-free and tested. COT picker now has an asset-group filter |
| Asset-id blocks | `src/db/asset_ids.py`, `src/collectors/asset_id_cleanup.py`, `scripts/renumber_misplaced_assets.py` | **KEEP** | Per-block allocation replaces global `MAX(id)+1`; renumber already run; tested |
| FRED vintage repair | `src/collectors/fred_cleanup.py`, `scripts/purge_stamped_fred_rows.py` | **KEEP** | One-off (C0): stamped rows replaced by real releases; backup + dry-run; tested. Safe to retire later |
| Point-in-time layer | `src/macro/{pit,inputs}.py` | **KEEP** | As-of vintage reconstruction; `inputs.py` is the only DB-touching module in the macro layer. |
| Macro / rates / market state | `src/macro/{state,rates,market}.py` | **KEEP** | Pure pandas, implements the frozen definitions; tested |
| Macro regimes + flags | `src/macro/{regimes,flags}.py` | **KEEP** | Six regimes in two layers via `REGIME_REGISTRY`; causal `min_duration`; pit-reviewed (0 mismatches on truncation). Coexists with the older `src/backtest/regimes.py` (vol/rates/trend), which stays as the market-layer defaults |
| Macro analysis + validation | `src/macro/{analysis,validation}.py`, `scripts/macro_state_validation.py`, `docs/research/macro_state_validation.md` | **KEEP** | Episode-aware statistics; validation write-up drove decision D1 (family v2) |
| Frozen definitions | `docs/research/macro_state_definitions_{v1,v2}.md`, `docs/research/tsmom_definitions_v1.md` | **KEEP** | tsmom v1 FROZEN 2026-10-26 (pre-freeze edits in its change log). Macro: v1 FROZEN; v2 status line still reads "awaiting the researcher's freeze" (guard G1 leaves that to the researcher) |
| Experiment macro diagnostics | `src/experiments/{config,runner}.py` (`DiagnosticsConfig`) | **KEEP** | Regime table by layer, optional state exposure. EXP-0001/2 not yet re-run with it |
| Trial ledger | `src/experiments/trials.py`, `scripts/trials.py`, migration v11 | **KEEP** | Feeds deflated Sharpe; notebook trials only counted if `log_trial()` is called |
| Macro state page | `dashboard/macro/state.py`, `dashboard/components.py` loaders | **KEEP** | C12; checked running by the researcher 2026-10-24 |
| Agentic guardrails | `.claude/hooks/{guard,scope}.py`, `.claude/agents/`, `.claude/skills/plan-step/`, `tests/hooks/` | **KEEP** | Phase A of `docs/plans/agentic_workflow.md`; stdlib-only hooks, tested. Phases B+ not built |

---

## 3. Known gaps / technical debt

- `commodity`/`equity`/`gsci` collectors still have zero pytest coverage (only `currency`/`macro`/
  `cot` were tested).
- Yield-curve regime signal is still inline-only — unlike the other two signals, its research
  hasn't settled on a final variant/weighting, so there's nothing concrete to extract yet.
- Neither live experiment has a held-out period (they predate `holdout_start`); the plan is to treat
  data arriving after the freeze date as their out-of-sample test. `git_sha`/`data_as_of` are `null`
  on both for the same reason — their original inputs can't be reconstructed.
- EXP-0001/EXP-0002 have not been re-run for the C9 macro diagnostics: both are `status: complete`
  (guard G2), so the researcher must choose between overwriting them and new EXP ids. Also open from
  C9: state-exposure timing (next-period vs contemporaneous) and whether a failing macro input should
  fail `run_experiment()` (current, since every run builds the full `StatePanel`) or fall back to the
  market-layer regimes.
- Macro state page (C12) open questions: card "level z" is a daily expanding z on forward-filled
  days, not once per release as definitions section 0 says (same question as the C7 divergence);
  positioning isn't compared with the direction of the macro move; one sidebar asset serves every
  divergence dimension.
- `load_reference_returns()` uses unadjusted front-month GC1/CL1, so contract rolls appear as
  one-day jumps in the reference returns (another consequence of the missing roll methodology).
- `docs/research/macro_state_definitions_v2.md`'s status line ("Frozen v2, awaiting the researcher's
  freeze") is ambiguous; the researcher should set it.
- Robustness gaps: no equity style factors (momentum/value/size) in the factor regression, and
  `git_sha` records HEAD, so uncommitted code isn't captured. ~~No walk-forward validation~~: built in C11
  (2026-10-25). It only tests selection inside a config's `param_grid`, not selection across signals.
- Returns layer (2026-10-25):
  - Checkpoint 0 is open: the R3 bond-return validation against IEF/TLT/SHY/IEI ETFs is deferred, because
    the researcher asked for no new data.
  - ~~`apply_costs` NaN-as-0~~ — settled in Phase 1 (2026-10-26): correct as long as a weight only changes on its
    instrument's own trading days, which `vol_target_weights` enforces and tests. Any other rule used on the
    returns layer must do the same. `apply_costs` also books no turnover on the first period, so initial
    positions enter without cost.
  - `dividends.py`'s `tr_index` path reindexes TR returns onto the price index's dates, so a TR move on a
    date the price index lacks is dropped.
- FX source quality (Twelve Data): 2008 EURUSD rows look day/month-swapped (repaired or nulled), weekend
  rows from 2024, GBPUSD quarterly before 1971 (excluded). BIS rate caveats (CHF to 1999, JPY discount rate and
  stale stretches, EUR MRO 2008–2024) bias carry and are reported, not corrected.
- Housekeeping: `data/quant.db.bak-pre-bis-tr` (pre-BIS/total-return backup) and the retired 1321.T rows
  (asset 500005) are still present.
- Cost realism: both experiments assume 1 bp one-way. EXP-0002's breakeven is only ~7 bps, so its
  cost assumption is the load-bearing one.
- No raw/processed data separation or futures roll methodology — both prerequisites for any
  commodity curve work.
- Macro explorer: FRED `macro_series.frequency` labels are unreliable (some weekly series tagged
  `daily`), so frequency is inferred from dates.
- ~~Stale FRED series / ICSA stamped on its period date~~ — resolved in C0 (2026-10-22): `DTB3`,
  `M2SL`, `PPIACO` had simply left `SERIES_CODES`; ICSA and four others were re-fetched with real
  vintages. ICSA is real-time only from 2009 (ALFRED's earliest vintage), so claims use NSA `ICNSA`
  as an unrevised series with a 5-day lag. International CPIs / OECD CLIs remain stale (global
  layer deferred).
- COT: leveraged-fund/asset-manager (TFF) columns were all NULL before 2026-10-22 (wrong API field
  names); fixed and backfilled. `data/quant.db.bak-20261022` (git-ignored) is the pre-purge backup.
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

1. Finish the macro research layer (`docs/plans/macro_research_layer.md`; C0–C12 done):
   decide and do the C9 EXP-0001/2 re-runs (they would also store `walk_forward`); C13 (test gaps).
1a. Cross-asset plan (`docs/plans/cross_asset_strategies.md`): Phase 1 is done (EXP-0003). Next: F0 forward
   tracking (open decision S5: public or local repo), the researcher's review and one-time holdout unlock of
   EXP-0003, then Phase 2 (macro duration timing) with its own plan and frozen definitions.
2. Add `tests/collectors/` coverage for `commodity`/`equity`/`gsci` (same monkeypatch pattern as
   `currency`/`macro`/`cot` — see `tests/collectors/test_cot.py` for the `_log_fetch` gotcha).
3. Close the open research questions in `yield_curve_regime.ipynb` (pick a signal variant via its
   existing IC analysis) before extracting it into `src/signals/`.
4. Fix `scripts/update_commodities.py`'s stale Stooq-era calls.
5. Re-run EXP-0002's robustness at a realistic cost (e.g. 5 bps); its breakeven is ~7 bps.
6. Commodity curve carry (the build plan's flagship) waits on a paid futures data source
   (Databento or Norgate); it would also bring the raw/processed data split and roll methodology.
   The researcher deferred new data on 2026-10-25 in favour of building strategies first.
7. Dashboard follow-ups: a Markets section, a Compare page once there are >2 experiments, then an
   approval state and paper trading once something survives the gate.

---

## 5. Maintaining this document

This file is kept current by the `update-architecture` skill (`.claude/skills/update-architecture/`).
Run it after any change that adds, removes, or reclassifies a component — new signals, new
collectors, a new `tests/` directory, the experiment registry going live, etc. Manual edits to
Sections 3–4 are expected between audits; the skill updates Sections 1–2 and the "Last audited"
line, and folds its findings into Sections 3–4 without discarding notes you've added.
