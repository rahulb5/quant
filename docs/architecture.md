# Architecture

**Last audited:** 2026-10-04 (updated same-day after the collector unification + first signal module work below)
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
Partially as a module now: `src/signals/cot_commercial_positioning.py` is the first signal
extracted into reusable functions — `load_data()`, `net_commercial_positioning()`,
`add_percentile_signals()`, `weekly_prices_with_trend()`, `add_forward_returns()`, and an
orchestrating `calculate_signal(cot_raw, prices_raw, SignalConfig())` matching the build plan's
Section 7 interface. `notebooks/signals/cot_commercial_positioning.ipynb` now imports from this
module instead of duplicating the logic inline; the notebook's IC analysis, regime-dependence, and
in-sample/out-of-sample variant-selection cells are unchanged (that's research process, not signal
plumbing, so it stays in the notebook on purpose — see the module's docstring).

Formulaic alphas (`formulaic_alphas_batch1.ipynb`) and yield-curve regime
(`yield_curve_regime.ipynb`) are not yet extracted and still have all logic inline.

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

Current API takes **weights directly** — there is no signal → weight translation step, because
that's exactly the Signal Layer / Portfolio Layer that doesn't exist yet (see Gaps).

### How are experiments identified?
They aren't. There is no `experiments/EXP-XXXX/` registry, no experiment config/result convention.
A "result" today is whatever state a notebook was last run in.

### Where are results stored?
Inside the notebooks themselves (cell outputs, inline charts). Nothing is persisted outside
`notebooks/signals/*.ipynb` as structured, queryable output.

### How is portfolio construction separated from signal research?
It isn't — there is no portfolio layer at all (no `src/portfolio/`, no CVXPY, no optimizer). Not
urgent yet: per the build plan, this is correctly deferred until multiple signals have survived
research.

### How are tests performed?
`pytest`, coverage is improving but still uneven:
- `tests/backtest/` — full coverage of `metrics`, `performance`, `execution`, `conditioning`,
  `regimes`, `reference`, `backtest`. Solid.
- `tests/db/` — covers `src/db/client.py`.
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
| Formulaic alphas / yield-curve regime signals | `notebooks/signals/{formulaic_alphas_batch1,yield_curve_regime}.ipynb` | **REFACTOR** | Same shape of problem the COT notebook had — logic inline, no shared module, not yet extracted |
| `reasearch/dim_redux.ipynb` | `reasearch/` | **DELETE or MERGE** | Misspelled directory, 239-byte stub, overlaps conceptually with `notebooks/signals/` |
| `scripts/data/quant.db.wal` | tracked in git | **DELETE** | Stray DuckDB WAL file, shouldn't be version-controlled |
| `node_modules/` | repo root | **DELETE** | Untracked but present on disk; contradicts commit `222401a "project is Python-only"` |
| Collector tests | `tests/collectors/` | **PARTIAL** | Covers `currency`/`macro`/`cot`; `commodity`/`equity`/`gsci` still have zero pytest coverage |
| Raw/processed data split | — | **MISSING** | Plan's Section 5.1; everything goes straight into DuckDB |
| Futures roll methodology | — | **MISSING** | Needed before any commodity curve work; no continuous-contract/roll logic exists for the 300xxx asset block |
| Experiment registry | — | **MISSING** | Phase 3, not started |
| Robustness engine (cost stress, param sweeps) | — | **MISSING** | Phase 4, not started |
| Portfolio/risk layer | — | **MISSING** | Phase 7 — correctly deferred |
| Dashboard | — | **MISSING** | Phase 6 — correctly deferred |

---

## 3. Known gaps / technical debt

- `commodity`/`equity`/`gsci` collectors still have zero pytest coverage (only `currency`/`macro`/
  `cot` were tested this round).
- Formulaic alphas and yield-curve regime signals are still inline-only in their notebooks — the
  COT extraction pattern (`src/signals/cot_commercial_positioning.py`) hasn't been applied to them
  yet.
- No experiment registry — nothing is reproducible from an ID alone.
- No cost-stress or parameter-sensitivity automation (Section 13 of the build plan).
- Stray artifacts: tracked WAL file, untracked `node_modules/`, duplicate/misspelled `reasearch/`
  directory — none of this was touched this round.
- No raw/processed data separation or futures roll methodology — both prerequisites for any
  commodity curve work.
- Collectors (`commodity`, `equity`, `gsci`, `currency`, `macro`, `cot`) import the global `db`
  singleton directly rather than accepting it as a constructor/method parameter, unlike
  `src/backtest/reference.py`'s `ReferenceData`, which takes an injectable `database=` arg. This
  makes collector tests rely on monkeypatching the singleton (works, but `src/backtest/` shows a
  cleaner pattern) — worth reconciling if collectors get more test coverage.

## 4. Suggested next steps (unordered priority, smallest-step first)

1. Clean up stray artifacts (untrack the WAL file, delete `node_modules/`, resolve `reasearch/`).
2. Add `tests/collectors/` coverage for `commodity`/`equity`/`gsci` (same monkeypatch pattern as
   `currency`/`macro`/`cot` — see `tests/collectors/test_cot.py` for the `_log_fetch` gotcha).
3. Extract `formulaic_alphas_batch1.ipynb` and `yield_curve_regime.ipynb` into `src/signals/`
   modules, following the `cot_commercial_positioning.py` pattern.
4. Stand up a minimal `experiments/` registry; promote COT positioning to `EXP-0001`.

---

## 5. Maintaining this document

This file is kept current by the `update-architecture` skill (`.claude/skills/update-architecture/`).
Run it after any change that adds, removes, or reclassifies a component — new signals, new
collectors, a new `tests/` directory, the experiment registry going live, etc. Manual edits to
Sections 3–4 are expected between audits; the skill updates Sections 1–2 and the "Last audited"
line, and folds its findings into Sections 3–4 without discarding notes you've added.
