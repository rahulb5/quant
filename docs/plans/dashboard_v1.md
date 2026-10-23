# Dashboard v1 — implementation plan

**Status:** approved by the researcher on 2026-10-18; not started.
**Read first:** `CLAUDE.md` (Python env + DB conventions), `docs/architecture.md` (repo map),
`quant_research_os_build_plan.md` Sections 23 (dashboard) and 5.2 (standard data interfaces).

## Goal

A local, read-only Streamlit app that is the researcher's screen for reviewing experiments, and
the start of the approval gate for paper trading. It is built as a **multi-section shell**, so
later Macro and Markets dashboards are added as one file plus one navigation line without
restructuring. This plan builds only the shell and the Research section; Macro/Markets come later.

## Decisions already made (don't re-litigate)

1. **The Research pages read experiment folders, not DuckDB.** DuckDB allows one read-write
   process at a time, and a long-running Streamlit process would block `run_experiment`,
   `run_robustness` and the collectors. Every number the Research pages need is on disk under
   `experiments/EXP-NNNN/`. `config.yaml` is kept in sync with the registry by the runner.
2. **The DB client gets a read-only mode now.** Future Macro/Markets pages must read DuckDB.
   They'll use short-lived read-only connections with cached results. No Research page uses it.
3. **Plotly for charts.** New pinned dependencies: `streamlit` (≥ 1.36, for `st.navigation`
   sections) and `plotly`. Neither is installed yet.
4. **Strictly read-only.** No run, approve, or edit buttons. Approval is a separate later step.
5. **An experiment classification block** with five fields, separate from the technical
   `data.asset_class` key. Exact values are in Step 1.
6. **No placeholder pages.** No empty "coming soon" Macro/Markets pages; the shell just makes
   them trivial to add.
7. **Compare page is deferred** until there are more than two experiments.

## Current state (verified 2026-10-18, commit `cf66df1`)

- Two experiments, both `complete`, both robustness verdict `NEEDS_REVIEW`:
  - EXP-0001: COT commercial positioning, 11 commodity futures, weekly, long-only.
  - EXP-0002: formulaic alpha_19, current S&P 500 constituents, daily, decile long/short.
- Per-experiment files, all present for both:

| File | Contents |
|---|---|
| `config.yaml` | `ExperimentConfig` (`src/experiments/config.py`): hypothesis, data, signal, portfolio, backtest, validation (`holdout_start`, `n_trials`), robustness thresholds, status, conclusion, `git_sha`, `data_as_of`. `git_sha`/`data_as_of` are `null` on both existing experiments. |
| `results.json` | `{"metrics": {sharpe, cagr, ann_return, ann_vol, max_drawdown, sortino, calmar, hit_rate, skew, excess_kurtosis, sharpe_tstat, n_periods, start, end, ...}, "execution_summary": {total_gross, total_net, total_cost, avg_turnover, ann_turnover}}` |
| `performance.parquet` | Date index; columns `gross_returns`, `net_returns`, `turnover`, `costs` |
| `diagnostics.json` | `yearly`: year → metrics. `by_regime`: regime (`volatility`, `rates_curve`, `equity_trend`) → label → {ann_return, ann_vol, sharpe, t_stat, hit_rate, pct_time, n_obs}. `correlations` / `betas`: factor → float. `factor_regression`: {table: term → {coef, t_stat, t_stat_hac}, ann_alpha, r_squared, n_obs, hac_maxlags}. `rolling_corr`: date string → float. `conditional_perf`: factor → {ann_return_up/down, hit_rate_up/down, n_up, n_down}. |
| `robustness.json` | `{experiment_id, generated_at, git_sha, n_trials, holdout_start, in_sample_end, n_obs, base: {sharpe, cost_bps, lag}, thresholds, checks: {name: {verdict, reason, detail}}, overall}`. Checks: `cost_stress`, `lag_sensitivity`, `outliers`, `subperiods`, `parameters`, `statistical`, `holdout`. Field-level details are in `src/experiments/robustness.py`. |
| `holdout.json` | Only exists once a holdout is unlocked. Neither experiment has one. |
| `research_note.md` | Markdown |
| `charts/*.png` | Static matplotlib charts. The dashboard re-draws interactively in Plotly; don't embed these. |

- Registry: DuckDB `experiments` table, schema at migration v9. Its `asset_class` column holds
  the **technical** key (`commodity` / `equity`), the same as `config.data.asset_class`.
- Vocabulary clash to be aware of: the `assets` table labels commodity instruments `futures`,
  experiments label them `commodity`. Don't rename either; the classification block below is the
  human-facing label.
- Tests: 409 passing. Run with
  `PYTHONPATH=/Users/rhlbh/projects/claudeProjects/quant /Users/rhlbh/projects/claudeProjects/bin/python -m pytest -q`.

---

## Step 1 — Experiment classification

**Config** (`src/experiments/config.py`): add a `ClassificationConfig` dataclass and an
`ExperimentConfig.classification: ClassificationConfig | None = None` field. It must be `None`
when absent so old YAML still loads. Fields and allowed values:

| Field | Allowed values |
|---|---|
| `asset_class` | `commodities`, `equities`, `fx`, `rates`, `macro`, `multi` |
| `universe` | free text, snake_case (e.g. `cot_11_futures`, `sp500_current`) |
| `signal_family` | `carry`, `momentum`, `reversal`, `value`, `positioning`, `macro`, `regime` |
| `frequency` | `daily`, `weekly`, `monthly` |
| `direction` | `long_only`, `long_short`, `market_neutral` |

Validate the enumerated fields in `ClassificationConfig.__post_init__` with a clear `ValueError`
that lists the allowed values. Keep the allowed values as module-level tuples so the dashboard
can reuse them for filter options. `load_config` / `save_config` must round-trip the block.

**Registry**:
- Migration v10 adds `universe`, `signal_family`, `frequency`, `direction`, `market` (all TEXT,
  nullable). `market` holds `classification.asset_class`; the existing `asset_class` column
  keeps the technical key, unchanged.
- Add the five fields to `ExperimentRecord` (optional, default `None`) and to
  `insert_experiment`'s SQL.
- Add them to `_UPDATABLE_FIELDS`.

**Scaffolding** (`scripts/new_experiment.py`): new required flags `--market`, `--universe`,
`--signal-family`, `--frequency`, `--direction`. Write them into the config's `classification`
block and the registry row.

**Research note** (`src/experiments/report.py`): show the classification in the Data section,
when present.

**Backfill** the two existing experiments. Use `load_config` / `save_config` for the YAML and
`update_experiment` for the registry rows:
- EXP-0001: `commodities / cot_11_futures / positioning / weekly / long_only`
- EXP-0002: `equities / sp500_current / reversal / daily / long_short`

Do **not** re-run either experiment or regenerate its notes; only the config and registry change.
Back up `data/quant.db` to the session scratchpad first, since migration v10 runs on first open.

**Tests**: round-trip with and without the block; an invalid enum value raises; the registry
accepts and updates the new columns; the existing migration test covers v10 automatically.

## Step 2 — Read-only DB mode

`src/db/client.py`: add `Database(path, read_only: bool = False)`.
- `read_only=True` opens `duckdb.connect(path, read_only=True)` and **skips migrations**.
- Write helpers (`run`, `transaction`) raise `RuntimeError` in read-only mode.
- Keep the module-level `db` singleton read-write, as it is now. The future market-data read
  layer will build its own short-lived read-only instances.
- When another process holds the write lock, DuckDB raises `duckdb.IOException`. Let it
  propagate; the future UI turns it into a "DB busy, retry" message. Don't add retry loops.

**Tests** (temp-file DB, not `:memory:`):
- Create with read-write, close, reopen read-only: reads work, writes raise, and
  `schema_version` is not touched.

Nothing in this plan uses read-only mode yet. It's groundwork for Macro/Markets.

## Step 3 — `src/experiments/artifacts.py` (loader layer, no Streamlit imports)

The experiments root defaults to `experiments/`, overridable by env var
`QUANT_EXPERIMENTS_DIR` so tests can point at a temp tree.

```python
@dataclass
class ExperimentSummary:   # one row in the list view
    experiment_id: str; hypothesis: str; signal_name: str; status: str
    classification: ClassificationConfig | None
    sharpe, cagr, max_drawdown, ann_turnover: float | None
    robustness_verdict: str | None; sharpe_2x_cost, deflated_sharpe, sharpe_ci_low,
    breakeven_bps: float | None
    holdout_state: Literal["none", "locked", "scored"]
    created_at, data_as_of, git_sha: str | None
    path: Path

@dataclass
class ExperimentArtifacts:  # everything for the detail view
    summary: ExperimentSummary; config: ExperimentConfig
    results: dict | None; diagnostics: dict | None; robustness: dict | None
    holdout: dict | None; performance: pd.DataFrame | None; note: str | None

def list_experiments(root=None) -> list[ExperimentSummary]   # sorted by id
def load_experiment(experiment_id, root=None) -> ExperimentArtifacts
def artifacts_mtime(root=None) -> float   # max mtime across experiment files; used as a cache key
```

Rules:
- A folder counts as an experiment if it contains `config.yaml`.
- **Every other file is optional.** A missing file becomes `None`, never an exception, so a
  `draft` or `failed` experiment still lists.
- `holdout_state`: `none` if `validation.holdout_start` is null; `scored` if `holdout.json`
  exists; otherwise `locked`.
- Robustness headline numbers come from `robustness.json`:
  - verdict = `overall`
  - `sharpe_2x_cost` = `checks.cost_stress.detail.sharpe_at_flag_multiplier`
  - `breakeven_bps` = `checks.cost_stress.detail.breakeven_bps`
  - `deflated_sharpe` and `sharpe_ci_low` from `checks.statistical.detail`

**Tests**:
- Against a tmp tree containing (a) a full copy of one real experiment folder and (b) a bare
  `config.yaml`-only draft. Both list without errors; the bare one has `None` metrics.
- `holdout_state` covers all three cases.
- The env var override works.

## Step 4 — Streamlit shell + Research section

```
dashboard/
  app.py                     # st.set_page_config + st.navigation({"Research": [...]}) only
  research/experiments.py    # list page (default page)
  research/experiment.py     # detail page, selected by query param ?id=EXP-0002
  components.py              # shared helpers: verdict badge, metric formatting, plotly theme
```

- Launch from the project root:
  `PYTHONPATH=/Users/rhlbh/projects/claudeProjects/quant /Users/rhlbh/projects/claudeProjects/bin/python -m streamlit run dashboard/app.py`.
  Document this in `CLAUDE.md`.
- Wrap loaders in `st.cache_data`, keyed on `artifacts_mtime()`, so re-running an experiment
  shows up on browser refresh.
- Verdict colours: `KILLED` red, `NEEDS_REVIEW` amber, `PASS` green, `FLAG` amber, `KILL` red,
  `SKIPPED` grey. Never display the word "approved".

**Experiments list:**
- Filters in the sidebar: market, signal family, frequency, direction, status, verdict.
  Options come from the allowed-value tuples.
- Table columns: ID, signal, market, universe, family, freq, direction, status, verdict,
  Sharpe, 2x-cost Sharpe, deflated Sharpe, CI low, max drawdown, ann. turnover,
  breakeven bps, holdout state, data as-of, git SHA (first 8 characters).
- Each row links to the detail page.
- Experiments with no classification show "unclassified" rather than being hidden.

**Experiment detail.** The header shows ID, signal, classification chips, status, the verdict
badge and the hypothesis. Show a visible warning when `git_sha` or `data_as_of` is null
("not reproducible from ID alone"). Tabs:

1. **Performance** (from `performance.parquet`):
   - gross vs net equity curve on a log y-axis
   - drawdown
   - rolling Sharpe with a window of `backtest.periods_per_year` periods
   - calendar-year net return bars
   - headline metric tiles from `results.json`
2. **Robustness** (from `robustness.json`; if it's missing, show
   "not run — `scripts/run_robustness.py <config>`"):
   - check table with verdict and reason
   - cost ladder (Sharpe vs multiplier) with a breakeven annotation
   - Sharpe by lag
   - parameter heatmap for 2-D grids, a line for 1-D grids
   - subperiod halves, decades and crisis windows
   - a statistical panel showing the bootstrap CI and deflated Sharpe with `n_trials`
   - the outlier panel shows `sharpe_excluding_tails` as the headline and
     `sharpe_excluding_top` next to `sharpe_excluding_top_if_gaussian` for context, with a
     one-line note that one-sided trimming is biased
3. **Diagnostics** (from `diagnostics.json`):
   - regime tables
   - factor regression table with Newey-West t-stats, alpha and R²
   - correlations and betas
   - conditional up/down performance
   - rolling correlation line
4. **Note:** `st.markdown(research_note.md)`.
5. **Config:** raw YAML in a code block.

## Step 5 — Dependencies, docs, verification

- Install `streamlit` and `plotly` into the shared venv. Pin the installed versions in
  `requirements.txt`.
- Tests in `tests/dashboard/`: `streamlit.testing.v1.AppTest` smoke tests.
  - Each page renders with no exception, against a tmp experiments tree via
    `QUANT_EXPERIMENTS_DIR`.
  - The detail page renders for an experiment with no `robustness.json`.
- Start the server and confirm it serves: `curl` its `/_stcore/health` endpoint. The session
  can't click through a browser, so tell the researcher the visual check is theirs, and list
  what to look at.
- Update `docs/architecture.md`: new `dashboard/` + `artifacts.py` rows, classification, the
  read-only DB mode, and the migration v10 note.

## Later (not in this plan)

- **Market/Macro read API** (build plan §5.2): e.g. `src/data_access/` with
  `get_prices(asset_class, symbols, start, end)`, `get_macro_series(codes, start, end)`,
  `get_cot(commodities)`. It uses read-only `Database` instances opened per call, and results
  are cached in Streamlit with a TTL of a few minutes. Then:
  - `dashboard/macro/overview.py`: yield curve, VIX, dollar, regime states from
    `src/backtest/regimes.py`
  - `dashboard/markets/*.py`
  - each added as a new navigation section
- **Compare page**, once there are more than two experiments. Put returns on a common weekly
  grid before computing correlation.
- **Approval state + freeze**, then the paper-trading engine and a Monitoring page.

## Working conventions for the implementing session

- Work one step at a time. Run the full test suite after each step. Show a short summary and
  **wait for the researcher's go-ahead before committing**. Commit per step and never push.
- Back up `data/quant.db` to the scratchpad before the first `db.open()` after adding a
  migration.
- Don't re-run experiments or robustness as part of this work. Existing artifacts are the
  fixtures and the record.
- No new abstractions beyond what's listed. No placeholder pages. Minimal comments.
