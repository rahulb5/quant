# Agentic workflow — skills, agents, orchestrators, guardrails

**Status:** draft for researcher review (2026-10-24). Nothing below is built yet except the two
existing skills (`notebook-to-signal`, `update-architecture`).
**Read first:** `CLAUDE.md`, `quant_research_os_build_plan.md` §14–16, `docs/plans/macro_research_layer.md`.

## Goal

Turn the build plan's §14–16 ("AI research agent", "responsibilities", "boundaries") into Claude Code
primitives. Agents take on the plumbing and the adversarial review. Hypotheses, frozen definitions,
conclusions and anything to do with capital stay with the researcher. The system stays local-first
with no "agent swarms" (build plan §21). Every component below either replaces a step that is
already done by hand or enforces a rule that currently depends on memory.

## Primitives and what each is for

| Primitive | Where | Use it for | Don't use it for |
|---|---|---|---|
| **Skill** | `.claude/skills/<name>/SKILL.md` | A repeatable procedure with a checklist: the "how we do X here" knowledge (`notebook-to-signal` is the model) | Anything that needs an independent view of the work |
| **Orchestrator** | A skill that runs in the main session | Multi-stage workflows: it runs scripts, spawns subagents in parallel and gathers their results. Subagents can't spawn subagents, so orchestration has to live in the main thread | Deterministic chains with no judgment calls (use a script) |
| **Subagent** | `.claude/agents/<name>.md` | Work with a fresh, isolated context and **restricted tools**, mostly reviewers that shouldn't see the author's reasoning and shouldn't be able to edit | Implementation work that needs the whole conversation |
| **Hook** | `.claude/settings.json` | Hard guardrails (§16 boundaries) that must hold even when a prompt forgets them | Soft guidance |
| **Script / CLI** | `scripts/` | Everything deterministic. Agents call scripts; they don't re-implement them | — |

**Design rule:** if a step gives the same answer every time, it's a script. If it needs judgment,
it's a skill. If it needs *independent* judgment, it's a subagent.

## Hard constraint: DuckDB has one writer

`data/quant.db` allows one writer process. `run_experiment()` and `run_robustness()` write the
`experiments` registry (and the `trials` ledger after C10), and collectors write data. So:

- Orchestrators **serialise** every step that writes to the DB. Parallel fan-out is only for read-only
  work: reviewers, and analysis that reads artifacts under `experiments/` or opens the DB `read_only=True`.
- Reviewer agents get no `Write`/`Edit` tools and are told to open DuckDB read-only.
- Cloud routines (`/schedule`) can't see the local DB, so scheduled work runs locally (see §6).

---

## 1. Guardrails first (hooks + permissions)

These enforce build plan §16 mechanically. Build them before any orchestrator, so that a prompt
which forgets a rule is caught.

`.claude/hooks/guard.py` is a `PreToolUse` hook on `Write|Edit|Bash`. It reads the tool-call JSON
from stdin and exits 2 (block, with the reason sent back to Claude) when a call matches one of these:

| Rule | Blocks | Why |
|---|---|---|
| G1 frozen definitions | `Write`/`Edit` on `docs/research/*_v[0-9]*.md` (e.g. `macro_state_definitions_v1.md`). The **change log** section is the only exception | Definitions are frozen before results are seen; changing them means a new version file, made by the researcher |
| G2 completed experiments | `Write`/`Edit` on `experiments/EXP-*/{results,robustness,diagnostics}.json`, `performance.parquet` and `config.yaml` once `status: complete` | Historical results are never edited (§16). Re-runs produce a new EXP id, or are explicitly requested |
| G3 conclusions | Edits that set `conclusion:` in a `config.yaml`, or fill the `## Conclusion` / `## Economic Rationale` sections of a `research_note.md` | "The AI drafts; the researcher approves the conclusion" (§11) |
| G4 destructive data ops | `Bash` matching `rm .*quant\.db`, `DROP TABLE`, `DELETE FROM (asset\|price\|macro_\|cot_)`, and `purge_*`/`migrate_*` scripts run without `--dry-run` | Raw data is never overwritten silently. One-off migrations (as with the COT fix) need explicit approval in the chat |
| G5 language | `Write`/`Edit` whose text contains `approved`, `profitable` or `ready to trade` in `experiments/**` | §16: never call a strategy profitable without qualification |

G2 and G3 can be overridden for a single call by the researcher (the hook's reason says how, e.g.
`QUANT_ALLOW_RESULT_EDIT=1`). That way an override is deliberate rather than accidental.

`.claude/settings.json` also gets:
- `permissions.allow` covering the venv python with `PYTHONPATH`, `pytest`, read-only git and
  `scripts/run_*.py` (run `/fewer-permission-prompts` once to seed it from real transcripts).
- `permissions.deny` covering `git push --force`, `rm -rf data`, and writes to `data/raw/**` once the
  raw/processed split exists.
- A `Stop` hook (optional, low priority): if `src/`, `scripts/` or `tests/` gained or lost files this
  session, print a reminder to run `/update-architecture`.

## 2. Research pipeline (the core loop)

```
 /new-hypothesis ──► notebook research ──► /notebook-to-signal ──► /run-experiment ──► /review-experiment
  (pre-register:       (researcher +          (exists)                (scaffold → run →      (parallel red team
   family, spec,        Claude, ad hoc)                                robustness → draft     → verdict summary
   kill criteria)                                                      note; serial)          → researcher decides)
```

### 2.1 `/new-hypothesis` (skill)

Pre-registration. It turns a one-paragraph idea into `docs/research/hypotheses/<family>.md`:
hypothesis, economic rationale (the researcher's words, which the skill asks for rather than writing
itself), universe, the planned variant grid, **what result would kill it**, and the ledger `family` name
(C10). It also creates the notebook stub. It refuses to proceed while the rationale is empty. Once the
researcher accepts the file, G1 freezes it (`<family>_v1.md`).

### 2.2 `/run-experiment` (orchestrator skill, serial)

It wraps what you currently stitch together by hand:
1. Checks that the signal module meets the `load_data`/`calculate_signal`/`SignalConfig` contract and that
   its tests pass.
2. Runs `scripts/new_experiment.py` using the hypothesis file's fields, so the family and `n_trials`
   come from the ledger rather than from memory.
3. Runs `scripts/run_experiment.py`, then `scripts/run_robustness.py`.
4. Drafts the factual sections of `research_note.md` (results, robustness table, regime tables, all
   with `n_obs`/episode counts). The rationale, interpretation and conclusion stay `_TODO: researcher_`, which G3 enforces.
5. Hands off to `/review-experiment`.

### 2.3 `/review-experiment EXP-XXXX` (orchestrator, parallel read-only fan-out)

It spawns three critic subagents **in parallel**. Each gets only the experiment folder, the signal
module and the source notebook, never this conversation. That's the point: they start without the
author's assumptions.

| Agent | Lens (build plan §13/§15 "research criticism") | Concrete checks in this repo |
|---|---|---|
| `bias-auditor` | Look-ahead, survivorship, leakage, PIT | `lag` and `release_date` alignment; whether macro inputs go through `pit.realtime()`; the `id < 100000` constituent filter and the survivorship caveat present; COT on `release_date`, not `report_date`; full-sample normalisation anywhere (expanding only) |
| `overfit-auditor` | Data mining, multiple testing, instability | Ledger `n_trials(family)` vs declared count; deflated Sharpe; parameter-grid surface shape; half-sample and decade splits; walk-forward decay (C11); how many independent regime episodes sit behind each claim |
| `implementation-auditor` | Bugs, unrealistic costs, hidden exposure | Signal module vs notebook (same numbers on the same dates); turnover vs `cost_bps` breakeven; factor-regression betas (HAC t-stats) for hidden equity/dollar/commodity beta; return-series quality (EXP-0001's extreme tails) |

Each returns findings as `{check, severity: KILL|FLAG|NOTE, evidence (file:line or number), suggested
test}`. The orchestrator merges these with the mechanical `robustness.json` verdict into
`experiments/EXP-XXXX/review.md` and puts the open questions to the researcher. The best possible
outcome is still `NEEDS_REVIEW`.

**Model choice:** run critics on a strong model (opus-class), because missed leakage is the expensive
failure. Mechanical sub-steps can use a cheaper model.

### 2.4 `/compare-experiments` (skill, read-only)

Lines up N experiments from the registry and their artifacts: headline metrics, robustness verdicts,
return correlation, regime-conditional Sharpe side by side. It answers "is this new signal just
EXP-0001 again?" It comes later, once there are more than two experiments.

## 3. Plan execution (how the macro layer actually gets built)

Your C0–C3 work already follows a pattern: read the plan → implement one step → tests → stop at the
checkpoint → write a "Cn outcome" block → update the status memory. Codify it:

### 3.1 `/plan-step <plan> <step>` (orchestrator skill)

1. Reads the plan, the "Decisions (don't re-litigate)" table and the previous step's outcome block.
2. If the plan marks the step as a **checkpoint**, it states up front what will be shown for review.
3. Implements in the main session (it needs full context; this is not a subagent job).
4. Runs the relevant tests, plus the full suite before finishing.
5. Spawns `spec-conformance-reviewer` (read-only) and, for `src/macro/**`, `pit-reviewer` in parallel.
6. Writes the `Cn outcome (date)` block into the plan, updates the plan's Status line and refreshes
   `memory/macro_research_layer_status.md`.
7. Stops. It never chains into the next step on its own. Each step gets one review.

### 3.2 Agents used by `/plan-step`

- **`spec-conformance-reviewer`**: diffs the implementation against the plan text and any frozen
  definitions doc (e.g. `macro_state_definitions_v1.md`): component lists, windows, valid-from dates,
  E1–E4 decisions. It reports every deviation, including ones that look like improvements, since any
  deviation is a hidden parameter (overview §7.6).
- **`pit-reviewer`**: checks for look-ahead in the macro layer. Truncation-invariance tests exist for
  every new output column; no `.shift(-k)`; no full-sample `mean()`/`std()`/`rank()`; vintage
  `as_of` logic respected; `UNREVISED_SERIES` and the E2 drop rule applied. It can *run* pytest and
  read-only DuckDB queries, but has no write tools.

## 4. Data pipeline

### 4.1 `/add-collector` (skill)

A checklist distilled from the bugs already paid for:
- Subclass `BaseCollector` and log fetches.
- Allocate asset ids through `next_asset_id(db, block)`, never `MAX(id)+1` (the 2026-10-22 renumber).
- Key series by the natural identifier (the COT exchange-vs-contract bug).
- Store the release date, not only the period date (the FRED stamped-row bug).
- Add an append-only migration, mirror it in `scripts/fetch_<x>.py`, register it in `update_all.py`,
  add tests in `tests/collectors/`, then run `/update-architecture`.

### 4.2 `data-auditor` (subagent, read-only)

Run after any fetch. Checks: `data_fetch_log` failures; staleness per series against its expected
frequency; row-count deltas against the previous run (a large drop means an overwrite or purge);
assets outside their id block; duplicate `(date, asset)` keys; NaN or zero-price runs; COT contracts
missing a week. It reports anomalies only and never fixes them. Fixes go through the main session with approval (G4).

### 4.3 `/data-refresh` (orchestrator)

Runs `scripts/update_all.py`, then `data-auditor`, then (optionally) a regime/state snapshot
("what changed in `macro_state()` since last week"). The output is a short report. The skill is the
interactive entry point; §6 covers scheduled runs.

## 5. Tool surface (build plan §14)

The build plan lists `search_experiments()`, `compare_experiments()`, `get_dataset()`, etc. Start
with a **thin JSON CLI**, `scripts/research.py <command>`, over the existing `registry.py`,
`robustness.py` and `macro/data.py` functions, rather than an MCP server. Agents call it through
Bash, it's testable with pytest, and it opens the DB read-only by default. Promote it to a local MCP
server only if several clients need it (e.g. Claude Desktop). Commands: `experiments list|show|compare`,
`dataset inspect <table|series>`, `trials count <family>`, `state now`.

## 6. Scheduled work

- **Daily/weekly data refresh:** plain `launchd`/cron runs `scripts/update_all.py`, which needs no LLM.
  A second job runs `claude -p "/data-refresh --audit-only" --permission-mode plan` locally **only if**
  the script's exit summary shows failures or anomalies. That keeps model calls to the cases that
  need judgment.
- **Not** `/schedule` cloud routines: they run remotely without `data/quant.db`.

## 7. What stays human

There are no agents for: writing the economic rationale, freezing definitions, choosing which variant
to promote, writing conclusions, raising `n_trials` downward (impossible after C10 anyway), deleting
experiments, or any portfolio or capital decision. Skills **ask** at these points rather than default.

---

## 8. Build order

| Phase | Deliverable | Why first | Depends on |
|---|---|---|---|
| A | `guard.py` hook (G1–G5) + `settings.json` permissions | Every later component relies on it | — |
| A | `/plan-step` + `spec-conformance-reviewer` + `pit-reviewer` | Immediately useful for C4–C13, which is the current work | — |
| B | `bias-auditor`, `overfit-auditor`, `implementation-auditor` + `/review-experiment` | Can be tested today on EXP-0001/0002 (see the known weaknesses in the overview) | A |
| B | `/run-experiment` | Removes manual stitching | Better after C10 (ledger) |
| C | `/new-hypothesis` | First used for EXP-0003 (yield curve) | C10 |
| C | `/add-collector`, `data-auditor`, `/data-refresh` | Next collector (sector ETFs, roll data) | — |
| D | `scripts/research.py` CLI, scheduled refresh, `/compare-experiments` | Worth it once there are more than 3 experiments | B |

**Acceptance test for phase B:** run `/review-experiment EXP-0001` and `EXP-0002` *without* telling
the critics the known issues. They should independently surface EXP-0001's return tails and
`n_trials=96`, and EXP-0002's survivorship bias and best-of-10 selection. If they don't, tighten the
agent prompts before trusting them on new work.

## 9. Open decisions for the researcher

| # | Question | Default |
|---|---|---|
| F1 | Should critics run on the strongest model only, or a mix? | Strongest for the three critics; default for `data-auditor` |
| F2 | G2 override mechanism: an environment variable, or the researcher edits by hand? | Environment variable, per call |
| F3 | Hypothesis files under `docs/research/hypotheses/`, or inside the experiment folder? | `docs/research/hypotheses/` (one family → many experiments) |
| F4 | Run phase B before or after C10 (trial ledger)? | Build the critics now; `overfit-auditor` falls back to the declared `n_trials` and flags it as unverified |
