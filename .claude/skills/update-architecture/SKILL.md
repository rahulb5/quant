---
name: update-architecture
description: Re-audit this repo against docs/architecture.md and update that file to reflect what changed — new/removed modules, reclassified components, resolved or new gaps. Use after adding a collector, signal, test directory, experiment registry, or any structural change, or whenever the user asks to refresh/update the architecture doc.
---

# Update architecture

`docs/architecture.md` is a living audit of this repo (inventory, KEEP/REFINE/REFACTOR/REPLACE/DELETE/MISSING
classification, gaps, next steps — see `quant_research_os_build_plan.md` Section 3 for why this
file exists). This skill re-runs that audit and updates the doc **in place**, as a diff against
its current content, not a rewrite.

## Process

1. **Read the current doc.** Open `docs/architecture.md`. If it doesn't exist, bootstrap it by
   following the structure below as if this were the first audit, then stop — there's no prior
   state to diff against.

2. **Re-inventory the repo.** Compare against what Section 1/2 of the doc currently claims:
   - `find src scripts tests notebooks data docs experiments .claude/skills -type f -name "*.py" -o -name "*.ipynb" 2>/dev/null | sort` (adjust for dirs that exist)
   - `git log --since="<last-audited date from the doc>" --oneline` to see what changed since the
     last audit, and `git diff --stat` for anything uncommitted
   - Specifically check whether these previously-MISSING items now exist:
     - `src/signals/` (signal module)
     - `experiments/` (experiment registry, `EXP-XXXX` folders)
     - `tests/collectors/`
     - a portfolio/risk layer (`src/portfolio/`, CVXPY usage)
     - a dashboard (Streamlit app)
     - raw/processed data split under `data/`
   - Check whether previously-flagged tech debt is resolved: stray `scripts/data/quant.db.wal`
     still tracked in git? `node_modules/` still present? `reasearch/` directory still around?
   - Check whether `scripts/fetch_{cot,macro,currencies}.py` now subclass `BaseCollector`
     (`grep -l BaseCollector scripts/fetch_*.py`)

3. **Classify deltas, don't re-derive everything.** For each change found:
   - A new module/file not in the doc → add a row to the Section 2 table with a classification
     (KEEP/REFINE/REFACTOR/MISSING) and a one-line reason.
   - A MISSING item that now exists → move it out of "Known gaps," into the inventory table with
     its real classification, and update the relevant narrative answer in Section 1 if the
     mechanism it describes changed (e.g. "How are signals represented?" once `src/signals/`
     exists).
   - A resolved tech-debt item → remove it from Section 3 and note it as done.
   - Something deleted/renamed → remove or update its row; don't leave stale paths in the table.

4. **Update the file:**
   - Bump "Last audited" to today's date.
   - Edit Section 1 narrative only where the underlying mechanism actually changed — leave
     unaffected prose alone.
   - Edit the Section 2 table: add/remove/reclassify rows per step 3.
   - Edit Section 3 (gaps): remove resolved items, add newly discovered ones.
   - Edit Section 4 (next steps): drop completed steps, keep anything the user added manually
     between audits, append new suggestions only if the gaps list changed enough to warrant it.
   - Do not touch Section 5 (this maintenance note) unless the maintenance process itself changes.

5. **Preserve manual edits.** If the user has clearly added their own notes/commentary to Sections
   3–4 since the last audit (text that doesn't match this skill's own prior phrasing), keep it —
   merge your findings alongside it rather than overwriting.

6. **Report back concisely.** After updating, summarize in chat what changed since the last audit
   — new components classified, gaps resolved, new gaps found. Don't re-paste the whole doc; point
   to `docs/architecture.md` for the full picture.

## Non-goals

- Don't do a full line-by-line code review each run — this is a structural/inventory diff, not a
  code review. Use `/code-review` for correctness issues.
- Don't add new sections or change the doc's overall structure without being asked.
- Don't classify something as KEEP/REFINE/etc. without actually looking at it — a file existing
  isn't enough; skim it enough to judge whether it follows the patterns already established
  (e.g. does a new collector actually subclass `BaseCollector`?).
