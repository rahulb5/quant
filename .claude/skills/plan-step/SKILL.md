---
name: plan-step
description: Build one new step of a docs/plans/*.md plan ("/plan-step docs/plans/macro_research_layer.md C7"), or make one scoped amendment to a finished step ("/plan-step amend <plan> <step>: <the change>"). Implements, tests, gets a diff-scoped review, records the outcome, then stops. Not for open-ended requests; do those directly.
---

# Plan step

One invocation = one declared scope. The failure this skill exists to prevent is **scope creep**:
reviewers find old problems, and the fixes grow into a different change from the one asked for.

## 0. Declare the scope (before reading anything else)

Work out which mode this is from the arguments:

- **build**: `<plan> <step id>` for a step with no outcome block yet.
- **amend**: a change to something already built (a decision, a window, a rule). Needs a plan
  and step to attach it to; if the arguments don't name them, ask in one line.
- Anything else (prose with several requests, no plan/step): don't run the routine. Say so and
  do the request directly, or ask which one thing to do.

Then write the **scope contract** into your first message, and keep it for the whole run:

```
Scope:        <the ask, max 3 bullets, in the researcher's words>
Files I expect to change: <explicit list; tests included>
Not doing:    <adjacent things you can see but were not asked for>
```

For `amend`, "Not doing" must include: regenerating validation write-ups, re-running studies,
new modules, new data fetches, and web research, unless the researcher asked for them.

Then make it binding:

```
python3 .claude/hooks/scope.py start --task "<plan> <step>: <one line>" <file> <file> <glob>...
```

List the plan file, any frozen doc whose change log you'll write and the test files. Globs are fine
(`tests/macro/test_regimes.py`, not `tests/**`). Files outside the project (scratchpad, memory) are never restricted.

**Tripwire (enforced by the guard hook):** Write/Edit outside the list is blocked (S1), and any
shell command that changes an out-of-scope file is reported straight after it runs. When that
happens: STOP, revert the stray change, and ask in one line (file, reason). You cannot widen the
scope yourself (S2); the researcher does it with `! python3 .claude/hooks/scope.py add <file>` or
by editing `.claude/scope.json`. Don't ask for blanket permission ("apply everything"); ask about one file.

## 1. Read (only what the scope needs)

- The target step in the plan, its "Decisions" tables (**do not re-litigate**), the previous
  step's outcome block, and the frozen definitions file's relevant section + change log.
- `CLAUDE.md` for the environment commands.
- Frozen definitions are read-only (guard hook G1). If the ask changes a frozen definition, the
  researcher must create the new `_v<N>` file; say so and stop, or record it in the change log
  only if they have already decided it.
- If the step needs paid data or a decision the plan doesn't cover: stop and ask.

## 2. Implement

Repo conventions: pure functions where the plan says pure; DB via `from src.db.client import db`
with the caller owning `open()/close()`; docstrings stating parameters and earliest valid date;
tests in the mirrored `tests/` dir on synthetic data; in `src/macro/` a truncation-invariance
test for every new output column. Where the plan is open, take its stated default and list the
choice in the outcome. Don't tune anything by looking at asset-performance numbers.

## 3. Test

New tests first, then the full suite (`PYTHONPATH=... bin/python -m pytest -q`). Report failures
as they are, including pre-existing ones (show how you know).

## 4. Review: diff-scoped, one round

Spawn in a single message, passing the exact file list and `git diff` base, and **the scope contract**:

- `spec-conformance-reviewer`: always.
- `pit-reviewer`: only if the diff touches `src/macro/**` or macro/COT/vintage consumers.

They report **In-diff findings** and **Pre-existing observations** separately. Triage like this:

| Finding type | Action |
|---|---|
| In-diff defect (the new/changed behaviour is wrong, or its tests can't fail) | Fix, re-run affected tests. One fix round only |
| In-diff, but a judgment call the plan doesn't settle | Ask the researcher, with a recommendation |
| Pre-existing observation (code or behaviour you didn't change) | **Do not fix. Do not investigate.** List it in the final report, one line each |
| Reviewer disagrees with a frozen/accepted decision | Report it as a question; don't act |

No second review round. Never "apply everything the reviewers suggested", and never offer the
researcher that as a menu option. If the researcher wants pre-existing items handled, each becomes its own
`/plan-step amend` with its own scope.

## 5. Record (small)

- **build:** add `### Cn outcome (YYYY-MM-DD)` to the plan (what was built, test counts, choices
  made where the plan was open, review results, what to look at first) and update its Status line;
  update the status memory file and its `MEMORY.md` line.
- **amend:** a dated amendment note under that step in the plan, plus the frozen doc's change
  log if a definition changed. Memory only if the "what's next" changed.
- Do **not** regenerate validation write-ups or re-run studies. If numbers in a doc are now
  stale, name the doc and the sections in the report and let the researcher decide.
- Don't commit unless asked. When asked, stage only the files on the scope list.

## 6. Final report (short)

First run `python3 .claude/hooks/scope.py end`. It prints any out-of-scope changes and removes
the scope. Also end the scope if you stop early or the researcher abandons the task; a stale scope
will block the next unrelated task.

1. What changed, files and test results (new / full suite).
2. Review verdicts: in-diff findings fixed, questions for the researcher.
3. **Pre-existing observations** (one line each, not acted on).
4. **Stale docs/numbers** you did not regenerate.
5. Anything you touched outside the scope list (should be none).

## Non-goals

- Don't chain into the next step, or start "while here" fixes.
- Don't weaken a finding to look clean, and don't expand a finding into a project.
- Don't edit completed experiments, write conclusions or rationale, or call a result profitable or
  approved (guard hook G2/G3/G5).
