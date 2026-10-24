#!/usr/bin/env python3
"""
.claude/hooks/guard.py

PreToolUse hook enforcing the build plan's Section 16 boundaries (see
docs/plans/agentic_workflow.md, Section 1). Reads the tool-call JSON on stdin; exits 2 with a
reason on stderr to block the call, or prints a permissionDecision "ask" (G4) so the researcher
gets the normal approve/deny prompt; exits 0 to allow. Stdlib only, so it runs under any python3.

Rules
  G1  frozen definitions: docs/research/*_v<N>.md whose **Status:** line says FROZEN is read-only
      except below its "## Change log". New files and DRAFT files can be written; only the
      researcher sets FROZEN
  G2  completed experiments: results/robustness/diagnostics/performance/config are read-only
      once config.yaml says `status: complete` (reading or copying them out is fine)
  G3  conclusions: no non-null `conclusion:` in config.yaml; the Conclusion, Economic Rationale
      and Economic Interpretation sections of a research_note.md hold only a _TODO_ placeholder
      until the researcher writes them
  G4  destructive ops (ask, not block): DROP/TRUNCATE/DELETE FROM on a data or ledger table in a
      command that runs SQL, rm of the DB (not its .bak-* copies), force-push, and running the
      one-off purge_/migrate_/renumber_ scripts without --dry-run
  G5  language: "approved" / "profitable" / "ready to trade" under experiments/, unless negated
      ("not profitable", "not yet approved")
  S1  scope: while .claude/scope.json exists, Write/Edit only to the files it lists (shell writes
      are caught afterwards by `scope.py check`, a PostToolUse hook)
  S2  only the researcher widens a scope: no edits to scope.json, no `scope.py add`

Override (F2): QUANT_ALLOW_RESULT_EDIT=1 skips G2, G3 and G5. It is read from this hook's
environment, or from a `QUANT_ALLOW_RESULT_EDIT=1` prefix on a Bash command. Write/Edit calls
carry no command text, so for those the variable must be set when Claude Code is launched.
G1 has no override (a frozen file changes through a new version file); G4 asks every time.
"""

from __future__ import annotations

import fnmatch
import json
import os
import re
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
OVERRIDE = "QUANT_ALLOW_RESULT_EDIT"
SCOPE_REL = ".claude/scope.json"  # written by .claude/hooks/scope.py

FROZEN_DEFS = re.compile(r"^docs/research/[^/]+_v\d+\.md$")
EXP_RESULT = re.compile(
    r"^experiments/(EXP-\d+)/(results\.json|robustness\.json|diagnostics\.json|performance\.parquet|config\.yaml)$"
)
EXP_NOTE = re.compile(r"^experiments/EXP-\d+/research_note\.md$")
EXP_ANY = re.compile(r"^experiments/")
HUMAN_SECTIONS = ("Economic Rationale", "Economic Interpretation", "Conclusion")
PLACEHOLDER = re.compile(r"^_TODO\b[^\n]*_$")
FROZEN_STATUS = re.compile(r"^\*\*Status:\*\*.*\bFROZEN\b", re.M)

SQL_RUNNER = re.compile(r"\b(python3?|duckdb|sqlite3)\b")
PROTECTED_TABLE = r"(\w+\.)?(assets?|prices?|macro_\w+|cot_\w+|experiments|trials)\b"
SQL_DESTRUCTIVE = re.compile(
    rf"\b(drop\s+table\s+(if\s+exists\s+)?|truncate\s+(table\s+)?|delete\s+from\s+){PROTECTED_TABLE}", re.I)
RM_DB = re.compile(r"\brm\b[^;&|\n]*(quant\.db(?!\.bak)|\.duckdb\b|\.wal\b)")
FORCE_PUSH = re.compile(r"\bgit\s+push\b[^;&|\n]*(--force\b|--force-with-lease\b|\s-f\b)")
ONE_OFF = re.compile(r"\bpython3?\b[^;&|\n]*?(scripts[/.](purge|migrate|renumber)_\w+)[^;&|\n]*")
LANGUAGE = re.compile(r"\b(approved|profitable|ready to trade)\b", re.I)
NEGATED = re.compile(r"\b(not|never|no|nor|isn't|wasn't|aren't|weren't)\W+(\w+\W+){0,2}$", re.I)
EXP_RESULT_PATH = r"experiments/(EXP-\d+)/(results\.json|robustness\.json|diagnostics\.json|performance\.parquet|config\.yaml)"


def block(rule: str, reason: str) -> None:
    print(f"[guard {rule}] {reason}", file=sys.stderr)
    sys.exit(2)


def ask(rule: str, reason: str) -> None:
    """Hand the call to the researcher's permission prompt instead of refusing it outright."""
    print(json.dumps({"hookSpecificOutput": {
        "hookEventName": "PreToolUse",
        "permissionDecision": "ask",
        "permissionDecisionReason": f"[guard {rule}] {reason}",
    }}))
    sys.exit(0)


def relpath(file_path: str) -> str | None:
    p = Path(file_path)
    if not p.is_absolute():
        p = Path.cwd() / p
    try:
        return p.resolve().relative_to(PROJECT_ROOT).as_posix()
    except ValueError:
        return None


def read(rel: str) -> str:
    try:
        return (PROJECT_ROOT / rel).read_text()
    except (OSError, UnicodeDecodeError):
        return ""


def new_content(rel: str, tool: str, ti: dict) -> str | None:
    """The file text after the call, or None if it cannot be determined."""
    if tool == "Write":
        return ti.get("content", "")
    old = read(rel)
    if tool == "Edit":
        o, n = ti.get("old_string", ""), ti.get("new_string", "")
        if o not in old:
            return None  # the Edit will fail on its own
        return old.replace(o, n) if ti.get("replace_all") else old.replace(o, n, 1)
    if tool == "MultiEdit":
        text = old
        for e in ti.get("edits", []):
            if e.get("old_string", "") not in text:
                return None
            text = text.replace(e["old_string"], e.get("new_string", ""), 1)
        return text
    return None


def added_text(tool: str, ti: dict) -> str:
    if tool == "Write":
        return ti.get("content", "")
    if tool == "Edit":
        return ti.get("new_string", "")
    if tool == "MultiEdit":
        return "\n".join(e.get("new_string", "") for e in ti.get("edits", []))
    return ""


def sections(md: str) -> dict[str, str]:
    out: dict[str, str] = {}
    parts = re.split(r"^## (.+)$", md, flags=re.M)
    for i in range(1, len(parts) - 1, 2):
        out[parts[i].strip()] = parts[i + 1].strip()
    return out


def exp_complete(exp_id: str) -> bool:
    return bool(re.search(r"^status:\s*complete\b", read(f"experiments/{exp_id}/config.yaml"), re.M))


def non_null_conclusion(yaml_text: str) -> str | None:
    m = re.search(r"^conclusion:[ \t]*(.*)$", yaml_text, re.M)
    if not m:
        return None
    val = m.group(1).strip()
    return None if val in ("", "null", "~") else val


def check_frozen(rel: str, tool: str, ti: dict) -> None:
    """G1: a FROZEN file takes edits only below its '## Change log' heading; drafts are free,
    but freezing one is the researcher's call."""
    old = read(rel)
    if not FROZEN_STATUS.search(old):
        text = new_content(rel, tool, ti)
        if text is not None and FROZEN_STATUS.search(text):
            block("G1", f"{rel}: marking a definitions file FROZEN is the researcher's call. Leave the "
                        "status as DRAFT and ask them to approve it.")
        return
    marker = re.search(r"^## Change log\s*$", old, re.M)
    if tool == "Edit" and marker:
        pos = old.find(ti.get("old_string", ""))
        if pos >= marker.start() and old.count(ti.get("old_string", "")) == 1:
            return
    if tool == "MultiEdit" and marker and all(
        old.find(e.get("old_string", "")) >= marker.start() and old.count(e.get("old_string", "")) == 1
        for e in ti.get("edits", [])
    ):
        return
    block("G1", f"{rel} is a frozen definitions file; only its '## Change log' section may be edited. "
                "A definitional change needs a new _v<N+1> file (draft it; the researcher freezes it).")


def active_scope() -> dict | None:
    try:
        return json.loads((PROJECT_ROOT / SCOPE_REL).read_text())
    except (OSError, json.JSONDecodeError):
        return None


def check_scope_edit(rel: str) -> None:
    """S1/S2: while a scope is active, only its files may be edited, and never the scope itself."""
    scope = active_scope()
    if scope is None:
        return
    if rel == SCOPE_REL:
        block("S2", "only the researcher can change the active scope.")
    if not any(rel == p or fnmatch.fnmatch(rel, p) for p in scope.get("files", [])):
        block("S1", f"{rel} is outside the declared scope ({scope.get('task')!r}). Stop and ask the "
                    "researcher whether to add it; only they can widen the scope "
                    f"(edit {SCOPE_REL}, or `! python3 .claude/hooks/scope.py add {rel}`).")


def check_file_edit(tool: str, ti: dict, override: bool) -> None:
    rel = relpath(ti.get("file_path", ""))
    if rel is None:
        return
    check_scope_edit(rel)
    if FROZEN_DEFS.match(rel):
        check_frozen(rel, tool, ti)
    if override:
        return

    m = EXP_RESULT.match(rel)
    if m and exp_complete(m.group(1)):
        text = new_content(rel, tool, ti)
        if rel.endswith("config.yaml") and text is not None:
            old = read(rel)
            if non_null_conclusion(text) and non_null_conclusion(text) != non_null_conclusion(old):
                block("G3", f"{rel}: setting `conclusion:` is the researcher's call.")
        block("G2", f"{rel} belongs to a completed experiment; results are never edited. "
                    f"Create a new EXP id, or set {OVERRIDE}=1 if the researcher asked for this edit.")

    if rel.endswith("config.yaml") and EXP_ANY.match(rel):
        text = new_content(rel, tool, ti)
        if text and non_null_conclusion(text) and non_null_conclusion(text) != non_null_conclusion(read(rel)):
            block("G3", f"{rel}: setting `conclusion:` is the researcher's call.")

    if EXP_NOTE.match(rel):
        text = new_content(rel, tool, ti)
        if text is not None:
            before, after = sections(read(rel)), sections(text)
            for name in HUMAN_SECTIONS:
                b, a = before.get(name, ""), after.get(name, "")
                unwritten = b == "" or PLACEHOLDER.match(b)
                if a != b and not (unwritten and (a == "" or PLACEHOLDER.match(a))):
                    block("G3", f"{rel}: the '{name}' section is written by the researcher. "
                                "Leave the _TODO_ placeholder and offer a draft in chat instead.")

    if EXP_ANY.match(rel):
        added = added_text(tool, ti)
        for hit in LANGUAGE.finditer(added):
            if not NEGATED.search(added[max(0, hit.start() - 40):hit.start()]):
                block("G5", f"{rel}: '{hit.group(0)}' is not allowed in experiment files (build plan s16). "
                            "Use qualified language, e.g. 'passed the robustness checks; needs review'.")


def check_bash(cmd: str, override_env: bool) -> None:
    writes_scope = SCOPE_REL in cmd and re.search(r"(>|\bsed\s+-i|\brm\b|\bmv\b|\btee\b|\bcp\b|\.write|open\()", cmd)
    if active_scope() is not None and (re.search(r"scope\.py\s+(add|start)\b", cmd) or writes_scope):
        block("S2", "only the researcher can widen or replace the active scope.")
    # G2 for shell writes into completed experiments: the path is a destination (redirect, tee,
    # sed -i, last argument of cp) or is removed/moved (rm, mv). Reading or copying out is fine.
    if not (override_env or re.match(rf"\s*{OVERRIDE}=1\b", cmd)):
        for em in re.finditer(EXP_RESULT_PATH, cmd):
            p = re.escape(em.group(0))
            writes = re.search(
                rf">>?\s*{p}|\b(tee|sed\s+-i|rm|mv|truncate)\b[^;&|\n]*{p}|\bcp\b[^;&|\n]*\s{p}\s*($|[;&|\n])", cmd)
            if writes and exp_complete(em.group(1)):
                block("G2", f"{em.group(0)} belongs to a completed experiment; results are never edited. "
                            f"Prefix the command with {OVERRIDE}=1 if the researcher asked for this.")
    # G4 asks rather than blocks: the researcher approves or denies in the permission prompt.
    if SQL_RUNNER.search(cmd) and SQL_DESTRUCTIVE.search(cmd):
        ask("G4", "destructive SQL on a data or ledger table; back up data/quant.db first.")
    if RM_DB.search(cmd):
        ask("G4", "removes a database file.")
    if FORCE_PUSH.search(cmd):
        ask("G4", "force-push.")
    m = ONE_OFF.search(cmd)
    if m and "--dry-run" not in m.group(0):
        ask("G4", f"{m.group(1)} changes stored data; has it been run with --dry-run first?")


def main() -> None:
    try:
        payload = json.load(sys.stdin)
    except json.JSONDecodeError:
        sys.exit(0)  # never block on a malformed payload
    tool = payload.get("tool_name", "")
    ti = payload.get("tool_input") or {}
    override_env = os.environ.get(OVERRIDE) == "1"
    if tool in ("Write", "Edit", "MultiEdit"):
        check_file_edit(tool, ti, override_env)
    elif tool == "Bash":
        cmd = ti.get("command", "")
        check_bash(cmd, override_env)
    sys.exit(0)


if __name__ == "__main__":
    main()
