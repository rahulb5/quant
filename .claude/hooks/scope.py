#!/usr/bin/env python3
"""
.claude/hooks/scope.py

Scope contract for one task (used by /plan-step; see docs/plans/agentic_workflow.md s1). While
.claude/scope.json exists, only the files it lists may change:

  - guard.py (PreToolUse) blocks Write/Edit/MultiEdit outside the list (rule S1), and blocks Claude
    from editing scope.json or running `scope.py add` (S2): only the researcher widens a scope.
  - `scope.py check` (PostToolUse on Bash) compares `git status` with the list, so files written by
    shell commands or python heredocs are caught right after the command that changed them.

Files already dirty when the scope started are recorded with a content hash. They only count
as violations if they change again.

CLI
  scope.py start --task "<one line>" FILE_OR_GLOB...   begin a scope (fails if one is active)
  scope.py add FILE_OR_GLOB...                         researcher only: widen the scope
  scope.py status                                      show the scope and any violations
  scope.py check                                       hook mode: exit 2 + stderr on violations
  scope.py end                                         finish; prints violations, removes the file
"""

from __future__ import annotations

import argparse
import fnmatch
import hashlib
import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCOPE_FILE = ROOT / ".claude" / "scope.json"
SCOPE_REL = ".claude/scope.json"


def load() -> dict | None:
    try:
        return json.loads(SCOPE_FILE.read_text())
    except (OSError, json.JSONDecodeError):
        return None


def save(scope: dict) -> None:
    SCOPE_FILE.write_text(json.dumps(scope, indent=2) + "\n")


def file_hash(rel: str) -> str:
    p = ROOT / rel
    if not p.exists():
        return "deleted"
    if p.is_dir():
        return "dir"
    return hashlib.sha256(p.read_bytes()).hexdigest()


def dirty_files() -> list[str]:
    """Modified, added, deleted and untracked files (each untracked file listed individually)."""
    out = subprocess.run(["git", "status", "--porcelain", "-uall", "--no-renames"], cwd=ROOT,
                         capture_output=True, text=True).stdout
    paths = []
    for line in out.splitlines():
        path = line[3:].strip().strip('"')
        if path and path != SCOPE_REL:
            paths.append(path)
    return paths


def allowed(rel: str, scope: dict) -> bool:
    return any(rel == pat or fnmatch.fnmatch(rel, pat) for pat in scope["files"])


def violations(scope: dict) -> list[str]:
    base = scope.get("baseline", {})
    bad = []
    for rel in dirty_files():
        if allowed(rel, scope):
            continue
        if rel in base and base[rel] == file_hash(rel):
            continue  # dirty before the scope began and untouched since
        bad.append(rel)
    return sorted(bad)


def normalise(paths: list[str]) -> list[str]:
    out = []
    for p in paths:
        q = Path(p)
        if q.is_absolute():
            try:
                q = q.resolve().relative_to(ROOT)
            except ValueError:
                continue
        out.append(q.as_posix())
    return out


def cmd_start(args) -> int:
    if load():
        print(f"a scope is already active ({load()['task']!r}); run `scope.py end` first", file=sys.stderr)
        return 1
    files = normalise(args.files)
    if not files:
        print("list at least one file or glob", file=sys.stderr)
        return 1
    save({
        "task": args.task,
        "started": datetime.now().isoformat(timespec="seconds"),
        "files": files,
        "baseline": {rel: file_hash(rel) for rel in dirty_files()},
    })
    print(f"scope started: {args.task}\n  " + "\n  ".join(files))
    return 0


def cmd_add(args) -> int:
    scope = load()
    if not scope:
        print("no active scope", file=sys.stderr)
        return 1
    scope["files"] += [f for f in normalise(args.files) if f not in scope["files"]]
    save(scope)
    print("scope now:\n  " + "\n  ".join(scope["files"]))
    return 0


def report(scope: dict) -> list[str]:
    bad = violations(scope)
    print(f"task: {scope['task']}  (since {scope['started']})")
    print("files:\n  " + "\n  ".join(scope["files"]))
    print("outside scope: " + (", ".join(bad) if bad else "none"))
    return bad


def cmd_status(_args) -> int:
    scope = load()
    if not scope:
        print("no active scope")
        return 0
    report(scope)
    return 0


def cmd_check(_args) -> int:
    scope = load()
    if not scope:
        return 0
    bad = violations(scope)
    if bad:
        print(
            "[guard S1] changed outside the declared scope: " + ", ".join(bad) + ". "
            "Stop. Revert these changes (git checkout -- <file>, or delete new files), or ask the researcher "
            f"whether to add them; only they can widen the scope (edit {SCOPE_REL}, or "
            "`! python3 .claude/hooks/scope.py add <file>`).",
            file=sys.stderr,
        )
        return 2
    return 0


def cmd_end(_args) -> int:
    scope = load()
    if not scope:
        print("no active scope")
        return 0
    bad = report(scope)
    SCOPE_FILE.unlink()
    print("scope ended" + (" WITH out-of-scope changes; list them in the final report" if bad else ""))
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("start"); s.add_argument("--task", required=True); s.add_argument("files", nargs="+")
    a = sub.add_parser("add"); a.add_argument("files", nargs="+")
    for name in ("status", "check", "end"):
        sub.add_parser(name)
    args = ap.parse_args()
    return {"start": cmd_start, "add": cmd_add, "status": cmd_status, "check": cmd_check, "end": cmd_end}[args.cmd](args)


if __name__ == "__main__":
    sys.exit(main())
