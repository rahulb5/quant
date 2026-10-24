"""Tests for .claude/hooks/guard.py: run the hook as Claude Code does (JSON on stdin, exit code out)."""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
GUARD = ROOT / ".claude" / "hooks" / "guard.py"
DEFS = ROOT / "docs/research/macro_state_definitions_v1.md"
NOTE = ROOT / "experiments/EXP-0001/research_note.md"
CONFIG = ROOT / "experiments/EXP-0001/config.yaml"


def run(tool, tool_input, env_extra=None):
    env = {k: v for k, v in os.environ.items() if k != "QUANT_ALLOW_RESULT_EDIT"}
    env.update(env_extra or {})
    p = subprocess.run([sys.executable, str(GUARD)], input=json.dumps({"tool_name": tool, "tool_input": tool_input}),
                       capture_output=True, text=True, env=env, cwd=ROOT)
    return p.returncode, p.stderr


def edit(path, old, new):
    return run("Edit", {"file_path": str(path), "old_string": old, "new_string": new})


def decision(cmd):
    """'block', 'ask' or 'allow' for a Bash command."""
    env = {k: v for k, v in os.environ.items() if k != "QUANT_ALLOW_RESULT_EDIT"}
    p = subprocess.run([sys.executable, str(GUARD)], capture_output=True, text=True, env=env, cwd=ROOT,
                       input=json.dumps({"tool_name": "Bash", "tool_input": {"command": cmd}}))
    if p.returncode == 2:
        return "block"
    out = json.loads(p.stdout) if p.stdout.strip() else {}
    return out.get("hookSpecificOutput", {}).get("permissionDecision", "allow")


@pytest.mark.skipif(not DEFS.exists(), reason="definitions file absent")
class TestG1:
    def test_write_blocked(self):
        assert run("Write", {"file_path": str(DEFS), "content": "x"})[0] == 2

    def test_edit_above_change_log_blocked(self):
        assert edit(DEFS, "# Macro state definitions", "# Changed")[0] == 2

    def test_edit_in_change_log_allowed(self):
        text = DEFS.read_text()
        tail = text[text.index("## Change log"):].splitlines()[-1]
        if tail.strip() and text.count(tail) == 1:
            assert edit(DEFS, tail, tail + "\n- note")[0] == 0

    def test_override_does_not_unlock(self):
        assert run("Write", {"file_path": str(DEFS), "content": "x"}, {"QUANT_ALLOW_RESULT_EDIT": "1"})[0] == 2


class TestG1Drafts:
    NEW = ROOT / "docs/research/guard_test_definitions_v9.md"

    def test_new_draft_allowed(self):
        assert run("Write", {"file_path": str(self.NEW), "content": "# X\n\n**Status:** DRAFT v9\n"})[0] == 0

    def test_new_file_marked_frozen_blocked(self):
        rc, err = run("Write", {"file_path": str(self.NEW), "content": "# X\n\n**Status:** **FROZEN v9**\n"})
        assert rc == 2 and "G1" in err

    def test_draft_edits_allowed_but_not_freezing(self):
        self.NEW.write_text("# X\n\n**Status:** DRAFT v9\n\n## 1. Body\n\nold\n")
        try:
            assert edit(self.NEW, "old", "new")[0] == 0
            rc, err = edit(self.NEW, "**Status:** DRAFT v9", "**Status:** FROZEN v9")
            assert rc == 2 and "G1" in err
        finally:
            self.NEW.unlink()


class TestG2G3G5:
    def test_results_blocked(self):
        assert run("Write", {"file_path": str(ROOT / "experiments/EXP-0001/results.json"), "content": "{}"})[0] == 2

    def test_results_override(self):
        rc, _ = run("Write", {"file_path": str(ROOT / "experiments/EXP-0001/results.json"), "content": "{}"},
                    {"QUANT_ALLOW_RESULT_EDIT": "1"})
        assert rc == 0

    def test_conclusion_in_config_blocked(self):
        rc, err = edit(CONFIG, "conclusion: null", "conclusion: it works")
        assert rc == 2 and "G3" in err

    def test_rationale_section_blocked(self):
        old = "## Economic Rationale\n\n_TODO: researcher to fill in before marking this experiment complete._"
        rc, err = edit(NOTE, old, "## Economic Rationale\n\nCommercials hedge.")
        assert rc == 2 and "G3" in err

    def test_other_section_allowed(self):
        assert edit(NOTE, "## Hypothesis", "## Hypothesis")[0] == 0

    def test_rationale_back_to_placeholder_blocked(self):
        # a written section can't be wiped back to a placeholder either
        note = ROOT / "experiments/EXP-9999/research_note.md"
        note.parent.mkdir(exist_ok=True)
        note.write_text("# N\n\n## Conclusion\n\nThe researcher's words.\n")
        try:
            rc, err = edit(note, "The researcher's words.", "_TODO: researcher_")
            assert rc == 2 and "G3" in err
        finally:
            note.unlink()
            note.parent.rmdir()

    def test_new_note_with_placeholders_allowed(self):
        content = ("# N\n\n## Results\n\nx\n\n## Economic Rationale\n\n_TODO: researcher_\n\n"
                   "## Conclusion\n\n_TODO: researcher to fill in before marking this experiment complete._\n")
        assert run("Write", {"file_path": str(ROOT / "experiments/EXP-9999/research_note.md"), "content": content})[0] == 0

    def test_new_note_with_conclusion_blocked(self):
        content = "# N\n\n## Conclusion\n\nIt works.\n"
        rc, err = run("Write", {"file_path": str(ROOT / "experiments/EXP-9999/research_note.md"), "content": content})
        assert rc == 2 and "G3" in err

    def test_language_blocked(self):
        rc, err = edit(NOTE, "## Failure Modes", "## Failure Modes\n\nThis is a profitable strategy.")
        assert rc == 2 and "G5" in err

    @pytest.mark.parametrize("text", [
        "The signal is not profitable after costs.",
        "It has not yet been approved.",
        "This is never ready to trade without a review.",
        "Unprofitable in 2008.",
    ])
    def test_negated_language_allowed(self, text):
        assert run("Write", {"file_path": str(ROOT / "experiments/EXP-9999/notes.md"), "content": text})[0] == 0

    def test_language_blocked_after_unrelated_negation(self):
        text = "Costs are not high. The strategy is clearly profitable."
        assert run("Write", {"file_path": str(ROOT / "experiments/EXP-9999/notes.md"), "content": text})[0] == 2

    @pytest.mark.parametrize("cmd", [
        "echo {} > experiments/EXP-0001/results.json",
        "echo x >> experiments/EXP-0001/config.yaml",
        "sed -i '' 's/a/b/' experiments/EXP-0001/config.yaml",
        "rm experiments/EXP-0001/results.json",
        "mv experiments/EXP-0001/results.json /tmp/r.json",
        "cp /tmp/r.json experiments/EXP-0001/results.json",
        "python x.py | tee experiments/EXP-0001/results.json",
    ])
    def test_shell_writes_into_results_blocked(self, cmd):
        assert decision(cmd) == "block"

    @pytest.mark.parametrize("cmd", [
        "cat experiments/EXP-0001/results.json | head",
        "cp experiments/EXP-0001/results.json /tmp/x.json",
        "python -c 'import json; json.load(open(\"experiments/EXP-0001/results.json\"))' > /tmp/out.txt",
        "git diff experiments/EXP-0001/config.yaml",
    ])
    def test_shell_reads_of_results_allowed(self, cmd):
        assert decision(cmd) == "allow"

    def test_source_files_unaffected(self):
        assert run("Write", {"file_path": str(ROOT / "src/macro/foo.py"), "content": "approved = 1"})[0] == 0


class TestG4:
    """G4 asks (permission prompt) rather than blocks, and only for commands that do the thing."""

    @pytest.mark.parametrize("cmd", [
        "python -c \"db.run('DROP TABLE trials')\"",
        "python -c \"db.run('drop table if exists main.prices')\"",
        "duckdb data/quant.db 'delete from macro_observations'",
        "duckdb data/quant.db 'TRUNCATE cot_positions'",
        "python - <<'EOF'\ncon.execute('DELETE FROM experiments WHERE id = 3')\nEOF",
        "rm data/quant.db",
        "rm -f data/quant.db.wal",
        "git push --force origin main",
        "python scripts/purge_stamped_fred_rows.py",
        "python scripts/migrate_cot_contract_keys.py --apply",
        "python -m scripts.renumber_misplaced_assets",
    ])
    def test_ask(self, cmd):
        assert decision(cmd) == "ask"

    @pytest.mark.parametrize("cmd", [
        "python scripts/purge_stamped_fred_rows.py --dry-run",
        "git push origin main",
        "pytest -q tests/",
        "python scripts/run_experiment.py experiments/EXP-0003/config.yaml",
        # mentions, not executions
        "grep -rni \"delete from\" src/",
        "git commit -m \"remove DROP TABLE from migration notes\"",
        "git diff scripts/renumber_misplaced_assets.py",
        "cat scripts/migrate_foo.py",
        "git add scripts/purge_stamped_fred_rows.py",
        # scratch tables and DB backups
        "python -c \"con.execute('DROP TABLE tmp_panel')\"",
        "rm data/quant.db.bak-20261020",
    ])
    def test_allowed(self, cmd):
        assert decision(cmd) == "allow"

    def test_ask_carries_reason(self):
        p = subprocess.run([sys.executable, str(GUARD)], capture_output=True, text=True, cwd=ROOT,
                           input=json.dumps({"tool_name": "Bash", "tool_input": {"command": "rm data/quant.db"}}))
        out = json.loads(p.stdout)["hookSpecificOutput"]
        assert out["hookEventName"] == "PreToolUse" and "G4" in out["permissionDecisionReason"]


def test_malformed_payload_does_not_block():
    p = subprocess.run([sys.executable, str(GUARD)], input="not json", capture_output=True, text=True)
    assert p.returncode == 0
