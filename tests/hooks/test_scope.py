"""Tests for the scope contract: .claude/hooks/scope.py plus guard.py rules S1/S2, run in a throwaway git repo."""

import json
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

HOOKS = Path(__file__).resolve().parents[2] / ".claude" / "hooks"


@pytest.fixture
def repo(tmp_path):
    (tmp_path / ".claude" / "hooks").mkdir(parents=True)
    for name in ("guard.py", "scope.py"):
        shutil.copy(HOOKS / name, tmp_path / ".claude" / "hooks" / name)
    for f in ("a.py", "b.py", "dirty.py"):
        (tmp_path / f).write_text("x = 1\n")
    git = lambda *a: subprocess.run(["git", *a], cwd=tmp_path, check=True, capture_output=True)
    git("init", "-q")
    git("-c", "user.email=t@t", "-c", "user.name=t", "add", ".")
    git("-c", "user.email=t@t", "-c", "user.name=t", "commit", "-qm", "init")
    (tmp_path / "dirty.py").write_text("x = 2\n")  # dirty before the scope starts
    return tmp_path


def scope(repo, *args):
    return subprocess.run([sys.executable, str(repo / ".claude/hooks/scope.py"), *args], cwd=repo,
                          capture_output=True, text=True)


def guard(repo, tool, tool_input):
    p = subprocess.run([sys.executable, str(repo / ".claude/hooks/guard.py")], cwd=repo, capture_output=True,
                       text=True, input=json.dumps({"tool_name": tool, "tool_input": tool_input}))
    return p.returncode, p.stderr


def test_no_scope_means_no_restriction(repo):
    assert guard(repo, "Write", {"file_path": str(repo / "b.py"), "content": "y"})[0] == 0
    assert scope(repo, "check").returncode == 0


def test_edit_outside_scope_blocked(repo):
    scope(repo, "start", "--task", "D1", "a.py")
    assert guard(repo, "Write", {"file_path": str(repo / "a.py"), "content": "y"})[0] == 0
    rc, err = guard(repo, "Write", {"file_path": str(repo / "b.py"), "content": "y"})
    assert rc == 2 and "S1" in err


def test_globs(repo):
    scope(repo, "start", "--task", "D1", "tests/*.py")
    assert guard(repo, "Write", {"file_path": str(repo / "tests/t.py"), "content": "y"})[0] == 0


def test_claude_cannot_widen(repo):
    scope(repo, "start", "--task", "D1", "a.py")
    assert guard(repo, "Bash", {"command": "python3 .claude/hooks/scope.py add b.py"})[0] == 2
    assert guard(repo, "Bash", {"command": "echo {} > .claude/scope.json"})[0] == 2
    assert guard(repo, "Edit", {"file_path": str(repo / ".claude/scope.json"), "old_string": "a", "new_string": "b"})[0] == 2
    assert guard(repo, "Bash", {"command": "python3 .claude/hooks/scope.py end"})[0] == 0
    assert guard(repo, "Bash", {"command": "cat .claude/scope.json"})[0] == 0


def test_shell_write_caught_after_the_fact(repo):
    scope(repo, "start", "--task", "D1", "a.py")
    (repo / "a.py").write_text("in scope\n")
    assert scope(repo, "check").returncode == 0
    (repo / "b.py").write_text("sneaky\n")
    p = scope(repo, "check")
    assert p.returncode == 2 and "b.py" in p.stderr


def test_new_untracked_file_caught(repo):
    scope(repo, "start", "--task", "D1", "a.py")
    (repo / "pkg").mkdir()
    (repo / "pkg" / "new_module.py").write_text("x\n")
    p = scope(repo, "check")
    assert p.returncode == 2 and "pkg/new_module.py" in p.stderr


def test_pre_dirty_file_ok_until_changed_again(repo):
    scope(repo, "start", "--task", "D1", "a.py")
    assert scope(repo, "check").returncode == 0
    (repo / "dirty.py").write_text("x = 3\n")
    assert scope(repo, "check").returncode == 2


def test_add_then_end(repo):
    scope(repo, "start", "--task", "D1", "a.py")
    (repo / "b.py").write_text("y\n")
    assert scope(repo, "check").returncode == 2
    scope(repo, "add", "b.py")  # the researcher's path
    assert scope(repo, "check").returncode == 0
    p = scope(repo, "end")
    assert "outside scope: none" in p.stdout
    assert not (repo / ".claude/scope.json").exists()


def test_start_refuses_when_active(repo):
    scope(repo, "start", "--task", "D1", "a.py")
    assert scope(repo, "start", "--task", "other", "b.py").returncode == 1
