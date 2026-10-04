"""
scripts/update_all.py

Runs every data source's incremental update in sequence. Each step is its
own script's default (no-flag) invocation, which is already the
"incremental from last stored date" behavior for every source below.

Skips commodities for now — scripts/update_commodities.py is currently
broken (stale Stooq-era API calls against the Yahoo Finance-based
CommodityCollector) and scripts/fetch_commodities.py always re-fetches full
history, which is safe but wasteful to include in a routine update.

A failing step does not stop the run — every step executes, and failures
are reported in the summary at the end.

Usage:
    python scripts/update_all.py
"""

from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent

STEPS: list[tuple[str, str]] = [
    ("Equities",        "scripts/update_equity.py"),
    ("GSCI indices",    "scripts/fetch_gsci.py"),
    ("FX / crypto",     "scripts/fetch_currencies.py"),
    ("Macro (FRED)",    "scripts/fetch_macro.py"),
    ("COT positioning", "scripts/fetch_cot.py"),
]

results: list[tuple[str, bool, float]] = []

for label, script in STEPS:
    print(f"\n{'=' * 70}\n{label}  ({script})\n{'=' * 70}")

    start = time.monotonic()
    proc = subprocess.run(
        [sys.executable, str(PROJECT_ROOT / script)],
        cwd=PROJECT_ROOT,
        env={**os.environ, "PYTHONPATH": str(PROJECT_ROOT)},
    )
    elapsed = time.monotonic() - start

    ok = proc.returncode == 0
    results.append((label, ok, elapsed))

    if not ok:
        print(f"\n  ⚠ {label} exited with code {proc.returncode} — continuing with next step.")

# ── Summary ───────────────────────────────────────────────────────────────────

print(f"\n{'=' * 70}\nSummary\n{'=' * 70}")
for label, ok, elapsed in results:
    status = "OK" if ok else "FAILED"
    print(f"  {label:<18} {status:<7} {elapsed:6.1f}s")

failed = [label for label, ok, _ in results if not ok]
if failed:
    print(f"\n{len(failed)} step(s) failed: {', '.join(failed)}")
    sys.exit(1)

print("\nAll steps completed successfully.")
