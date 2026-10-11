---
name: notebook-to-signal
description: Promote a research notebook's signal logic into a reusable src/signals/ module and run it as an experiment. Use when a notebook's screen/analysis has produced a resolved, concrete signal spec the researcher wants in the experiment system (not for signals still mid-research).
---

# Notebook to signal

A research notebook (`notebooks/signals/*.ipynb`) is where signal *research* happens — screening
variants, IC analysis, regime dependence, in-sample/out-of-sample checks. `src/signals/*.py` is
where a signal's *construction logic* lives once that research has settled on something concrete
enough to backtest repeatedly through the experiment system
(`src/experiments/runner.run_experiment()`). This skill is the checklist for that promotion step.

Two worked examples already in this repo: `src/signals/cot_commercial_positioning.py` (extracted
from `cot_commercial_positioning.ipynb`) and `src/signals/formulaic_alpha_19.py` (extracted from
`formulaic_alphas_batch1.ipynb`, the best-of-10 alpha from that screen). Read whichever is closer
in shape to the new notebook before starting — don't design the contract from scratch each time.

## 0. Is this notebook actually ready?

Not every notebook should be extracted yet. Check:

- **Has the notebook committed to one spec?** A fixed set of parameters (window, lookback,
  threshold variant), not "best variant TBD" or "determined by the analysis above." If the
  notebook screens N candidates (like `formulaic_alphas_batch1.ipynb`'s 10 alphas), only the
  winner gets promoted — the notebook keeps screening the rest, unchanged.
- **Are known limitations identified, even if unresolved?** Survivorship bias, look-ahead risk,
  thin universes, etc. These do **not** block extraction — document them in the module docstring
  as an explicit caveat (see both existing signals' docstrings) rather than silently fixing them
  or silently ignoring them. Extraction is a snapshot of the notebook's current state, not a
  cleanup pass.
- If the notebook hasn't settled on a spec (variant undecided, asset weights hand-wavy, "next
  steps" list includes picking the signal itself), it isn't ready — do the closure work in the
  notebook first, or extract explicitly "as-is with caveats" only if the researcher says that's
  fine for this one (ask, don't assume).

## 1. Separate signal logic from portfolio logic

Build plan Section 7: a signal module says "asset X scored 0.72 today." It must **not** decide
position sizing, long/short selection, or how many names to hold — that's
`src/experiments/portfolio.py`'s job. When reading the notebook, classify every cell into one of
these two buckets before writing any code:

- **Signal-layer logic** (→ the new module): data loading, feature construction, the score
  itself. Becomes `load_data()` + `calculate_signal()`.
- **Portfolio-layer logic** (→ `src/experiments/portfolio.py`, if a matching rule doesn't already
  exist there): thresholding, decile bucketing, long/short construction, weighting. Becomes a new
  function there, registered in `src/experiments/runner.py`'s `PORTFOLIO_REGISTRY` — or reuses an
  existing one (`equal_weight_long_basket`, `decile_long_short`) if the notebook's construction
  matches it.

If a notebook's decile/threshold cutoffs or coverage minimums (e.g. "need >=200 valid names to
trade a day") look like signal logic, check whether they're actually a portfolio-construction
concern in disguise — `decile_long_short`'s `min_names` absorbed exactly this from
`formulaic_alphas_batch1.ipynb`'s `MIN_COVERAGE`, rather than the signal module filtering days
itself.

## 2. Write the module: `src/signals/<name>.py`

Match the existing contract exactly — `src/experiments/runner.py`'s docstring names it as the
standard interface:

```python
def load_data() -> tuple[pd.DataFrame, ...]:
    """Query the DB, return raw frames. Caller manages db.open()/db.close()."""

@dataclass(frozen=True)
class SignalConfig:
    """Signal-construction parameters only — not portfolio params like threshold/n_deciles."""

def calculate_signal(*data: pd.DataFrame, config: SignalConfig = SignalConfig()) -> pd.DataFrame:
    """Long-format frame: one row per (date, asset), plus the signal column(s)."""
```

- `load_data()` imports `from src.db.client import db` and queries directly — no provider-specific
  logic (that already lives in `src/collectors/`).
- `calculate_signal()`'s output must be joinable with a portfolio rule's `date_column`/
  `asset_column`/`signal_column` convention — a long-format frame, not wide. If the notebook
  computed a wide date x asset panel (common for cross-sectional signals), melt it at the end
  (watch the index name: `wide.index.name = <date_column>` before `reset_index().melt(...)`, since
  a panel built in a test or a differently-shaped pivot may not carry the name you expect).
- Module docstring: cite the source notebook, note which research-process details stay in the
  notebook on purpose (IC analysis, variant screening), and state known limitations explicitly —
  copy the tone of the two existing modules' docstrings.
- Don't invent a different signal/config shape "to be more general" — matching the existing
  contract is what lets `runner.py` call any signal module identically.

## 3. Check the asset universe

Does `src/experiments/universe.py`'s `UNIVERSE_REGISTRY` already have the asset class this signal
needs (`commodity`, `equity`, ...)? If not, add one subclass of `AssetUniverse` (implement
`asset_ids()` only — `load_prices()`/`returns_panel()` are inherited) and register it. Don't touch
existing universe classes.

## 4. Check the portfolio rule

Does `src/experiments/portfolio.py` already have a rule matching this signal's construction
(threshold basket, decile long/short, ...)? If not:
- Add the function there, with the same `(signal_df, date_column, asset_column, signal_column,
  ...)` calling convention as the existing rules — no hardcoded column names.
- Register it in `src/experiments/runner.py`'s `PORTFOLIO_REGISTRY`.
- Extend `PortfolioConfig` (`src/experiments/config.py`) with any new fields the rule needs, as
  optional fields with defaults — don't repurpose an existing field (e.g. `threshold`) for a
  different meaning under a different rule.
- If the research note renderer (`src/experiments/report.py`'s `render_research_note`) describes
  portfolio construction in prose, check whether it needs a rule-specific branch so the note
  doesn't print a nonsensical field for the new rule (see `_portfolio_section()`'s dispatch there).

## 5. Tests

Mirror `tests/signals/test_cot_commercial_positioning.py` / `test_formulaic_alpha_19.py`:
- Pure-pandas unit tests for `calculate_signal()` and any helper transforms — construct small
  synthetic frames by hand, no DB.
- A `load_data()` test against an in-memory DuckDB (`Database(":memory:")`), monkeypatched into
  the signal module's own `db` reference (`monkeypatch.setattr(sig_module, "db", database)`).
- If you added a portfolio rule, test it in `tests/experiments/test_portfolio.py` the same way
  (small synthetic signal frames, check the weight panel shape/values directly).
- Run the full suite (`PYTHONPATH=... pytest -q`) before moving on — a new portfolio rule touches
  shared dispatch code in `runner.py`, so regressions there are easy to introduce silently.

## 6. Scaffold and run the experiment

```
PYTHONPATH=... python scripts/new_experiment.py \
    --hypothesis "..." --signal-module src.signals.<name> \
    --asset-class <class> --date-column <col> --asset-column <col> --signal-column <col> \
    --portfolio-rule <rule> [rule-specific flags] --researcher <name>

PYTHONPATH=... python scripts/run_experiment.py experiments/EXP-000N/config.yaml
```

Check `experiments/EXP-000N/research_note.md` renders sensibly (especially the Portfolio
Construction section if you added a new rule) and the registry row lands as `complete` with
sane-looking headline metrics, not `failed`.

## 7. Update the architecture doc

Run the `update-architecture` skill so `docs/architecture.md` reflects the new signal, any new
portfolio rule/universe class, and the new experiment — don't leave the promotion undocumented
there.

## Non-goals

- Don't rewrite or delete the source notebook — it remains the research record. The module is a
  narrow, reusable extraction of the part that's settled, not a replacement.
- Don't "clean up" the signal's known limitations as part of extraction (fix bugs, yes; change
  the economic construction or the universe to address bias, no) — that's a separate research
  decision, not a plumbing one.
- Don't add a new portfolio rule when an existing one already fits — check first.
- Don't make `SignalConfig` hold portfolio-construction parameters (thresholds, decile counts,
  weighting schemes) — those belong in `config.portfolio` / `PortfolioConfig`.
