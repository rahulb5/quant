"""
src/experiments/report.py

render_research_note(): fills the build plan's Section 11 research-note
template. Factual sections (hypothesis, data, construction, headline
results) are populated from the config and BacktestReport; judgment
sections are left as explicit TODOs for the researcher — "the AI agent can
draft this, the researcher must approve the conclusion."
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any

from src.experiments.config import ExperimentConfig

if TYPE_CHECKING:
    from src.backtest.backtest import BacktestReport

_TODO = "_TODO: researcher to fill in before marking this experiment complete._"


def _portfolio_section(config: ExperimentConfig) -> str:
    """Render the Portfolio Construction section, shaped per portfolio rule."""
    p = config.portfolio
    if p.rule == "decile_long_short":
        return (
            f"- Rule: {p.rule}\n"
            f"- Signal column: `{p.signal_column}`\n"
            f"- Deciles: {p.n_deciles} (long top, short bottom, dollar-neutral)\n"
            f"- Minimum valid names/day: {p.min_names}\n"
            f"- Transaction cost: {config.backtest.cost_bps} bps, lag {config.backtest.lag}"
        )
    return (
        f"- Rule: {p.rule}\n"
        f"- Signal column: `{p.signal_column}` (threshold {p.threshold})\n"
        f"- Trend filter: `{p.trend_filter}`\n"
        f"- Transaction cost: {config.backtest.cost_bps} bps, lag {config.backtest.lag}"
    )


def _fmt(value: Any, spec: str = ".3f") -> str:
    return "n/a" if value is None else format(value, spec)


def render_robustness_section(robustness: dict[str, Any]) -> str:
    """Render the body of the note's Robustness section from a robustness.json dict."""
    lines = [
        f"**Overall: {robustness['overall']}** (generated {robustness['generated_at']}, "
        f"code `{(robustness.get('git_sha') or 'unknown')[:8]}`, n_trials={robustness['n_trials']}). "
        "Verdicts are mechanical; approval remains a researcher decision.",
        "",
        "| Check | Verdict | Reason |",
        "|---|---|---|",
    ]
    for name, check in robustness["checks"].items():
        lines.append(f"| {name} | {check['verdict']} | {check['reason']} |")

    stat = robustness["checks"].get("statistical", {}).get("detail", {})
    cost = robustness["checks"].get("cost_stress", {}).get("detail", {})
    lines += [
        "",
        f"- Sharpe 95% bootstrap CI: [{_fmt(stat.get('ci_low'), '.2f')}, {_fmt(stat.get('ci_high'), '.2f')}]",
        f"- Deflated Sharpe (P[true Sharpe > 0]): {_fmt(stat.get('deflated_sharpe'), '.3f')}",
        f"- Breakeven one-sided cost: {_fmt(cost.get('breakeven_bps'), '.1f')} bps",
    ]
    return "\n".join(lines)


def replace_robustness_section(note: str, section_body: str) -> str:
    """Swap the body of the '## Robustness' section, leaving every other section untouched."""
    pattern = re.compile(r"(## Robustness\n\n).*?(?=\n## |\Z)", re.DOTALL)
    return pattern.sub(lambda m: m.group(1) + section_body + "\n", note, count=1)


def render_research_note(
    config: ExperimentConfig,
    report: "BacktestReport",
    robustness: dict[str, Any] | None = None,
) -> str:
    """Render a Section-11-shaped research note for one experiment."""
    metrics = report.performance.metrics()
    exec_summary = report.execution.summary(periods_per_year=config.backtest.periods_per_year)

    return f"""# {config.experiment_id} — {config.signal.module.rsplit('.', 1)[-1]}

## Hypothesis

{config.hypothesis}

## Economic Rationale

{_TODO}

## Data

- Asset class: {config.data.asset_class}
- Resample: {config.data.resample or "native frequency"}
- Date range used: {metrics.get("start")} to {metrics.get("end")}

## Methodology

{config.validation.methodology or _TODO}

## Signal Construction

- Module: `{config.signal.module}`
- Parameters: {config.signal.parameters}

## Portfolio Construction

{_portfolio_section(config)}

## Results

| Metric | Value |
|---|---|
| Sharpe | {metrics.get("sharpe"):.3f} |
| CAGR | {metrics.get("cagr"):.4f} |
| Annualized volatility | {metrics.get("ann_vol"):.4f} |
| Max drawdown | {metrics.get("max_drawdown"):.4f} |
| Hit rate | {metrics.get("hit_rate"):.3f} |
| Annualized turnover | {exec_summary.get("ann_turnover"):.3f} |
| Total cost (compound) | {exec_summary.get("total_cost"):.4f} |

## Out-of-Sample Results

{_TODO}

## Robustness

{render_robustness_section(robustness) if robustness else _TODO}

## Failure Modes

{_TODO}

## Economic Interpretation

{_TODO}

## Conclusion

{config.conclusion or _TODO}

## Next Experiments

{_TODO}
"""
