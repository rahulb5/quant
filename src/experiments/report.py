"""
src/experiments/report.py

render_research_note(): fills the build plan's Section 11 research-note
template. Factual sections (hypothesis, data, construction, headline
results) are populated from the config and BacktestReport; judgment
sections are left as explicit TODOs for the researcher — "the AI agent can
draft this, the researcher must approve the conclusion."
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from src.experiments.config import ExperimentConfig

if TYPE_CHECKING:
    from src.backtest.backtest import BacktestReport

_TODO = "_TODO: researcher to fill in before marking this experiment complete._"


def render_research_note(config: ExperimentConfig, report: "BacktestReport") -> str:
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

- Rule: {config.portfolio.rule}
- Signal column: `{config.portfolio.signal_column}` (threshold {config.portfolio.threshold})
- Trend filter: `{config.portfolio.trend_filter}`
- Transaction cost: {config.backtest.cost_bps} bps, lag {config.backtest.lag}

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

{_TODO}

## Failure Modes

{_TODO}

## Economic Interpretation

{_TODO}

## Conclusion

{config.conclusion or _TODO}

## Next Experiments

{_TODO}
"""
