# EXP-0001 — cot_commercial_positioning

## Hypothesis

Extreme commercial (producer/merchant) net positioning in CFTC disaggregated futures, expressed as a 52-week rolling percentile rank of net_long contracts, predicts positive forward commodity returns over the following week (via weekly rebalance). This is the headline variant the research notebook's OOS section centers on (raw_pctile_52w >= 90), not a Sharpe-optimized choice -- see src/signals/cot_commercial_positioning.py's own docstring caveat.

## Economic Rationale

_TODO: researcher to fill in before marking this experiment complete._

## Data

- Asset class: commodity
- Resample: W-MON
- Date range used: 2006-06-26T00:00:00 to 2026-05-11T00:00:00

## Methodology

_TODO: researcher to fill in before marking this experiment complete._

## Signal Construction

- Module: `src.signals.cot_commercial_positioning`
- Parameters: {}

## Portfolio Construction

- Rule: equal_weight_long_threshold
- Signal column: `raw_pctile_52w` (threshold 90.0)
- Trend filter: `trend_none`
- Transaction cost: 1.0 bps, lag 1

## Results

| Metric | Value |
|---|---|
| Sharpe | 0.271 |
| CAGR | 0.0470 |
| Annualized volatility | 0.3727 |
| Max drawdown | -0.7747 |
| Hit rate | 0.436 |
| Annualized turnover | 12.390 |
| Total cost (compound) | 0.0245 |

## Out-of-Sample Results

_TODO: researcher to fill in before marking this experiment complete._

## Robustness

**Overall: NEEDS_REVIEW** (generated 2026-10-18T22:03:58+00:00, code `b43e16db`, n_trials=96). Verdicts are mechanical; approval remains a researcher decision.

| Check | Verdict | Reason |
|---|---|---|
| cost_stress | PASS | breakeven cost 82.5 bps; survives 2.0x costs |
| lag_sensitivity | PASS | Sharpe retains 114% at lag+1 |
| outliers | PASS | Sharpe 0.27 after dropping the best and worst 1% of periods |
| subperiods | FLAG | only 52% of years positive (< 55%) |
| parameters | PASS | 0% of 12 settings have Sharpe < 0; range [0.09, 0.46] |
| statistical | FLAG | deflated Sharpe 0.05 < 0.9 at n_trials=96; CI includes 0 |
| holdout | SKIPPED | no validation.holdout_start configured |

- Sharpe 95% bootstrap CI: [-0.12, 0.59]
- Deflated Sharpe (P[true Sharpe > 0]): 0.053
- Breakeven one-sided cost: 82.5 bps

## Failure Modes

_TODO: researcher to fill in before marking this experiment complete._

## Economic Interpretation

_TODO: researcher to fill in before marking this experiment complete._

## Conclusion

_TODO: researcher to fill in before marking this experiment complete._

## Next Experiments

_TODO: researcher to fill in before marking this experiment complete._
