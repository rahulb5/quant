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

_TODO: researcher to fill in before marking this experiment complete._

## Failure Modes

_TODO: researcher to fill in before marking this experiment complete._

## Economic Interpretation

_TODO: researcher to fill in before marking this experiment complete._

## Conclusion

_TODO: researcher to fill in before marking this experiment complete._

## Next Experiments

_TODO: researcher to fill in before marking this experiment complete._
