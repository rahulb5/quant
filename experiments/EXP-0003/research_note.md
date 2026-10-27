# EXP-0003 — tsmom

## Hypothesis

A standard time-series momentum strategy (blended 1/3/12-month sign of excess return, EWMA vol scaling, equal risk per sleeve, 10% portfolio vol target, weekly rebalance) earns a positive risk-adjusted excess return across equity indices, US duration and G10 FX, as documented for CTA trend following. Baseline per docs/research/tsmom_definitions_v1.md (FROZEN v1).

## Economic Rationale

_TODO: researcher to fill in before marking this experiment complete._

## Data

- Asset class: returns_layer
- Market: multi
- Universe: cross_asset_futures_proxies
- Signal family: momentum
- Frequency: weekly
- Direction: long_short
- Resample: native frequency
- Date range used: 1990-01-03T00:00:00 to 2025-10-24T00:00:00

## Methodology

Frozen definitions docs/research/tsmom_definitions_v1.md (v1, 2026-10-26); standard specification, not selected from variants.

## Signal Construction

- Module: `src.signals.tsmom`
- Parameters: {'lookbacks': [21, 63, 252], 'vol_com': 60, 'instruments': ['EQ.SPX', 'EQ.NDX', 'EQ.RUT', 'EQ.SX5E', 'EQ.DAX', 'EQ.NKY', 'RT.UST2', 'RT.UST5', 'RT.UST10', 'RT.UST30', 'FX.EUR', 'FX.JPY', 'FX.GBP', 'FX.AUD', 'FX.CAD', 'FX.CHF']}

## Portfolio Construction

- Rule: vol_target
- Signal column: `signal` (threshold 0.0)
- Trend filter: `trend_none`
- Transaction cost: 2.0 bps, lag 1

## Results

| Metric | Value |
|---|---|
| Sharpe | 0.569 |
| CAGR | 0.0554 |
| Annualized volatility | 0.1043 |
| Max drawdown | -0.2418 |
| Hit rate | 0.521 |
| Annualized turnover | 31.442 |
| Total cost (compound) | 0.2328 |

## Out-of-Sample Results

_TODO: researcher to fill in before marking this experiment complete._

## Robustness

**Overall: NEEDS_REVIEW** (generated 2026-10-27T00:47:39+00:00, code `c0fb340d`, n_trials=1). Verdicts are mechanical; approval remains a researcher decision.

Trials in family `tsmom`: declared 1, ledger 1, used 1 (the larger). The ledger counts distinct experiment and logged-notebook configurations; notebook variants are counted only if `log_trial()` was called for them.

| Check | Verdict | Reason |
|---|---|---|
| cost_stress | PASS | breakeven cost 20.9 bps; survives 2.0x costs |
| lag_sensitivity | PASS | Sharpe retains 103% at lag+1 |
| outliers | PASS | Sharpe 0.68 after dropping the best and worst 1% of periods |
| subperiods | PASS | 72% of years positive; both halves above -0.2 |
| parameters | PASS | 0% of 9 settings have Sharpe < 0; range [0.56, 0.67] |
| walk_forward | PASS | OOS Sharpe 0.56 vs in-sample 0.67 over 31 window(s) from 1995-01-04; parameter changed 2x |
| statistical | PASS | deflated Sharpe 1.00 at n_trials=1; CI excludes 0 |
| holdout | SKIPPED | holdout from 2025-10-25 is locked (run with --unlock-holdout, once, when in-sample checks are final) |

- Sharpe 95% bootstrap CI: [0.25, 0.89]
- Deflated Sharpe (P[true Sharpe > 0]): 1.000
- Breakeven one-sided cost: 20.9 bps

## Failure Modes

_TODO: researcher to fill in before marking this experiment complete._

## Economic Interpretation

_TODO: researcher to fill in before marking this experiment complete._

## Conclusion

_TODO: researcher to fill in before marking this experiment complete._

## Next Experiments

_TODO: researcher to fill in before marking this experiment complete._
