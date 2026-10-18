# EXP-0002 — formulaic_alpha_19

## Hypothesis

Formulaic alpha_19 (Kakushadze & Tulchinsky, 2015) -- fading a stock's 7-day price move, scaled by its cross-sectional rank of 250-day momentum -- predicts next-day cross-sectional relative returns on the current S&P 500 universe, traded as a daily-rebalanced decile long/short dollar-neutral portfolio. This was the standout of a 10-alpha screen in notebooks/signals/formulaic_alphas_batch1.ipynb (highest Sharpe, lowest turnover of the batch) -- see src/signals/formulaic_alpha_19.py's docstring for the survivorship-bias caveat (current constituents only, no point-in-time index membership).

## Economic Rationale

_TODO: researcher to fill in before marking this experiment complete._

## Data

- Asset class: equity
- Resample: native frequency
- Date range used: 2000-01-05T00:00:00 to 2026-10-02T00:00:00

## Methodology

_TODO: researcher to fill in before marking this experiment complete._

## Signal Construction

- Module: `src.signals.formulaic_alpha_19`
- Parameters: {}

## Portfolio Construction

- Rule: decile_long_short
- Signal column: `alpha_19`
- Deciles: 10 (long top, short bottom, dollar-neutral)
- Minimum valid names/day: 200
- Transaction cost: 1.0 bps, lag 1

## Results

| Metric | Value |
|---|---|
| Sharpe | 0.660 |
| CAGR | 0.0864 |
| Annualized volatility | 0.1407 |
| Max drawdown | -0.3349 |
| Hit rate | 0.493 |
| Annualized turnover | 148.926 |
| Total cost (compound) | 0.3975 |

## Out-of-Sample Results

_TODO: researcher to fill in before marking this experiment complete._

## Robustness

**Overall: NEEDS_REVIEW** (generated 2026-10-18T22:04:12+00:00, code `b43e16db`, n_trials=10). Verdicts are mechanical; approval remains a researcher decision.

| Check | Verdict | Reason |
|---|---|---|
| cost_stress | PASS | breakeven cost 7.2 bps; survives 2.0x costs |
| lag_sensitivity | PASS | Sharpe retains 108% at lag+1 |
| outliers | PASS | Sharpe 0.74 after dropping the best and worst 1% of periods |
| subperiods | PASS | 74% of years positive; both halves above -0.2 |
| parameters | PASS | 0% of 9 settings have Sharpe < 0; range [0.66, 0.75] |
| statistical | PASS | deflated Sharpe 0.97 at n_trials=10; CI excludes 0 |
| holdout | SKIPPED | no validation.holdout_start configured |

- Sharpe 95% bootstrap CI: [0.32, 1.00]
- Deflated Sharpe (P[true Sharpe > 0]): 0.968
- Breakeven one-sided cost: 7.2 bps

## Failure Modes

_TODO: researcher to fill in before marking this experiment complete._

## Economic Interpretation

_TODO: researcher to fill in before marking this experiment complete._

## Conclusion

_TODO: researcher to fill in before marking this experiment complete._

## Next Experiments

_TODO: researcher to fill in before marking this experiment complete._
