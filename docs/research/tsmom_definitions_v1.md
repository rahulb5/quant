# Trend baseline (time-series momentum) definitions — v1

**Status:** **FROZEN v1**, approved by the researcher on 2026-10-26 (T1–T4: defaults). Written before any backtest of this strategy was run. Only the researcher
changes this line; once it says FROZEN, changes go in the change log with a new trial-ledger `family_version`.
**Implements:** Phase 1 of `docs/plans/cross_asset_strategies.md`, the trend baseline over equity, US
duration and FX. No commodities (no clean returns yet).
**Parameters** come from standard published practice: Moskowitz, Ooi and Pedersen (2012) for the
instrument-level signal and vol scaling, and Hurst, Ooi and Pedersen (2017) for blending lookbacks. None
was chosen by looking at how this strategy performs. The point of a baseline is to be the standard thing,
done cleanly.

## 1. Universe

From `src/returns/layer.py` (daily excess returns over cash; see its docstring for construction). This is the
S1 draft universe in the plan, each instrument mapped to a liquid future.

| Sleeve | Keys | Trade vehicle (to verify before live) |
|---|---|---|
| Equity | `EQ.SPX`, `EQ.NDX`, `EQ.RUT`, `EQ.SX5E`, `EQ.DAX`, `EQ.NKY` | MES, MNQ, M2K; FESX/FDXS (Eurex); NKD/NIY or OSE micro |
| Duration | `RT.UST2`, `RT.UST5`, `RT.UST10`, `RT.UST30` | ZT, ZF, ZN, ZB/UB (or micro yield futures) |
| FX | `FX.EUR`, `FX.JPY`, `FX.GBP`, `FX.AUD`, `FX.CAD`, `FX.CHF` | M6E, MJY?, M6B, M6A, MCD, MSF (CME micro FX; availability to verify) |

The universe grows as instruments' histories start, with no back-filling. Equity uses `pre_coverage="drop"`,
so `EQ.SX5E` and `EQ.NKY` enter around 2009–10 when their dividend source starts. `EQ.SPX` and `EQ.DAX` are
there from 1988, `EQ.RUT` from 1995, `EQ.NDX` from 2000. Rates and FX run back to the 1970s.

## 2. Signal (per instrument, on its own trading days)

| Item | Definition |
|---|---|
| Trend | For each lookback L ∈ {21, 63, 252} trading days: sign of the cumulative excess return over the last L observations. Signal = mean of the three signs ∈ {−1, −⅓, +⅓, +1} (a cumulative return of exactly 0 counts as 0, so 0 and ±⅔ are possible but practically never occur) |
| Needs | All three lookbacks: no signal until 252 observations |
| Ex-ante vol σ | Annualised EWMA standard deviation of daily excess returns, centre of mass 60 days (`ewm(com=60).std() × √252`), minimum 60 observations |
| Timing | Signal and σ at close t use returns up to and including t. The weight decided at t earns t+1 (backtest `lag: 1`) |

## 3. Portfolio construction (`vol_target_weights`)

| Item | Definition |
|---|---|
| Risk units | Raw weight = signal × (1/G) × (1/N_g,t) × (τ / σ). G = number of sleeves with any live instrument; N_g,t = live instruments in sleeve g; τ = 10%. "Live" = has a signal and vol, carried over the instrument's own non-trading days for up to 10 rows (`hold_limit`), so a holiday doesn't shift its sleeve-mates' shares. Equal risk per sleeve holds only if instruments were perfectly correlated; there's no covariance model (T4) |
| Portfolio vol target | Scale all raw weights by λ_t = τ / σ̂_p,t. σ̂_p,t is the realised vol of the raw portfolio's daily returns over the past 252 rows on which at least one of its instruments traded (known at t), annualised with √252. λ = 1 before 126 such rows; λ is clipped to [0.5, 5.0] (see the note below) |
| Gross leverage cap | Σ\|w\| ≤ 8 for each traded target (scaled down proportionally if exceeded). A held weight next to newly traded ones can briefly take gross slightly above 8 |
| Rebalance | Calendar schedule: every Friday (W-FRI). Each instrument trades on its first own trading day on or after the scheduled Friday, at that day's target, and holds the weight in between. The schedule doesn't depend on which later dates exist, so it's the same in a backtest and live |
| Holidays | Follows from the rule above: an instrument closed on the Friday trades on its next trading day (with that day's target) instead. A weight is never set with information from other markets on its holiday and then used for a return measured from before the holiday |
| Stale data | Weight → 0 once an instrument's data has been missing for more than `hold_limit` rows (a dead or stale series is exited, not held forever) |
| Direction | Long/short, net exposure unconstrained |

Why [0.5, 5.0] rather than the first draft's [0.5, 2.0]: the raw weights give each instrument τ/(G·N) of risk, as if
all instruments were perfectly correlated. With 16 partly uncorrelated instruments the raw portfolio runs well
below τ. In a positions-only smoke check (2026-10-25; no returns or P&L computed), the unbounded λ had a median
of 2.9 and a 1st–99th percentile range of 1.6–4.3, so a cap of 2.0 bound 94% of the time and the target was never
reached. This is a property of the construction, not of performance. With [0.5, 5.0], gross leverage would have a
median of about 3.2, and the cap of 8 binds only in the top few percent of days, mostly from the 2-year Treasury
position.

## 4. Backtest settings

| Item | Value | Note |
|---|---|---|
| Costs | 2 bp one-way on traded notional, all instruments | The plan wanted costs per sleeve; `apply_costs` takes one rate. 2 bp is conservative for liquid index, Treasury and FX futures; the robustness cost-stress runs to 5× |
| Lag | 1 day | |
| Sample | From 1990-01-01: the first full calendar year after the equity sleeve's warm-up (SPX and DAX returns start in Jan 1988, so their 252-day signal is live from about Jan 1989) plus λ's 126-row warm-up | Signals and λ use history before 1990. Positions held on 1990-01-01 are entered without cost (`apply_costs` books no turnover in the first period) |
| Holdout | `validation.holdout_start` = 2025-10-25 (`new_experiment.py` default: one year before scaffolding) | Scored once, with `--unlock-holdout` |
| Trial family | `tsmom`, v1; `n_trials` declared 1 | Standard specification, not selected from variants |

## 5. Robustness grid (recorded, doesn't count toward n_trials, E4)

`signal.parameters.lookbacks` ∈ {[21, 63, 252], [63, 252], [252]}; `signal.parameters.vol_com` ∈ {30, 60, 90}.
These are modest perturbations. The walk-forward check (C11) uses the same grid.

### Proposed experiment config (EXP-0003, created with `scripts/new_experiment.py` after the freeze)

```yaml
data:       {asset_class: returns_layer, resample: null, start: '1990-01-01', end: null,
             instruments: [EQ.SPX, EQ.NDX, EQ.RUT, EQ.SX5E, EQ.DAX, EQ.NKY,
                           RT.UST2, RT.UST5, RT.UST10, RT.UST30,
                           FX.EUR, FX.JPY, FX.GBP, FX.AUD, FX.CAD, FX.CHF]}
signal:     {module: src.signals.tsmom, parameters: {lookbacks: [21, 63, 252], vol_com: 60,
             instruments: <same list as data.instruments>}}   # the runner refuses a mismatch
portfolio:  {rule: vol_target, date_column: date, asset_column: instrument, signal_column: signal,
             target_vol: 0.10, port_vol_window: 252, port_vol_min_obs: 126,
             scaler_low: 0.5, scaler_high: 5.0, max_gross: 8.0, rebalance: W-FRI, hold_limit: 10}
backtest:   {cost_bps: 2.0, lag: 1, periods_per_year: 252}
validation: {family: tsmom, family_version: v1, n_trials: 1, holdout_start: '2025-10-25'}
classification: {asset_class: multi, universe: cross_asset_futures_proxies, signal_family: momentum,
                 frequency: weekly, direction: long_short}
robustness: {param_grid: {signal.parameters.lookbacks: [[21, 63, 252], [63, 252], [252]],
                          signal.parameters.vol_com: [30, 60, 90]}}
```

## 6. Known limitations (stated in the research note)

- **No commodities**, so this is not a full CTA replica. Correlation with the SG Trend Index is expected to be
  lower than for a diversified CTA. SG Trend data isn't in the DB, so that comparison is deferred.
- **Duration is US-only**: four tenors on one curve are one or two bets, not four.
- **Bond returns are built from CMT yields**, not futures. ETF validation (R3) was deferred.
- **FX carry uses policy rates**, with the biases listed in `src/returns/cash.RATE_CAVEATS`. They're small
  relative to spot moves for a trend strategy.
- **Nine bad prints were repaired using the next day's price** (returns-layer attrs); signals on those days use
  repaired values.
- **Non-US equity returns are local-currency** excess returns; no FX translation of futures P&L.
- **Asynchronous closes:** a weight decided at the US close earns Nikkei's and DAX's returns from their own
  later closes, and FX from its source's fixing time. Each instrument's signal uses only its own history, so any
  bias comes only through λ and the live counts. It is likely small.

## 6a. Validation to report (from the plan, not pass/fail rules)

- The robustness suite, including walk-forward (C11), and the macro-regime table (C9 diagnostics, on by default).
- Behaviour in 2008, 2014 and 2022 (crisis alpha), from the yearly and crisis-window tables.
- Contribution by sleeve.
- The SG Trend / BTOP50 correlation is deferred (no data).

## 7. Questions for the researcher (answered 2026-10-26: all defaults)

| # | Question | Answer (default) |
|---|---|---|
| T1 | Universe: the S1 draft (16 instruments), or all 23 layer keys (adds UKX, HSI, TSX, AS51, NZD, SEK, NOK)? | S1 draft |
| T2 | Equity before dividend coverage: drop (SX5E/NKY from ~2009), or `hold_first` (approximate earlier history)? | drop |
| T3 | Costs: flat 2 bp, or extend `apply_costs` to per-instrument costs first? | flat 2 bp |
| T4 | Sleeve weighting: equal risk per sleeve (1/3 each), or equal per instrument (1/N)? | equal per sleeve |

## Change log

Pre-freeze edits to this DRAFT, all on 2026-10-25, before any P&L was computed:
- λ upper bound raised from 2.0 to 5.0 after the positions-only smoke check (section 3 note).
- Rebalance changed from "last date in each week" to a calendar Friday schedule with per-instrument trading
  days, following the look-ahead review: the old rule depended on which later dates exist.
- The 10-row hold, stale-data exit, and λ computed on the portfolio's own trading rows were written in.
