# Macro state definitions — v1 (frozen)

**Status:** **FROZEN v1**, approved by the researcher on 2026-10-24 with every default in section 6 (J1–J9) kept. Committed *before any asset-return numbers were computed* (plan step C8.1). Any later change needs a reason written in the "Change log" at the bottom and a new trial-ledger `family_version`.
**Implements:** C3 (state vector), C4 (rates), C5 (market and positioning), C6 (regimes) in
`docs/plans/macro_research_layer.md`.
**Parameters** come from standard practice, not from looking at how assets behave.

## 0. Conventions used everywhere

| Item | Definition |
|---|---|
| Point-in-time | Every input goes through `src/macro/pit.py`: `realtime()` for revised series, `unrevised()` (latest value + fixed lag) for `UNREVISED_SERIES`. Transforms below are applied **inside** that call, to each snapshot |
| Calendar | Business days (`pd.bdate_range`), forward-filled from release dates (`to_daily`) |
| Year-ago | The last observation on or before the same calendar date one year earlier (as in `transforms._year_ago`), so 53-week years and holidays don't drift |
| Expanding z-score | `z_t = (x_t − mean(x_≤t)) / std(x_≤t)`, computed **once per release** on the release-dated series (not on forward-filled days, which would overweight slow series), including the current observation (contemporaneous, as in `src/backtest/regimes.py`), then clipped to ±3 |
| Burn-in | No z-score until 60 observations for monthly series (5 years), 156 for weekly (3 years), 756 for daily (3 years) |
| Composite | Mean of its component z-scores, each signed so that **positive = more of the thing named**. Needs at least 2 components, otherwise NaN. The number of components used is stored |
| Sign of an impulse | Positive = accelerating / rising / tightening |
| Valid-from | The first date all required components exist in real time (after burn-in). Each output carries it in `.attrs` |

## 1. State vector (C3)

### Growth

| | Definition |
|---|---|
| `G_level` | Composite of: IP YoY % (`INDPRO`); payrolls YoY % (`PAYEMS`); −(`UNRATE` − its expanding median) |
| `G_impulse` | Composite of 6-month changes in: IP YoY (pp); payrolls YoY (pp); −`UNRATE` (pp); −(13-week average of NSA initial claims, YoY %) (`ICNSA`, change in pp) |
| Valid from | Level ~1965 (payrolls 1955, UNRATE 1960, IP, plus 5-year burn-in); impulse ~1973 (claims need 1967 + YoY + 6m + burn-in) |

### Inflation

| | Definition |
|---|---|
| `pi_level` | Core CPI NSA YoY % (`CPILFENS`) − 2.0 |
| `pi_impulse` | 6-month change in core CPI NSA YoY (pp) |
| Supplements (reported, **not** in the composite, so the history is one consistent measure) | headline CPI NSA YoY; SA core CPI 3-month annualised minus its YoY (fast impulse, from 1996); 6-month change in `T10YIE` (from 2003, used in pricing, C7) |
| Valid from | ~1958 for the level (core NSA from 1957 + YoY); ~1958 for the impulse (no z-score needed) |

Core inflation is the target-gap reference, not headline: headline CPI NSA is kept for diagnostics.

### Labor (for the composite, the flags and cross-checks)

| | Definition |
|---|---|
| `L_level` | `UNRATE` − its expanding median (pp). Positive = slack |
| `L_impulse` | Deterioration composite (positive = worsening): 3-month change in `UNRATE`; 13-week average of NSA initial claims YoY; 13-week average of continuing claims YoY (`CCSA`); 6-month % change in temp-help employment (`TEMPHELPS`, real-time from 2011); −6-month change in average weekly hours (`AWHAETP`, from 2010). Components enter as they become available (needs ≥ 2) |
| Valid from | ~1974 with three components; five from 2011 |

### Policy

| | Definition |
|---|---|
| `P_stance_raw` | `DFF` − core CPI NSA YoY (real policy rate, pp), with CPI as of its latest release |
| `P_stance` | `P_stance_raw` − its expanding median. Positive = restrictive relative to history. r\* is not estimated (no as-of series exists) |
| `P_impulse` | `DFF`_t − `DFF` 126 business days earlier (pp) |
| Valid from | ~1958 (raw); ~1963 (stance, 5-year burn-in on the monthly CPI grid) |

### Financial conditions

| | Definition |
|---|---|
| `FC` | Composite of daily-sampled expanding z-scores of: HY OAS (`BAMLH0A0HYM2`, +1 business day); VIX (`VIXCLS`); −`T10Y3M` (inversion = tighter). Same recipe before and after 2011 |
| `FC_impulse` | 63-business-day change in `FC` |
| Cross-check | `NFCI` real-time level, from 2011 only; never mixed into `FC` |
| Valid from | ~2000 (HY OAS from 1997 + 3-year burn-in) |

Equities, USD and real rates are excluded on purpose: they belong to market state (section 3), so
`FC` stays a credit / volatility / curve measure.

### Credit supply

| | Definition |
|---|---|
| `CS` | Composite of: SLOOS net % of banks tightening C&I standards (`DRTSCILM`, 60-day lag); −(bank credit YoY %, `TOTBKCR`, real-time) |
| `CS_impulse` | 6-month change in `CS` |
| Valid from | ~2001 (bank credit vintages from 1996 + YoY + burn-in) |

This differs from the plan, which also put HY OAS in `CS`. HY OAS is already in `FC`; keeping it out
here is what separates "Fed tightens through rates and spreads" from "banks stop lending".

Output: `macro_state()` returns `G_level, G_impulse, pi_level, pi_impulse, L_level, L_impulse,
P_stance, P_impulse, FC, FC_impulse, CS, CS_impulse`, one `*_n` (components used) and one
`*_staleness` (days since the newest input release) per column.

## 2. Rates decomposition (C4)

All daily. Changes are over 63 business days (~3 months) unless stated.

| Quantity | Definition | From |
|---|---|---|
| Nominal / real / breakeven | `DGS10`, `DFII10`, `T10YIE`; same for 5Y (`DGS5`, `DFII5`, `T5YIE`). `resid = DGS10 − DFII10 − T10YIE` is reported (it's ≈ 0 by construction) | 2003 |
| Attribution | `Δnominal = Δreal + Δbreakeven`; shares are reported only when `|Δnominal| > 10 bp` | 2003 |
| 5y5y forward | `T5YIFR` | 2003 |
| Term premium | `THREEFYTP10` level and change, shown **beside** the decomposition, labelled "model estimate, real-time from 2016". Never added to it (see plan: it double-counts) | 1990 |
| Curve | `T10Y2Y`, `T10Y3M`; Δ`DGS10`; Δ`T10Y2Y`. State: `bull`/`bear` = Δ`DGS10` < 0 / ≥ 0; `steepener`/`flattener` = Δ`T10Y2Y` ≥ 0 / < 0 | 1977 / 1982 |
| Policy expectations (OIS proxy) | `DGS2 − DFF`, `DGS1 − DFF`. Includes term premium, so it's a proxy, labelled as one | 1976 |
| Treasury proxy return | `r_t = DGS10_{t−1}/100/252 − 8.5 · (DGS10_t − DGS10_{t−1})/100` | 1962 |

## 3. Market state and positioning (C5)

| Variable | Definition | From |
|---|---|---|
| Credit | HY OAS level; 63-day change; BBB OAS − IG OAS | 1997 |
| Dollar | `DTWEXBGS` / its 200-day mean − 1; sign gives `strong_usd` (> 0) / `weak_usd` | 2006 |
| Volatility | `VIXCLS` level; expanding terciles (burn-in 756) via the existing `VolRegime` | 1990 |
| Equity trend | S&P 500 (100001) vs its 200-day mean (existing `TrendRegime`) | 1970 |
| Growth pricing | Composite of 63-day changes in: `T10Y2Y`; −HY OAS; log(Russell 2000 100003 / S&P 500 100001); log(copper `HG1` 300017 / gold `GC1` 300009) | 2000 |
| Inflation pricing | 63-day change in `T10YIE` (expanding z) | 2003 |
| Policy pricing | 63-day change in `DGS2 − DFF` (expanding z) | 1976 |
| **Positioning** | Net % of open interest, expanding z (burn-in 156 weeks), available on `report_date + 3` days: leveraged funds for 10Y note, E-mini S&P, EUR, JPY; managed money for crude, gold, copper | 2006 |

## 4. Regimes (C6)

Each is a `Regime` subclass reading the series above. "Sign" of a z-scored composite: > 0 / ≤ 0.
A value exactly at the boundary keeps the previous label.

| `name` (layer) | Labels | Rule | `min_duration` |
|---|---|---|---|
| `growth_inflation` (macro) | `goldilocks` (G↑ π↓), `reflation` (G↑ π↑), `stagflation` (G↓ π↑), `deflation` (G↓ π↓) | sign of `G_impulse` × sign of `pi_impulse` (z-scored, expanding) | 21 business days |
| `growth_level` (macro) | `above_trend`, `below_trend` | sign of `G_level` ("trend" = its own expanding mean) | 0 |
| `policy` (macro) | `restrictive_tightening`, `restrictive_easing`, `accommodative_tightening`, `accommodative_easing` | stance: sign of `P_stance`. Direction: `tightening` once `P_impulse` ≥ +25 bp, `easing` once ≤ −25 bp; between, the previous direction holds | 0 |
| `financial_conditions` (macro) | `loose`, `normal`, `tight` | expanding terciles of `FC` (burn-in 756) | 0 |
| `curve_dynamics` (market) | `bull_steepener`, `bear_steepener`, `bull_flattener`, `bear_flattener` | section 2 curve state | 0 |
| `dollar_trend` (market) | `strong_usd`, `weak_usd` | section 3 | 0 |

**Event flags** (booleans, not regimes): `sahm_triggered` (real-time 3-month average `UNRATE` minus
its minimum over the prior 12 months ≥ 0.5 pp, checked against `SAHMREALTIME`);
`labor_deteriorating` (`L_impulse` above its expanding 80th percentile); `curve_inverted`
(`T10Y3M` < 0).

## 5. Pricing divergence (used in C7)

For each dimension, `gap = z_macro − z_market`, both expanding z-scores sampled the same way:

| Dimension | Macro side | Market side |
|---|---|---|
| Growth | `G_impulse` | growth pricing composite (section 3) |
| Inflation | `pi_impulse` | 63-day change in `T10YIE` |
| Policy | `P_impulse` | 63-day change in `DGS2 − DFF` |

A positive gap means the data is stronger than the market is pricing. Forward returns are studied
by gap tercile in C7.

## 6. Judgement calls (all defaults confirmed 2026-10-24)

The choices a reviewer could reasonably argue with. The default column is what v1 uses.

| # | Choice | Default | The alternative |
|---|---|---|---|
| J1 | Unemployment gap | `UNRATE` − its expanding median | Trailing 10-year median (adapts faster to structural shifts, loses early history) |
| J2 | z-score method | Expanding mean and std, clipped at ±3 | Median / MAD (robust to 2008 and 2020 outliers) |
| J3 | Burn-in | 5 years monthly, 3 years weekly/daily | Shorter gives earlier start dates but noisier early z-scores |
| J4 | Impulse window | 6 months (63 business days for market series) | 3 months (faster, noisier) or 12 |
| J5 | Policy stance baseline | Real rate minus its expanding median | A fixed neutral (e.g. 0.5% real) |
| J6 | Policy impulse dead-band | ±25 bp over 6 months, direction held between | No dead-band (more flips on small moves) |
| J7 | `CS` excludes HY OAS | Excluded (it's in `FC`) | Include it, as in the original plan |
| J8 | Regime sign threshold | Composite > 0 | A ±0.25 dead-band around 0 (fewer, longer episodes) |
| J9 | `min_duration` | 21 business days on `growth_inflation` only | Apply to every regime, or remove |

## 7. What the freeze means

The code in C3–C6 is written to match this file, and C8 checks
the result against `USREC` and known episodes (1974, 1980–82, 2001, 2008, 2020, 2022). If C8 shows a
definition is wrong, we change it here with a written reason and start a new family version; we don't
tune it against asset returns.

## Change log

| Date | Version | Change | Reason |
|---|---|---|---|
| 2026-10-24 | v1 | Frozen as written | Researcher approved defaults J1–J9 |
| 2026-10-24 | v1 (clarification) | Ambiguities resolved while writing C3; **no definition changed**: see below | Implementation needed a single reading |

### Clarifications made while implementing C3 (2026-10-24)

1. **Quarterly burn-in** is 20 observations (5 years); section 0 only listed monthly, weekly and daily.
2. **`FC` and `CS` require all their components** (3 and 2). The "at least 2" rule applies to the
   growth and labor composites. This keeps `FC` and `CS` one recipe across their whole history, as
   the spec intends.
3. **`growth_inflation` signs:** growth = sign of `G_impulse` (a z-composite); inflation = sign of the
   **raw** `pi_impulse` in pp, i.e. whether core YoY is rising or falling. The spec wording
   "z-scored, expanding" for the inflation side would shift the zero point to the expanding mean, which
   makes "rising" ambiguous. `pi_impulse` is not z-scored in the state vector.
4. **Staleness** is reported per dimension (`G`, `pi`, `L`, `P`, `FC`, `CS`) as days since the newest
   input release, not per output column.
5. **Release timing for unrevised series.** For core CPI NSA, SLOOS and the real-time Sahm rule
   (first print is final), a period becomes available on its **actual first-release date** where ALFRED
   recorded one, and on `period + lag` only for history ALFRED loaded in bulk. `ICNSA` and `CCSA` keep
   the fixed lag because their early prints are revised. This moved current core-CPI staleness from 21
   to 9 days; it changes when a value is usable, not what it is.
6. **Actual valid-from dates** (spec section 1 gave approximations): `G_level` and `G_impulse` 1960-04
   (two components, three from ~1965, four from ~1971); `pi_level` 1958-03; `pi_impulse` 1958-09;
   `L_level` 1960-03; `L_impulse` 1971-04; `P_stance_raw` 1958-03; `P_stance` 1963-02;
   `P_impulse` 1954-12; `FC` 1999-11; `CS` 1999-11 (spec said ~2001).


### Clarifications made while implementing C5 (2026-10-24)

No definition changed.

1. **Credit:** the frozen table says `BBB OAS − IG OAS`; the plan's C5 table said IG − BBB. Implemented as in this file.
2. **Dollar valid-from:** the spec's "from 2006" assumed the full `DTWEXBGS` history. Its ALFRED vintages start
   2019-02 and the series is revised (mean 0.96% in level), so under the E2 "drop" rule `usd_state` starts 2019-11.
   Not changed; options are `pre_vintage="latest_lagged"` (labelled exploration) or accepting the short sample.
3. **Growth pricing** follows the ≥ 2 components rule of section 0, so its recipe has 2, 3, then 4
   components over its history (~1993, 1997, 2003). The count is stored as `growth_pricing_n`.
4. **Positioning** is dated `report_date + 3` days, per this section, rather than the DB `release_date`.
5. **Volatility terciles** use `expanding_bucket_labels` on the release-dated VIX series (same function and labels
   as `VolRegime`, no DB access).
