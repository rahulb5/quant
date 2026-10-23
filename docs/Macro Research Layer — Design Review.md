# Macro Research Layer — Design Review

Oct 22, 2026 · @Rahul

The infrastructure plan is strong. From a **macro research** perspective, I would change the center of gravity: the current plan is too focused on **classifying regimes**, and not enough on **what changed, what was expected, and what markets are pricing**.

The proposed layer currently emphasizes growth/inflation regimes, financial stress, policy, curve shape, dollar trend, Sahm rule and COT positioning. I would build a continuous macro state vector first and make discrete regimes a reporting layer on top of it.

## 1. Separate state, impulse, and pricing

This is the biggest change I would make.

For each major macro dimension, track:

```latex
\underbrace{\text{State}}_{\text{where are we?}}, \qquad \underbrace{\Delta\text{State}}_{\text{where are we going?}}, \qquad \underbrace{\text{Pricing}}_{\text{what does the market expect?}}
```

For growth, for example:

- **State:** growth relative to trend / potential.
- **Impulse:** acceleration/deceleration in activity.
- **Pricing:** what rates, equities, credit, commodities are implying about growth.

Same for inflation.

Your planned growth/inflation regime appears primarily momentum-oriented. That is useful, but `growth ↑ / inflation ↓` can occur in very different environments depending on whether growth is 4% or 0%.

I would therefore make the core macro vector something like:

```latex
X_t = [\, G_{\text{level}},\ G_{\text{impulse}},\ \pi_{\text{level}},\ \pi_{\text{impulse}},\ FC,\ P_{\text{stance}},\ P_{\text{impulse}} \,]
```

and then derive the four-state regime from G\_impulse and π\_impulse.

## 2. Add an explicit market-pricing layer

This is the biggest thing currently missing.

Macro investing usually isn't about whether inflation is high. It is about whether:

```latex
\text{Macro reality} - \text{market expectations}
```

is positive or negative.

I would eventually have:

| Macro dimension | Economic data | Market pricing |
| --- | --- | --- |
| Growth | payrolls, IP, retail sales, claims | curve, credit, cyclicals/defensives |
| Inflation | CPI/PCE/wages | breakevens |
| Policy | Fed funds / real policy rate | 2Y / OIS expectations |
| Long rates | nominal growth/inflation | real yields + breakevens + term premium |
| Risk | macro/financial stress | credit spreads, VIX |
| USD | relative fundamentals | broad dollar |

This lets you study questions such as:

*Growth is deteriorating, but has the bond market already priced it?*

That is much closer to a macro research process than simply asking which regime we are in.

## 3. Change the policy regime

I would not classify policy primarily as:

easing / hold / tightening.

A central bank holding rates at 5.5% can still be exerting progressively tighter real monetary conditions if inflation collapses.

Have two variables:

```latex
\text{Policy Stance} \approx r_{\text{policy}} - \pi - r^*
```

and

```latex
\text{Policy Impulse} = \Delta r_{\text{policy}}
```

Even if r\* is noisy, you can initially use a simpler real policy rate.

So you get:

- restrictive vs accommodative **stance**
- easing vs tightening **impulse**

Much more informative.

## 4. Add real rates, breakevens and term premium explicitly

This is especially important if you're going to analyze bonds, USD, gold, growth equities and commodities.

A 100 bp increase in 10Y nominal yields has very different implications if it comes from:

```latex
\Delta y^{\text{nominal}} = \Delta y^{\text{real}} + \Delta BE + \Delta TP
```

where BE is inflation compensation and TP is term premium.

I would make this a first-class macro module rather than burying it inside "curve dynamics."

For macro research this will probably prove more useful than several of the discrete regime classifiers.

## 5. Separate macro regimes from market regimes

I would move these out of the macro-regime taxonomy:

- dollar trend
- COT positioning
- probably VIX/stress depending on implementation

COT crowding in particular is a **positioning variable**, not a macro state variable.

I'd conceptually separate:

**Macro state**

- growth
- inflation
- labor
- policy
- financial conditions

**Market state**

- yields
- curve
- credit
- USD
- volatility
- commodities

**Positioning/technical state**

- COT
- trend
- momentum
- crowding

You can then test:

```latex
E[R_{t+1} \mid M_t, P_t]
```

where M\_t is macro state and P\_t is positioning.

That is considerably more interesting than making COT another regime.

## 6. Your PIT methodology needs one important tightening

First-release data is necessary, but **"use the first release of every observation" isn't quite sufficient.**

Suppose payrolls for September are released in October. At that same release, August and July may be revised.

A true PIT calculation at October's release date should use the entire dataset that was known **as of that release date**, including the revisions that were available then.

So you want:

```latex
X_t^{(v)}
```

where v is the vintage available at the decision date, rather than stitching together each month's individual first release.

This matters especially when calculating:

- YoY changes
- rolling means
- acceleration
- gaps to trends
- diffusion indicators.

Your plan correctly treats PIT alignment as fundamental and already flags first-release/latest-vintage comparisons. I'd strengthen the specification to **full as-of vintage reconstruction.**

Also explicitly do this for COT:

Tuesday position date ≠ information availability date.

The positioning should become available when the CFTC releases the report, usually Friday.

## 7. Don't make discrete regimes carry too much analytical weight

With 50 years of data, four-state regimes sound like \~600 monthly observations. Economically, however, you may only have perhaps 5–15 truly independent episodes.

Your plan already recognizes this problem and proposes episode counts alongside n. I would take it one step further:

**Continuous variables should be the primary research object; regimes should summarize them.**

Then do:

```latex
R_{t+h} = \alpha + \beta_G G_t + \beta_\pi \pi_t + \beta_F FC_t + \varepsilon_{t+h}
```

alongside the regime tables.

For regime statistics, bootstrap **episodes**, not daily observations.

And for 3m/6m/12m forward returns, account for overlapping horizons rather than interpreting thousands of overlapping observations as independent evidence.

## 8. Demote the Sahm Rule

Useful diagnostic. I wouldn't make it one of nine equal-status regimes.

The Sahm Rule is essentially a recession-onset detector. It is:

- asymmetric,
- labor-market-specific,
- relatively late,
- rarely triggered.

I would display it as an **event/risk flag**.

For ongoing labor momentum I'd rather have something like:

- unemployment rate momentum
- initial claims
- payroll diffusion/momentum
- hours worked
- temporary employment

and derive a labor deterioration composite.

## 9. Add credit/liquidity transmission

Your "financial stress" bucket should probably be decomposed.

At minimum I'd distinguish:

```latex
\text{Financial Conditions} = f(\text{real rates}, \text{credit}, \text{equities}, \text{USD}, \text{funding})
```

from:

```latex
\text{Credit Supply}
```

The latter could eventually include lending standards / bank credit.

This allows you to distinguish:

Fed restrictive → real rates rise

from:

credit transmission deteriorates → HY spreads widen / lending standards tighten.

That distinction is important for both recession research and cross-asset positioning.

## 10. Add global/relative macro later rather than more US regimes

Your initial layer appears predominantly US-centric. That is fine for V1.

But after this, I would prioritize:

```latex
\text{US} - \text{Europe}, \qquad \text{US} - \text{Japan}, \qquad \text{US} - \text{China}
```

growth, inflation and policy differentials before building another five US classifiers.

FX and global rates are fundamentally relative-value macro assets.

A later country representation could be:

```latex
X_{c,t} = [\, G,\ \Delta G,\ \pi,\ \Delta\pi,\ r_c^{\text{real}},\ FC \,]
```

which naturally gives you relative macro signals.

## I would therefore change Part B to something closer to this

| Layer | What I would build |
| --- | --- |
| PIT data | True vintage-as-of-date datasets + publication timestamps |
| Macro state | Growth level/impulse, inflation level/impulse, labor, financial conditions |
| Rates decomposition | policy stance/impulse, real rates, breakevens, term premium, curve |
| Market state | credit, USD, equities, volatility, commodities |
| Positioning | COT and later other positioning measures |
| Continuous analysis | betas, conditional regressions, forward returns |
| Regime layer | small number of interpretable discretizations of the above |
| Pricing/divergence | macro fundamentals versus what markets appear to price |
| Global layer | relative growth/inflation/policy across economies |

Then your research loop becomes:

```latex
\boxed{\text{Macro state} \rightarrow \text{Macro impulse} \rightarrow \text{Market pricing} \rightarrow \text{Positioning} \rightarrow \text{Asset implications}}
```

rather than predominantly:

```latex
\text{Macro data} \rightarrow \text{Regime label} \rightarrow \text{Asset returns}.
```

## I would also change the roadmap priority

The architecture currently puts macro regimes first, then collector tests/yield-curve work, while the immutable data layer remains deferred.

From a research perspective I'd do:

1. **Fix PIT/vintage methodology.**
2. **Create continuous macro state + impulse variables.**
3. **Add rates/market-pricing variables.**
4. **Build a small regime layer over them.**
5. **Add episode-aware conditional analysis.**
6. **Add walk-forward/OOS validation.**
7. **Start generating macro hypotheses.**
8. Only then expand the regime catalogue.

I would also move **walk-forward testing** substantially higher. The current architecture explicitly identifies it as missing. Given how easy the system makes it to test variants, research overfitting will become a bigger problem than software reliability fairly quickly.

One architectural addition I'd make specifically for that reason is a **research-family / trial ledger**. Every variant you test—even discarded notebook versions—should belong to something like:

`growth_regime_001`

with the total number of variants recorded automatically. Your current Deflated Sharpe approach depends on manually declaring `n_trials`, which the project itself correctly identifies as a weakness. AI-assisted research makes this problem much worse because trying twenty specifications becomes nearly frictionless.

Overall, I would **not add many more indicators yet**. The major missing ingredient isn't another macro series; it is the conceptual layer connecting **economic state → change in expectations → market pricing → returns**. Once that exists, the OS starts looking like a genuine macro research platform rather than a very good systematic backtesting platform with a macro dashboard.
