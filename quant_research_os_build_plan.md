# Quant Research OS — Build Plan

## 0. Purpose

This project is a personal systematic research platform designed to make the path from:

> **Research hypothesis → data → experiment → backtest → robustness → portfolio → monitoring**

fast, reproducible, and extensible.

The goal is **not** to build a production trading platform immediately.

The initial goal is to create a research environment where one person can operate with leverage comparable to a small quantitative research team.

AI coding/research agents should remove engineering and research-plumbing overhead while the researcher remains responsible for hypotheses, economic reasoning, interpretation, and capital allocation decisions.

---

# 1. Guiding Principles

### 1.1 Refine before rebuilding

There is already an existing repository containing much of the desired functionality.

**Do not rewrite the repository from scratch.**

First:

1. Inventory the existing code.
2. Identify what already works.
3. Identify duplicated or inconsistent functionality.
4. Identify architectural gaps.
5. Refactor only where the expected benefit is clear.
6. Preserve working research wherever possible.

The first milestone is an **architecture and codebase audit**, not new infrastructure.

### 1.2 Research first, infrastructure second

Every architectural decision should support faster, more reliable research.

Avoid premature:

- microservices
- Kubernetes
- distributed computing
- elaborate cloud infrastructure
- complex agent orchestration
- proprietary databases

Start local-first.

### 1.3 Reproducibility over convenience

Every meaningful result should be reproducible from:

- data version
- code version
- configuration
- parameters
- experiment ID
- methodology
- timestamp

### 1.4 AI should amplify research judgment

AI should primarily handle:

- code implementation
- refactoring
- tests
- data plumbing
- experiment execution
- diagnostics
- documentation
- research organization

AI should **not** be treated as an oracle for profitable strategies.

The researcher owns:

- hypothesis generation
- economic intuition
- methodology
- interpretation
- deciding whether evidence is convincing
- deciding whether something deserves capital

---

# 2. Target Architecture

```text
                         RESEARCHER
                             |
                  hypothesis / judgment
                             |
                             v
                  +---------------------+
                  |   AI Research Agent |
                  | code / experiments  |
                  | diagnostics / docs  |
                  +----------+----------+
                             |
                             v
+-------------------------------------------------------------+
|                    RESEARCH PLATFORM                        |
|                                                             |
|  DATA --> FEATURES --> SIGNALS --> BACKTEST --> ROBUSTNESS |
|    |                                           |            |
|    |                                           v            |
|    |                                      RESEARCH DB      |
|    |                                           |            |
|    +-------------------------------------------+            |
|                                                |            |
|                                                v            |
|                                       PORTFOLIO / RISK      |
|                                                |            |
|                                                v            |
|                                       PAPER PORTFOLIO       |
|                                                |            |
|                                                v            |
|                                           MONITORING        |
+-------------------------------------------------------------+
```

---

# 3. Current-Repository Audit

Before adding major functionality, perform a structured audit.

## 3.1 Inventory

Document:

- current directory structure
- Python/package setup
- data sources
- data storage formats
- existing pipelines
- signal implementations
- backtesting code
- portfolio construction
- research notebooks
- tests
- dashboards
- scripts
- configuration
- external dependencies
- deployment assumptions

## 3.2 Classify existing code

Every meaningful component should be classified as:

- **KEEP** — good architecture and reliable
- **REFINE** — useful but needs cleanup
- **REFACTOR** — functionality is right but structure is poor
- **REPLACE** — fundamentally problematic
- **DELETE** — obsolete/duplicated
- **MISSING** — required but not implemented

Do not refactor simply because something is not stylistically perfect.

## 3.3 Produce an architecture document

Create:

`docs/architecture.md`

It should answer:

- Where does data enter?
- Where is raw data stored?
- How is data transformed?
- How are signals represented?
- How are backtests run?
- How are experiments identified?
- Where are results stored?
- How is portfolio construction separated from signal research?
- How are tests performed?
- How does an AI agent interact with the repository?

---

# 4. Recommended Initial Technology Stack

Keep the stack deliberately simple.

## Core

- Python
- Git / GitHub
- `uv` or existing equivalent Python environment manager
- DuckDB
- Parquet

## Research

- NumPy
- pandas and/or Polars
- SciPy
- scikit-learn
- statsmodels
- CVXPY

## Visualization

- Plotly
- Matplotlib where appropriate

## Testing

- pytest
- static/type checking as appropriate

## Dashboard

- Streamlit initially

## AI coding

Use a repo-level coding agent such as:

- Claude Code
- Codex

The architecture should remain **model-agnostic**.

---

# 5. Data Layer

The data layer should become the foundation of the system.

## 5.1 Separate raw and processed data

Example:

```text
data/
    raw/
        fred/
        cftc/
        futures/
        equities/
        fx/

    processed/
        macro/
        futures/
        equities/
        fx/
```

Raw data should generally be immutable.

Processed datasets should be reproducible from raw data plus transformation code.

## 5.2 Standardize interfaces

Research code should not directly contain provider-specific download logic.

Prefer:

```python
prices = get_prices(
    asset_class="futures",
    symbols=["CL", "GC", "ES"],
    start="2005-01-01",
    end="2026-01-01",
)
```

over embedding API calls throughout notebooks.

## 5.3 Dataset metadata

Track at minimum:

```text
dataset
source
download_timestamp
coverage_start
coverage_end
frequency
timezone
adjustment_method
transformation_version
```

Where appropriate, also track:

- contract methodology
- roll methodology
- point-in-time availability
- revision policy
- corporate-action methodology

---

# 6. Research Data Integrity

This is a critical area.

The platform must make it difficult to accidentally introduce:

- look-ahead bias
- survivorship bias
- selection bias
- future-revised macro data
- incorrect futures rolls
- timestamp misalignment
- stale prices
- unrealistic execution prices

Every backtest should explicitly define:

```text
signal timestamp
information availability timestamp
trade timestamp
return measurement window
```

The system should favor explicit lagging rather than relying on researcher memory.

---

# 7. Signal Layer

Signals should be modular.

Example:

```text
signals/
    carry.py
    momentum.py
    value.py
    macro.py
    regime.py
```

A signal should ideally expose something conceptually similar to:

```python
signal = calculate_signal(data, config)
```

Signals should not contain portfolio-specific logic.

For example:

**Signal layer:**

> "Commodity X has a carry score of 0.72."

**Portfolio layer:**

> "Given all signals, risk constraints, correlations and target volatility, allocate 3.5%."

Keep these concerns separate.

---

# 8. Backtest Engine

The backtest engine should be reusable across asset classes and strategies.

Conceptually:

```python
result = run_backtest(
    signal=signal,
    returns=returns,
    costs=cost_model,
    portfolio_config=config,
)
```

The engine should handle:

- signal timing
- execution timing
- position sizing
- transaction costs
- turnover
- leverage
- exposure constraints
- missing data
- rebalancing
- P&L attribution

Avoid putting strategy-specific logic directly into the engine.

---

# 9. Transaction Costs

Transaction costs should be explicit rather than an afterthought.

Support progressively better models:

### Version 1

Simple fixed cost assumption.

### Version 2

Asset-specific costs.

### Version 3

Costs dependent on:

- turnover
- volatility
- liquidity
- bid/ask spread
- market impact

Do not build an elaborate market-impact model until the research warrants it.

---

# 10. Experiment Registry

Every research experiment should receive a unique ID.

Example:

```text
EXP-0001
EXP-0002
EXP-0003
```

Each experiment should record:

```yaml
experiment_id:
hypothesis:
researcher:
created_at:

data:
  datasets:
  start:
  end:

signal:
  name:
  parameters:

portfolio:
  rebalance:
  constraints:

backtest:
  costs:
  execution:

validation:
  methodology:

status:
conclusion:
```

A completed experiment should produce something like:

```text
experiments/
    EXP-0001/
        config.yaml
        results.json
        performance.parquet
        diagnostics.json
        charts/
        research_note.md
```

---

# 11. Standard Research Report

Every meaningful experiment should automatically produce a research note.

Suggested structure:

```markdown
# EXP-0001 — Commodity Curve Carry

## Hypothesis

...

## Economic Rationale

...

## Data

...

## Methodology

...

## Signal Construction

...

## Portfolio Construction

...

## Results

Sharpe:
Return:
Volatility:
Max Drawdown:
Turnover:

## Out-of-Sample Results

...

## Robustness

...

## Failure Modes

...

## Economic Interpretation

...

## Conclusion

...

## Next Experiments

...
```

The AI agent can draft this.

The researcher must approve the conclusion.

---

# 12. Standard Diagnostics

Every backtest should automatically generate a common diagnostic suite.

## Performance

- CAGR / annualized return
- volatility
- Sharpe
- Sortino
- max drawdown
- Calmar
- hit rate
- skew
- kurtosis

## Trading

- turnover
- average holding period
- transaction costs
- exposure
- leverage

## Stability

- rolling Sharpe
- rolling volatility
- rolling drawdown
- calendar-year performance
- subperiod performance
- regime performance

## Statistical

Where appropriate:

- confidence intervals
- bootstrap
- t-statistics
- IC
- rank IC
- parameter sensitivity
- factor regression

---

# 13. Robustness / Adversarial Research

Build a standard "try to kill the strategy" process.

For each promising strategy, test:

### Bias

- look-ahead
- survivorship
- selection
- data leakage

### Parameter sensitivity

Does performance disappear if parameters change modestly?

### Timing

Does performance survive different:

- signal lags
- rebalance dates
- holding periods?

### Costs

Does the strategy survive:

- 1x costs
- 2x costs
- 3x costs?

### Sample stability

Does the result survive:

- different decades
- bull/bear markets
- high/low volatility
- inflation regimes
- crisis periods?

### Alternative specifications

Does the economic relationship survive reasonable changes to:

- signal construction
- universe
- normalization
- portfolio weighting?

The system should produce a standardized robustness report.

---

# 14. AI Research Agent

Once the underlying system is stable, expose tools to the AI agent.

Potential tools:

```text
search_experiments()
get_dataset()
inspect_dataset()
create_experiment()
run_backtest()
run_diagnostics()
run_robustness()
compare_experiments()
plot_performance()
create_research_note()
```

The agent should be able to operate the research environment without having unrestricted authority to silently change core infrastructure.

---

# 15. AI Agent Responsibilities

The agent should be particularly good at:

### Engineering

- implementing functions
- refactoring
- writing tests
- fixing bugs
- documentation
- dependency management
- data ingestion
- plotting

### Research execution

- translating hypotheses into experiments
- running existing pipelines
- generating diagnostics
- comparing experiments
- identifying anomalies
- documenting results

### Research criticism

Give the agent explicit instructions to search for:

- leakage
- overfitting
- data-mining
- unstable relationships
- unrealistic costs
- hidden factor exposure
- implementation errors

---

# 16. AI Agent Boundaries

The agent should **not** autonomously:

- change production methodology without approval
- delete historical results
- overwrite raw data
- modify benchmark definitions silently
- deploy live trading
- allocate real capital
- label a strategy "profitable" without qualification

Core infrastructure changes should go through review.

---

# 17. First End-to-End Project: Commodity Curve Carry

Use an existing research idea rather than inventing a new one.

Initial objective:

> Turn the existing commodity curve carry research into the first fully reproducible experiment in the platform.

Conceptual pipeline:

```text
Futures data
      ↓
Contract / curve construction
      ↓
F0 / F3 relationship
      ↓
Carry signal
      ↓
Forward returns
      ↓
Portfolio construction
      ↓
Transaction costs
      ↓
Backtest
      ↓
Robustness
      ↓
Research report
```

Do not optimize for finding the best Sharpe.

Optimize for building the reusable pipeline.

---

# 18. Phase 1 — Repository Audit

### Deliverables

- `docs/architecture.md`
- repository inventory
- list of duplicated functionality
- list of architectural gaps
- proposed refactoring plan
- current technical debt list

### Success criterion

You can explain the entire repository in one diagram.

---

# 19. Phase 2 — Research Kernel

Refine existing code into reusable components:

```text
data
features
signals
backtest
portfolio
risk
experiments
```

### Success criterion

A new signal can be added without modifying the core backtest engine.

---

# 20. Phase 3 — Experiment System

Implement:

- experiment IDs
- configurations
- result storage
- reproducible execution
- research reports
- standardized diagnostics

### Success criterion

You can reproduce an experiment from its ID alone.

---

# 21. Phase 4 — Robustness Engine

Automate:

- subperiod tests
- parameter sweeps
- cost stress
- timing sensitivity
- regime analysis
- statistical diagnostics

### Success criterion

Every serious strategy receives the same minimum robustness suite.

---

# 22. Phase 5 — AI Integration

Connect Claude Code/Codex to the repository.

Start with constrained prompts.

Example:

```text
Inspect the existing repository.

Do not modify code yet.

Understand:
1. data architecture
2. signal architecture
3. backtest architecture
4. experiment workflow
5. tests

Identify:
- duplicated functionality
- architectural inconsistencies
- missing abstractions
- likely sources of look-ahead bias
- areas where refactoring would improve research velocity

Produce a proposed implementation plan.

Do not implement it.
```

After reviewing the plan:

```text
Implement Phase 1 only.

Do not refactor unrelated code.
Do not introduce new infrastructure unless necessary.
Preserve existing research results.
Add tests for all changed behavior.

At the end:
1. summarize changes
2. list tests run
3. identify anything uncertain
4. propose the next smallest step
```

Use small, reviewable tasks rather than asking the agent to build the entire platform.

---

# 23. Phase 6 — Research Dashboard

Build a lightweight Streamlit dashboard.

Core pages:

### Experiments

List:

- ID
- hypothesis
- status
- Sharpe
- OOS Sharpe
- drawdown
- date

### Experiment Detail

Display:

- hypothesis
- methodology
- equity curve
- rolling Sharpe
- exposure
- turnover
- robustness
- research note

### Strategy Library

Group by:

- asset class
- signal type
- status
- live/paper/retired

---

# 24. Phase 7 — Portfolio Layer

Once multiple signals survive research:

```text
Signal forecasts
       ↓
Expected returns
       ↓
Risk model
       ↓
Correlation
       ↓
CVXPY optimizer
       ↓
Constraints
       ↓
Target portfolio
```

Potential constraints:

- position limits
- volatility target
- leverage
- asset-class exposure
- turnover
- concentration
- correlation/risk contribution

This is an area where the existing portfolio-construction experience can become a major strength.

---

# 25. Phase 8 — Paper Trading

Only after research and walk-forward validation:

```text
Research
   ↓
Backtest
   ↓
Walk-forward
   ↓
Paper portfolio
   ↓
Live monitoring
```

Track:

- expected vs realized return
- slippage
- turnover
- exposure
- signal drift
- portfolio volatility
- drawdown
- data failures

---

# 26. Phase 9 — Live Capital

This should be a separate project phase.

Do not connect real capital simply because a backtest looks good.

Before live deployment establish:

- operational procedures
- data monitoring
- execution assumptions
- risk limits
- kill switches
- position reconciliation
- broker/exchange controls
- logging
- failure handling

Start with very small capital.

---

# 27. Long-Term Expansion

Once the core system works, add research domains without changing the architecture.

Potential modules:

```text
Equities
FX
Rates
Commodities
Macro
Prediction Markets
Options
Alternative Data
```

The architecture should allow:

```text
new dataset
     +
new signal
     +
existing backtest
     +
existing robustness
     +
existing portfolio layer
```

without creating a new bespoke system.

---

# 28. Research Memory

A major long-term objective is to make previous research searchable.

The system should eventually answer questions such as:

> Have we tested this before?

> Which commodity carry experiments survived transaction costs?

> What happened when we conditioned carry on momentum?

> Which signals worked only before 2015?

> Which failed strategies had similar characteristics?

> Which assumptions have repeatedly caused strategies to fail?

This turns the accumulated experiments into a proprietary research knowledge base.

Do not overbuild this initially. Structured experiment metadata should come first; semantic/vector search can be added later.

---

# 29. Definition of a Successful System

The platform is successful when this workflow becomes normal:

```text
"I have a hypothesis."

        ↓

Create experiment

        ↓

Data automatically retrieved

        ↓

Signal implemented

        ↓

Backtest runs

        ↓

Standard diagnostics

        ↓

Robustness tests

        ↓

AI drafts research note

        ↓

Human reviews

        ↓

Promising?

   YES          NO
    ↓            ↓
portfolio      archive
research       result
    ↓
paper trade
```

The key metric is **research velocity × research quality**, not lines of code.

---

# 30. First 30 Days

## Week 1 — Audit

- inspect current repository
- map architecture
- identify reusable components
- identify technical debt
- document target architecture
- avoid major rewrites

## Week 2 — Kernel

- standardize data interfaces
- clean signal interfaces
- isolate backtest engine
- establish experiment configuration
- add/repair tests

## Week 3 — Commodity Carry

- migrate existing carry research
- make it reproducible
- add transaction costs
- add diagnostics
- add robustness tests

## Week 4 — AI Layer

- configure Claude Code or Codex
- add repository instructions
- expose research workflows
- test AI on small refactoring/experiment tasks
- generate automated research notes

### 30-day milestone

You should be able to say:

> "Given a new quantitative hypothesis, I can create a reproducible experiment and get a full research report in an afternoon."

That is the first real product.

---

# 31. What NOT to Build Yet

Avoid:

- live trading infrastructure
- Kubernetes
- distributed compute
- complicated agent swarms
- custom vector databases
- elaborate web applications
- expensive cloud architecture
- sophisticated execution algorithms
- ML pipelines for their own sake
- automated capital allocation

Build these only when the research system demonstrates a need.

---

# 32. 6–12 Month Vision

Eventually:

```text
                  PERSONAL RESEARCH OS

                       Researcher
                           |
                           v
                    AI Research Agent
                           |
       +-------------------+-------------------+
       |                   |                   |
       v                   v                   v
     Data              Experiments          Literature
       |                   |                   |
       +-------------------+-------------------+
                           |
                           v
                       Signals
                           |
                           v
                     Backtesting
                           |
                           v
                    Robustness Lab
                           |
                           v
                  Portfolio Construction
                           |
                           v
                    Paper Portfolio
                           |
                           v
                     Live Research
```

The end state is not "an AI trading bot."

It is a **personal quantitative research organization in software**.

The researcher supplies judgment and direction; the system supplies memory, computation, experimentation, engineering leverage, and consistency.

---

# 33. Immediate Next Action

Do **not** start implementing the entire architecture.

Instead:

1. Open the existing repository.
2. Have an AI coding agent inspect it.
3. Produce an inventory of what already exists.
4. Compare it against this plan.
5. Identify the smallest set of changes needed to create the research kernel.
6. Refine the existing commodity carry project as the first complete end-to-end experiment.
7. Only then generalize abstractions that have proven useful.

The repository should evolve from actual research needs rather than from an imagined future architecture.
