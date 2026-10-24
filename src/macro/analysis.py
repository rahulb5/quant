"""
src/macro/analysis.py

Analysis of asset returns against the macro state (C7 of docs/plans/macro_research_layer.md).
Continuous analysis comes first; regime tables summarise it. Definitions:
docs/research/macro_state_definitions_v2.md (section 5 for the pricing divergence).

Pure pandas / statsmodels: no DB access. Callers pass daily simple returns (a pd.Series on a
DatetimeIndex, e.g. trading days), state columns on a business-day grid, and regime labels
(categorical Series, NaN = unlabelled). Everything is aligned to the returns' index.

Timing. A forward return R_{t→t+h} = prod(1 + r_{t+1..t+h}) - 1 starts the day after t, so it
uses only returns after the state X_t (known at the close of t) was observed. h counts rows of
the returns index (trading days).

Sample size. Regime statistics have as many independent observations as there are episodes
(maximal runs of one label), not days: every table reports `n_episodes`, and the episode
bootstrap resamples whole episodes.

    forward_regression(state, returns)             R_{t→t+h} on X_t, HAC lag >= h + non-overlapping check
    performance_by_regime(labels, returns)         Conditioner.by_regime() table (incl. episode columns)
    episode_bootstrap_ci(labels, returns)          CI for mean return and Sharpe, resampling episodes
    forward_returns_by_regime(labels, returns)     forward return after entering each regime
    divergence(macro_z, market_z)                  gap, its expanding z, and gap terciles
    pricing_divergence(state, market)              the three section-5 dimensions
    divergence_forward_returns(gap_z, returns)     forward returns by gap tercile
    episodes / transition_matrix / regime_durations / current_regime
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import statsmodels.api as sm

from src.backtest.conditioning import REGIME_PERF_COLS, regime_performance
from src.backtest.metrics import sharpe_ratio
from src.backtest.regimes import expanding_bucket_labels
from src.macro.state import BURN_IN, expanding_z
from src.shared.utils import logger

HORIZONS = (21, 63, 126, 252)
ENTRY_HORIZONS = (21, 63)
MIN_OBS = 30                 # fewer aligned observations than this -> NaN statistics
MIN_EPISODES = 5             # below this the bootstrap and tables warn
GAP_LABELS = ["market_ahead", "in_line", "data_ahead"]   # gap terciles: low / mid / high
MACRO_RAW = {"pi_impulse", "P_impulse"}                   # state columns in pp, not z-scores
STATE_VECTOR = ["G_level", "G_impulse", "pi_level", "pi_impulse", "L_level", "L_impulse",
                "P_stance", "P_impulse", "FC", "FC_impulse", "CS", "CS_impulse"]   # macro_state() outputs
DIVERGENCE_DIMENSIONS = {    # dimension -> (macro_state column, market_state column)
    "growth": ("G_impulse", "growth_pricing"),
    "inflation": ("pi_impulse", "inflation_pricing"),
    "policy": ("P_impulse", "policy_pricing"),
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def forward_returns(returns: pd.Series, h: int) -> pd.Series:
    """R_{t→t+h} = prod(1 + r_{t+1..t+h}) - 1 on the returns index; NaN for the last h rows
    or where any of those returns is NaN."""
    log_r = np.log1p(returns.astype(float))
    fwd = log_r.rolling(h, min_periods=h).sum().shift(-h)
    return np.expm1(fwd).rename(f"fwd_{h}")


def _is_missing(v) -> bool:
    return v is None or (isinstance(v, float) and np.isnan(v))


def _hac_lag(h: int, hac_lag: int | None) -> int:
    if hac_lag is None:
        return h
    if hac_lag < h:
        raise ValueError(f"hac_lag={hac_lag} < horizon {h}: overlapping {h}-day returns need a lag >= h")
    return int(hac_lag)


def episodes(labels: pd.Series) -> pd.DataFrame:
    """Maximal runs of one label. A NaN ends a run (and is not an episode).

    Returns a DataFrame with columns label, start, end, length (rows of `labels`), pos
    (position of the first row), one row per episode in time order.
    """
    rows = []
    vals = labels.to_numpy(dtype=object)
    idx = labels.index
    cur, start = None, 0
    for i, v in enumerate(list(vals) + [None]):
        if cur is not None and (_is_missing(v) or v != cur):
            rows.append({"label": cur, "start": idx[start], "end": idx[i - 1],
                         "length": i - start, "pos": start})
            cur = None
        if not _is_missing(v) and cur is None:
            cur, start = v, i
    return pd.DataFrame(rows, columns=["label", "start", "end", "length", "pos"])


def _align_labels(labels: pd.Series, returns: pd.Series) -> pd.Series:
    return labels.reindex(returns.index)


# ── 1. Forward-return regressions ─────────────────────────────────────────────

def forward_regression(
    state: pd.DataFrame | pd.Series,
    returns: pd.Series,
    horizons: tuple[int, ...] = HORIZONS,
    hac_lag: int | None = None,
) -> pd.DataFrame:
    """R_{t→t+h} = α + β·X_t + ε on the daily overlapping sample, one regression per horizon.

    All columns of `state` enter together (pass one column for a univariate fit). Standard errors
    are Newey-West HAC with lag L = h by default; `hac_lag` overrides it but must be >= every h
    (ValueError otherwise), because overlapping h-day returns are autocorrelated to lag h - 1.
    As a cross-check the same regression is run with plain OLS errors on the non-overlapping
    sample (every h-th aligned row, starting from the first).

    Args:
        state: State variables X_t indexed by date (reindexed to the returns' index).
        returns: Daily simple returns.
        horizons: Forward horizons in rows of the returns index.
        hac_lag: Optional HAC lag, >= max(horizons).

    Returns:
        DataFrame with a (horizon, term) MultiIndex, term in ["const", *state columns], columns
        coef, t_stat_hac, n_obs, hac_lag, r_squared, coef_nonoverlap, t_stat_nonoverlap,
        n_nonoverlap. Statistics are NaN where fewer than MIN_OBS rows align.
    """
    X = state.to_frame() if isinstance(state, pd.Series) else state
    X = X.reindex(returns.index)
    terms = ["const", *X.columns]
    out = []
    for h in horizons:
        L = _hac_lag(h, hac_lag)
        data = pd.concat([forward_returns(returns, h).rename("_y"), X], axis=1).dropna()
        row = {t: dict(coef=np.nan, t_stat_hac=np.nan, n_obs=len(data), hac_lag=L, r_squared=np.nan,
                       coef_nonoverlap=np.nan, t_stat_nonoverlap=np.nan, n_nonoverlap=np.nan)
               for t in terms}
        if len(data) >= MIN_OBS:
            y = data["_y"].to_numpy()
            Xc = sm.add_constant(data[X.columns].to_numpy(), has_constant="add")
            fit = sm.OLS(y, Xc).fit(cov_type="HAC", cov_kwds={"maxlags": L})
            non = data.iloc[::h]
            fit_n = None
            if len(non) >= max(MIN_OBS // 3, len(terms) + 2):
                Xn = sm.add_constant(non[X.columns].to_numpy(), has_constant="add")
                fit_n = sm.OLS(non["_y"].to_numpy(), Xn).fit()
            for k, t in enumerate(terms):
                row[t].update(coef=fit.params[k], t_stat_hac=fit.tvalues[k], r_squared=fit.rsquared,
                              n_nonoverlap=len(non))
                if fit_n is not None:
                    row[t].update(coef_nonoverlap=fit_n.params[k], t_stat_nonoverlap=fit_n.tvalues[k])
        else:
            logger.warning(f"[forward_regression] h={h}: only {len(data)} aligned rows (need {MIN_OBS})")
        out += [((h, t), row[t]) for t in terms]
    frame = pd.DataFrame([r for _, r in out], index=pd.MultiIndex.from_tuples([k for k, _ in out],
                                                                              names=["horizon", "term"]))
    return frame


# ── 2. Regime tables ──────────────────────────────────────────────────────────

def performance_by_regime(
    labels: pd.Series,
    returns: pd.Series,
    rf: float | pd.Series = 0.0,
    periods_per_year: int = 252,
) -> pd.DataFrame:
    """Conditioner.by_regime() columns (the shared `regime_performance` helper, including
    n_episodes and mean_episode_length).

    Labels are reindexed to the returns' index; episodes are counted on that aligned series (a
    NaN ends an episode) and their length is in rows of the returns index.

    Returns:
        DataFrame indexed by label (order of first appearance), columns REGIME_PERF_COLS.
        Empty if there are no labels in the window.
    """
    aligned = _align_labels(labels, returns)
    table = regime_performance(aligned, returns, rf, periods_per_year)
    if table.empty:
        return table
    few = table.index[table["n_episodes"] < MIN_EPISODES].tolist()
    if few:
        logger.warning(f"[performance_by_regime] fewer than {MIN_EPISODES} episodes for {few}")
    return table


# ── 3. Episode bootstrap ──────────────────────────────────────────────────────

def episode_bootstrap_ci(
    labels: pd.Series,
    returns: pd.Series,
    stat: tuple[str, ...] = ("mean", "sharpe"),
    n_boot: int = 2000,
    ci: float = 0.95,
    periods_per_year: int = 252,
    seed: int = 0,
) -> pd.DataFrame:
    """Bootstrap CIs for each label's annualised mean return and Sharpe, resampling whole episodes.

    For each label with k episodes, each draw takes k of its episodes with replacement and pools
    their daily returns. Days within an episode stay together, so the CI reflects how few
    independent regime spells there are. Sharpe uses rf = 0. Warns when n_episodes < MIN_EPISODES.

    Args:
        stat: Any of "mean" (annualised mean return) and "sharpe".
        ci: Two-sided coverage of the percentile interval.

    Returns:
        DataFrame indexed by label, columns n_episodes, n_obs, few_episodes, then for each stat:
        <stat>, <stat>_lo, <stat>_hi (point estimate on the actual sample, percentile CI).
    """
    unknown = set(stat) - {"mean", "sharpe"}
    if unknown:
        raise ValueError(f"unknown stat {sorted(unknown)}; use 'mean' and/or 'sharpe'")
    aligned = _align_labels(labels, returns)
    ep = episodes(aligned)
    r = returns.to_numpy(dtype=float)
    rng = np.random.default_rng(seed)
    lo_q, hi_q = (1 - ci) / 2, 1 - (1 - ci) / 2

    def compute(x: np.ndarray) -> dict[str, float]:
        x = x[~np.isnan(x)]
        res = {}
        if "mean" in stat:
            res["mean"] = float(np.mean(x) * periods_per_year) if len(x) else np.nan
        if "sharpe" in stat:
            res["sharpe"] = sharpe_ratio(x, 0.0, periods_per_year) if len(x) >= 2 else np.nan
        return res

    rows = {}
    for lbl in dict.fromkeys(ep["label"]):
        chunks = [r[p:p + n] for p, n in ep.loc[ep["label"] == lbl, ["pos", "length"]].to_numpy()]
        k = len(chunks)
        row = {"n_episodes": k, "n_obs": int(sum(np.isfinite(c).sum() for c in chunks)),
               "few_episodes": k < MIN_EPISODES}
        point = compute(np.concatenate(chunks))
        draws = {s: np.empty(n_boot) for s in stat}
        for b in range(n_boot):
            pick = rng.integers(0, k, size=k)
            res = compute(np.concatenate([chunks[i] for i in pick]))
            for s in stat:
                draws[s][b] = res[s]
        for s in stat:
            row[s] = point[s]
            row[f"{s}_lo"] = float(np.nanquantile(draws[s], lo_q))
            row[f"{s}_hi"] = float(np.nanquantile(draws[s], hi_q))
        rows[lbl] = row
    cols = ["n_episodes", "n_obs", "few_episodes"] + [c for s in stat for c in (s, f"{s}_lo", f"{s}_hi")]
    table = pd.DataFrame.from_dict(rows, orient="index", columns=cols) if rows else pd.DataFrame(columns=cols)
    few = table.index[table["few_episodes"].astype(bool)].tolist()
    if few:
        logger.warning(f"[episode_bootstrap_ci] fewer than {MIN_EPISODES} episodes for {few}: CI is unreliable")
    return table


# ── 4. Forward returns by regime entry ────────────────────────────────────────

def _overlap_se(pos: np.ndarray, h: int, sigma2: float) -> float:
    """Standard error of the mean of h-day forward returns starting at rows `pos`, under i.i.d.
    daily returns with variance sigma2: Cov(R_i, R_j) = sigma2 * (days the two windows share)
    = sigma2 * max(0, h - |pos_i - pos_j|). Non-overlapping entries give sqrt(sigma2 * h / n);
    entries closer than h days apart share days and widen it (Hansen-Hodrick, lag h - 1)."""
    n = len(pos)
    if n == 0 or not np.isfinite(sigma2):
        return np.nan
    shared = np.clip(h - np.abs(pos[:, None] - pos[None, :]), 0, None)
    return float(np.sqrt(sigma2 * shared.sum()) / n)


def forward_returns_by_regime(
    labels: pd.Series,
    returns: pd.Series,
    horizons: tuple[int, ...] = ENTRY_HORIZONS,
) -> pd.DataFrame:
    """Average forward return after entering each regime ("what happens next").

    An entry is a switch: the first row of an episode whose previous aligned row carries a
    different, non-NaN label (labels already carry the causal min_duration delay). The first
    labelled row of the sample and the first row after a NaN gap are not entries: nothing switched
    there. The forward return is R_{t→t+h} from
    the entry date t. Standard errors allow for overlap at lag h: two entries closer than h days
    apart share daily returns, and the covariance counts the shared days (`_overlap_se`), with the
    daily variance taken from the returns inside that label's forward windows. A Bartlett HAC on
    the entry series is not used: with all entries inside one window its weights are all ~1 and
    the variance of the demeaned mean collapses to 0.

    `uncond_fwd` is the full-sample mean forward return: an ex-post benchmark for describing the
    sample, never an input to a signal or threshold. `t_stat_excess` treats it as known (its own
    sampling error is ignored).

    Returns:
        DataFrame with a (horizon, label) MultiIndex, columns n_entries (switches into the label
        with a full forward window), n_episodes, mean_fwd, uncond_fwd, excess (mean_fwd -
        uncond_fwd), se, t_stat (mean_fwd / se), t_stat_excess (excess / se), overlap_days (= h).
    """
    aligned = _align_labels(labels, returns)
    ep = episodes(aligned)
    n_ep = ep.groupby("label").size()
    prev = ep["pos"] - 1
    prev_lbl = pd.Series(aligned.to_numpy(dtype=object)[prev.clip(lower=0)], index=ep.index)
    is_switch = (prev >= 0) & prev_lbl.map(lambda v: not _is_missing(v)) & (prev_lbl != ep["label"])
    entries = ep[is_switch]
    r = returns.to_numpy(dtype=float)
    rows, keys = [], []
    for h in horizons:
        fwd = forward_returns(returns, h).to_numpy()
        uncond = float(np.nanmean(fwd)) if np.isfinite(fwd).any() else np.nan
        for lbl in dict.fromkeys(ep["label"]):
            pos = entries.loc[entries["label"] == lbl, "pos"].to_numpy()
            pos = pos[np.isfinite(fwd[pos])]
            y = fwd[pos]
            window = np.unique((pos[:, None] + np.arange(1, h + 1)[None, :]).ravel()) if len(pos) else pos
            daily = r[window] if len(window) else np.array([])
            sigma2 = float(np.var(daily, ddof=1)) if len(daily) >= 2 else np.nan
            se = _overlap_se(pos.astype(float), h, sigma2)
            mean = float(y.mean()) if len(y) else np.nan
            ok = np.isfinite(se) and se > 0
            keys.append((h, lbl))
            rows.append({"n_entries": len(y), "n_episodes": int(n_ep[lbl]), "mean_fwd": mean,
                         "uncond_fwd": uncond, "excess": mean - uncond, "se": se,
                         "t_stat": mean / se if ok else np.nan,
                         "t_stat_excess": (mean - uncond) / se if ok else np.nan, "overlap_days": h})
    cols = ["n_entries", "n_episodes", "mean_fwd", "uncond_fwd", "excess", "se", "t_stat",
            "t_stat_excess", "overlap_days"]
    if not rows:
        return pd.DataFrame(columns=cols)
    return pd.DataFrame(rows, index=pd.MultiIndex.from_tuples(keys, names=["horizon", "label"]))[cols]


# ── 5. Pricing divergence ─────────────────────────────────────────────────────

def divergence(macro_z: pd.Series, market_z: pd.Series, burn_in: int = BURN_IN["daily"]) -> pd.DataFrame:
    """gap = macro_z - market_z (definitions section 5), with its expanding z-score and terciles.

    Both inputs must already be expanding z-scores on the same daily grid. Positive gap = the data
    is stronger than the market is pricing.

    Returns:
        DataFrame on the union index with columns macro_z, market_z, gap, gap_z (expanding z of gap,
        burn-in `burn_in`, clipped ±3) and gap_tercile (expanding terciles of gap, burn-in
        `burn_in`, labels GAP_LABELS: market_ahead / in_line / data_ahead). Both are valid from
        `burn_in` rows after the first date where both inputs exist.
    """
    frame = pd.concat([macro_z.rename("macro_z"), market_z.rename("market_z")], axis=1, sort=True)
    frame["gap"] = frame["macro_z"] - frame["market_z"]
    gap_z = expanding_z(frame["gap"], burn_in)
    frame["gap_z"] = gap_z.reindex(frame.index)
    gap = frame["gap"].dropna()
    frame["gap_tercile"] = (expanding_bucket_labels(gap, 3, burn_in=burn_in, labels=GAP_LABELS)
                            .reindex(frame.index) if len(gap) >= burn_in else np.nan)
    return frame


def pricing_divergence(
    state: pd.DataFrame,
    market: pd.DataFrame,
    burn_in: int = BURN_IN["daily"],
) -> dict[str, pd.DataFrame]:
    """The three section-5 dimensions (DIVERGENCE_DIMENSIONS): growth (G_impulse vs growth
    pricing), inflation (pi_impulse vs 63-day change in T10YIE), policy (P_impulse vs 63-day change
    in DGS2 - DFF).

    `state` is macro_state() on a business-day grid, `market` is market_state() on the same grid.
    Growth: both sides are already means of expanding z-scores (G_impulse and growth_pricing) and are
    used as they are. Inflation and policy: the market side is a daily expanding z-score (burn-in
    756); pi_impulse and P_impulse are in pp in the state vector (MACRO_RAW), so they are z-scored
    here the same way (daily, expanding, burn-in `burn_in`, clip ±3).

    Returns:
        Dict dimension -> divergence() frame. A dimension whose columns are missing is skipped.
    """
    out = {}
    for dim, (mcol, kcol) in DIVERGENCE_DIMENSIONS.items():
        if mcol not in state or kcol not in market:
            logger.warning(f"[pricing_divergence] skipping {dim}: missing {mcol} or {kcol}")
            continue
        mz = expanding_z(state[mcol], burn_in) if mcol in MACRO_RAW else state[mcol].dropna()
        out[dim] = divergence(mz, market[kcol].dropna(), burn_in)
    return out


def divergence_forward_returns(
    gap: pd.DataFrame | pd.Series,
    returns: pd.Series,
    horizons: tuple[int, ...] = ENTRY_HORIZONS,
    hac_lag: int | None = None,
) -> pd.DataFrame:
    """Forward returns of an asset by gap tercile, on the daily overlapping sample.

    R_{t→t+h} is regressed on tercile dummies (no constant) with Newey-West HAC lag h (or
    `hac_lag` >= h), so each coefficient is that tercile's mean forward return with a robust
    t-stat; the high-minus-low spread (data_ahead - market_ahead) gets its own HAC t-stat.

    Args:
        gap: divergence() output (its gap_tercile column is used) or a tercile label Series.

    Returns:
        DataFrame with a (horizon, tercile) MultiIndex, tercile in GAP_LABELS + ["spread"],
        columns mean_fwd, t_stat_hac, n_obs, n_episodes, hac_lag.
    """
    terc = gap["gap_tercile"] if isinstance(gap, pd.DataFrame) else gap
    terc = _align_labels(terc, returns)
    rows, keys = [], []
    for h in horizons:
        L = _hac_lag(h, hac_lag)
        fwd = forward_returns(returns, h)
        n_ep = episodes(terc.where(fwd.notna())).groupby("label").size()
        data = pd.concat([fwd.rename("_y"), terc.rename("_t")], axis=1).dropna()
        present = [g for g in GAP_LABELS if (data["_t"] == g).any()]
        res = {g: dict(mean_fwd=np.nan, t_stat_hac=np.nan, n_obs=int((data["_t"] == g).sum()),
                       n_episodes=int(n_ep.get(g, 0)), hac_lag=L) for g in GAP_LABELS}
        res["spread"] = dict(mean_fwd=np.nan, t_stat_hac=np.nan, n_obs=len(data), n_episodes=np.nan, hac_lag=L)
        if len(data) >= MIN_OBS and present:
            D = pd.get_dummies(data["_t"])[present].astype(float).to_numpy()
            fit = sm.OLS(data["_y"].to_numpy(), D).fit(cov_type="HAC", cov_kwds={"maxlags": L})
            for k, g in enumerate(present):
                res[g].update(mean_fwd=fit.params[k], t_stat_hac=fit.tvalues[k])
            if "data_ahead" in present and "market_ahead" in present:
                c = np.zeros(len(present))
                c[present.index("data_ahead")], c[present.index("market_ahead")] = 1, -1
                tt = fit.t_test(c)
                res["spread"].update(mean_fwd=float(np.squeeze(tt.effect)), t_stat_hac=float(np.squeeze(tt.tvalue)))
        for g in GAP_LABELS + ["spread"]:
            keys.append((h, g))
            rows.append(res[g])
    return pd.DataFrame(rows, index=pd.MultiIndex.from_tuples(keys, names=["horizon", "tercile"]))


# ── 6. Transitions, durations, current regime ─────────────────────────────────

def transition_matrix(labels: pd.Series, normalize: bool = True) -> pd.DataFrame:
    """Episode-to-episode transitions: row = label of an episode, column = label of the next one.

    Only consecutive episodes with no NaN between them count (a gap in the labels is not a
    transition). Diagonal entries are 0 by construction (consecutive episodes differ).
    `normalize=True` gives row probabilities, otherwise counts.
    """
    ep = episodes(labels)
    order = list(dict.fromkeys(ep["label"]))
    m = pd.DataFrame(0.0, index=order, columns=order)
    for a, b in zip(ep.itertuples(), ep.iloc[1:].itertuples()):
        if b.pos == a.pos + a.length:
            m.loc[a.label, b.label] += 1
    if normalize:
        m = m.div(m.sum(axis=1).replace(0, np.nan), axis=0)
    m.index.name, m.columns.name = "from", "to"
    return m


def regime_durations(labels: pd.Series) -> pd.DataFrame:
    """Episode length statistics per label, in rows of `labels` (business days for regime labels).

    The last episode is still running, so its length is a lower bound; it is counted, and
    `last_censored` marks the label it belongs to.

    Returns:
        DataFrame indexed by label: n_episodes, mean, median, min, max, pct_time, last_censored.
    """
    ep = episodes(labels)
    cols = ["n_episodes", "mean", "median", "min", "max", "pct_time", "last_censored"]
    if ep.empty:
        return pd.DataFrame(columns=cols)
    g = ep.groupby("label", sort=False)["length"]
    out = pd.DataFrame({"n_episodes": g.size(), "mean": g.mean(), "median": g.median(),
                        "min": g.min(), "max": g.max()})
    out["pct_time"] = g.sum() / ep["length"].sum()
    last = ep.iloc[-1]
    out["last_censored"] = (out.index == last["label"]) & (last["pos"] + last["length"] == len(labels))
    return out[cols]


def current_regime(labels: pd.Series) -> dict:
    """The label on the last row, the date its episode started and its length so far (rows).
    {'label': None, ...} if the last row is NaN or the series is empty."""
    ep = episodes(labels)
    if ep.empty or _is_missing(labels.iloc[-1]):
        return {"label": None, "since": None, "days_in_regime": 0}
    last = ep.iloc[-1]
    return {"label": last["label"], "since": last["start"], "days_in_regime": int(last["length"])}
