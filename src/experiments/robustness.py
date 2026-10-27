"""
src/experiments/robustness.py

The standard "try to kill the strategy" suite (build plan Sections 13 / Phase 4).
run_robustness() takes an experiment's config.yaml, rebuilds its weights and
returns once, then runs eight checks and writes robustness.json plus charts
next to the config, updates the research note's Robustness section, and
records headline numbers on the registry row.

Checks (each returns PASS / FLAG / KILL / SKIPPED; thresholds live in
config.robustness): cost_stress, lag_sensitivity, outliers, subperiods,
parameters, walk_forward, statistical, holdout. parameters and walk_forward
share one backtest per param_grid point (grid_net_returns).

Verdicts are mechanical. The overall result is KILLED (any check KILL) or
NEEDS_REVIEW; this module never emits "approved" -- that is the researcher's call.

Holdout: runs and sweeps never see data on/after validation.holdout_start. The
holdout is scored only when run_robustness(unlock_holdout=True) is called, once;
the result is frozen in holdout.json and never recomputed.

Caller manages db.open()/db.close().
"""

from __future__ import annotations

import copy
import itertools
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from src.backtest import stats
from src.backtest.execution import apply_costs
from src.backtest.metrics import sharpe_ratio
from src.experiments import runner, trials
from src.experiments.config import ExperimentConfig, RobustnessConfig, load_config
from src.experiments.registry import update_experiment
from src.experiments.report import render_robustness_section, replace_robustness_section
from src.shared.utils import logger

PASS, FLAG, KILL, SKIPPED = "PASS", "FLAG", "KILL", "SKIPPED"


@dataclass
class CheckResult:
    verdict: str
    reason: str
    detail: dict[str, Any]


@dataclass
class RobustnessResult:
    config: ExperimentConfig
    results: dict[str, Any]
    experiment_dir: Path


# ── Helpers ───────────────────────────────────────────────────────────────────

def _sharpe(returns, ppy: int) -> float | None:
    arr = np.asarray(returns, dtype=float)
    value = sharpe_ratio(arr, periods_per_year=ppy)
    return None if value is None or np.isnan(value) else float(value)


def _f(value: float | None, spec: str = ".2f") -> str:
    return "n/a" if value is None else format(value, spec)


def _with_overrides(config: ExperimentConfig, overrides: dict[str, Any]) -> ExperimentConfig:
    """Deep-copy config and apply dotted-path overrides, e.g. {"portfolio.threshold": 85}."""
    cfg = copy.deepcopy(config)
    for path, value in overrides.items():
        *parents, leaf = path.split(".")
        obj: Any = cfg
        for part in parents:
            obj = obj[part] if isinstance(obj, dict) else getattr(obj, part)
        if isinstance(obj, dict):
            obj[leaf] = value
        elif hasattr(obj, leaf):
            setattr(obj, leaf, value)
        else:
            raise AttributeError(f"Unknown config path in param_grid: {path}")
    return cfg


# ── Checks ────────────────────────────────────────────────────────────────────

def check_cost_stress(
    gross: pd.Series, turnover: pd.Series, base_cost_bps: float, ppy: int, rc: RobustnessConfig
) -> CheckResult:
    def net_sharpe(mult: float) -> float | None:
        return _sharpe(gross - mult * base_cost_bps / 1e4 * turnover, ppy)

    mean_turnover = float(turnover.mean())
    breakeven = float(gross.mean()) / mean_turnover * 1e4 if mean_turnover > 0 else None
    detail: dict[str, Any] = {
        "base_cost_bps": base_cost_bps,
        "breakeven_bps": breakeven,
        "sharpe_by_multiplier": {str(m): net_sharpe(m) for m in rc.cost_multipliers},
        "sharpe_at_kill_multiplier": net_sharpe(rc.cost_kill_multiplier),
        "sharpe_at_flag_multiplier": net_sharpe(rc.cost_flag_multiplier),
    }

    if base_cost_bps <= 0:
        return CheckResult(SKIPPED, "base cost_bps is 0, so cost multipliers are undefined", detail)

    kill_s, flag_s = detail["sharpe_at_kill_multiplier"], detail["sharpe_at_flag_multiplier"]
    if kill_s is not None and kill_s < 0:
        return CheckResult(
            KILL, f"net Sharpe {kill_s:.2f} < 0 at {rc.cost_kill_multiplier}x costs", detail
        )
    problems = []
    if flag_s is not None and flag_s < 0:
        problems.append(f"net Sharpe {flag_s:.2f} < 0 at {rc.cost_flag_multiplier}x costs")
    if breakeven is not None and breakeven < rc.breakeven_flag_bps:
        problems.append(f"breakeven cost {breakeven:.1f} bps < {rc.breakeven_flag_bps} bps")
    if problems:
        return CheckResult(FLAG, "; ".join(problems), detail)
    return CheckResult(PASS, f"breakeven cost {_f(breakeven, '.1f')} bps; survives {rc.cost_flag_multiplier}x costs", detail)


def check_lag_sensitivity(
    weights: pd.DataFrame, asset_returns: pd.DataFrame, base_lag: int, cost_bps: float,
    ppy: int, rc: RobustnessConfig,
) -> CheckResult:
    by_lag = {
        base_lag + off: _sharpe(apply_costs(weights, asset_returns, cost_bps, base_lag + off).net_returns, ppy)
        for off in sorted(set([0, *rc.lag_offsets]))
    }
    detail: dict[str, Any] = {"sharpe_by_lag": {str(k): v for k, v in by_lag.items()}}

    base = by_lag[base_lag]
    positive_offsets = [o for o in rc.lag_offsets if o > 0]
    if base is None or base <= 0:
        return CheckResult(SKIPPED, "base Sharpe <= 0, retention undefined", detail)
    if not positive_offsets:
        return CheckResult(SKIPPED, "no positive lag_offsets configured", detail)

    shifted = by_lag[base_lag + min(positive_offsets)]
    retention = None if shifted is None else shifted / base
    detail["retention"] = retention
    if retention is None or retention < rc.lag_kill_retention:
        return CheckResult(KILL, f"Sharpe retains {_f(retention, '.0%')} at lag+{min(positive_offsets)} (< {rc.lag_kill_retention:.0%})", detail)
    if retention < rc.lag_flag_retention:
        return CheckResult(FLAG, f"Sharpe retains {retention:.0%} at lag+{min(positive_offsets)} (< {rc.lag_flag_retention:.0%})", detail)
    return CheckResult(PASS, f"Sharpe retains {retention:.0%} at lag+{min(positive_offsets)}", detail)


def check_outliers(net: pd.Series, ppy: int, rc: RobustnessConfig) -> CheckResult:
    base = _sharpe(net, ppy)
    tails = stats.sharpe_excluding_tails(net, rc.outlier_trim_frac, ppy)
    top = stats.sharpe_excluding_top(net, rc.outlier_trim_frac, ppy)
    ex_year, year = stats.sharpe_excluding_best_year(net, ppy)
    shift = stats.gaussian_top_trim_shift(rc.outlier_trim_frac, ppy)
    detail = {
        "base_sharpe": base,
        "trim_frac": rc.outlier_trim_frac,
        "sharpe_excluding_tails": None if np.isnan(tails) else tails,
        "sharpe_excluding_top": None if np.isnan(top) else top,
        "sharpe_excluding_top_if_gaussian": None if base is None else base - shift,
        "sharpe_excluding_best_year": None if np.isnan(ex_year) else ex_year,
        "best_year": year,
    }
    value = detail["sharpe_excluding_tails"]
    if value is not None and value < 0:
        return CheckResult(
            FLAG,
            f"Sharpe {value:.2f} < 0 after dropping the best and worst {rc.outlier_trim_frac:.0%} of periods",
            detail,
        )
    return CheckResult(
        PASS,
        f"Sharpe {_f(value)} after dropping the best and worst {rc.outlier_trim_frac:.0%} of periods",
        detail,
    )


def check_subperiods(net: pd.Series, ppy: int, rc: RobustnessConfig) -> CheckResult:
    mid = len(net) // 2
    halves = {"first_half": _sharpe(net.iloc[:mid], ppy), "second_half": _sharpe(net.iloc[mid:], ppy)}

    min_obs = max(ppy // 4, 2)
    yearly = ((1 + net).groupby(net.index.year).prod() - 1)[net.groupby(net.index.year).size() >= min_obs]

    decades = {}
    for decade, grp in net.groupby(net.index.year // 10 * 10):
        if len(grp) >= max(ppy // 2, 10):
            decades[f"{decade}s"] = _sharpe(grp, ppy)

    crisis = {}
    for name, (start, end) in rc.crisis_windows.items():
        window = net.loc[start:end]
        if len(window) >= 5:
            crisis[name] = {"n": len(window), "total_return": float((1 + window).prod() - 1)}

    pos_frac = float((yearly > 0).mean()) if len(yearly) else None
    detail = {
        "halves": halves,
        "decades": decades,
        "yearly_returns": {str(k): float(v) for k, v in yearly.items()},
        "positive_year_fraction": pos_frac,
        "crisis_windows": crisis,
    }

    problems = []
    if pos_frac is not None and pos_frac < rc.positive_year_flag_frac:
        problems.append(f"only {pos_frac:.0%} of years positive (< {rc.positive_year_flag_frac:.0%})")
    for label, value in halves.items():
        if value is not None and value < rc.half_sharpe_flag:
            problems.append(f"{label} Sharpe {value:.2f} < {rc.half_sharpe_flag}")
    if problems:
        return CheckResult(FLAG, "; ".join(problems), detail)
    return CheckResult(PASS, f"{_f(pos_frac, '.0%')} of years positive; both halves above {rc.half_sharpe_flag}", detail)


def check_statistical(net: pd.Series, ppy: int, n_trials: int, rc: RobustnessConfig) -> CheckResult:
    low, high = stats.block_bootstrap_sharpe_ci(net, ppy, n_boot=rc.n_boot, seed=rc.seed)
    dsr = stats.deflated_sharpe_ratio(net, n_trials=n_trials)
    detail = {
        "sharpe": _sharpe(net, ppy),
        "ci_low": None if np.isnan(low) else low,
        "ci_high": None if np.isnan(high) else high,
        "deflated_sharpe": None if np.isnan(dsr) else dsr,
        "n_trials": n_trials,
    }
    ci_note = "CI includes 0" if detail["ci_low"] is not None and detail["ci_low"] < 0 else "CI excludes 0"
    if detail["deflated_sharpe"] is None:
        return CheckResult(SKIPPED, "deflated Sharpe undefined (too few observations or zero volatility)", detail)
    if detail["deflated_sharpe"] < rc.deflated_sharpe_flag:
        return CheckResult(FLAG, f"deflated Sharpe {detail['deflated_sharpe']:.2f} < {rc.deflated_sharpe_flag} at n_trials={n_trials}; {ci_note}", detail)
    return CheckResult(PASS, f"deflated Sharpe {detail['deflated_sharpe']:.2f} at n_trials={n_trials}; {ci_note}", detail)


def grid_net_returns(
    config: ExperimentConfig, signal_data: tuple, asset_returns: pd.DataFrame
) -> tuple[list[tuple[dict[str, Any], pd.Series]], list[dict[str, Any]]]:
    """Net returns of every param_grid point (holdout excluded), in grid order.

    Returns ([(params, net returns)], [{"params", "error"}] for points that failed).
    Shared by check_parameters and check_walk_forward so each point is backtested once.
    """
    grid = config.robustness.param_grid
    keys = list(grid)
    nets, failures = [], []
    for values in itertools.product(*(grid[k] for k in keys)):
        params = dict(zip(keys, values))
        try:
            cfg = _with_overrides(config, params)
            weights = runner._build_weights(cfg, runner._compute_signal(cfg, signal_data))
            weights, returns = runner._apply_holdout(weights, asset_returns, config.validation.holdout_start)
            nets.append((params, apply_costs(weights, returns, cfg.backtest.cost_bps, cfg.backtest.lag).net_returns))
        except Exception as exc:
            logger.warning(f"[robustness] param combo {params} failed: {exc}")
            failures.append({"params": params, "error": str(exc)})
    return nets, failures


def check_parameters(
    config: ExperimentConfig, signal_data: tuple, asset_returns: pd.DataFrame,
    grid_nets: tuple[list, list] | None = None,
) -> CheckResult:
    rc = config.robustness
    grid = rc.param_grid
    if not grid:
        return CheckResult(SKIPPED, "no param_grid configured", {})

    keys = list(grid)
    nets, failures = grid_nets if grid_nets is not None else grid_net_returns(config, signal_data, asset_returns)
    rows = [{"params": params, "n_obs": len(net),   # each point's own periods_per_year, in case the grid sets it
             "sharpe": _sharpe(net, _with_overrides(config, params).backtest.periods_per_year)}
            for params, net in nets]

    detail = {"keys": keys, "grid": rows, "failures": failures}
    scored = [r["sharpe"] for r in rows if r["sharpe"] is not None]
    if not scored:
        return CheckResult(SKIPPED, f"no grid point produced a Sharpe ({len(failures)} failed)", detail)

    bad_fraction = sum(s < 0 for s in scored) / len(scored)
    detail["bad_fraction"] = bad_fraction
    failure_note = f"; {len(failures)} setting(s) errored" if failures else ""
    if bad_fraction > rc.param_kill_frac:
        return CheckResult(KILL, f"{bad_fraction:.0%} of {len(scored)} settings have Sharpe < 0 (> {rc.param_kill_frac:.0%}){failure_note}", detail)
    return CheckResult(PASS, f"{bad_fraction:.0%} of {len(scored)} settings have Sharpe < 0; range [{min(scored):.2f}, {max(scored):.2f}]{failure_note}", detail)


def walk_forward(
    nets: list[tuple[dict[str, Any], pd.Series]], ppy: int, min_train_years: float, test_years: float
) -> dict[str, Any] | None:
    """Expanding-window parameter selection over grid net-return series.

    Train windows run from the first date to train_end (at least min_train_years);
    the grid point with the best train Sharpe (first in grid order on ties) is
    held over (train_end, train_end + test_years]; train_end then moves to the
    end of that test window. The last test window may be shorter. Returns None
    when the history is too short for one train + test window.

    Valid because each grid point's net returns at t depend only on data up to t
    (signals are causal); the grid is fixed in advance, so only the selection is tested.
    """
    if not nets:
        return None
    dates = pd.DatetimeIndex(sorted(set().union(*(net.index for _, net in nets))))
    years = lambda y: pd.DateOffset(days=round(365.25 * y))
    train_end = dates[0] + years(min_train_years)
    if train_end >= dates[-1]:
        return None

    windows, segments = [], []
    while train_end < dates[-1]:
        test_end = train_end + years(test_years)
        train = [_sharpe(net[net.index <= train_end], ppy) for _, net in nets]
        scored = [(s, i) for i, s in enumerate(train) if s is not None]
        if scored:
            best_sharpe = max(s for s, _ in scored)
            best = next(i for s, i in scored if s == best_sharpe)
            params, net = nets[best]
            seg = net[(net.index > train_end) & (net.index <= test_end)]
            if len(seg):
                segments.append(seg)
                windows.append({
                    "train_end": str(train_end.date()), "test_end": str(min(test_end, dates[-1]).date()),
                    "params": params, "train_sharpe": best_sharpe,
                    "test_sharpe": _sharpe(seg, ppy), "n_test": len(seg),
                })
        train_end = test_end

    if not segments:
        return None
    oos = pd.concat(segments)
    full = [(_sharpe(net, ppy), params, net) for params, net in nets]
    full = [f for f in full if f[0] is not None]
    is_sharpe, is_params, is_net = max(full, key=lambda f: f[0]) if full else (None, None, None)
    changes = sum(a["params"] != b["params"] for a, b in zip(windows, windows[1:]))
    return {
        "oos_sharpe": _sharpe(oos, ppy),
        "in_sample_sharpe": is_sharpe,
        "in_sample_params": is_params,
        # The best full-sample point over the OOS dates only: separates selection decay from period effects.
        "in_sample_sharpe_oos_period": None if is_net is None else _sharpe(is_net[is_net.index.isin(oos.index)], ppy),
        "n_oos": len(oos),
        "oos_start": str(oos.index[0].date()),
        "n_windows": len(windows),
        "param_changes": changes,
        "param_change_rate": changes / (len(windows) - 1) if len(windows) > 1 else 0.0,
        "windows": windows,
    }


def check_walk_forward(config: ExperimentConfig, grid_nets: tuple[list, list]) -> CheckResult:
    """C11 walk-forward check: does picking the best grid point in-sample hold up out of sample?"""
    rc = config.robustness
    if not rc.param_grid:
        return CheckResult(SKIPPED, "no param_grid configured", {})
    nets, failures = grid_nets
    ppy = config.backtest.periods_per_year
    detail = walk_forward(nets, ppy, rc.wf_min_train_years, rc.wf_test_years)
    if detail is None:
        return CheckResult(SKIPPED, f"history shorter than {rc.wf_min_train_years:g}y train + one test window "
                                    f"(or no grid point scored)", {"failures": failures})
    detail["failures"] = failures
    oos, ins = detail["oos_sharpe"], detail["in_sample_sharpe"]
    detail["decay_ratio"] = oos / ins if oos is not None and ins not in (None, 0) else None
    summary = (f"OOS Sharpe {_f(oos)} vs in-sample {_f(ins)} over {detail['n_windows']} window(s) from "
               f"{detail['oos_start']}; parameter changed {detail['param_changes']}x")
    if oos is None:
        return CheckResult(FLAG, f"out-of-sample Sharpe undefined; {summary}", detail)
    if oos <= rc.wf_kill_sharpe:
        return CheckResult(KILL, f"{summary} (OOS <= {rc.wf_kill_sharpe:g})", detail)
    if ins is not None and ins > 0 and oos < rc.wf_flag_ratio * ins:
        return CheckResult(FLAG, f"{summary} (OOS < {rc.wf_flag_ratio:g} x in-sample)", detail)
    return CheckResult(PASS, summary, detail)


def check_holdout(
    config: ExperimentConfig, full_net: pd.Series, unlock: bool, experiment_dir: Path
) -> CheckResult:
    rc, start = config.robustness, config.validation.holdout_start
    if start is None:
        return CheckResult(SKIPPED, "no validation.holdout_start configured", {})

    frozen_path = experiment_dir / "holdout.json"
    if frozen_path.exists():
        detail = json.loads(frozen_path.read_text())
    elif not unlock:
        return CheckResult(SKIPPED, f"holdout from {start} is locked (run with --unlock-holdout, once, when in-sample checks are final)", {})
    else:
        held = full_net[full_net.index >= pd.Timestamp(start)]
        if len(held) < 5:
            return CheckResult(SKIPPED, f"only {len(held)} observations on/after {start}; holdout not scored", {})
        detail = {
            "unlocked_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "holdout_start": start,
            "n_obs": len(held),
            "sharpe": _sharpe(held, config.backtest.periods_per_year),
            "total_return": float((1 + held).prod() - 1),
            "git_sha": runner._git_sha(),
        }
        frozen_path.write_text(json.dumps(detail, indent=2))
        logger.info(f"[robustness] holdout scored and frozen at {frozen_path}")

    sharpe = detail["sharpe"]
    if sharpe is None:
        return CheckResult(FLAG, "held-out Sharpe undefined (no variation in returns)", detail)
    if sharpe < rc.holdout_kill_sharpe:
        return CheckResult(KILL, f"held-out Sharpe {sharpe:.2f} < {rc.holdout_kill_sharpe} over {detail['n_obs']} obs", detail)
    return CheckResult(PASS, f"held-out Sharpe {sharpe:.2f} over {detail['n_obs']} obs", detail)


# ── Charts ────────────────────────────────────────────────────────────────────

def _label(value: Any) -> Any:
    """Grid value as a chart label: lists (e.g. lookbacks) become strings; scalars stay as-is."""
    return str(value) if isinstance(value, (list, tuple, dict)) else value


def _write_charts(results: dict[str, Any], charts_dir: Path) -> None:
    charts_dir.mkdir(exist_ok=True)
    exp_id = results["experiment_id"]
    checks = results["checks"]

    by_mult = checks["cost_stress"]["detail"].get("sharpe_by_multiplier", {})
    points = [(float(m), s) for m, s in by_mult.items() if s is not None]
    if points:
        fig, ax = plt.subplots()
        ax.plot(*zip(*sorted(points)), marker="o")
        ax.axhline(0, color="grey", linewidth=0.8)
        ax.set(title=f"{exp_id} — Net Sharpe vs cost multiplier", xlabel="x base cost", ylabel="Sharpe")
        fig.savefig(charts_dir / "robustness_cost.png")
        plt.close(fig)

    yearly = checks["subperiods"]["detail"].get("yearly_returns", {})
    if yearly:
        fig, ax = plt.subplots()
        ax.bar(list(yearly), list(yearly.values()))
        ax.axhline(0, color="grey", linewidth=0.8)
        ax.tick_params(axis="x", rotation=90)
        ax.set(title=f"{exp_id} — Calendar-year returns")
        fig.savefig(charts_dir / "robustness_yearly.png")
        plt.close(fig)

    param = checks["parameters"]["detail"]
    grid = [r for r in param.get("grid", []) if r["sharpe"] is not None]
    keys = param.get("keys", [])
    if grid and len(keys) == 1:
        fig, ax = plt.subplots()
        ax.plot([_label(r["params"][keys[0]]) for r in grid], [r["sharpe"] for r in grid], marker="o")
        ax.axhline(0, color="grey", linewidth=0.8)
        ax.set(title=f"{exp_id} — Sharpe vs {keys[0]}", xlabel=keys[0], ylabel="Sharpe")
        fig.savefig(charts_dir / "robustness_params.png")
        plt.close(fig)
    elif grid and len(keys) == 2:
        xs = list(dict.fromkeys(_label(r["params"][keys[0]]) for r in grid))   # grid order; lists allowed
        ys = list(dict.fromkeys(_label(r["params"][keys[1]]) for r in grid))
        table = np.full((len(ys), len(xs)), np.nan)
        for r in grid:
            table[ys.index(_label(r["params"][keys[1]])), xs.index(_label(r["params"][keys[0]]))] = r["sharpe"]
        fig, ax = plt.subplots()
        image = ax.imshow(table, aspect="auto", origin="lower")
        ax.set(xticks=range(len(xs)), xticklabels=xs, yticks=range(len(ys)), yticklabels=ys,
               xlabel=keys[0], ylabel=keys[1], title=f"{exp_id} — Sharpe by parameters")
        fig.colorbar(image)
        fig.savefig(charts_dir / "robustness_params.png")
        plt.close(fig)


# ── Orchestration ─────────────────────────────────────────────────────────────

def run_robustness(config_path: str, unlock_holdout: bool = False) -> RobustnessResult:
    """Run the full suite for one experiment and write robustness.json next to its config."""
    config = load_config(config_path)
    experiment_dir = Path(config_path).parent
    rc, bt = config.robustness, config.backtest
    ppy = bt.periods_per_year
    holdout_start = config.validation.holdout_start

    signal_data = runner._load_signal_data(config)
    signal_df = runner._compute_signal(config, signal_data)
    weights_full, returns_full = runner._build_panels(config, signal_df)
    weights, asset_returns = runner._apply_holdout(weights_full, returns_full, holdout_start)

    base = apply_costs(weights, asset_returns, bt.cost_bps, bt.lag)
    if base.net_returns.empty:
        raise ValueError(f"{config.experiment_id}: no overlapping dates between weights and returns")
    net = base.net_returns

    family = config.validation.family
    if family:   # the base configuration is a trial too, even if run_experiment() never logged it
        trials.log_trial(family, trials.experiment_params(config), net, kind="experiment",
                         experiment_id=config.experiment_id, family_version=config.validation.family_version,
                         periods_per_year=ppy)
    n_used, n_declared, n_ledger = trials.effective_n_trials(config)
    grid_nets = grid_net_returns(config, signal_data, returns_full) if rc.param_grid else ([], [])

    checks = {
        "cost_stress": check_cost_stress(base.gross_returns, base.turnover, bt.cost_bps, ppy, rc),
        "lag_sensitivity": check_lag_sensitivity(weights, asset_returns, bt.lag, bt.cost_bps, ppy, rc),
        "outliers": check_outliers(net, ppy, rc),
        "subperiods": check_subperiods(net, ppy, rc),
        "parameters": check_parameters(config, signal_data, returns_full, grid_nets),
        "walk_forward": check_walk_forward(config, grid_nets),
        "statistical": check_statistical(net, ppy, n_used, rc),
        "holdout": check_holdout(
            config, apply_costs(weights_full, returns_full, bt.cost_bps, bt.lag).net_returns,
            unlock_holdout, experiment_dir,
        ),
    }
    overall = "KILLED" if any(c.verdict == KILL for c in checks.values()) else "NEEDS_REVIEW"

    if family:   # robustness-grid variants are recorded but do not count towards n_trials (E4)
        for row in checks["parameters"].detail.get("grid", []):
            trials.record_trial(
                family, trials.experiment_params(_with_overrides(config, row["params"])),
                row["sharpe"], row.get("n_obs"), kind="robustness", experiment_id=config.experiment_id,
                family_version=config.validation.family_version,
            )

    results = runner._to_jsonable({
        "experiment_id": config.experiment_id,
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "git_sha": runner._git_sha(),
        "n_trials": n_used,
        "n_trials_declared": n_declared,
        "n_trials_ledger": n_ledger,
        "family": family,
        "holdout_start": holdout_start,
        "in_sample_end": str(net.index.max()),
        "n_obs": len(net),
        "base": {"sharpe": _sharpe(net, ppy), "cost_bps": bt.cost_bps, "lag": bt.lag},
        "thresholds": asdict(rc),
        "checks": {name: asdict(c) for name, c in checks.items()},
        "overall": overall,
    })

    (experiment_dir / "robustness.json").write_text(json.dumps(results, indent=2))
    _write_charts(results, experiment_dir / "charts")

    note_path = experiment_dir / "research_note.md"
    if note_path.exists():
        note_path.write_text(replace_robustness_section(note_path.read_text(), render_robustness_section(results)))

    stat = results["checks"]["statistical"]["detail"]
    update_experiment(
        config.experiment_id,
        robustness_verdict=overall,
        sharpe_2x_cost=results["checks"]["cost_stress"]["detail"].get("sharpe_at_flag_multiplier"),
        sharpe_ci_low=stat.get("ci_low"),
        deflated_sharpe=stat.get("deflated_sharpe"),
    )

    return RobustnessResult(config=config, results=results, experiment_dir=experiment_dir)
