"""tests/experiments/test_portfolio.py"""

from __future__ import annotations

import pandas as pd
import pytest

from src.experiments.portfolio import decile_long_short, equal_weight_long_basket


def test_equal_weight_basic_threshold():
    signal_df = pd.DataFrame({
        "date": ["2024-01-01", "2024-01-01", "2024-01-08", "2024-01-08"],
        "asset": ["A", "B", "A", "B"],
        "sig": [95, 50, 20, 10],
    })

    weights = equal_weight_long_basket(
        signal_df, date_column="date", asset_column="asset", signal_column="sig", threshold=90
    )

    assert weights.loc["2024-01-01", "A"] == 1.0
    assert weights.loc["2024-01-01", "B"] == 0.0
    assert weights.loc["2024-01-08"].sum() == 0.0  # no asset crosses threshold


def test_equal_weight_splits_across_multiple_selected():
    signal_df = pd.DataFrame({
        "date": ["2024-01-01"] * 3,
        "asset": ["A", "B", "C"],
        "sig": [95, 92, 10],
    })

    weights = equal_weight_long_basket(
        signal_df, date_column="date", asset_column="asset", signal_column="sig", threshold=90
    )

    assert weights.loc["2024-01-01", "A"] == 0.5
    assert weights.loc["2024-01-01", "B"] == 0.5
    assert weights.loc["2024-01-01", "C"] == 0.0


def test_trend_filter_gates_selection():
    signal_df = pd.DataFrame({
        "date": ["2024-01-01", "2024-01-01"],
        "asset": ["A", "B"],
        "sig": [95, 95],
        "trend_up": [True, False],
    })

    weights = equal_weight_long_basket(
        signal_df,
        date_column="date",
        asset_column="asset",
        signal_column="sig",
        threshold=90,
        trend_filter="trend_up",
    )

    assert weights.loc["2024-01-01", "A"] == 1.0
    assert weights.loc["2024-01-01", "B"] == 0.0


def test_trend_none_skips_gating():
    signal_df = pd.DataFrame({
        "date": ["2024-01-01"],
        "asset": ["A"],
        "sig": [95],
    })

    weights = equal_weight_long_basket(
        signal_df,
        date_column="date",
        asset_column="asset",
        signal_column="sig",
        threshold=90,
        trend_filter="trend_none",
    )

    assert weights.loc["2024-01-01", "A"] == 1.0


# ── decile_long_short ──────────────────────────────────────────────────────

def _ten_name_signal(date: str = "2024-01-01") -> pd.DataFrame:
    assets = [f"A{i}" for i in range(1, 11)]
    return pd.DataFrame({
        "date": [date] * 10,
        "asset": assets,
        "sig": list(range(1, 11)),  # A1 lowest, A10 highest
    })


def test_decile_long_short_longs_top_shorts_bottom():
    signal_df = _ten_name_signal()

    weights = decile_long_short(
        signal_df, date_column="date", asset_column="asset", signal_column="sig",
        n_deciles=10, min_names=5,
    )

    row = weights.loc["2024-01-01"]
    assert row["A10"] == 1.0   # highest score -> top decile, long
    assert row["A1"] == -1.0   # lowest score -> bottom decile, short
    assert (row.drop(["A1", "A10"]) == 0.0).all()
    assert row.sum() == 0.0          # dollar-neutral
    assert row.abs().sum() == 2.0    # gross long 1.0 / gross short 1.0


def test_decile_long_short_splits_ties_across_multiple_names():
    signal_df = pd.DataFrame({
        "date": ["2024-01-01"] * 4,
        "asset": ["A", "B", "C", "D"],
        "sig": [1, 1, 10, 10],
    })

    weights = decile_long_short(
        signal_df, date_column="date", asset_column="asset", signal_column="sig",
        n_deciles=2, min_names=2,
    )

    row = weights.loc["2024-01-01"]
    assert row["C"] == pytest.approx(0.5)
    assert row["D"] == pytest.approx(0.5)
    assert row["A"] == pytest.approx(-0.5)
    assert row["B"] == pytest.approx(-0.5)


def test_decile_long_short_below_min_names_is_flat():
    signal_df = _ten_name_signal().iloc[:3]  # only 3 names, below min_names

    weights = decile_long_short(
        signal_df, date_column="date", asset_column="asset", signal_column="sig",
        n_deciles=10, min_names=5,
    )

    assert (weights.loc["2024-01-01"] == 0.0).all()


def test_decile_long_short_ignores_nan_scores():
    signal_df = _ten_name_signal()
    signal_df.loc[signal_df["asset"] == "A5", "sig"] = float("nan")

    weights = decile_long_short(
        signal_df, date_column="date", asset_column="asset", signal_column="sig",
        n_deciles=10, min_names=5,
    )

    assert weights.loc["2024-01-01", "A5"] == 0.0


# ── vol_target_weights (trend baseline) ───────────────────────────────────────

import numpy as np  # noqa: E402

from src.experiments.portfolio import vol_target_weights  # noqa: E402

VT_DAYS = pd.bdate_range("2020-01-06", periods=400)          # starts on a Monday


def _vt_frame(spec: dict[str, dict], days=VT_DAYS) -> pd.DataFrame:
    """spec: asset -> {sleeve, signal, vol, ret (scalar or array), holidays (list of positions)}."""
    rows = []
    for asset, s in spec.items():
        ret = np.broadcast_to(s.get("ret", 0.0), len(days)).astype(float).copy()
        trading = np.ones(len(days), dtype=bool)
        trading[s.get("holidays", [])] = False
        df = pd.DataFrame({
            "date": days, "instrument": asset, "sleeve": s["sleeve"],
            "signal": np.where(trading, s.get("signal", 1.0), np.nan),
            "vol": np.where(trading, s.get("vol", 0.10), np.nan),
            "ret": np.where(trading, ret, np.nan), "trading": trading,
        })
        rows.append(df)
    return pd.concat(rows, ignore_index=True)


def _vt(frame, **kw):
    return vol_target_weights(frame, date_column="date", asset_column="instrument",
                              signal_column="signal", **kw)


def test_vol_target_equal_risk_per_sleeve_and_instrument():
    frame = _vt_frame({
        "EQ.A": {"sleeve": "EQ", "vol": 0.20}, "EQ.B": {"sleeve": "EQ", "vol": 0.10},
        "RT.A": {"sleeve": "RT", "vol": 0.05, "signal": -1.0},
    })
    w = _vt(frame)
    row = w.iloc[10]                                          # before 126 days: scaler = 1
    assert row["EQ.A"] == pytest.approx(0.10 / 0.20 / (2 * 2))
    assert row["EQ.B"] == pytest.approx(0.10 / 0.10 / (2 * 2))
    assert row["RT.A"] == pytest.approx(-0.10 / 0.05 / (2 * 1))


def test_vol_target_rebalances_weekly_only():
    rng = np.random.default_rng(0)
    frame = _vt_frame({"EQ.A": {"sleeve": "EQ"}})
    frame["vol"] = 0.10 + rng.uniform(0, 0.05, len(frame))   # target moves every day
    w = _vt(frame)["EQ.A"]
    changed = w.diff().fillna(w).ne(0)
    assert (w.index[changed].dayofweek == 4).all()            # set at Friday's close, held to next Friday
    assert changed.sum() > 50


def test_vol_target_holds_weight_through_a_holiday_rebalance():
    friday = 9                                                 # VT_DAYS[9] is the second Friday
    assert VT_DAYS[friday].dayofweek == 4
    frame = _vt_frame({"EQ.A": {"sleeve": "EQ", "holidays": [friday]},
                       "FX.A": {"sleeve": "FX"}})
    frame.loc[(frame.instrument == "EQ.A") & (frame.date > VT_DAYS[4]), "vol"] = 0.05   # target doubles
    frame.loc[(frame.instrument == "EQ.A") & (frame.date == VT_DAYS[friday]), "vol"] = np.nan
    w = _vt(frame)["EQ.A"]
    assert w.loc[VT_DAYS[friday]] == pytest.approx(w.loc[VT_DAYS[4]])   # held: no trade on its holiday
    assert w.loc[VT_DAYS[14]] == pytest.approx(2 * w.loc[VT_DAYS[4]])   # next rebalance picks it up


def test_vol_target_holiday_does_not_change_others_share():
    frame = _vt_frame({"EQ.A": {"sleeve": "EQ", "holidays": [9]}, "EQ.B": {"sleeve": "EQ"}})
    w = _vt(frame)
    assert w.loc[VT_DAYS[9], "EQ.B"] == pytest.approx(w.loc[VT_DAYS[4], "EQ.B"])


def test_vol_target_portfolio_scaler_and_bounds():
    rng = np.random.default_rng(1)
    frame = _vt_frame({"EQ.A": {"sleeve": "EQ", "vol": 0.10, "ret": rng.normal(0, 0.10 / np.sqrt(252) * 2, 400)}})
    scaler = _vt(frame, diagnostics=True)[1]["scaler"]
    assert (scaler.iloc[:126] == 1.0).all()
    assert scaler.iloc[300:].between(0.5, 0.6).all()          # realised 2x target -> about 0.5 (floor)


def test_vol_target_gross_cap():
    frame = _vt_frame({"RT.A": {"sleeve": "RT", "vol": 0.001}})   # would need 100x
    w = _vt(frame, max_gross=8.0)
    assert w["RT.A"].abs().max() == pytest.approx(8.0)
    assert w.attrs == {}                                         # nothing that breaks pd.concat downstream


def _random_frame(seed=3):
    rng = np.random.default_rng(seed)
    frame = _vt_frame({"EQ.A": {"sleeve": "EQ", "ret": rng.normal(0, 0.01, 400), "holidays": [9, 200]},
                       "EQ.B": {"sleeve": "EQ", "ret": rng.normal(0, 0.01, 400)},
                       "FX.A": {"sleeve": "FX", "ret": rng.normal(0, 0.006, 400)}})
    tr = frame["trading"]
    frame.loc[tr, "signal"] = rng.choice([-1, -1 / 3, 1 / 3, 1], tr.sum())
    frame.loc[tr, "vol"] = rng.uniform(0.05, 0.2, tr.sum())
    frame.loc[(frame.instrument == "FX.A") & (frame.date < VT_DAYS[50]), ["signal", "vol"]] = np.nan
    return frame


def test_vol_target_zero_before_first_signal():
    full = _vt(_random_frame())
    first_friday_after = VT_DAYS[VT_DAYS >= VT_DAYS[50]][VT_DAYS[VT_DAYS >= VT_DAYS[50]].dayofweek == 4][0]
    assert (full.loc[:first_friday_after - pd.Timedelta(days=1), "FX.A"] == 0).all()
    assert full.loc[first_friday_after, "FX.A"] != 0


def test_vol_target_truncation_invariant_random_inputs():
    """Weights and scaler up to any cut are identical with or without later data (no look-ahead)."""
    frame = _random_frame()
    full, full_diag = _vt(frame, diagnostics=True)
    # Fridays (trade days, where a leak in the day's target would show) and mid-week cuts.
    for k in (119, 120, 249, 302, 304):
        cut = VT_DAYS[k]
        trunc, trunc_diag = _vt(frame[frame.date <= cut], diagnostics=True)
        pd.testing.assert_frame_equal(trunc, full.loc[:cut])
        pd.testing.assert_series_equal(trunc_diag["scaler"], full_diag["scaler"].loc[:cut])


def test_vol_target_holiday_hold_end_to_end_through_apply_costs():
    """Closed on the scheduled Friday: the Monday return is earned with the weight set before the holiday."""
    from src.backtest.execution import apply_costs

    frame = _random_frame()
    w = _vt(frame)
    r = frame.pivot(index="date", columns="instrument", values="ret")
    res = apply_costs(w, r, cost_bps=0.0, lag=1)
    friday, monday = VT_DAYS[9], VT_DAYS[10]
    assert w.loc[friday, "EQ.A"] == w.loc[VT_DAYS[4], "EQ.A"]     # no trade on its holiday
    assert w.loc[monday, "EQ.A"] != w.loc[VT_DAYS[4], "EQ.A"]     # trades on its first day back
    expected = (w.loc[friday] * r.loc[monday]).sum()
    assert res.gross_returns.loc[monday] == pytest.approx(expected)


def test_vol_target_exits_a_dead_series():
    frame = _random_frame()
    frame = frame[~((frame.instrument == "EQ.B") & (frame.date > VT_DAYS[300]))]   # EQ.B data stops
    w = _vt(frame)
    assert w.loc[VT_DAYS[300], "EQ.B"] != 0
    assert (w.loc[VT_DAYS[312]:, "EQ.B"] == 0).all()             # beyond hold_limit rows: exited


def test_vol_target_scaler_upper_bound():
    frame = _vt_frame({"EQ.A": {"sleeve": "EQ", "ret": np.tile([1e-4, -1e-4], 200)}})   # tiny realised vol
    assert (_vt(frame, diagnostics=True)[1]["scaler"].iloc[200:] == 5.0).all()


def test_vol_target_new_sleeve_halves_the_first_sleeves_share():
    frame = _vt_frame({"EQ.A": {"sleeve": "EQ"}, "FX.A": {"sleeve": "FX"}})
    frame.loc[(frame.instrument == "FX.A") & (frame.date < VT_DAYS[50]), ["signal", "vol"]] = np.nan
    w = _vt(frame)["EQ.A"]
    assert w.loc[VT_DAYS[44]] == pytest.approx(2 * w.loc[VT_DAYS[99]])          # G goes 1 -> 2
