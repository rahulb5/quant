"""Tests for src/macro/validation.py."""

import numpy as np
import pandas as pd
import pytest

from src.macro import validation as v

IDX = pd.bdate_range("2000-01-03", periods=100)


def _lab(seq, idx=None):
    return pd.Series(seq, index=idx if idx is not None else IDX[:len(seq)], dtype=object)


def test_episodes_runs_and_nan_breaks():
    s = _lab(["a", "a", "b", "b", "b", np.nan, "b", "a"])
    ep = v.episodes(s)
    assert ep["label"].tolist() == ["a", "b", "b", "a"]
    assert ep["n_days"].tolist() == [2, 3, 1, 1]
    assert ep.loc[1, "start"] == IDX[2] and ep.loc[1, "end"] == IDX[4]
    assert v.episodes(_lab([np.nan] * 3)).empty


def test_episodes_break_on_index_gap_with_same_label():
    idx = IDX[[0, 1, 5, 6]]
    s = pd.Series(["a", "a", "a", "a"], index=idx, dtype=object)
    full = s.reindex(IDX[:7])             # NaN rows in between
    assert v.episodes(full)["n_days"].tolist() == [2, 2]


def test_episode_summary_usable_threshold():
    seq = ["a", "b"] * 5 + ["c"] * 4
    out = v.episode_summary(_lab(seq))
    assert out.loc["a", "n_episodes"] == 5 and out.loc["a", "usable"]
    assert out.loc["c", "n_episodes"] == 1 and not out.loc["c", "usable"]
    assert out["share"].sum() == pytest.approx(1.0)
    assert out.loc["c", "median_days"] == 4


def test_cramers_v_identical_independent_and_degenerate():
    a = _lab(["x", "y"] * 40)
    assert v.cramers_v(a, a) == pytest.approx(1.0)
    assert v.cramers_v(a, a.map({"x": "p", "y": "q"})) == pytest.approx(1.0)     # relabelled partition
    rng = np.random.default_rng(0)
    b = _lab(rng.choice(["p", "q"], 80).tolist())
    assert v.cramers_v(a, b) < 0.3
    assert np.isnan(v.cramers_v(a, _lab(["z"] * 80)))


def test_label_association_matrix_and_counts():
    a, b = _lab(["x", "y"] * 20), _lab(["p", "q"] * 10 + [np.nan] * 20)
    m, n = v.label_association({"a": a, "b": b})
    assert m.loc["a", "a"] == 1.0 and m.loc["a", "b"] == m.loc["b", "a"]
    assert n.loc["a", "b"] == 20 and n.loc["a", "a"] == 40


def test_label_disagreement_and_column_correlations():
    a, b = _lab(["x", "x", "y", "y"]), _lab(["x", "y", "y", "x"])
    assert v.label_disagreement(a, b) == (0.5, 4)
    assert np.isnan(v.label_disagreement(a, _lab([np.nan] * 4))[0])
    x = pd.DataFrame({"u": np.arange(10.0), "w": np.arange(10.0)})
    y = pd.DataFrame({"u": np.arange(10.0) * 2, "w": np.arange(10.0)[::-1]})
    out = v.column_correlations(x, y)
    assert out.loc["u", "corr"] == pytest.approx(1.0) and out.loc["w", "corr"] == pytest.approx(-1.0)
    assert out.loc["u", "max_abs_diff"] == 9 and out.loc["w", "n_days"] == 10


def test_recession_windows_and_usrec_daily():
    m = pd.Series([0, 1, 1, 0, 0, 1], index=pd.date_range("2000-01-01", periods=6, freq="MS"))
    w = v.recession_windows(m)
    assert w["start"].tolist() == [pd.Timestamp("2000-02-01"), pd.Timestamp("2000-06-01")]
    assert w["end"].tolist() == [pd.Timestamp("2000-03-31"), pd.Timestamp("2000-06-30")]
    d = v.usrec_daily(m, pd.bdate_range("2000-01-03", "2000-09-29"))
    assert d["2000-02-15"] == 1 and d["2000-04-17"] == 0 and np.isnan(d["2000-09-01"])


def test_auc_known_values():
    y = pd.Series([0, 0, 0, 1, 1, 1], dtype=float)
    assert v.auc(pd.Series([1, 2, 3, 4, 5, 6.0]), y) == 1.0
    assert v.auc(pd.Series([6, 5, 4, 3, 2, 1.0]), y) == 0.0
    assert v.auc(pd.Series([1.0] * 6), y) == 0.5
    assert np.isnan(v.auc(pd.Series(range(6), dtype=float), pd.Series([1.0] * 6)))


def test_recession_table_and_false_flag_runs():
    idx = pd.bdate_range("2000-01-03", "2004-12-31")
    g = pd.Series(1.0, index=idx); g["2001-03-01":"2001-11-30"] = -1.0
    gi = pd.Series(0.5, index=idx)
    flag = pd.Series(False, index=idx)
    flag["2001-05-01":"2001-12-31"] = True          # inside the recession, lags its start
    flag["2003-06-02":"2003-06-30"] = True          # a false run
    w = pd.DataFrame({"start": [pd.Timestamp("2001-03-01")], "end": [pd.Timestamp("2001-11-30")]})
    t = v.recession_table(w, g, gi, flag)
    assert t.loc[0, "G_level_below0"] == 1.0 and t.loc[0, "sahm_first"] == pd.Timestamp("2001-05-01").date()
    assert t.loc[0, "sahm_lag_days"] == (pd.Timestamp("2001-05-01") - pd.Timestamp("2001-03-01")).days
    fr = v.false_flag_runs(flag, w, grace_days=180)
    assert len(fr) == 1 and fr.loc[0, "start"] == pd.Timestamp("2003-06-02")
    t2 = v.recession_table(w, g, gi, pd.Series(False, index=idx))
    assert t2.loc[0, "sahm_first"] is None and np.isnan(t2.loc[0, "sahm_lag_days"])


def test_window_extremes_and_label_shares():
    s = pd.Series(np.arange(len(IDX), dtype=float), index=IDX)
    ex = v.window_extremes(s, {"w": (str(IDX[10].date()), str(IDX[20].date())), "none": ("1990-01-01", "1990-02-01")})
    assert ex.loc["w", "peak"] == 20 and ex.loc["w", "peak_date"] == IDX[20].date()
    assert ex.loc["w", "peak_pctile"] == pytest.approx(21 / 100) and np.isnan(ex.loc["none", "peak"])
    lab = _lab(["a"] * 5 + ["b"] * 5)
    sh = v.label_shares_by_window(lab, {"first": (str(IDX[0].date()), str(IDX[4].date())), "all": (str(IDX[0].date()), str(IDX[9].date()))})
    assert sh.loc["first", "a"] == 1.0 and sh.loc["all", "b"] == 0.5


def test_recession_table_looks_past_the_end_for_short_recessions():
    idx = pd.bdate_range("2020-01-01", "2020-12-31")
    flag = pd.Series(False, index=idx); flag["2020-05-08":] = True
    w = pd.DataFrame({"start": [pd.Timestamp("2020-03-01")], "end": [pd.Timestamp("2020-04-30")]})
    g = pd.Series(0.0, index=idx)
    assert v.recession_table(w, g, g, flag).loc[0, "sahm_lag_days"] == (pd.Timestamp("2020-05-08") - w.loc[0, "start"]).days
    assert pd.isna(v.recession_table(w, g, g, flag, after_days=3).loc[0, "sahm_lag_days"])
