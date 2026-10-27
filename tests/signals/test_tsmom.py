"""
tests/signals/test_tsmom.py

Tests for src/signals/tsmom on synthetic returns.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from src.signals.tsmom import SignalConfig, calculate_signal, ewma_vol, trend_signal

DAYS = pd.bdate_range("2000-01-03", periods=600)


def test_trend_signal_blends_signs_across_lookbacks():
    r = pd.Series(-0.001, index=DAYS[:300])
    r.iloc[-21:] = 0.01                                  # last month up, longer history down
    s = trend_signal(r, (21, 63, 252))
    assert s.iloc[:251].isna().all()                     # needs the 252-day lookback
    # 21d: up; 63d: 21*0.01 vs 42*-0.001 -> up; 252d: 21*0.01 vs 231*-0.001 = -0.021 -> down
    assert s.iloc[-1] == pytest.approx(1 / 3)


def test_trend_signal_all_up_is_plus_one():
    s = trend_signal(pd.Series(0.001, index=DAYS[:300]), (21, 63, 252))
    assert s.dropna().eq(1.0).all()


def test_ewma_vol_recovers_constant_vol():
    rng = np.random.default_rng(0)
    r = pd.Series(rng.normal(0, 0.01, 5000), index=pd.bdate_range("2000-01-03", periods=5000))
    v = ewma_vol(r, com=60, min_obs=60)
    assert v.iloc[:59].isna().all()
    assert v.iloc[1000:].mean() == pytest.approx(0.01 * np.sqrt(252), rel=0.05)


def _panel():
    rng = np.random.default_rng(1)
    df = pd.DataFrame({"EQ.SPX": rng.normal(0.0004, 0.01, len(DAYS)),
                       "FX.EUR": rng.normal(0.0, 0.006, len(DAYS))}, index=DAYS)
    df.loc[DAYS[300:303], "EQ.SPX"] = np.nan             # equity holiday block
    return df


def test_calculate_signal_long_format_and_trading_flags():
    cfg = SignalConfig(instruments=("EQ.SPX", "FX.EUR"))
    out = calculate_signal(_panel(), cfg)
    assert list(out.columns) == ["date", "instrument", "sleeve", "signal", "vol", "ret", "trading"]
    spx = out[out.instrument == "EQ.SPX"].set_index("date")
    assert not spx.loc[DAYS[301], "trading"]
    assert spx.loc[DAYS[301], ["signal", "vol", "ret"]].isna().all()
    assert set(out.sleeve) == {"EQ", "FX"}


def test_calculate_signal_is_truncation_invariant():
    """A value at t must not change when data after t is removed (no look-ahead)."""
    cfg = SignalConfig(instruments=("EQ.SPX", "FX.EUR"))
    panel = _panel()
    full = calculate_signal(panel, cfg).set_index(["date", "instrument"])
    cut = DAYS[450]
    trunc = calculate_signal(panel.loc[:cut], cfg).set_index(["date", "instrument"])
    pd.testing.assert_frame_equal(trunc, full.loc[trunc.index])


def test_signal_config_validation_and_yaml_lists():
    cfg = SignalConfig(instruments=["EQ.SPX"], lookbacks=[21, 252])
    assert cfg.instruments == ("EQ.SPX",) and cfg.lookbacks == (21, 252)
    with pytest.raises(ValueError, match="Unknown instruments"):
        SignalConfig(instruments=("EQ.XYZ",))


def test_calculate_signal_requires_instruments_in_panel():
    with pytest.raises(ValueError, match="lacks instruments"):
        calculate_signal(_panel(), SignalConfig(instruments=("RT.UST10",)))
