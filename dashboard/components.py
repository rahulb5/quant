"""dashboard/components.py — shared helpers for dashboard pages."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from src.db.client import Database
from src.experiments import artifacts
from src.macro import data as macro_data
from src.shared.config import config

# Streamlit badge colours; the word "approved" is deliberately never shown.
VERDICT_COLORS = {
    "KILLED": "red",
    "KILL": "red",
    "NEEDS_REVIEW": "orange",
    "FLAG": "orange",
    "PASS": "green",
    "SKIPPED": "gray",
}

PALETTE = {"primary": "#2f6fdb", "secondary": "#8b95a5", "negative": "#d1453b", "positive": "#2e9e6b"}


def verdict_badge(verdict: str | None) -> str:
    """Markdown badge for a verdict; 'not run' when there is none."""
    if not verdict:
        return ":gray-badge[not run]"
    return f":{VERDICT_COLORS.get(verdict, 'gray')}-badge[{verdict}]"


def chip(text: str | None) -> str:
    return f":blue-badge[{text}]" if text else ":gray-badge[unclassified]"


def fmt_num(value: Any, spec: str = ".2f") -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "—"
    return format(value, spec)


def fmt_pct(value: Any, spec: str = ".1%") -> str:
    return fmt_num(value, spec)


def style_fig(fig: go.Figure, height: int = 340) -> go.Figure:
    fig.update_layout(
        height=height,
        margin=dict(l=10, r=10, t=30, b=10),
        legend=dict(orientation="h", yanchor="bottom", y=1.0, x=0),
        hovermode="x unified",
    )
    return fig


def plot(container: Any, fig: go.Figure, title: str, height: int = 340) -> None:
    """Draw a chart with its title as page text above it, not inside the figure."""
    container.markdown(f"**{title}**")
    container.plotly_chart(style_fig(fig, height), width="stretch")


@st.cache_data(show_spinner=False)
def cached_list(mtime: float):
    return artifacts.list_experiments()


@st.cache_data(show_spinner=False)
def cached_load(experiment_id: str, mtime: float):
    return artifacts.load_experiment(experiment_id)


def list_experiments():
    return cached_list(artifacts.artifacts_mtime())


def load_experiment(experiment_id: str):
    return cached_load(experiment_id, artifacts.artifacts_mtime())


# ── Macro (read-only DuckDB) ─────────────────────────────────────────────────


def _db() -> Database:
    """Fresh read-only handle; query_df() opens and closes it per call so the
    dashboard never holds a lock a collector needs. DB_PATH is re-read so tests
    can point at a temp file."""
    return Database(os.environ.get("DB_PATH", config.db_path), read_only=True)


def _db_mtime() -> float:
    path = Path(_db().path)
    return path.stat().st_mtime if path.exists() else 0.0


@st.cache_data(show_spinner=False)
def _cached_catalog(path: str, mtime: float) -> pd.DataFrame:
    return macro_data.list_series(_db())


@st.cache_data(show_spinner=False)
def _cached_series(path: str, mtime: float, source: str, code: str, vintage: str,
                   measure: str | None, pct_of_oi: bool) -> pd.Series:
    if source == "FRED":
        return macro_data.load_fred(_db(), code, vintage=vintage)
    return macro_data.load_cot(_db(), code, measure, pct_of_oi=pct_of_oi)


def load_catalog() -> pd.DataFrame:
    return _cached_catalog(_db().path, _db_mtime())


def load_series(source: str, code: str, vintage: str = "latest",
                measure: str | None = None, pct_of_oi: bool = False) -> pd.Series:
    return _cached_series(_db().path, _db_mtime(), source, code, vintage, measure, pct_of_oi)
