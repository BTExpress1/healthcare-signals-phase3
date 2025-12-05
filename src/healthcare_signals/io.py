from __future__ import annotations

from pathlib import Path
from typing import Iterable, Sequence

import pandas as pd

# Project root: repo_root / src / healthcare_signals / io.py → go 2 levels up
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_RAW = PROJECT_ROOT / "data" / "raw"
DATA_PROCESSED = PROJECT_ROOT / "data" / "processed"


def load_facts_daily() -> pd.DataFrame:
    """
    Load daily provider facts from:
        data/raw/facts_daily.csv

    Expected columns:
        - date
        - provider_id
        - state
        - claims_cnt
        - avg_allowed_amt
        - zscore_allowed_amt
    """
    path = DATA_RAW / "facts_daily.parquet"
    if not path.exists():
        raise FileNotFoundError(f"facts_daily.csv not found at {path}")

    df = pd.read_parquet(path)
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    return df


def infer_month_end_snapshots(df: pd.DataFrame) -> pd.DatetimeIndex:
    """
    Infer monthly snapshot dates between the min/max date in the input.
    Returns month-end dates as a DatetimeIndex.
    """
    if df.empty:
        return pd.DatetimeIndex([])

    start = df["date"].min().normalize()
    end = df["date"].max().normalize()
    return pd.date_range(start=start, end=end, freq="ME")


def save_provider_panel_snapshot(panel: pd.DataFrame, as_of_date: str | pd.Timestamp) -> Path:
    """
    Save a single provider snapshot to:

        data/processed/provider_panel_asof=<YYYY-MM-DD>.parquet
    """
    DATA_PROCESSED.mkdir(parents=True, exist_ok=True)
    ts = pd.to_datetime(as_of_date).date()
    out_path = DATA_PROCESSED / f"provider_panel_asof={ts}.parquet"
    panel.to_parquet(out_path, index=False)
    return out_path


def save_provider_panel_full(panel: pd.DataFrame, name: str = "provider_panel_all_dates.parquet") -> Path:
    """
    Save the full provider panel (all snapshot dates concatenated) to:

        data/processed/<name>
    """
    DATA_PROCESSED.mkdir(parents=True, exist_ok=True)
    out_path = DATA_PROCESSED / name
    panel.to_parquet(out_path, index=False)
    return out_path
