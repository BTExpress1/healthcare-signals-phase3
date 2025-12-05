from __future__ import annotations

from pathlib import Path
import pandas as pd

# Project paths (repo_root / src / healthcare_signals / io.py → go 2 levels up)
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_RAW = PROJECT_ROOT / "data" / "raw"
DATA_PROCESSED = PROJECT_ROOT / "data" / "processed"


def load_facts_daily(as_of_date: str) -> pd.DataFrame:
    """
    Load daily provider claim facts up to and including `as_of_date`.

    Expects:
        data/raw/facts_daily.parquet

    Columns (from Phase 1):
        - date
        - provider_id
        - state
        - claims_cnt
        - avg_allowed_amt
        - zscore_allowed_amt
    """
    path = DATA_RAW / "facts_daily.parquet"
    df = pd.read_parquet(path)

    df = df.copy()
    # Standardize to a single date column name used downstream
    df["fact_date"] = pd.to_datetime(df["date"])
    df = df.drop(columns=["date"])

    cutoff = pd.to_datetime(as_of_date)
    df = df[df["fact_date"] <= cutoff].reset_index(drop=True)

    return df


def save_patient_signals(signals: pd.DataFrame, as_of_date: str) -> Path:
    """
    Save provider-level signals to:
        data/processed/patient_signals_asof=<as_of_date>.parquet
    (Name kept for compatibility with existing imports; contents are provider-level.)
    """
    DATA_PROCESSED.mkdir(parents=True, exist_ok=True)

    out_path = DATA_PROCESSED / f"patient_signals_asof={as_of_date}.parquet"
    signals.to_parquet(out_path, index=False)
    return out_path
