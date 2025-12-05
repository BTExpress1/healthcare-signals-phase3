from __future__ import annotations

import pandas as pd
from .io import load_facts_daily


def build_patient_signals(as_of_date: str) -> pd.DataFrame:
    """
    Build provider-level signals for a single snapshot date using
    Phase 1 `facts_daily.parquet`.

    Snapshot window: all rows with fact_date <= as_of_date.

    Signals (MVP):
        - n_active_days:        number of distinct days with any claims
        - total_claims:         total claims volume up to snapshot
        - mean_daily_claims:    average claims per active day
        - first_activity_dt:    first day with activity
        - last_activity_dt:     most recent day with activity
        - days_since_last:      days from last_activity_dt to as_of_date
        - mean_zscore_allowed:  average zscore of allowed amounts
    """
    facts = load_facts_daily(as_of_date)
    facts = facts.copy()

    # Ensure datetime
    facts["fact_date"] = pd.to_datetime(facts["fact_date"])

    as_of_ts = pd.to_datetime(as_of_date)

    grouped = (
        facts
        .groupby("provider_id", as_index=False)
        .agg(
            n_active_days=("fact_date", "nunique"),
            total_claims=("claims_cnt", "sum"),
            mean_daily_claims=("claims_cnt", "mean"),
            first_activity_dt=("fact_date", "min"),
            last_activity_dt=("fact_date", "max"),
            mean_zscore_allowed=("zscore_allowed_amt", "mean"),
        )
    )

    grouped["days_since_last"] = (as_of_ts - grouped["last_activity_dt"]).dt.days

    cols = [
        "provider_id",
        "n_active_days",
        "total_claims",
        "mean_daily_claims",
        "first_activity_dt",
        "last_activity_dt",
        "days_since_last",
        "mean_zscore_allowed",
    ]
    return grouped[cols]
