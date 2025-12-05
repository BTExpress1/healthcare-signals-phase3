import pandas as pd
import numpy as np

def normalize(series):
    """Min-max normalize a pandas Series."""
    return (series - series.min()) / (series.max() - series.min() + 1e-9)

def percentile_rank(series):
    """Convert raw values to percentile ranks."""
    return series.rank(pct=True)

def compute_risk_score(df):
    """
    Compute a unified provider risk score using weighted anomaly inputs.
    Requires columns:
        - iforest_score
        - lof_score
        - anomaly_total_flags
        - claims_90d_vs_prev90d
        - zscore_90d_vs_prev90d
        - days_since_last
    """

    # Normalize signals (higher = riskier)
    df['iforest_norm'] = normalize(-df['iforest_score'])        # decision_function: lower = worse
    df['lof_norm'] = normalize(-df['lof_score'])                 # more negative = worse
    df['flags_norm'] = normalize(df['anomaly_total_flags'])
    df['momentum_norm'] = normalize(df['claims_90d_vs_prev90d'])
    df['zscore_shift_norm'] = normalize(df['zscore_90d_vs_prev90d'])
    df['recency_norm'] = normalize(df['days_since_last'])

    # Composite weights (tunable)
    weights = {
        'iforest_norm': 0.30,
        'lof_norm': 0.25,
        'flags_norm': 0.20,
        'momentum_norm': 0.10,
        'zscore_shift_norm': 0.10,
        'recency_norm': 0.05,
    }

    df['provider_risk_raw'] = sum(
        df[col] * w for col, w in weights.items()
    )

    # Percentile rank for interpretability
    df['provider_risk_score'] = percentile_rank(df['provider_risk_raw'])

    return df
