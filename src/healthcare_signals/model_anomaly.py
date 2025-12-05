import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.neighbors import LocalOutlierFactor

def run_isolation_forest(df, feature_cols, contamination=0.02, random_state=42):
    model = IsolationForest(
        contamination=contamination,
        n_estimators=400,
        max_samples='auto',
        random_state=random_state
    )
    scores = model.fit_predict(df[feature_cols])
    df['iforest_score'] = model.decision_function(df[feature_cols])
    df['iforest_flag'] = (scores == -1).astype(int)
    return df

def run_lof(df, feature_cols, n_neighbors=20, contamination=0.02):
    lof = LocalOutlierFactor(
        n_neighbors=n_neighbors,
        contamination=contamination
    )
    labels = lof.fit_predict(df[feature_cols])
    df['lof_score'] = lof.negative_outlier_factor_
    df['lof_flag'] = (labels == -1).astype(int)
    return df

def add_zscore_flags(df, cols, threshold=3.0):
    for col in cols:
        z = (df[col] - df[col].mean()) / df[col].std(ddof=0)
        df[f'{col}_z'] = z
        df[f'{col}_z_flag'] = (z.abs() > threshold).astype(int)
    return df

def combine_flags(df):
    flag_cols = [c for c in df.columns if c.endswith("_flag")]
    df['anomaly_total_flags'] = df[flag_cols].sum(axis=1)
    df['anomaly_rank'] = df['anomaly_total_flags'].rank(method='dense', ascending=False)
    return df
