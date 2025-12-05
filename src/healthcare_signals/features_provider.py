from __future__ import annotations

from typing import Sequence, Optional

import numpy as np
import pandas as pd

from .io import load_facts_daily

# Default rolling windows in days
DEFAULT_WINDOWS: tuple[int, ...] = (30, 90, 180, 365)


def _window_slice(df: pd.DataFrame, as_of_ts: pd.Timestamp, days: int) -> pd.DataFrame:
    """Return rows within the last `days` days up to as_of_ts."""
    start = as_of_ts - pd.Timedelta(days=days - 1)
    mask = (df["date"] >= start) & (df["date"] <= as_of_ts)
    return df.loc[mask]


def _summarize_window(df_win: pd.DataFrame) -> pd.Series:
    """Aggregate per-provider behavior in a given window."""
    if df_win.empty:
        return pd.Series(
            {
                "n_active_days": 0,
                "total_claims": 0,
                "mean_daily_claims": 0.0,
                "mean_allowed_amt": 0.0,
                "mean_zscore_allowed": 0.0,
                "claims_std": 0.0,
                "zscore_std": 0.0,
            }
        )

    return pd.Series(
        {
            "n_active_days": df_win["date"].nunique(),
            "total_claims": float(df_win["claims_cnt"].sum()),
            "mean_daily_claims": float(df_win["claims_cnt"].mean()),
            "mean_allowed_amt": float(df_win["avg_allowed_amt"].mean()),
            "mean_zscore_allowed": float(df_win["zscore_allowed_amt"].mean()),
            "claims_std": float(df_win["claims_cnt"].std(ddof=0) or 0.0),
            "zscore_std": float(df_win["zscore_allowed_amt"].std(ddof=0) or 0.0),
        }
    )


def build_provider_panel_for_date(
    as_of_date: str | pd.Timestamp,
    facts_daily: Optional[pd.DataFrame] = None,
    windows: Sequence[int] = DEFAULT_WINDOWS,
) -> pd.DataFrame:
    """
    Build provider-level signals for a single snapshot date.

    Inputs
    ------
    as_of_date:
        Snapshot date (e.g. '2010-12-31') — history is truncated at this date.
    facts_daily:
        Daily provider facts (date, provider_id, claims_cnt, avg_allowed_amt, zscore_allowed_amt).
        If None, will be loaded via `load_facts_daily()`.
    windows:
        Rolling windows (in days) to compute behavior over (e.g. [30, 90, 365]).

    Output
    ------
    DataFrame with one row per provider_id and columns:
        - lifetime activity (first/last activity, days_since_last, etc.)
        - rolling window aggregates per window
        - simple trend features when 90/180 are present
        - as_of_date
    """
    if facts_daily is None:
        facts_daily = load_facts_daily()

    df = facts_daily.copy()
    df["date"] = pd.to_datetime(df["date"])
    as_of_ts = pd.to_datetime(as_of_date)

    # Restrict to history up to as_of_date
    df_hist = df[df["date"] <= as_of_ts].copy()
    if df_hist.empty:
        # No history yet → empty panel
        return pd.DataFrame(
            columns=[
                "provider_id",
                "first_activity_dt",
                "last_activity_dt",
                "total_claims_lifetime",
                "n_active_days_lifetime",
                "mean_zscore_lifetime",
                "days_since_last",
                "as_of_date",
            ]
        )

    # Lifetime-level aggregates
    base = (
        df_hist.groupby("provider_id", as_index=False)
        .agg(
            first_activity_dt=("date", "min"),
            last_activity_dt=("date", "max"),
            total_claims_lifetime=("claims_cnt", "sum"),
            n_active_days_lifetime=("date", "nunique"),
            mean_zscore_lifetime=("zscore_allowed_amt", "mean"),
        )
    )
    base["days_since_last"] = (as_of_ts - base["last_activity_dt"]).dt.days

    panel = base.copy()

    # Rolling windows
    for w in windows:
        df_win = _window_slice(df_hist, as_of_ts, w)
        if df_win.empty:
            continue

        agg_win = (
            df_win.groupby("provider_id")
            .apply(_summarize_window, include_groups=False)
            .reset_index()
        )

        rename_map = {
            col: f"{col}_{w}d"
            for col in agg_win.columns
            if col != "provider_id"
        }
        agg_win = agg_win.rename(columns=rename_map)
        panel = panel.merge(agg_win, on="provider_id", how="left")

    # Fill NaNs for window features with 0
    num_cols = panel.select_dtypes(include=["number"]).columns.tolist()
    panel[num_cols] = panel[num_cols].fillna(0)

    # Simple trend features when both 90d and 180d windows exist
    if {"total_claims_90d", "total_claims_180d"}.issubset(panel.columns):
        # previous 90d = last 180d minus last 90d
        panel["claims_90d_vs_prev90d"] = (
            panel["total_claims_90d"]
            - (panel["total_claims_180d"] - panel["total_claims_90d"])
        )

    if {"mean_zscore_allowed_90d", "mean_zscore_allowed_180d"}.issubset(panel.columns):
        panel["zscore_90d_vs_prev90d"] = (
            panel["mean_zscore_allowed_90d"]
            - panel["mean_zscore_allowed_180d"]
        )

    panel["as_of_date"] = as_of_ts.normalize()
    return panel


def build_provider_panel_over_range(
    snapshot_dates: Sequence[str | pd.Timestamp],
    facts_daily: Optional[pd.DataFrame] = None,
    windows: Sequence[int] = DEFAULT_WINDOWS,
) -> pd.DataFrame:
    """
    Build a concatenated provider panel for multiple snapshot dates.

    Each snapshot uses only history up to that as_of_date.
    """
    if facts_daily is None:
        facts_daily = load_facts_daily()

    panels = []
    for d in snapshot_dates:
        p = build_provider_panel_for_date(d, facts_daily=facts_daily, windows=windows)
        if not p.empty:
            panels.append(p)

    if not panels:
        return pd.DataFrame()

    return pd.concat(panels, ignore_index=True)
