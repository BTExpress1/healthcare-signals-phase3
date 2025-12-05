import pandas as pd
import panel as pn
import hvplot.pandas  # noqa: F401
import holoviews as hv

pn.extension("tabulator")


# --- Load the risk-scored panel --------------------------------------------
def load_panel():
    candidate_paths = [
        "../../data/processed/provider_panel_risk_scored.csv",
        "../data/processed/provider_panel_risk_scored.csv",
        "data/processed/provider_panel_risk_scored.csv",
    ]
    for path in candidate_paths:
        try:
            df = pd.read_csv(path)
            break
        except Exception:
            df = None

    if df is None:
        # Browser (Pyodide) → load over HTTP from docs/
        try:
            from pyodide.http import open_url

            f = open_url("provider_panel_risk_scored.csv")
            df = pd.read_csv(f)
        except Exception as e:
            raise FileNotFoundError(
                "provider_panel_risk_scored.csv not found. "
                "Ensure docs/provider_panel_risk_scored.csv exists and is committed."
            ) from e

    # Normalize types once here
    df["provider_id"] = (
        df["provider_id"]
        .astype(str)
        .str.replace(".0", "", regex=False)
        .str.replace(",", "", regex=False)
        .str.strip()
    )
    df["as_of_date"] = pd.to_datetime(df["as_of_date"])
    return df


provider_panel = load_panel()

# --- Global provider list (sorted by latest risk) --------------------------
risk_by_provider = (
    provider_panel.sort_values("as_of_date")
    .groupby("provider_id")["provider_risk_score"]
    .last()
    .sort_values(ascending=False)
)

provider_ids_sorted = risk_by_provider.index.tolist()  # already strings
provider_ids_sorted_str = provider_ids_sorted

# Search widget (free text)
provider_search = pn.widgets.TextInput(
    name="Search Provider",
    placeholder="Type part of a provider_id…",
)

# Dropdown that will update based on search
provider_dropdown = pn.widgets.Select(
    name="Select Provider",
    options=provider_ids_sorted_str,
    value=provider_ids_sorted_str[0] if provider_ids_sorted_str else None,
)


# Filter logic
@pn.depends(provider_search.param.value, watch=True)
def update_dropdown(search_text):
    if not search_text:
        provider_dropdown.options = provider_ids_sorted_str
    else:
        filtered = [pid for pid in provider_ids_sorted_str if search_text in pid]
        provider_dropdown.options = filtered or provider_ids_sorted_str
        if provider_dropdown.value not in provider_dropdown.options:
            provider_dropdown.value = provider_dropdown.options[0]


# --- Per-provider views -----------------------------------------------------
def provider_view(pid):
    if not pid:
        return pn.pane.Markdown("### Select a provider to see their history.")

    pid = str(pid).strip()
    df = (
        provider_panel[provider_panel["provider_id"] == pid]
        .rename(columns={"as_of_date": "snapshot_dt"})
        .sort_values("snapshot_dt")
    )

    if df.empty:
        return pn.pane.Markdown(f"### No data available for provider {pid}")

    df["snapshot_dt"] = pd.to_datetime(df["snapshot_dt"])

    line_plot = df.hvplot.line(
        x="snapshot_dt",
        y="mean_daily_claims_90d",
        title=f"Provider {pid} — 90d Claims Trend",
        width=500,
        height=320,
    ).opts(xrotation=45, xticks=6)

    risk_line = df.hvplot.line(
        x="snapshot_dt",
        y="provider_risk_score",
        title=f"Provider {pid} — Risk Score Trend",
        width=500,
        height=320,
        color="red",
    ).opts(xrotation=45, xticks=6)

    anomaly_dates = df.loc[df["anomaly_total_flags"] > 0, "snapshot_dt"].unique()

    if len(anomaly_dates):
        spans = hv.Overlay(
            [
                hv.VSpan(
                    d - pd.Timedelta(days=0.5),
                    d + pd.Timedelta(days=0.5),
                ).opts(alpha=0.15, color="orange")
                for d in anomaly_dates
            ]
        )
        risk_plot = risk_line * spans
    else:
        risk_plot = risk_line

    latest = df.iloc[-1]

    comp_df = pd.DataFrame(
        {
            "component": [
                "Isolation Forest",
                "LOF",
                "Z-score Anomalies",
                "Momentum (Claims 90d Δ)",
                "Recency",
                "Z-score Shift",
            ],
            "value": [
                latest.get("iforest_norm", 0.0),
                latest.get("lof_norm", 0.0),
                latest.get("flags_norm", 0.0),
                latest.get("momentum_norm", 0.0),
                latest.get("recency_norm", 0.0),
                latest.get("zscore_shift_norm", 0.0),
            ],
        }
    )

    risk_decomp_plot = comp_df.hvplot.bar(
        x="component",
        y="value",
        ylim=(0, 1),
        title="Risk Decomposition (Latest Snapshot)",
        width=1000,
        height=300,
        rot=45,
    )

    summary_table = df[
        [
            "provider_id",
            "snapshot_dt",
            "provider_risk_score",
            "risk_rank",
            "anomaly_total_flags",
            "claims_90d_vs_prev90d",
            "days_since_last",
        ]
    ].sort_values("snapshot_dt")

    return pn.Column(
        pn.Row(line_plot, risk_plot),
        pn.pane.Markdown("### Risk Decomposition"),
        risk_decomp_plot,
        pn.pane.Markdown("### Historical Summary"),
        summary_table,
    )


def stability_view(pid):
    if not pid:
        return pn.pane.Markdown("### Stability / Volatility\n\nSelect a provider.")

    pid = str(pid).strip()
    df = (
        provider_panel[provider_panel["provider_id"] == pid]
        .rename(columns={"as_of_date": "snapshot_dt"})
        .sort_values("snapshot_dt")
    )

    if df.empty:
        return pn.pane.Markdown(
            f"### Stability / Volatility\n\nNo data available for provider {pid}."
        )

    df["snapshot_dt"] = pd.to_datetime(df["snapshot_dt"])

    latest = df.iloc[-1]

    vol_90 = latest.get("claims_std_90d", float("nan"))
    vol_180 = latest.get("claims_std_180d", float("nan"))
    vol_365 = latest.get("claims_std_365d", float("nan"))

    stability_markdown = f"""
### Stability / Volatility (Latest Snapshot)

- **90d volatility (claims_std_90d)**: {vol_90:.2f}
- **180d volatility (claims_std_180d)**: {vol_180:.2f}
- **365d volatility (claims_std_365d)**: {vol_365:.2f}

Lower volatility ⇒ more stable utilization pattern.
"""

    return pn.pane.Markdown(stability_markdown)


# --- Top Risk Providers table (left side) ----------------------------------
TOP_N = 10

_latest = (
    provider_panel.sort_values("as_of_date")
    .groupby("provider_id")
    .tail(1)
)

top_risk_df = (
    _latest.sort_values("provider_risk_score", ascending=False)
    .head(TOP_N)[
        [
            "provider_id",
            "provider_risk_score",
            "risk_rank",
            "anomaly_total_flags",
            "days_since_last",
        ]
    ]
    .reset_index(drop=True)
)

# Ensure ids are strings here too (prevents numeric spinners, thousands separators)
top_risk_df["provider_id"] = (
    top_risk_df["provider_id"]
    .astype(str)
    .str.replace(".0", "", regex=False)
    .str.replace(",", "", regex=False)
    .str.strip()
)

top_risk_table = pn.widgets.Tabulator(
    top_risk_df,
    selectable=True,   # rows are still visually selectable
    height=500,
    width=350,
)


def _on_top_risk_click(event):
    # event.row is the integer row index in top_risk_df
    row_idx = event.row
    if not (0 <= row_idx < len(top_risk_df)):
        return

    pid = str(top_risk_df.iloc[row_idx]["provider_id"]).strip()

    provider_search.value = pid
    if pid in provider_dropdown.options:
        provider_dropdown.value = pid
    elif provider_dropdown.options:
        provider_dropdown.options = [pid] + list(provider_dropdown.options)
        provider_dropdown.value = pid
    else:
        provider_dropdown.options = [pid]
        provider_dropdown.value = pid

top_risk_table.on_click(_on_top_risk_click)

# --- Layout -----------------------------------------------------------------
left_panel = pn.Column(
    pn.pane.Markdown("### Top Risk Providers"),
    top_risk_table,
)

stability_section = pn.Accordion(
    ("Stability / Volatility", pn.bind(stability_view, provider_dropdown)),
    active=[],  # collapsed by default
)

right_panel = pn.Column(
    pn.pane.Markdown("# Provider Risk Dashboard"),
    pn.Row(provider_search, provider_dropdown),
    pn.bind(provider_view, provider_dropdown),
    stability_section,
)

dashboard = pn.Row(left_panel, right_panel)
dashboard.servable()
