import pandas as pd
import panel as pn
import hvplot.pandas
import os
import holoviews as hv

pn.extension('tabulator')



# Load the risk-scored panel
def load_panel():
    # Local paths for panel convert / dev (try a few common roots)
    candidate_paths = [
        "../../data/processed/provider_panel_risk_scored.csv",
        "../data/processed/provider_panel_risk_scored.csv",
        "data/processed/provider_panel_risk_scored.csv",
    ]
    for path in candidate_paths:
        try:
            return pd.read_csv(path)
        except Exception:
            pass

    # Browser (Pyodide) → load over HTTP from docs/
    try:
        from pyodide.http import open_url
        f = open_url("provider_panel_risk_scored.csv")
        return pd.read_csv(f)
    except Exception as e:
        raise FileNotFoundError(
            "provider_panel_risk_scored.csv not found. "
            "Ensure docs/provider_panel_risk_scored.csv exists and is committed."
        ) from e

panel = load_panel()

# Sort providers by latest risk score (highest risk first)
# Risk-ranked provider list
risk_by_provider = (
    panel.sort_values("as_of_date")
         .groupby("provider_id")["provider_risk_score"]
         .last()
         .sort_values(ascending=False)
)

provider_ids_sorted = risk_by_provider.index.tolist()
provider_ids_sorted_str = [str(pid) for pid in provider_ids_sorted]

# Search widget (free text)
provider_search = pn.widgets.TextInput(
    name="Search Provider",
    placeholder="Type part of a provider_id…",
)

# Dropdown that will update based on search
provider_dropdown = pn.widgets.Select(
    name="Select Provider",
    options=provider_ids_sorted_str,
)

# Filter logic
@pn.depends(provider_search.param.value, watch=True)
def update_dropdown(search_text):
    if not search_text:
        provider_dropdown.options = provider_ids_sorted_str
    else:
        filtered = [pid for pid in provider_ids_sorted_str if search_text in pid]
        provider_dropdown.options = filtered if filtered else ["No matches"]

def provider_from_dropdown(value):
    try:
        pid = int(value)
    except:
        pid = provider_ids_sorted[0]
    return provider_view(pid)


def provider_view(pid):
    df = (
        panel[panel.provider_id == pid]
        .rename(columns={"as_of_date": "snapshot_dt"})
        .sort_values("snapshot_dt")
    )

    # Ensure datetime for plotting & spans
    df["snapshot_dt"] = pd.to_datetime(df["snapshot_dt"])

    # === Main trends ===
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

    # === Anomaly shading on risk chart ===
    anomaly_dates = df.loc[df["anomaly_total_flags"] > 0, "snapshot_dt"].unique()

    if len(anomaly_dates):
        spans = hv.Overlay([
            hv.VSpan(d - pd.Timedelta(days=0.5),
                     d + pd.Timedelta(days=0.5)).opts(alpha=0.15, color="orange")
            for d in anomaly_dates
        ])
        risk_plot = (risk_line * spans)
    else:
        risk_plot = risk_line

    # === Risk decomposition (latest snapshot) ===
    latest = df.iloc[-1]

    comp_df = pd.DataFrame({
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
    })

    risk_decomp_plot = comp_df.hvplot.bar(
        x="component",
        y="value",
        ylim=(0, 1),
        title="Risk Decomposition (Latest Snapshot)",
        width=1000,
        height=300,
        rot=45,
    )

    # === Historical summary table ===
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
        pn.pane.Markdown(f"## Provider {pid} Dashboard"),
        pn.Row(line_plot, risk_plot),
        pn.pane.Markdown("### Risk Decomposition"),
        risk_decomp_plot,
        pn.pane.Markdown("### Historical Summary"),
        summary_table,
    )




# Bind interactive component
dashboard = pn.Column(
    pn.pane.Markdown("# Provider Risk Dashboard"),
    pn.Row(provider_search, provider_dropdown),
    pn.bind(provider_from_dropdown, provider_dropdown),
)

dashboard.servable()
