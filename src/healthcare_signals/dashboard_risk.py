import pandas as pd
import panel as pn
import hvplot.pandas

pn.extension('tabulator')

# Load the risk-scored panel
panel = pd.read_csv("../../data/processed/provider_panel_risk_scored.csv")

# Prepare provider list for dropdown
provider_ids = sorted(panel['provider_id'].unique())

provider_selector = pn.widgets.Select(
    name="Provider ID",
    options=provider_ids,
)

def provider_view(pid):
    df = (
        panel[panel.provider_id == pid]
        .rename(columns={"as_of_date": "snapshot_dt"})
        .sort_values("snapshot_dt")
    )

    line_plot = df.hvplot.line(
        x="snapshot_dt",
        y="mean_daily_claims_90d",
        title=f"Provider {pid} — 90d Claims Trend",
        width=800,
        height=300,
    )

    risk_plot = df.hvplot.line(
        x="snapshot_dt",
        y="provider_risk_score",
        title=f"Provider {pid} — Risk Score Trend",
        width=800,
        height=300,
        color="red",
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
        pn.pane.Markdown(f"## Provider {pid} Dashboard"),
        line_plot,
        risk_plot,
        pn.pane.Markdown("### Historical Summary"),
        summary_table
    )

# Bind interactive component
dashboard = pn.Column(
    pn.pane.Markdown("# Provider Risk Dashboard"),
    provider_selector,
    pn.bind(provider_view, provider_selector),
)

dashboard.servable()
