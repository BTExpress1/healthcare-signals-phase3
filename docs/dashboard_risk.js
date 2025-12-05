importScripts("https://cdn.jsdelivr.net/pyodide/v0.28.2/full/pyodide.js");

function sendPatch(patch, buffers, msg_id) {
  self.postMessage({
    type: 'patch',
    patch: patch,
    buffers: buffers
  })
}

async function startApplication() {
  console.log("Loading pyodide...");
  self.postMessage({type: 'status', msg: 'Loading pyodide'})
  self.pyodide = await loadPyodide();
  self.pyodide.globals.set("sendPatch", sendPatch);
  console.log("Loaded pyodide!");
  const data_archives = [];
  for (const archive of data_archives) {
    let zipResponse = await fetch(archive);
    let zipBinary = await zipResponse.arrayBuffer();
    self.postMessage({type: 'status', msg: `Unpacking ${archive}`})
    self.pyodide.unpackArchive(zipBinary, "zip");
  }
  await self.pyodide.loadPackage("micropip");
  self.postMessage({type: 'status', msg: `Installing environment`})
  try {
    await self.pyodide.runPythonAsync(`
      import micropip
      await micropip.install(['https://cdn.holoviz.org/panel/wheels/bokeh-3.8.1-py3-none-any.whl', 'https://cdn.holoviz.org/panel/1.8.4/dist/wheels/panel-1.8.4-py3-none-any.whl', 'pyodide-http', 'holoviews', 'hvplot', 'pandas']);
    `);
  } catch(e) {
    console.log(e)
    self.postMessage({
      type: 'status',
      msg: `Error while installing packages`
    });
  }
  console.log("Environment loaded!");
  self.postMessage({type: 'status', msg: 'Executing code'})
  try {
    const [docs_json, render_items, root_ids] = await self.pyodide.runPythonAsync(`\nimport asyncio\n\nfrom panel.io.pyodide import init_doc, write_doc\n\ninit_doc()\n\nimport pandas as pd\nimport panel as pn\nimport hvplot.pandas\nimport os\nimport holoviews as hv\n\npn.extension('tabulator')\n\n\n\n# Load the risk-scored panel\ndef load_panel():\n    # Local paths for panel convert / dev (try a few common roots)\n    candidate_paths = [\n        "../../data/processed/provider_panel_risk_scored.csv",\n        "../data/processed/provider_panel_risk_scored.csv",\n        "data/processed/provider_panel_risk_scored.csv",\n    ]\n    for path in candidate_paths:\n        try:\n            return pd.read_csv(path)\n        except Exception:\n            pass\n\n    # Browser (Pyodide) \u2192 load over HTTP from docs/\n    try:\n        from pyodide.http import open_url\n        f = open_url("provider_panel_risk_scored.csv")\n        return pd.read_csv(f)\n    except Exception as e:\n        raise FileNotFoundError(\n            "provider_panel_risk_scored.csv not found. "\n            "Ensure docs/provider_panel_risk_scored.csv exists and is committed."\n        ) from e\n\npanel = load_panel()\n\n# Sort providers by latest risk score (highest risk first)\n# Risk-ranked provider list\nrisk_by_provider = (\n    panel.sort_values("as_of_date")\n         .groupby("provider_id")["provider_risk_score"]\n         .last()\n         .sort_values(ascending=False)\n)\n\nprovider_ids_sorted = risk_by_provider.index.tolist()\nprovider_ids_sorted_str = [str(pid) for pid in provider_ids_sorted]\n\n# Search widget (free text)\nprovider_search = pn.widgets.TextInput(\n    name="Search Provider",\n    placeholder="Type part of a provider_id\u2026",\n)\n\n# Dropdown that will update based on search\nprovider_dropdown = pn.widgets.Select(\n    name="Select Provider",\n    options=provider_ids_sorted_str,\n)\n\n# Filter logic\n@pn.depends(provider_search.param.value, watch=True)\ndef update_dropdown(search_text):\n    if not search_text:\n        provider_dropdown.options = provider_ids_sorted_str\n    else:\n        filtered = [pid for pid in provider_ids_sorted_str if search_text in pid]\n        provider_dropdown.options = filtered if filtered else ["No matches"]\n\ndef provider_from_dropdown(value):\n    try:\n        pid = int(value)\n    except:\n        pid = provider_ids_sorted[0]\n    return provider_view(pid)\n\n\ndef provider_view(pid):\n    df = (\n        panel[panel.provider_id == pid]\n        .rename(columns={"as_of_date": "snapshot_dt"})\n        .sort_values("snapshot_dt")\n    )\n\n    # Ensure datetime for plotting & spans\n    df["snapshot_dt"] = pd.to_datetime(df["snapshot_dt"])\n\n    # === Main trends ===\n    line_plot = df.hvplot.line(\n        x="snapshot_dt",\n        y="mean_daily_claims_90d",\n        title=f"Provider {pid} \u2014 90d Claims Trend",\n        width=500,\n        height=320,\n    ).opts(xrotation=45, xticks=6)\n\n    risk_line = df.hvplot.line(\n        x="snapshot_dt",\n        y="provider_risk_score",\n        title=f"Provider {pid} \u2014 Risk Score Trend",\n        width=500,\n        height=320,\n        color="red",\n    ).opts(xrotation=45, xticks=6)\n\n    # === Anomaly shading on risk chart ===\n    anomaly_dates = df.loc[df["anomaly_total_flags"] > 0, "snapshot_dt"].unique()\n\n    if len(anomaly_dates):\n        spans = hv.Overlay([\n            hv.VSpan(d - pd.Timedelta(days=0.5),\n                     d + pd.Timedelta(days=0.5)).opts(alpha=0.15, color="orange")\n            for d in anomaly_dates\n        ])\n        risk_plot = (risk_line * spans)\n    else:\n        risk_plot = risk_line\n\n    # === Risk decomposition (latest snapshot) ===\n    latest = df.iloc[-1]\n\n    comp_df = pd.DataFrame({\n        "component": [\n            "Isolation Forest",\n            "LOF",\n            "Z-score Anomalies",\n            "Momentum (Claims 90d \u0394)",\n            "Recency",\n            "Z-score Shift",\n        ],\n        "value": [\n            latest.get("iforest_norm", 0.0),\n            latest.get("lof_norm", 0.0),\n            latest.get("flags_norm", 0.0),\n            latest.get("momentum_norm", 0.0),\n            latest.get("recency_norm", 0.0),\n            latest.get("zscore_shift_norm", 0.0),\n        ],\n    })\n\n    risk_decomp_plot = comp_df.hvplot.bar(\n        x="component",\n        y="value",\n        ylim=(0, 1),\n        title="Risk Decomposition (Latest Snapshot)",\n        width=1000,\n        height=300,\n        rot=45,\n    )\n\n    # === Stability / Volatility snapshot (latest window std devs) ===\n    vol_90 = latest.get("claims_std_90d", float("nan"))\n    vol_180 = latest.get("claims_std_180d", float("nan"))\n    vol_365 = latest.get("claims_std_365d", float("nan"))\n\n    stability_markdown = f"""\n    ### Stability / Volatility (Latest Snapshot)\n\n    - **90d volatility (claims_std_90d)**: {vol_90:.2f}\n    - **180d volatility (claims_std_180d)**: {vol_180:.2f}\n    - **365d volatility (claims_std_365d)**: {vol_365:.2f}\n\n    Lower volatility \u21d2 more stable utilization pattern.\n    """\n\n    stability_panel = pn.pane.Markdown(stability_markdown)\n\n    # === Historical summary table ===\n    summary_table = df[\n        [\n            "provider_id",\n            "snapshot_dt",\n            "provider_risk_score",\n            "risk_rank",\n            "anomaly_total_flags",\n            "claims_90d_vs_prev90d",\n            "days_since_last",\n        ]\n    ].sort_values("snapshot_dt")\n\n    return pn.Column(\n        pn.pane.Markdown(f"## Provider {pid} Dashboard"),\n        stability_panel,\n        pn.Row(line_plot, risk_plot),\n        pn.pane.Markdown("### Risk Decomposition"),\n        risk_decomp_plot,\n        pn.pane.Markdown("### Historical Summary"),\n        summary_table,\n    )\n\n\n\n\n# === Top Risk Providers table (left side) ===\nTOP_N = 10\n\n_latest = (\n    panel.sort_values("as_of_date")\n         .groupby("provider_id")\n         .tail(1)\n)\n\ntop_risk_df = (\n    _latest.sort_values("provider_risk_score", ascending=False)\n           .head(TOP_N)[\n               [\n                   "provider_id",\n                   "provider_risk_score",\n                   "risk_rank",\n                   "anomaly_total_flags",\n                   "days_since_last",\n               ]\n           ]\n           .reset_index(drop=True)\n)\n\ntop_risk_table = pn.widgets.Tabulator(\n    top_risk_df,\n    selectable=True,\n    height=500,\n    width=350,\n)\n\ndef _on_top_risk_select(event):\n    if not event.new:\n        return\n    row_idx = event.new[0]\n    raw_id = top_risk_df.iloc[row_idx]["provider_id"]\n    try:\n        pid_int = int(raw_id)\n    except (TypeError, ValueError):\n        pid_int = provider_ids_sorted[0]\n\n    pid = str(pid_int)\n\n    # Update search text and dropdown selection\n    provider_search.value = pid\n    provider_dropdown.value = pid  # triggers provider_from_dropdown\n\n\n\ntop_risk_table.param.watch(_on_top_risk_select, "selection")\n\n# Bind interactive component\nleft_panel = pn.Column(\n    pn.pane.Markdown("### Top Risk Providers"),\n    top_risk_table,\n)\n\nright_panel = pn.Column(\n    pn.pane.Markdown("# Provider Risk Dashboard"),\n    pn.Row(provider_search, provider_dropdown),\n    pn.bind(provider_from_dropdown, provider_dropdown),\n)\n\n\ndashboard = pn.Row(left_panel, right_panel)\n\ndashboard.servable()\n\n\n\nawait write_doc()`)
    self.postMessage({
      type: 'render',
      docs_json: docs_json,
      render_items: render_items,
      root_ids: root_ids
    })
  } catch(e) {
    const traceback = `${e}`
    const tblines = traceback.split('\n')
    self.postMessage({
      type: 'status',
      msg: tblines[tblines.length-2]
    });
    throw e
  }
}

self.onmessage = async (event) => {
  const msg = event.data
  if (msg.type === 'rendered') {
    self.pyodide.runPythonAsync(`
    from panel.io.state import state
    from panel.io.pyodide import _link_docs_worker

    _link_docs_worker(state.curdoc, sendPatch, setter='js')
    `)
  } else if (msg.type === 'patch') {
    self.pyodide.globals.set('patch', msg.patch)
    self.pyodide.runPythonAsync(`
    from panel.io.pyodide import _convert_json_patch
    state.curdoc.apply_json_patch(_convert_json_patch(patch), setter='js')
    `)
    self.postMessage({type: 'idle'})
  } else if (msg.type === 'location') {
    self.pyodide.globals.set('location', msg.location)
    self.pyodide.runPythonAsync(`
    import json
    from panel.io.state import state
    from panel.util import edit_readonly
    if state.location:
        loc_data = json.loads(location)
        with edit_readonly(state.location):
            state.location.param.update({
                k: v for k, v in loc_data.items() if k in state.location.param
            })
    `)
  }
}

startApplication()