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
      await micropip.install(['https://cdn.holoviz.org/panel/wheels/bokeh-3.8.1-py3-none-any.whl', 'https://cdn.holoviz.org/panel/1.8.4/dist/wheels/panel-1.8.4-py3-none-any.whl', 'pyodide-http', 'hvplot', 'pandas']);
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
    const [docs_json, render_items, root_ids] = await self.pyodide.runPythonAsync(`\nimport asyncio\n\nfrom panel.io.pyodide import init_doc, write_doc\n\ninit_doc()\n\nimport pandas as pd\nimport panel as pn\nimport hvplot.pandas\n\npn.extension('tabulator')\n\n# Load the risk-scored panel\nimport pandas as pd\n\ntry:\n    # Local path for build time (when running panel convert)\n    panel = pd.read_csv("../../data/processed/provider_panel_risk_scored.csv")\nexcept FileNotFoundError:\n    # Browser / GitHub Pages path (same folder as index.html)\n    panel = pd.read_csv("provider_panel_risk_scored.csv")\n\n\n# Prepare provider list for dropdown\nprovider_ids = sorted(panel['provider_id'].unique())\n\nprovider_selector = pn.widgets.Select(\n    name="Provider ID",\n    options=provider_ids,\n)\n\ndef provider_view(pid):\n    df = (\n        panel[panel.provider_id == pid]\n        .rename(columns={"as_of_date": "snapshot_dt"})\n        .sort_values("snapshot_dt")\n    )\n\n    line_plot = df.hvplot.line(\n        x="snapshot_dt",\n        y="mean_daily_claims_90d",\n        title=f"Provider {pid} \u2014 90d Claims Trend",\n        width=800,\n        height=300,\n    )\n\n    risk_plot = df.hvplot.line(\n        x="snapshot_dt",\n        y="provider_risk_score",\n        title=f"Provider {pid} \u2014 Risk Score Trend",\n        width=800,\n        height=300,\n        color="red",\n    )\n\n    summary_table = df[\n        [\n            "provider_id",\n            "snapshot_dt",\n            "provider_risk_score",\n            "risk_rank",\n            "anomaly_total_flags",\n            "claims_90d_vs_prev90d",\n            "days_since_last",\n        ]\n    ].sort_values("snapshot_dt")\n\n    return pn.Column(\n        pn.pane.Markdown(f"## Provider {pid} Dashboard"),\n        line_plot,\n        risk_plot,\n        pn.pane.Markdown("### Historical Summary"),\n        summary_table\n    )\n\n# Bind interactive component\ndashboard = pn.Column(\n    pn.pane.Markdown("# Provider Risk Dashboard"),\n    provider_selector,\n    pn.bind(provider_view, provider_selector),\n)\n\ndashboard.servable()\n\n\nawait write_doc()`)
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