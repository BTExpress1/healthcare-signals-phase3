Healthcare Signals Dashboard (Phase 3)
Provider-Level Risk & Anomaly Detection with Interactive Analytics

This project implements a full provider risk-scoring pipeline and an interactive, browser-based Healthcare Signals Dashboard built with Panel, Holoviews, Pyodide, and GitHub Pages. Users can explore claims patterns, anomalies, utilization momentum, recency signals, and a composite risk score for every provider in the dataset—directly in their browser with no Python environment required.

👉 Live Dashboard: https://btexpress1.github.io/healthcare-signals-phase3/

📌 Project Overview

Healthcare claims are noisy and often difficult to interpret at scale. This project extracts meaningful patterns and risk signals from longitudinal provider claims histories to surface providers who exhibit:

Unusual utilization spikes or drops

Statistically abnormal behavior (IForest, LOF)

Momentum shifts in rolling 90-day windows

Recency-based inactivity patterns

Rule-based anomalies and z-score shifts

These signals combine into a composite Provider Risk Score, then exposed in a fully interactive dashboard engineered to run entirely on GitHub Pages via Pyodide (WebAssembly).

🚀 Key Features
🔍 Multivariate Provider Risk Scoring

Ensemble of anomaly signals:

Isolation Forest

Local Outlier Factor

Z-score anomaly flags

Claims momentum

Recency signal

Z-shift drift detector

All components normalized and combined into a single interpretable score.

📈 Multi-Metric Trend Visualization

A unified timeline showing:

90-day rolling claims trend

Provider risk score (dual axis)

Shaded anomaly windows

Markers for flagged days

Enables fast pattern recognition across multiple signals.

🧠 Explainable Risk Decomposition

Normalized bar chart showing contribution of each signal

Textual summary highlighting the dominant driver of risk

Helps translate model output into an actionable narrative

🏅 Top N Provider Explorer

Ranked list of highest-risk providers

Click to instantly slice the dashboard

Clean UX with read-only table and 1-based rank

📊 Provider Historical Summary

For every provider:

Snapshot-level metrics

Risk score and percentile rank

Anomaly flags

Claims momentum

Days since last claim

🌐 Zero-Backend Deployment (Pyodide)

Runs entirely inside the browser

No backend server, API, or Python installation needed

Compatible with any device that can open a webpage

🛠️ Technical Stack
Area	Technologies
Data Processing	Pandas, NumPy
Anomaly Detection	Isolation Forest, LOF, z-scores
Visualization	Panel, Holoviews, hvPlot
Web Runtime	Pyodide (WebAssembly)
Deployment	GitHub Pages
Packaging	panel convert --to pyodide-worker

📁 Project Structure
healthcare-signals-phase3/
│
├── src/healthcare_signals/
│   ├── dashboard_risk.py          # Main Panel + Pyodide dashboard
│   ├── risk_scoring.py            # Risk scoring pipeline
│   └── ...
│
├── data/processed/
│   └── provider_panel_risk_scored.csv
│
├── docs/                          # GitHub Pages deployment root
│   ├── index.html
│   ├── dashboard_risk.html
│   ├── dashboard_risk.js
│   ├── provider_panel_risk_scored.csv
│   └── favicon.png
│
└── README.md

⚙️ Running Locally (Optional)
1. Install dependencies
pip install panel holoviews hvplot numpy pandas scikit-learn

2. Serve the dashboard locally
panel serve src/healthcare_signals/dashboard_risk.py

This launches the dashboard in a local Python server environment (non-Pyodide).

📘 How the Provider Risk Score Works
Component	What it Detects
Isolation Forest	Irregular global patterns
Local Outlier Factor	Local density anomalies
Z-score anomaly flags	Abrupt deviations
Momentum (90-day delta)	Trend acceleration/decline
Recency	Inactivity or drop-off
Z-shift	Drift-based anomaly

The final score is a percentile-normalized, explainable signal engineered for interpretability rather than opacity.

🎯 Why This Project Matters

Healthcare operations teams often struggle to detect behavioral shifts in provider utilization until long after they occur. This dashboard demonstrates how machine learning + time-series analytics + lightweight deployment can:

Surface early warnings

Support audit prioritization

Enable proactive provider outreach

Turn raw claim histories into actionable intelligence

It showcases practical, production-minded data science:
signal engineering → anomaly modeling → interactive visualization → zero-backend deployment.

📬 Questions or feedback?

I’m always happy to discuss:

Healthcare analytics

ML-based anomaly detection

Pyodide/Panel deployment

End-to-end DS engineering workflows

Feel free to reach out or open issues/discussions.