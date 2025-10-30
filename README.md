Demo Streamlit dashboard for exploring txgen reports

Quick start (basic venv)
- Ensure Python 3.9+ and pip are available.
- Create a virtualenv (from repo root):
  `python3 -m venv .venv`
- Activate it:
  `source .venv/bin/activate`
- Install dependencies:
  `pip install -r examples/txdash/requirements.txt`
- Launch the app:
  `streamlit run examples/txdash/app.py`

Notes
- The app reads JSON reports produced by txgen (Report::to_json_file).
- It operates purely offline: no live Prometheus queries.
- Point it at the folder containing `*-report-*.json` files (default: `reports`).
- Comparison focuses on runs from the same workload group for clarity.
