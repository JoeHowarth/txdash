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

Views
- **Overview:** recent runs table with filters, per-workload counts, and quick navigation into details.
- **Run detail & compare:** pick a baseline run, match prior runs by workload name or exact workload-config hash, optionally fine-tune the set, and review deltas/flags.

Notes
- The app reads JSON reports produced by txgen (Report::to_json_file).
- It operates purely offline: no live Prometheus queries.
- Point it at the folder containing `*-report-*.json` files (default: `reports`).
- Comparison defaults to workload-name matching; switch to hash mode for identical configs or use the advanced filter to tweak manually.
