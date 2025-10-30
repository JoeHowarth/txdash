import os
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, List, Optional

import streamlit as st

from data import RunRow, format_duration, load_reports

DEFAULT_REPORTS_DIR = "workload-reports"

def _init_session_state() -> None:
    if "reports_dir" not in st.session_state:
        st.session_state["reports_dir"] = DEFAULT_REPORTS_DIR


def _aggregate_runs(rows: Iterable[RunRow]) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for row in rows:
        counts[row.workload_name] = counts.get(row.workload_name, 0) + 1
    return counts


def _filter_rows(
    rows: List[RunRow],
    days_back: int,
    workload_filter: Optional[str],
    search_text: str,
) -> List[RunRow]:
    if not rows:
        return []

    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=days_back) if days_back is not None else None
    search_lower = search_text.lower().strip()

    filtered: List[RunRow] = []
    for row in rows:
        if cutoff and row.start < cutoff:
            continue
        if workload_filter and workload_filter != "All" and row.workload_name != workload_filter:
            continue
        if search_lower:
            haystack = " ".join(
                [
                    row.workload_name,
                    row.gen_mode,
                    os.path.basename(row.file),
                    row.workload_config_hash,
                ]
            ).lower()
            if search_lower not in haystack:
                continue
        filtered.append(row)
    return sorted(filtered, key=lambda r: r.start, reverse=True)


def _build_table(rows: List[RunRow]) -> List[Dict[str, object]]:
    table: List[Dict[str, object]] = []
    for row in rows:
        table.append(
            {
                "Start": row.start.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M"),
                "Workload": row.workload_name,
                "Gen mode": row.gen_mode,
                "Target TPS": row.target_tps,
                "Achieved TPS": round(row.achieved_tps, 2),
                "Drop rate": f"{row.drop_rate * 100:.2f}%",
                "Duration": format_duration(row.duration_s),
                "Config hash": row.workload_config_hash[:12] if row.workload_config_hash else "",
                "Report file": os.path.basename(row.file),
            }
        )
    return table


def _select_run_label(row: RunRow) -> str:
    start = row.start.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M")
    return f"{start} | {row.workload_name} | {row.gen_mode} | {row.workload_config_hash[:8]}"


st.set_page_config(page_title="txgen reports overview", layout="wide")
_init_session_state()


with st.sidebar:
    st.header("Data source")
    reports_dir_input = st.text_input(
        "Reports folder",
        value=st.session_state["reports_dir"],
    )
    if reports_dir_input != st.session_state["reports_dir"]:
        st.session_state["reports_dir"] = reports_dir_input
    reload_clicked = st.button("Reload reports")
    if reload_clicked:
        load_reports.clear()

reports_dir = st.session_state["reports_dir"]
rows = load_reports(reports_dir)

if not rows:
    st.title("txgen report explorer")
    st.info("No reports found. Update the reports folder in the sidebar and reload.")
    st.stop()


st.title("txgen report explorer")
st.caption(f"Source folder: {os.path.abspath(reports_dir)}")

total_runs = len(rows)
unique_workloads = len({r.workload_name for r in rows})
latest_start = max(r.start for r in rows)
earliest_start = min(r.start for r in rows)

col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Runs loaded", total_runs)
with col2:
    st.metric("Workload groups", unique_workloads)
with col3:
    st.metric("Earliest run", earliest_start.astimezone(timezone.utc).strftime("%Y-%m-%d"))
with col4:
    st.metric("Latest run", latest_start.astimezone(timezone.utc).strftime("%Y-%m-%d"))

st.subheader("Recent reports")

days_back = st.slider("Show runs from the last N days", min_value=1, max_value=90, value=30)
workload_options = ["All"] + sorted({r.workload_name for r in rows})
workload_filter = st.selectbox("Workload filter", options=workload_options)
search_text = st.text_input("Search (name, gen mode, file, hash)")

filtered_rows = _filter_rows(rows, days_back=days_back, workload_filter=workload_filter, search_text=search_text)
if not filtered_rows:
    st.warning("No reports match the current filters.")
else:
    max_rows = st.slider("Rows to show", min_value=10, max_value=200, value=min(50, len(filtered_rows)))
    table_data = _build_table(filtered_rows[:max_rows])
    st.dataframe(table_data, use_container_width=True, hide_index=True)

    selected_label = st.selectbox(
        "Open run in detail view",
        options=[_select_run_label(r) for r in filtered_rows],
        index=0,
    )
    label_to_row = {_select_run_label(r): r for r in filtered_rows}
    selected_row = label_to_row[selected_label]

    st.caption("Use the button below to jump to the detailed comparison page for the selected run.")
    if st.button("Go to Run Detail & Compare", use_container_width=False):
        query_params = st.query_params
        query_params["file"] = selected_row.file
        query_params["workload"] = selected_row.workload_name
        query_params["match"] = "name"
        st.switch_page("pages/1_Run_Detail_and_Compare.py")

st.subheader("Runs per workload")
counts = _aggregate_runs(filtered_rows if filtered_rows else rows)
chart_data = {"Workload": list(counts.keys()), "Runs": list(counts.values())}
if counts:
    st.bar_chart(chart_data, x="Workload", y="Runs", height=260, use_container_width=True)
else:
    st.info("No runs available for charting.")
