import os
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, List, Optional, Tuple

import streamlit as st

from data import (
    DEFAULT_REPORTS_DIR,
    RunRow,
    compute_version_bounds,
    format_duration,
    format_version_label,
    load_reports,
)

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


def _build_table(rows: List[RunRow], version_bounds: Dict[str, Dict[str, datetime]]) -> List[Dict[str, object]]:
    table: List[Dict[str, object]] = []
    for row in rows:
        version = row.client_version or "Unknown"
        table.append(
            {
                "Start": row.start.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M"),
                "Workload": row.workload_name,
                "Gen mode": row.gen_mode,
                "Client version": format_version_label(version, version_bounds),
                "Target TPS": row.target_tps,
                "Achieved TPS": round(row.achieved_tps, 2),
                "Drop rate": f"{row.drop_rate * 100:.2f}%",
                "Duration": format_duration(row.duration_s),
                "Config hash": row.workload_config_hash[:12] if row.workload_config_hash else "",
                "Report file": os.path.basename(row.file),
            }
        )
    return table


def _select_run_label(row: RunRow, version_bounds: Dict[str, Dict[str, datetime]]) -> str:
    start = row.start.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M")
    version = format_version_label(row.client_version or "Unknown", version_bounds)
    config_hash = row.workload_config_hash[:8] if row.workload_config_hash else "nohash"
    return f"{start} | {row.workload_name} | {row.gen_mode} | {version} | {config_hash}"


def _group_rows_by_version(rows: List[RunRow]) -> List[Tuple[str, List[RunRow]]]:
    version_map: Dict[str, List[RunRow]] = {}
    for row in rows:
        version = row.client_version or "Unknown"
        version_map.setdefault(version, []).append(row)
    sentinel = datetime.min.replace(tzinfo=timezone.utc)
    for version_rows in version_map.values():
        version_rows.sort(key=lambda r: r.start, reverse=True)
    return sorted(
        version_map.items(),
        key=lambda item: item[1][0].start if item[1] else sentinel,
        reverse=True,
    )


def _version_summary_rows(
    grouped_versions: List[Tuple[str, List[RunRow]]],
    version_bounds: Dict[str, Dict[str, datetime]],
) -> List[Dict[str, object]]:
    summary: List[Dict[str, object]] = []
    for version, rows in grouped_versions:
        if not rows:
            continue
        latest = rows[0].start.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M")
        summary.append(
            {
                "Client version": format_version_label(version, version_bounds),
                "Runs": len(rows),
                "Distinct workloads": len({r.workload_name for r in rows}),
                "Latest run": latest,
                "Avg achieved TPS": round(sum(r.achieved_tps for r in rows) / len(rows), 2),
                "Avg drop rate": f"{(sum(r.drop_rate for r in rows) / len(rows)) * 100:.2f}%",
            }
        )
    return summary


st.set_page_config(page_title="Txgen Reports Overview", layout="wide")
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
version_bounds = compute_version_bounds(rows)

if not rows:
    st.title("Txgen Report Explorer")
    st.info("No reports found. Update the reports folder in the sidebar and reload.")
    st.stop()


st.title("Txgen Report Explorer")
st.caption(f"Source folder: {os.path.abspath(reports_dir)}")

total_runs = len(rows)
unique_workloads = len({r.workload_name for r in rows})
unique_versions = len(version_bounds)
latest_start = max(r.start for r in rows)
earliest_start = min(r.start for r in rows)

metrics = st.columns(5)
with metrics[0]:
    st.metric("Runs loaded", total_runs)
with metrics[1]:
    st.metric("Workload groups", unique_workloads)
with metrics[2]:
    st.metric("Client versions", unique_versions)
with metrics[3]:
    st.metric("Earliest run", earliest_start.astimezone(timezone.utc).strftime("%Y-%m-%d"))
with metrics[4]:
    st.metric("Latest run", latest_start.astimezone(timezone.utc).strftime("%Y-%m-%d"))

st.subheader("Recent reports")

days_back = st.slider("Show runs from the last N days", min_value=1, max_value=90, value=30)
workload_options = ["All"] + sorted({r.workload_name for r in rows})
workload_filter = st.selectbox("Workload filter", options=workload_options)
search_text = st.text_input("Search (name, gen mode, file, hash)")

filtered_rows = _filter_rows(rows, days_back=days_back, workload_filter=workload_filter, search_text=search_text)

if filtered_rows:
    version_latest: Dict[str, datetime] = {}
    for row in filtered_rows:
        version = row.client_version or "Unknown"
        latest = version_latest.get(version)
        if latest is None or row.start > latest:
            version_latest[version] = row.start
    version_options = sorted(version_latest.keys(), key=lambda v: version_latest[v], reverse=True)
    version_filter = st.multiselect(
        "Client version filter",
        options=version_options,
        default=version_options,
        help="Select one or more client versions (leave empty to view all).",
        format_func=lambda v: format_version_label(v, version_bounds),
    )
    if version_filter:
        filtered_rows = [row for row in filtered_rows if (row.client_version or "Unknown") in version_filter]

if not filtered_rows:
    st.warning("No reports match the current filters.")
else:
    version_tab, list_tab = st.tabs(["By version", "Flat list"])

    with version_tab:
        grouped_versions = _group_rows_by_version(filtered_rows)
        if not grouped_versions:
            st.info("No client versions to display.")
        else:
            summary_rows = _version_summary_rows(grouped_versions, version_bounds)
            chart_cols = st.columns((3, 2))
            with chart_cols[0]:
                st.caption("Per-version snapshot (sorted by most recent run)")
                st.dataframe(summary_rows, use_container_width=True, hide_index=True)
            with chart_cols[1]:
                st.caption("Runs per client version")
                chart_data = {
                    "Client version": [
                        format_version_label(item[0], version_bounds) for item in grouped_versions
                    ],
                    "Runs": [len(item[1]) for item in grouped_versions],
                }
                st.bar_chart(chart_data, x="Client version", y="Runs", height=260, use_container_width=True)

            rows_max = max(1, len(filtered_rows))
            rows_per_version = st.slider(
                "Rows to list per version",
                min_value=1,
                max_value=max(25, rows_max),
                value=min(25, rows_max),
            )

            for version, version_rows in grouped_versions:
                latest = version_rows[0].start.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M")
                version_label = format_version_label(version, version_bounds)
                header = f"{version_label} — {len(version_rows)} runs, latest {latest}"
                with st.expander(header, expanded=False):
                    table_data = _build_table(version_rows[:rows_per_version], version_bounds)
                    st.dataframe(table_data, use_container_width=True, hide_index=True)

    with list_tab:
        flat_min = 1 if len(filtered_rows) < 10 else 10
        max_rows = st.slider(
            "Rows to show",
            min_value=flat_min,
            max_value=max(50, len(filtered_rows)),
            value=min(50, len(filtered_rows)),
        )
        table_data = _build_table(filtered_rows[:max_rows], version_bounds)
        st.dataframe(table_data, use_container_width=True, hide_index=True)

        select_options = [_select_run_label(r, version_bounds) for r in filtered_rows]
        selected_label = st.selectbox(
            "Open run in detail view",
            options=select_options,
            index=0,
        )
        label_to_row = {label: row for label, row in zip(select_options, filtered_rows)}
        selected_row = label_to_row[selected_label]

        st.caption("Use the button below to jump to the detailed comparison page for the selected run.")
        if st.button("Go to Run Detail & Compare", use_container_width=False):
            query_params = st.query_params
            query_params["file"] = selected_row.file
            query_params["workload"] = selected_row.workload_name
            query_params["match"] = "name"
            st.switch_page("pages/1_Run_Detail_and_Compare.py")

    st.subheader("Runs per workload")
    counts = _aggregate_runs(filtered_rows)
    chart_data = {"Workload": list(counts.keys()), "Runs": list(counts.values())}
    if counts:
        st.bar_chart(chart_data, x="Workload", y="Runs", height=260, use_container_width=True)
    else:
        st.info("No runs available for charting.")
