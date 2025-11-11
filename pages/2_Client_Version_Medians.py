import statistics
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
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


def _format_percent(value: Optional[float]) -> str:
    if value is None:
        return "n/a"
    return f"{value * 100:.2f}%"


def _format_tps_value(value: Optional[float]) -> str:
    if value is None:
        return "n/a"
    return f"{value:.2f}"


def _format_runs(value: Optional[int]) -> str:
    if value is None:
        return "n/a"
    return str(value)


def _format_time(ts: Optional[datetime]) -> str:
    if ts is None:
        return "n/a"
    return ts.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M")


def _format_duration_value(value: Optional[float]) -> str:
    if value is None:
        return "n/a"
    return format_duration(value)


def _format_pair(primary: Optional[Any], secondary: Optional[Any], formatter) -> str:
    return f"{formatter(primary)} ({formatter(secondary)})"


def _tps_delta(primary: Optional[float], secondary: Optional[float]) -> Tuple[Optional[float], str]:
    if primary is None or secondary is None:
        return None, "n/a"
    delta = primary - secondary
    pct = None
    if secondary != 0:
        pct = (delta / secondary) * 100
    pct_str = f"{pct:+.1f}%" if pct is not None else "—"
    return delta, f"{delta:+.2f} ({pct_str})"


def _drop_delta(primary: Optional[float], secondary: Optional[float]) -> Tuple[Optional[float], str]:
    if primary is None or secondary is None:
        return None, "n/a"
    delta = primary - secondary
    pct = None
    if secondary != 0:
        pct = (delta / secondary) * 100
    pct_str = f"{pct:+.1f}%" if pct is not None else "—"
    return delta, f"{delta * 100:+.2f}pp ({pct_str})"


def _make_delta_styler(values: List[Optional[float]], positive_good: bool):
    def _styler(_col):
        styles: List[str] = []
        for value in values:
            if value is None:
                styles.append("")
                continue
            good = value > 0 if positive_good else value < 0
            color = "#17833b" if good else "#c03540"
            styles.append(f"color: {color}; font-weight: 600;")
        return styles

    return _styler


def _compute_version_workload_stats(rows: List[RunRow]) -> Dict[str, Dict[str, Dict[str, object]]]:
    stats: Dict[str, Dict[str, Dict[str, object]]] = {}
    for row in rows:
        version = row.client_version or "Unknown"
        version_stats = stats.setdefault(version, {})
        workload_rows = version_stats.setdefault(row.workload_name, {}).setdefault("rows", [])
        workload_rows.append(row)
    for version, workloads in stats.items():
        for workload, payload in workloads.items():
            workload_rows = payload["rows"]
            workload_rows.sort(key=lambda r: r.start, reverse=True)
            payload.clear()
            payload.update(
                {
                    "runs": len(workload_rows),
                    "median_tps": statistics.median([r.achieved_tps for r in workload_rows]) if workload_rows else None,
                    "median_drop": statistics.median([r.drop_rate for r in workload_rows]) if workload_rows else None,
                    "median_duration": statistics.median([r.duration_s for r in workload_rows]) if workload_rows else None,
                    "latest": workload_rows[0].start if workload_rows else None,
                }
            )
    return stats


def _workload_order(workloads: List[str], versions: List[str], stats: Dict[str, Dict[str, Dict[str, object]]]) -> List[str]:
    def sort_key(workload: str) -> datetime:
        timestamps = []
        for version in versions:
            entry = stats.get(version, {}).get(workload)
            if entry and entry.get("latest"):
                timestamps.append(entry["latest"])
        if timestamps:
            return max(timestamps)
        return datetime.min.replace(tzinfo=timezone.utc)

    return sorted(workloads, key=sort_key, reverse=True)


def _build_base_table(version: str, workloads: List[str], stats: Dict[str, Dict[str, Dict[str, object]]]) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    version_stats = stats.get(version, {})
    for workload in workloads:
        entry = version_stats.get(workload)
        if not entry:
            continue
        rows.append(
            {
                "Workload": workload,
                "Runs": entry.get("runs", 0),
                "Median achieved TPS": _format_tps_value(entry.get("median_tps")),
                "Median drop rate": _format_percent(entry.get("median_drop")),
                "Median duration": _format_duration_value(entry.get("median_duration")),
                "Latest run": _format_time(entry.get("latest")),
            }
        )
    return rows


def _build_comparison_table(
    base_version: str,
    compare_version: str,
    workloads: List[str],
    stats: Dict[str, Dict[str, Dict[str, object]]],
) -> Tuple[List[Dict[str, object]], List[Optional[float]], List[Optional[float]]]:
    table_rows: List[Dict[str, object]] = []
    tps_deltas: List[Optional[float]] = []
    drop_deltas: List[Optional[float]] = []
    base_stats = stats.get(base_version, {})
    compare_stats = stats.get(compare_version, {})
    for workload in workloads:
        base_entry = base_stats.get(workload)
        compare_entry = compare_stats.get(workload)
        if base_entry is None and compare_entry is None:
            continue
        row: Dict[str, object] = {"Workload": workload}

        base_runs = base_entry.get("runs") if base_entry else None
        compare_runs = compare_entry.get("runs") if compare_entry else None
        row["Runs"] = _format_pair(base_runs, compare_runs, _format_runs)

        base_tps = base_entry.get("median_tps") if base_entry else None
        compare_tps = compare_entry.get("median_tps") if compare_entry else None
        row["Median TPS"] = _format_pair(base_tps, compare_tps, _format_tps_value)
        tps_delta_value, tps_delta_str = _tps_delta(base_tps, compare_tps)
        tps_deltas.append(tps_delta_value)
        row["TPS Δ"] = tps_delta_str

        base_drop = base_entry.get("median_drop") if base_entry else None
        compare_drop = compare_entry.get("median_drop") if compare_entry else None
        row["Drop rate"] = _format_pair(base_drop, compare_drop, _format_percent)
        drop_delta_value, drop_delta_str = _drop_delta(base_drop, compare_drop)
        drop_deltas.append(drop_delta_value)
        row["Drop Δ"] = drop_delta_str

        base_latest = base_entry.get("latest") if base_entry else None
        compare_latest = compare_entry.get("latest") if compare_entry else None
        row["Latest run"] = _format_pair(base_latest, compare_latest, _format_time)

        table_rows.append(row)
    return table_rows, tps_deltas, drop_deltas


st.set_page_config(page_title="Client version medians", layout="wide")
_init_session_state()

with st.sidebar:
    st.header("Data source")
    reports_dir_input = st.text_input(
        "Reports folder",
        value=st.session_state["reports_dir"],
    )
    if reports_dir_input != st.session_state["reports_dir"]:
        st.session_state["reports_dir"] = reports_dir_input
    if st.button("Reload reports"):
        load_reports.clear()

reports_dir = st.session_state["reports_dir"]
rows = load_reports(reports_dir)

if not rows:
    st.title("Client version medians")
    st.info("No reports found. Update the reports folder in the sidebar and reload.")
    st.stop()

version_bounds = compute_version_bounds(rows)
version_order = sorted(
    version_bounds.keys(), key=lambda v: version_bounds[v]["latest"], reverse=True
)
reference_version = st.selectbox(
    "Reference version",
    options=version_order,
    index=0,
    format_func=lambda v: format_version_label(v, version_bounds),
)
comparison_candidates = [v for v in version_order if v != reference_version]
comparison_default = comparison_candidates[:2]
comparison_versions = st.multiselect(
    "Versions to compare",
    options=comparison_candidates,
    default=comparison_default,
    format_func=lambda v: format_version_label(v, version_bounds),
)

selected_versions = [reference_version] + comparison_versions
stats = _compute_version_workload_stats(rows)

workload_pool = sorted({workload for version in selected_versions for workload in stats.get(version, {})})
if not workload_pool:
    st.warning("No workloads available for the selected versions.")
    st.stop()

workload_selection = st.multiselect(
    "Workloads to include",
    options=workload_pool,
    default=workload_pool,
)
shared_only = st.checkbox("Only show workloads present in every selected version", value=True)

display_workloads = workload_selection or workload_pool
if shared_only:
    display_workloads = [
        workload
        for workload in display_workloads
        if all(workload in stats.get(version, {}) for version in selected_versions)
    ]

if not display_workloads:
    st.warning("No workloads meet the current selection criteria.")
    st.stop()

ordered_workloads = _workload_order(display_workloads, selected_versions, stats)

st.title("Client version medians")
st.caption("Compare workload medians for a client version versus historical versions.")

reference_label = format_version_label(reference_version, version_bounds)
st.subheader(f"{reference_label} medians")
base_table = _build_base_table(reference_version, ordered_workloads, stats)
if not base_table:
    st.info("No data for the reference version with the selected workload filters.")
else:
    st.dataframe(base_table, use_container_width=True, hide_index=True)

if not comparison_versions:
    st.info("Add comparison versions to see deltas.")
else:
    for compare_version in comparison_versions:
        compare_label = format_version_label(compare_version, version_bounds)
        st.subheader(f"Comparison vs {compare_label}")
        comparison_table, tps_deltas, drop_deltas = _build_comparison_table(
            reference_version, compare_version, ordered_workloads, stats
        )
        if not comparison_table:
            st.info("No overlapping workloads for this comparison.")
        else:
            columns = [
                "Workload",
                "Runs",
                "Median TPS",
                "TPS Δ",
                "Drop rate",
                "Drop Δ",
                "Latest run",
            ]
            df = pd.DataFrame(comparison_table, columns=columns)
            styled = df.style
            styled = styled.apply(
                _make_delta_styler(tps_deltas, positive_good=True), subset=["TPS Δ"]
            )
            styled = styled.apply(
                _make_delta_styler(drop_deltas, positive_good=False), subset=["Drop Δ"]
            )
            st.dataframe(styled, use_container_width=True, hide_index=True)
