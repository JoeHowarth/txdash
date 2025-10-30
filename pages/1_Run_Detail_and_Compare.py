import json
import os
from collections import OrderedDict
from datetime import datetime, timezone
from typing import Dict, List, Optional

import streamlit as st

from data import RunRow, format_duration, load_reports, DEFAULT_REPORTS_DIR

def _init_session_state() -> None:
    if "reports_dir" not in st.session_state:
        st.session_state["reports_dir"] = DEFAULT_REPORTS_DIR


def _label(row: RunRow) -> str:
    start = row.start.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M")
    return f"{start} | {row.workload_name} | {row.gen_mode} | {row.workload_config_hash[:8]}"


def _find_row_by_file(rows: List[RunRow], file_path: str) -> Optional[RunRow]:
    for row in rows:
        if row.file == file_path:
            return row
    return None


def _stat_overall(row: RunRow, key: str) -> Dict[str, float]:
    stat = row.stats.get(key)
    if not isinstance(stat, dict):
        return {}
    overall = stat.get("overall")
    return overall if isinstance(overall, dict) else {}


def _stat_value(row: RunRow, key: str, field: str) -> Optional[float]:
    overall = _stat_overall(row, key)
    val = overall.get(field)
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _collect_stat_keys(rows: List[RunRow]) -> List[str]:
    keys = {k for row in rows for k in row.stats.keys()}
    return sorted(keys)


def _format_percent(value: float) -> str:
    return f"{value * 100:.2f}%"


def _format_delta_percent(base: float, other: float) -> str:
    if base == 0:
        return "—"
    pct = (other - base) / base * 100
    return f"{pct:+.1f}%"


def _format_delta_pp(base: float, other: float) -> str:
    delta = (other - base) * 100
    return f"{delta:+.2f}pp"


def _regression_notes(base: RunRow, other: RunRow, stat_key: Optional[str]) -> str:
    notes: List[str] = []
    if base.achieved_tps > 0:
        pct = (other.achieved_tps - base.achieved_tps) / base.achieved_tps * 100
        if pct <= -10:
            notes.append(f"achieved {pct:.1f}%")
    drop_pp = (other.drop_rate - base.drop_rate) * 100
    if drop_pp >= 5:
        notes.append(f"drop +{drop_pp:.1f}pp")
    if stat_key:
        base_p90 = _stat_value(base, stat_key, "p90")
        other_p90 = _stat_value(other, stat_key, "p90")
        if base_p90 and other_p90:
            change = (other_p90 - base_p90) / base_p90 * 100
            if change >= 10:
                notes.append(f"{stat_key} p90 +{change:.1f}%")
    if not notes:
        return ""
    return "⚠️ " + ", ".join(notes)


def _set_query_params(file_path: str, workload: str, match_mode: str) -> None:
    params = st.query_params
    if (
        params.get("file") == file_path
        and params.get("workload") == workload
        and params.get("match") == match_mode
    ):
        return
    params["file"] = file_path
    params["workload"] = workload
    params["match"] = match_mode


st.set_page_config(page_title="Run detail & compare", layout="wide")
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
    st.title("Run detail & compare")
    st.info("No reports found. Update the reports folder in the sidebar and reload.")
    st.stop()

query_params = st.query_params
selected_workload_param = query_params.get("workload")
selected_file_param = query_params.get("file")
match_mode_param = query_params.get("match")
if isinstance(selected_workload_param, list):
    selected_workload_param = selected_workload_param[-1] if selected_workload_param else None
if isinstance(selected_file_param, list):
    selected_file_param = selected_file_param[-1] if selected_file_param else None
if isinstance(match_mode_param, list):
    match_mode_param = match_mode_param[-1] if match_mode_param else None
if match_mode_param not in ("name", "config"):
    match_mode_param = "name"

workload_names = sorted({row.workload_name for row in rows})
if selected_workload_param not in workload_names:
    selected_workload_param = workload_names[0]

selected_workload = st.selectbox("Workload group", options=workload_names, index=workload_names.index(selected_workload_param))
workload_rows = [row for row in rows if row.workload_name == selected_workload]
if not workload_rows:
    st.warning("No runs for the selected workload group.")
    st.stop()

workload_rows_sorted = sorted(workload_rows, key=lambda r: r.start, reverse=True)
base_default = _find_row_by_file(workload_rows_sorted, selected_file_param) or workload_rows_sorted[0]

base_label_options = [_label(r) for r in workload_rows_sorted]
base_label_default = base_label_options.index(_label(base_default))
base_label = st.selectbox("Baseline run", options=base_label_options, index=base_label_default)
label_to_row = {label: row for label, row in zip(base_label_options, workload_rows_sorted)}
base_run = label_to_row[base_label]

match_mode = st.radio(
    "Match previous runs by",
    options=("name", "config"),
    format_func=lambda v: "Workload name" if v == "name" else "Exact config hash",
    index=0 if match_mode_param != "config" else 1,
)

_set_query_params(file_path=base_run.file, workload=base_run.workload_name, match_mode=match_mode)

st.title("Run detail & comparison")

primary_cols = st.columns(4)
with primary_cols[0]:
    st.metric("Run start", base_run.start.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M"))
with primary_cols[1]:
    st.metric("Duration", format_duration(base_run.duration_s))
with primary_cols[2]:
    st.metric("Target TPS", base_run.target_tps)
with primary_cols[3]:
    st.metric("Achieved TPS", f"{base_run.achieved_tps:.2f}")

secondary_cols = st.columns(3)
with secondary_cols[0]:
    st.metric("Committed", base_run.txs_committed)
with secondary_cols[1]:
    st.metric("Dropped", base_run.txs_dropped)
with secondary_cols[2]:
    st.metric("Drop rate", _format_percent(base_run.drop_rate))

st.write("### Workload config")
st.caption("Hash includes the entire workload config block. Use match mode 'Exact config hash' to limit comparisons to this hash.")
st.info(f"Config hash: {base_run.workload_config_hash or 'n/a'}")
st.code(json.dumps(base_run.workload_config, indent=2), language="json")

st.write("### Comparison set")

matching_rows: List[RunRow] = []
for row in workload_rows_sorted:
    if row.file == base_run.file:
        continue
    if match_mode == "config" and base_run.workload_config_hash and row.workload_config_hash != base_run.workload_config_hash:
        continue
    matching_rows.append(row)

base_key = str(abs(hash(base_run.file)))
advanced = st.expander("Advanced filters", expanded=False)
with advanced:
    manual_include_labels = st.multiselect(
        "Force include runs",
        options=[_label(r) for r in workload_rows_sorted if r.file != base_run.file],
        default=[],
        key=f"manual_include_{base_key}",
    )
    manual_exclude_labels = st.multiselect(
        "Exclude runs",
        options=[_label(r) for r in matching_rows],
        default=[],
        key=f"manual_exclude_{base_key}",
    )

manual_include = {label: label_to_row[label] for label in manual_include_labels if label in label_to_row}
manual_exclude_files = {label_to_row[label].file for label in manual_exclude_labels if label in label_to_row}

selected_map: OrderedDict[str, RunRow] = OrderedDict()
for row in matching_rows:
    if row.file in manual_exclude_files:
        continue
    selected_map[row.file] = row
for label, row in manual_include.items():
    if row is not None:
        selected_map[row.file] = row

comparison_rows = list(selected_map.values())

if not comparison_rows:
    st.info("No comparison runs match the current filters.")
    st.stop()

stat_keys = _collect_stat_keys([base_run] + comparison_rows)
stat_key = None
if stat_keys:
    stat_key = st.selectbox("Prometheus metric to include", options=["None"] + stat_keys, index=0)
    if stat_key == "None":
        stat_key = None

if len(comparison_rows) <= 1:
    comparison_limit = len(comparison_rows)
else:
    comparison_limit = st.slider(
        "Maximum comparison runs",
        min_value=1,
        max_value=len(comparison_rows),
        value=min(len(comparison_rows), 10),
    )
comparison_rows = comparison_rows[:comparison_limit]

comparison_table: List[Dict[str, object]] = []
comparison_table.append(
    {
        "Role": "Baseline",
        "Start": base_run.start.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M"),
        "Target TPS": base_run.target_tps,
        "Achieved TPS": round(base_run.achieved_tps, 2),
        "Drop rate": _format_percent(base_run.drop_rate),
        "Report file": os.path.basename(base_run.file),
        "Regression": "",
    }
)

for row in comparison_rows:
    entry: Dict[str, object] = {
        "Role": "Comparison",
        "Start": row.start.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M"),
        "Target TPS": row.target_tps,
        "Achieved TPS": f"{row.achieved_tps:.2f} ({_format_delta_percent(base_run.achieved_tps, row.achieved_tps)})",
        "Drop rate": f"{_format_percent(row.drop_rate)} ({_format_delta_pp(base_run.drop_rate, row.drop_rate)})",
        "Report file": os.path.basename(row.file),
        "Regression": _regression_notes(base_run, row, stat_key),
    }
    if stat_key:
        base_p90 = _stat_value(base_run, stat_key, "p90")
        row_p90 = _stat_value(row, stat_key, "p90")
        entry[f"{stat_key} p90"] = (
            "n/a"
            if row_p90 is None or base_p90 is None
            else f"{row_p90:.3f} ({_format_delta_percent(base_p90, row_p90)})"
        )
        base_p50 = _stat_value(base_run, stat_key, "p50")
        row_p50 = _stat_value(row, stat_key, "p50")
        entry[f"{stat_key} p50"] = (
            "n/a"
            if row_p50 is None or base_p50 is None
            else f"{row_p50:.3f} ({_format_delta_percent(base_p50, row_p50)})"
        )
    comparison_table.append(entry)

st.dataframe(comparison_table, use_container_width=True, hide_index=True)

st.write("### Baseline stats")
if not base_run.stats:
    st.info("No Prometheus stats in this report.")
else:
    stats_rows = []
    for key, value in sorted(base_run.stats.items()):
        overall = _stat_overall(base_run, key)
        stats_rows.append(
            {
                "Metric": key,
                "Mean": overall.get("mean"),
                "p50": overall.get("p50"),
                "p90": overall.get("p90"),
                "p99": overall.get("p99"),
                "Samples": overall.get("samples"),
            }
        )
    st.dataframe(stats_rows, use_container_width=True, hide_index=True)
