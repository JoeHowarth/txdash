import json
import os
from dataclasses import dataclass
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import streamlit as st


# -----------------------
# Parsing and data models
# -----------------------


def _parse_rfc3339(s: str) -> datetime:
    # Normalize RFC3339 timestamps that may contain nanoseconds (9 digits)
    # to Python's datetime.fromisoformat-compatible microseconds (max 6 digits).
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    # Match base, optional fractional, and timezone
    m = re.match(r"^(?P<base>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?P<f>\.\d+)?(?P<tz>[+-]\d{2}:\d{2})$", s)
    if m:
        f = m.group("f") or ""
        if f:
            digits = f[1:]
            if len(digits) > 6:
                f = "." + digits[:6]
        s = m.group("base") + f + m.group("tz")
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _safe_get(d: Dict[str, Any], path: List[Any], default: Any = None) -> Any:
    cur = d
    for key in path:
        if not isinstance(cur, dict) or key not in cur:
            return default
        cur = cur[key]
    return cur


def _gen_mode_label(gen_mode_val: Any) -> str:
    # Serde external-tagged enum: may be a string (unit variant) or {Variant: payload}
    if isinstance(gen_mode_val, str):
        return gen_mode_val
    if isinstance(gen_mode_val, dict) and gen_mode_val:
        # return the single key (variant name)
        return next(iter(gen_mode_val.keys()))
    return "unknown"


@dataclass
class RunRow:
    file: str
    start: datetime
    end: datetime
    duration_s: float
    workload_idx: int
    workload_name: str
    gen_mode: str
    target_tps: int
    txs_sent: int
    txs_committed: int
    txs_dropped: int
    achieved_tps: float
    drop_rate: float
    stats: Dict[str, Any]


def _derive_row(data: Dict[str, Any], file: str) -> Optional[RunRow]:
    try:
        start = _parse_rfc3339(data["start_time"])  # chrono::DateTime<Utc> serialized
        end = _parse_rfc3339(data["end_time"])      # chrono::DateTime<Utc>
        dur_s = max((end - start).total_seconds(), 0.0)

        workload_idx = int(data.get("workload_idx", 0))
        cfg = data.get("config", {})
        wgs = cfg.get("workload_groups", [])
        workload_name = (
            wgs[workload_idx].get("name") if 0 <= workload_idx < len(wgs) else None
        ) or f"workload_{workload_idx}"

        # target_tps is already on the report; trust it for simplicity
        target_tps = int(data.get("target_tps", 0))

        # gen_mode from the first traffic gen of the selected workload group
        gms: str = "unknown"
        if 0 <= workload_idx < len(wgs):
            tgs = wgs[workload_idx].get("traffic_gens", [])
            if tgs:
                gms = _gen_mode_label(tgs[0].get("gen_mode"))

        txs_sent = int(data.get("txs_sent", 0))
        txs_committed = int(data.get("txs_committed", 0))
        txs_dropped = int(data.get("txs_dropped", max(0, txs_sent - txs_committed)))
        achieved_tps = (txs_committed / dur_s) if dur_s > 0 else 0.0
        drop_rate = (txs_dropped / txs_sent) if txs_sent > 0 else 0.0

        stats = data.get("stats", {}) or {}

        return RunRow(
            file=file,
            start=start,
            end=end,
            duration_s=dur_s,
            workload_idx=workload_idx,
            workload_name=workload_name,
            gen_mode=gms,
            target_tps=target_tps,
            txs_sent=txs_sent,
            txs_committed=txs_committed,
            txs_dropped=txs_dropped,
            achieved_tps=achieved_tps,
            drop_rate=drop_rate,
            stats=stats,
        )
    except Exception:
        return None


def _is_report_file(path: str) -> bool:
    name = os.path.basename(path)
    return name.endswith(".json") and "-report-" in name


@st.cache_data(show_spinner=False)
def load_reports(dir_path: str) -> List[RunRow]:
    print(f"Loading reports from {dir_path}")
    rows: List[RunRow] = []
    if not os.path.isdir(dir_path):
        return rows
    print(f"Found {len(os.listdir(dir_path))} files in {dir_path}")
    for root, _dirs, files in os.walk(dir_path):
        for f in files:
            p = os.path.join(root, f)
            if not _is_report_file(p):
                continue
            try:
                with open(p, "r") as fp:
                    data = json.load(fp)
                row = _derive_row(data, p)
                if row is not None:
                    rows.append(row)
            except Exception as e:
                print(f"Error loading {p}: {e}")
                # best-effort loader: skip bad files
                continue
    print(f"Loaded {len(rows)} reports")
    rows.sort(key=lambda r: (r.workload_name, r.start), reverse=True)
    return rows


def _stats_metric_keys(rows: List[RunRow]) -> List[str]:
    # Collect union of metric keys across rows, sorted
    keys = set()
    for r in rows:
        keys.update(r.stats.keys())
    return sorted(keys)


def _extract_stat_value(stat: Dict[str, Any], which: str) -> Optional[float]:
    # stat is CounterStatsReport; we show overall percentiles
    overall = stat.get("overall") if isinstance(stat, dict) else None
    if not isinstance(overall, dict):
        return None
    val = overall.get(which)
    try:
        return float(val)
    except Exception:
        return None


# ---------------
# Streamlit Views
# ---------------


st.set_page_config(page_title="txgen reports", layout="wide")
st.title("txgen report explorer (offline demo)")

with st.sidebar:
    st.header("Data source")
    default_dir = "reports"
    reports_dir = st.text_input("Reports folder", value=default_dir)
    reload_clicked = st.button("Reload reports")
    if reload_clicked:
        load_reports.clear()

rows = load_reports(reports_dir)

if not rows:
    st.info("No reports found. Point the app to a folder containing '*-report-*.json'.")
    st.stop()

# Overview filter by workload group
workload_names = sorted({r.workload_name for r in rows})
selected_workload = st.selectbox("Workload group", options=workload_names)
filtered = [r for r in rows if r.workload_name == selected_workload]

if not filtered:
    st.warning("No runs for the selected workload group.")
    st.stop()

st.subheader("Runs overview")

# Build table
table = [
    {
        "start": r.start.isoformat(),
        "end": r.end.isoformat(),
        "duration_s": round(r.duration_s, 2),
        "gen_mode": r.gen_mode,
        "target_tps": r.target_tps,
        "achieved_tps": round(r.achieved_tps, 2),
        "txs_sent": r.txs_sent,
        "txs_committed": r.txs_committed,
        "txs_dropped": r.txs_dropped,
        "drop_rate": round(r.drop_rate, 4),
        "file": r.file,
    }
    for r in filtered
]
st.dataframe(table, use_container_width=True, hide_index=True)

# Trend: achieved TPS over time
st.subheader("Trend: achieved TPS over time")
trend_data = {
    "start": [r.start for r in filtered],
    "achieved_tps": [r.achieved_tps for r in filtered],
}
st.line_chart(trend_data, x="start", y="achieved_tps", height=220)

# Comparison section
st.subheader("Compare runs (same workload)")
options = [f"{r.start.isoformat()} | {r.gen_mode} | {r.file}" for r in filtered]
label_to_row = {options[i]: filtered[i] for i in range(len(filtered))}
default_sel = options[:2]
selected_labels = st.multiselect("Select runs to compare", options=options, default=default_sel)
selected_rows = [label_to_row[l] for l in selected_labels]

if len(selected_rows) >= 1:
    # Choose which stats metric to compare (if available)
    stat_keys = _stats_metric_keys(selected_rows)
    stat_key = None
    if stat_keys:
        stat_key = st.selectbox("Prometheus metric (from stats)", options=stat_keys, index=0)

    # Build comparison table
    def row_summary(r: RunRow) -> Dict[str, Any]:
        row = {
            "start": r.start.isoformat(),
            "gen_mode": r.gen_mode,
            "target_tps": r.target_tps,
            "achieved_tps": round(r.achieved_tps, 2),
            "txs_committed": r.txs_committed,
            "txs_dropped": r.txs_dropped,
            "drop_rate": round(r.drop_rate, 4),
            "file": r.file,
        }
        if stat_key and stat_key in r.stats:
            st_overall = r.stats[stat_key].get("overall", {})
            for k in ["mean", "p25", "p50", "p90", "p99"]:
                v = st_overall.get(k)
                try:
                    row[f"{stat_key}.{k}"] = round(float(v), 6)
                except Exception:
                    row[f"{stat_key}.{k}"] = None
        return row

    comp_rows = [row_summary(r) for r in selected_rows]
    st.dataframe(comp_rows, use_container_width=True, hide_index=True)

# Single run details
st.subheader("Run details")
detail_label = st.selectbox("Choose a run", options=options)
detail_run = label_to_row[detail_label]

col1, col2, col3 = st.columns(3)
with col1:
    st.metric("Target TPS", detail_run.target_tps)
    st.metric("Achieved TPS", f"{detail_run.achieved_tps:.2f}")
with col2:
    st.metric("Committed", detail_run.txs_committed)
    st.metric("Dropped", detail_run.txs_dropped)
with col3:
    st.metric("Drop rate", f"{detail_run.drop_rate:.4f}")
    st.text(" ")

st.write("Stats (overall percentiles)")
if not detail_run.stats:
    st.info("No stats available in this report.")
else:
    rows_stats = []
    for key, rep in sorted(detail_run.stats.items()):
        overall = rep.get("overall", {}) if isinstance(rep, dict) else {}
        rows_stats.append({
            "metric": key,
            "mean": overall.get("mean"),
            "p50": overall.get("p50"),
            "p90": overall.get("p90"),
            "p99": overall.get("p99"),
            "samples": overall.get("samples"),
        })
    st.dataframe(rows_stats, use_container_width=True, hide_index=True)
