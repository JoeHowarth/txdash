import hashlib
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional

import streamlit as st


DEFAULT_REPORTS_DIR = "workload-reports"

VersionBounds = Dict[str, Dict[str, datetime]]


@dataclass
class RunRow:
    file: str
    start: datetime
    end: datetime
    duration_s: float
    workload_idx: int
    workload_name: str
    workload_config: Dict[str, Any]
    workload_config_hash: str
    gen_mode: str
    client_version: str
    target_tps: int
    txs_sent: int
    txs_committed: int
    txs_dropped: int
    achieved_tps: float
    drop_rate: float
    stats: Dict[str, Any]
    stats_str: str


def _parse_rfc3339(s: str) -> datetime:
    # Normalize RFC3339 timestamps that may contain nanoseconds (9 digits)
    # to Python's datetime.fromisoformat-compatible microseconds (max 6 digits).
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    match = re.match(
        r"^(?P<base>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?P<f>\.\d+)?(?P<tz>[+-]\d{2}:\d{2})$",
        s,
    )
    if match:
        fraction = match.group("f") or ""
        if fraction:
            digits = fraction[1:]
            if len(digits) > 6:
                fraction = "." + digits[:6]
        s = match.group("base") + fraction + match.group("tz")
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _gen_mode_label(gen_mode_val: Any) -> str:
    # Serde external-tagged enum: may be a string (unit variant) or {Variant: payload}
    if isinstance(gen_mode_val, str):
        return gen_mode_val
    if isinstance(gen_mode_val, dict) and gen_mode_val:
        return next(iter(gen_mode_val.keys()))
    return "unknown"


def _sanitize_for_hash(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: _sanitize_for_hash(value[k]) for k in sorted(value.keys())}
    if isinstance(value, list):
        return [_sanitize_for_hash(v) for v in value]
    return value


def _compute_workload_hash(workload_cfg: Dict[str, Any]) -> str:
    sanitized = _sanitize_for_hash(workload_cfg)
    canonical = json.dumps(sanitized, separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _derive_row(data: Dict[str, Any], file: str) -> Optional[RunRow]:
    try:
        start = _parse_rfc3339(data["start_time"])
        end = _parse_rfc3339(data["end_time"])
        duration_s = max((end - start).total_seconds(), 0.0)

        workload_idx = int(data.get("workload_idx", 0))
        config = data.get("config", {})
        workload_groups = config.get("workload_groups", [])

        workload_cfg: Dict[str, Any] = {}
        workload_name = f"workload_{workload_idx}"
        if 0 <= workload_idx < len(workload_groups):
            workload_cfg = workload_groups[workload_idx] or {}
            workload_name = workload_cfg.get("name", workload_name)

        workload_config_copy = json.loads(json.dumps(workload_cfg)) if workload_cfg else {}
        workload_hash = _compute_workload_hash(workload_config_copy) if workload_cfg else ""

        gen_mode = "unknown"
        traffic_gens = workload_cfg.get("traffic_gens", []) if workload_cfg else []
        if traffic_gens:
            gen_mode = _gen_mode_label(traffic_gens[0].get("gen_mode"))

        target_tps = int(data.get("target_tps", 0))
        txs_sent = int(data.get("txs_sent", 0))
        txs_committed = int(data.get("txs_committed", 0))
        txs_dropped = int(data.get("txs_dropped", max(0, txs_sent - txs_committed)))

        achieved_tps = (txs_committed / duration_s) if duration_s > 0 else 0.0
        drop_rate = (txs_dropped / txs_sent) if txs_sent > 0 else 0.0

        stats = data.get("stats", {}) or {}
        stats_str = data.get("stats_str", "") or ""
        client_version = data.get("client_version") or "Unknown"

        return RunRow(
            file=file,
            start=start,
            end=end,
            duration_s=duration_s,
            workload_idx=workload_idx,
            workload_name=workload_name,
            workload_config=workload_config_copy,
            workload_config_hash=workload_hash,
            gen_mode=gen_mode,
            client_version=client_version,
            target_tps=target_tps,
            txs_sent=txs_sent,
            txs_committed=txs_committed,
            txs_dropped=txs_dropped,
            achieved_tps=achieved_tps,
            drop_rate=drop_rate,
            stats=stats,
            stats_str=stats_str,
        )
    except Exception as exc:
        print(f"Error deriving row from {file}: {exc}")
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
    for root, _dirs, files in os.walk(dir_path):
        for name in files:
            path = os.path.join(root, name)
            if not _is_report_file(path):
                continue
            try:
                with open(path, "r", encoding="utf-8") as handle:
                    data = json.load(handle)
                row = _derive_row(data, path)
                if row is not None:
                    rows.append(row)
            except Exception as exc:
                print(f"Error loading {path}: {exc}")
                continue
    rows.sort(key=lambda r: r.start, reverse=True)
    print(f"Loaded {len(rows)} reports")
    return rows


def compute_version_bounds(rows: Iterable[RunRow]) -> VersionBounds:
    bounds: VersionBounds = {}
    for row in rows:
        version = row.client_version or "Unknown"
        start = row.start
        entry = bounds.setdefault(version, {"earliest": start, "latest": start})
        if start < entry["earliest"]:
            entry["earliest"] = start
        if start > entry["latest"]:
            entry["latest"] = start
    return bounds


def format_version_label(version: str, bounds: VersionBounds) -> str:
    entry = bounds.get(version)
    if not entry:
        return version
    earliest = entry.get("earliest")
    if not earliest:
        return version
    return f"{version} ({earliest.astimezone(timezone.utc).strftime('%Y-%m-%d')})"


def format_duration(seconds: float) -> str:
    minutes, secs = divmod(int(seconds), 60)
    hours, mins = divmod(minutes, 60)
    if hours > 0:
        return f"{hours}h {mins}m"
    if mins > 0:
        return f"{mins}m {secs}s"
    return f"{secs}s"
