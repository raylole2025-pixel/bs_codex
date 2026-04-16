from __future__ import annotations

import csv
import json
import time
from collections import Counter, defaultdict
from copy import deepcopy
from dataclasses import asdict, dataclass
from pathlib import Path
from statistics import median
from typing import Any, Iterable

from apps.run_stage1_workbook_batch import read_task_sets, workbook_task_to_payload
from bs3.models import Allocation, ScheduledWindow, Stage1BaselineTrace
from bs3.scenario import load_scenario
from bs3.stage1 import RegularEvaluator, activation_count, gateway_count
from bs3.stage2 import run_stage2

EPS = 1e-9
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "results" / "stage2_results"

DEFAULT_SMOKE_CASES = (
    {
        "name": "empty_control",
        "case_type": "empty",
        "description": "No emergency task. Stage2 should strictly degenerate to the baseline trace.",
        "source": {"mode": "generate", "params": {"num_emergencies": 0}},
        "seed_offset": 0,
    },
    {
        "name": "light_load",
        "case_type": "light",
        "description": "A few loosely timed emergencies with dispersed arrivals.",
        "source": {
            "mode": "generate",
            "params": {
                "num_emergencies": 3,
                "arrival_pattern": "uniform",
                "deadline_tightness": "loose",
                "data_scale": "low",
                "weight_scale": "medium",
                "hotspot_bias": False,
            },
        },
        "seed_offset": 11,
    },
)

DEFAULT_SMALL_VALIDATION_CASES = (
    {
        "name": "empty_control",
        "case_type": "empty",
        "description": "No emergency task. Used to verify strict degeneration to baseline.",
        "source": {"mode": "generate", "params": {"num_emergencies": 0}},
        "seed_offset": 0,
    },
    {
        "name": "light_load",
        "case_type": "light",
        "description": "3-5 emergencies, dispersed arrivals, loose deadlines.",
        "source": {
            "mode": "generate",
            "params": {
                "num_emergencies": 4,
                "arrival_pattern": "uniform",
                "deadline_tightness": "loose",
                "data_scale": "low",
                "weight_scale": "medium",
                "hotspot_bias": False,
            },
        },
        "seed_offset": 11,
    },
    {
        "name": "medium_load",
        "case_type": "medium",
        "description": "8-12 emergencies with local overlap and moderate deadlines.",
        "source": {
            "mode": "generate",
            "params": {
                "num_emergencies": 10,
                "arrival_pattern": "clustered",
                "deadline_tightness": "medium",
                "data_scale": "medium",
                "weight_scale": "medium_high",
                "hotspot_bias": False,
            },
        },
        "seed_offset": 23,
    },
    {
        "name": "heavy_load",
        "case_type": "heavy",
        "description": "15+ emergencies with concentrated arrivals and tight deadlines.",
        "source": {
            "mode": "generate",
            "params": {
                "num_emergencies": 16,
                "arrival_pattern": "clustered",
                "deadline_tightness": "tight",
                "data_scale": "high",
                "weight_scale": "high",
                "hotspot_bias": False,
            },
        },
        "seed_offset": 37,
    },
    {
        "name": "hotspot_bias",
        "case_type": "hotspot",
        "description": "Emergencies biased toward stage1 hotspot regions.",
        "source": {
            "mode": "generate",
            "params": {
                "num_emergencies": 8,
                "arrival_pattern": "clustered",
                "deadline_tightness": "medium",
                "data_scale": "medium",
                "weight_scale": "medium_high",
                "hotspot_bias": True,
            },
        },
        "seed_offset": 53,
    },
)

SCALE_PRESETS = {
    "low": 0.6,
    "medium": 1.0,
    "medium_high": 1.25,
    "high": 1.6,
    "loose": 3.0,
    "tight": 1.25,
}

DEADLINE_PRESETS = {
    "loose": 3.0,
    "medium": 2.0,
    "tight": 1.25,
}


@dataclass(frozen=True)
class LoadedStage1Artifacts:
    stage1_result_path: Path
    payload: dict[str, Any]
    selected_candidate_index: int | None
    selected_candidate_source: str | None
    selected_plan_rows: list[dict[str, Any]]
    selected_plan: list[ScheduledWindow]
    best_feasible: list[dict[str, Any]]
    baseline_trace: Stage1BaselineTrace | None
    baseline_trace_rho: float | None

    def resolve_candidate(
        self,
        candidate_index: int,
    ) -> tuple[list[ScheduledWindow], list[dict[str, Any]], dict[str, Any] | None, bool]:
        if candidate_index < 0:
            raise IndexError(f"candidate_index must be >= 0, got {candidate_index}")
        if candidate_index == self.selected_candidate_index and self.selected_plan:
            candidate_info = None
            if 0 <= candidate_index < len(self.best_feasible):
                candidate_info = dict(self.best_feasible[candidate_index])
            return list(self.selected_plan), list(self.selected_plan_rows), candidate_info, True
        if candidate_index == 0 and self.selected_plan and not self.best_feasible:
            return list(self.selected_plan), list(self.selected_plan_rows), None, True
        if candidate_index >= len(self.best_feasible):
            raise IndexError(
                f"candidate_index {candidate_index} out of range; best_feasible count={len(self.best_feasible)}"
            )
        candidate_info = dict(self.best_feasible[candidate_index])
        plan_rows = list(candidate_info.get("plan") or [])
        if not plan_rows:
            raise ValueError(f"best_feasible[{candidate_index}] does not contain a plan")
        return _scheduled_windows_from_rows(plan_rows), plan_rows, candidate_info, False


def _float(value: Any, default: float = 0.0) -> float:
    if value in {None, ""}:
        return default
    return float(value)


def _parse_scale(value: Any, *, presets: dict[str, float], default: float) -> float:
    if value in {None, ""}:
        return default
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().lower()
    if text in presets:
        return float(presets[text])
    return float(text)


def _sanitize_slug(value: str) -> str:
    chars: list[str] = []
    for ch in str(value).strip().lower():
        if ch.isalnum():
            chars.append(ch)
        else:
            chars.append("_")
    slug = "".join(chars).strip("_")
    while "__" in slug:
        slug = slug.replace("__", "_")
    return slug or "case"


def _stable_round(value: float, digits: int = 9) -> float:
    return round(float(value), digits)


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def scheduled_window_to_dict(window: ScheduledWindow) -> dict[str, Any]:
    return asdict(window)


def allocation_to_dict(allocation: Allocation) -> dict[str, Any]:
    return asdict(allocation)


def baseline_trace_to_dict(trace: Stage1BaselineTrace | None) -> dict[str, Any] | None:
    if trace is None:
        return None
    payload = asdict(trace)
    payload["allocations"] = [allocation_to_dict(item) for item in trace.allocations]
    return payload


def stage2_result_to_dict(result, t_pre: float) -> dict[str, Any]:
    payload = asdict(result)
    payload["gateway_count"] = gateway_count(result.plan)
    payload["activation_count"] = activation_count(result.plan, t_pre)
    payload["plan"] = [scheduled_window_to_dict(window) for window in result.plan]
    payload["allocations"] = [allocation_to_dict(item) for item in result.allocations]
    return payload


def load_stage1_artifacts(stage1_result_path: str | Path) -> LoadedStage1Artifacts:
    path = Path(stage1_result_path)
    payload = json.loads(path.read_text(encoding="utf-8-sig"))
    if "stage1" in payload and isinstance(payload["stage1"], dict):
        payload = payload["stage1"]
    selected_plan_rows = list(payload.get("selected_plan") or [])
    selected_plan = _scheduled_windows_from_rows(selected_plan_rows)
    baseline_trace = _load_baseline_trace(path, payload)
    baseline_trace_rho = baseline_trace.rho if baseline_trace is not None else None
    return LoadedStage1Artifacts(
        stage1_result_path=path.resolve(),
        payload=payload,
        selected_candidate_index=payload.get("selected_candidate_index"),
        selected_candidate_source=payload.get("selected_candidate_source"),
        selected_plan_rows=selected_plan_rows,
        selected_plan=selected_plan,
        best_feasible=list(payload.get("best_feasible") or []),
        baseline_trace=baseline_trace,
        baseline_trace_rho=baseline_trace_rho,
    )


def _scheduled_windows_from_rows(plan_rows: Iterable[dict[str, Any]]) -> list[ScheduledWindow]:
    return [
        ScheduledWindow(
            window_id=str(item["window_id"]),
            a=str(item["a"]),
            b=str(item["b"]),
            start=float(item["start"]),
            end=float(item["end"]),
            on=float(item["on"]),
            off=float(item["off"]),
            value=item.get("value"),
            delay=_float(item.get("delay"), 0.0),
            distance_km=item.get("distance_km"),
        )
        for item in plan_rows
    ]


def _load_baseline_trace(stage1_result_path: Path, payload: dict[str, Any]) -> Stage1BaselineTrace | None:
    trace_payload = payload.get("baseline_trace")
    baseline_trace_file = payload.get("baseline_trace_file")
    if baseline_trace_file:
        trace_path = Path(str(baseline_trace_file))
        if not trace_path.is_absolute():
            trace_path = stage1_result_path.parent / trace_path
        trace_payload = json.loads(trace_path.read_text(encoding="utf-8-sig"))
    if not isinstance(trace_payload, dict):
        return None
    return Stage1BaselineTrace(
        rho=float(trace_payload.get("rho", 0.0)),
        segments=list(trace_payload.get("segments") or []),
        allocations=[
            Allocation(
                task_id=str(item["task_id"]),
                segment_index=int(item["segment_index"]),
                path_id=str(item["path_id"]),
                edge_ids=tuple(item.get("edge_ids") or ()),
                rate=float(item["rate"]),
                delivered=float(item["delivered"]),
                task_type=str(item["task_type"]),
                cross_window_id=item.get("cross_window_id"),
                is_preempted=bool(item.get("is_preempted", False)),
            )
            for item in (trace_payload.get("allocations") or [])
        ],
        task_states=list(trace_payload.get("task_states") or []),
        window_states=list(trace_payload.get("window_states") or []),
        remaining_before_by_segment={
            str(task_id): {int(index): float(value) for index, value in values.items()}
            for task_id, values in (trace_payload.get("remaining_before_by_segment") or {}).items()
        },
        remaining_after_by_segment={
            str(task_id): {int(index): float(value) for index, value in values.items()}
            for task_id, values in (trace_payload.get("remaining_after_by_segment") or {}).items()
        },
        remaining_end={str(task_id): float(value) for task_id, value in (trace_payload.get("remaining_end") or {}).items()},
        completed={str(task_id): bool(value) for task_id, value in (trace_payload.get("completed") or {}).items()},
        cross_window_usage_by_segment={
            int(segment_index): {str(window_id): float(value) for window_id, value in usage.items()}
            for segment_index, usage in (trace_payload.get("cross_window_usage_by_segment") or {}).items()
        },
        available_cross_capacity_by_segment={
            int(segment_index): {str(window_id): float(value) for window_id, value in usage.items()}
            for segment_index, usage in (trace_payload.get("available_cross_capacity_by_segment") or {}).items()
        },
        occupied_cross_windows_by_segment={
            int(segment_index): list(window_ids)
            for segment_index, window_ids in (trace_payload.get("occupied_cross_windows_by_segment") or {}).items()
        },
        active_cross_windows_by_segment={
            int(segment_index): list(window_ids)
            for segment_index, window_ids in (trace_payload.get("active_cross_windows_by_segment") or {}).items()
        },
        active_intra_edges_by_segment={
            int(segment_index): {str(domain): list(edge_ids) for domain, edge_ids in values.items()}
            for segment_index, values in (trace_payload.get("active_intra_edges_by_segment") or {}).items()
        },
        available_intra_capacity_by_segment={
            int(segment_index): {str(edge_id): float(value) for edge_id, value in values.items()}
            for segment_index, values in (trace_payload.get("available_intra_capacity_by_segment") or {}).items()
        },
        summary=dict(trace_payload.get("summary") or {}),
    )


def normalize_task_payload(
    item: dict[str, Any],
    *,
    default_task_type: str = "emg",
    force_task_type: str | None = "emg",
) -> dict[str, Any]:
    task_id = item.get("id", item.get("task_id"))
    src = item.get("src", item.get("src_sat"))
    dst = item.get("dst", item.get("dst_sat"))
    arrival = item.get("arrival", item.get("arrival_sec"))
    deadline = item.get("deadline", item.get("deadline_sec"))
    data = item.get("data", item.get("data_volume_Mb"))
    weight = item.get("weight", item.get("priority_weight"))
    max_rate = item.get("max_rate", item.get("b_max_Mbps"))
    if task_id in {None, ""}:
        raise ValueError("Emergency task is missing id/task_id")
    if src in {None, ""} or dst in {None, ""}:
        raise ValueError(f"Emergency task {task_id} is missing src/dst")
    task_type = str(item.get("type", item.get("task_type", default_task_type))).strip().lower() or default_task_type
    if force_task_type is not None:
        task_type = force_task_type
    payload = {
        "id": str(task_id),
        "src": str(src),
        "dst": str(dst),
        "arrival": float(arrival),
        "deadline": float(deadline),
        "data": float(data),
        "weight": float(weight),
        "max_rate": float(max_rate),
        "type": task_type,
        "preemption_priority": float(item.get("preemption_priority", item.get("priority_weight", weight))),
    }
    for extra_key in ("task_class", "arrival_utcg", "deadline_utcg", "avg_required_Mbps", "notes", "source_task_id"):
        if extra_key in item and item[extra_key] not in {None, ""}:
            payload[extra_key] = item[extra_key]
    return payload


def load_emergency_tasks_from_json(path: str | Path) -> list[dict[str, Any]]:
    payload = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    if isinstance(payload, dict):
        raw_tasks = payload.get("tasks", payload.get("emergencies", payload.get("items", [])))
    else:
        raw_tasks = payload
    if not isinstance(raw_tasks, list):
        raise ValueError(f"Emergency JSON {path} must contain a task list")
    return [normalize_task_payload(dict(item)) for item in raw_tasks]


def load_emergency_tasks_from_csv(path: str | Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with Path(path).open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            if not any(str(value).strip() for value in row.values()):
                continue
            rows.append(normalize_task_payload(dict(row)))
    return rows


def load_emergency_tasks_from_workbook(path: str | Path, sheet: str) -> list[dict[str, Any]]:
    _, task_sets = read_task_sets(Path(path))
    if sheet not in task_sets:
        raise KeyError(f"Workbook sheet {sheet} not found in {path}")
    return [normalize_task_payload(workbook_task_to_payload(row)) for row in task_sets[sheet]]


def ensure_unique_task_ids(
    emergency_tasks: list[dict[str, Any]],
    existing_ids: set[str],
    *,
    prefix: str,
) -> tuple[list[dict[str, Any]], dict[str, str]]:
    seen = set(existing_ids)
    mapping: dict[str, str] = {}
    normalized: list[dict[str, Any]] = []
    for index, item in enumerate(emergency_tasks, start=1):
        payload = dict(item)
        original_id = str(payload["id"])
        task_id = original_id
        if task_id in seen:
            task_id = f"{prefix}_{index:02d}_{original_id}"
        while task_id in seen:
            task_id = f"{task_id}_x"
        payload["source_task_id"] = original_id
        payload["id"] = task_id
        seen.add(task_id)
        normalized.append(payload)
        mapping[original_id] = task_id
    return normalized, mapping


def summarize_task_set(tasks: list[dict[str, Any]]) -> dict[str, Any]:
    if not tasks:
        return {
            "count": 0,
            "count_emg": 0,
            "total_data_Mb": 0.0,
            "mean_data_Mb": 0.0,
            "mean_weight": 0.0,
            "arrival_min": None,
            "arrival_max": None,
            "deadline_min": None,
            "deadline_max": None,
            "mean_window_s": 0.0,
            "peak_overlap": 0,
        }
    total_data = sum(float(task["data"]) for task in tasks)
    mean_data = total_data / len(tasks)
    mean_weight = sum(float(task["weight"]) for task in tasks) / len(tasks)
    arrivals = [float(task["arrival"]) for task in tasks]
    deadlines = [float(task["deadline"]) for task in tasks]
    window_lengths = [max(float(task["deadline"]) - float(task["arrival"]), 0.0) for task in tasks]
    events: list[tuple[float, int]] = []
    for task in tasks:
        events.append((float(task["arrival"]), 1))
        events.append((float(task["deadline"]), -1))
    events.sort(key=lambda item: (item[0], -item[1]))
    peak_overlap = 0
    active = 0
    for _, delta in events:
        active += delta
        peak_overlap = max(peak_overlap, active)
    return {
        "count": len(tasks),
        "count_emg": sum(1 for task in tasks if str(task.get("type", "")).lower() == "emg"),
        "total_data_Mb": float(total_data),
        "mean_data_Mb": float(mean_data),
        "mean_weight": float(mean_weight),
        "arrival_min": float(min(arrivals)),
        "arrival_max": float(max(arrivals)),
        "deadline_min": float(min(deadlines)),
        "deadline_max": float(max(deadlines)),
        "mean_window_s": float(sum(window_lengths) / len(window_lengths)),
        "peak_overlap": int(peak_overlap),
    }


def _hotspot_a_nodes(base_payload: dict[str, Any], plan_rows: list[dict[str, Any]]) -> list[str]:
    hotspots_cfg = base_payload.get("hotspots", {})
    raw_regions = hotspots_cfg.get("A", []) if isinstance(hotspots_cfg, dict) else []
    nodes: list[str] = []
    seen: set[str] = set()
    for region in raw_regions:
        for node in region.get("nodes", []):
            text = str(node)
            if text not in seen:
                seen.add(text)
                nodes.append(text)
        for interval in region.get("intervals", []):
            for node in interval.get("nodes", []):
                text = str(node)
                if text not in seen:
                    seen.add(text)
                    nodes.append(text)
    if nodes:
        return nodes
    plan_counter = Counter(str(row["a"]) for row in plan_rows)
    return [node for node, _ in plan_counter.most_common(3)]


def _hotspot_intervals(base_payload: dict[str, Any]) -> list[tuple[float, float]]:
    hotspots_cfg = base_payload.get("hotspots", {})
    raw_regions = hotspots_cfg.get("A", []) if isinstance(hotspots_cfg, dict) else []
    intervals: list[tuple[float, float]] = []
    for region in raw_regions:
        for interval in region.get("intervals", []):
            start = interval.get("start")
            end = interval.get("end")
            if start is None or end is None:
                continue
            intervals.append((float(start), float(end)))
    return intervals


def generate_emergency_tasks(
    *,
    base_payload: dict[str, Any],
    plan_rows: list[dict[str, Any]],
    case_name: str,
    seed: int,
    num_emergencies: int,
    arrival_pattern: str = "uniform",
    deadline_tightness: Any = "medium",
    data_scale: Any = "medium",
    weight_scale: Any = "medium",
    hotspot_bias: bool = False,
) -> list[dict[str, Any]]:
    if num_emergencies <= 0:
        return []

    import random

    rng = random.Random(int(seed))
    nodes_cfg = dict(base_payload.get("nodes", {}))
    a_nodes = [str(node) for node in nodes_cfg.get("A", [])]
    b_nodes = [str(node) for node in nodes_cfg.get("B", [])]
    candidate_windows = list(base_payload.get("candidate_windows", []))
    regular_tasks = [dict(task) for task in base_payload.get("tasks", []) if str(task.get("type", "")).lower() == "reg"]
    planning_end = float(base_payload.get("planning_end", 0.0))
    cross_capacity = float(base_payload.get("capacities", {}).get("X", 1.0))

    hotspot_a_nodes = _hotspot_a_nodes(base_payload, plan_rows)
    hotspot_intervals = _hotspot_intervals(base_payload)

    candidate_pairs = [
        (str(task["src"]), str(task["dst"]))
        for task in regular_tasks
        if str(task["src"]) in a_nodes and str(task["dst"]) in b_nodes
    ]
    candidate_pairs.extend((str(item["a"]), str(item["b"])) for item in candidate_windows)
    candidate_pairs = list(dict.fromkeys(candidate_pairs))
    if not candidate_pairs:
        candidate_pairs = [(src, dst) for src in a_nodes for dst in b_nodes]
    if not candidate_pairs:
        raise ValueError("Unable to generate emergencies: no A->B candidate pairs available in scenario")

    hotspot_pairs = [(src, dst) for src, dst in candidate_pairs if src in hotspot_a_nodes]
    pair_pool = hotspot_pairs if hotspot_bias and hotspot_pairs else candidate_pairs

    regular_data = [float(task["data"]) for task in regular_tasks] or [cross_capacity * 2.0]
    regular_weights = [float(task["weight"]) for task in regular_tasks] or [1.0]
    regular_rates = [float(task["max_rate"]) for task in regular_tasks] or [min(cross_capacity, 2.0)]
    cross_durations = [max(float(item["end"]) - float(item["start"]), 0.0) for item in candidate_windows] or [max(planning_end / 6.0, 1.0)]
    base_data = max(float(median(regular_data)), cross_capacity * max(float(median(cross_durations)) / 2.0, 1.0))
    base_weight = max(float(median(regular_weights)), 1.0)
    base_rate = max(min(float(median(regular_rates)), cross_capacity), 0.5)

    data_multiplier = _parse_scale(data_scale, presets=SCALE_PRESETS, default=1.0)
    weight_multiplier = _parse_scale(weight_scale, presets=SCALE_PRESETS, default=1.0)
    slack_multiplier = _parse_scale(deadline_tightness, presets=DEADLINE_PRESETS, default=DEADLINE_PRESETS["medium"])

    arrivals: list[float] = []
    if arrival_pattern == "clustered":
        if hotspot_bias and hotspot_intervals:
            center = sum(start + end for start, end in hotspot_intervals[:2]) / max(2 * min(len(hotspot_intervals), 2), 1)
        else:
            center = planning_end * 0.45 if planning_end > EPS else 0.0
        spread = max(planning_end * 0.08, 0.5)
        for _ in range(num_emergencies):
            arrivals.append(min(max(rng.gauss(center, spread), 0.0), max(planning_end - 0.2, 0.0)))
    else:
        latest = max(planning_end * 0.75, 0.0)
        if latest <= EPS:
            arrivals = [0.0 for _ in range(num_emergencies)]
        else:
            step = latest / (num_emergencies + 1)
            for index in range(num_emergencies):
                jitter = rng.uniform(-0.25 * step, 0.25 * step)
                arrivals.append(min(max((index + 1) * step + jitter, 0.0), latest))
    arrivals.sort()

    tasks: list[dict[str, Any]] = []
    for index in range(num_emergencies):
        src, dst = rng.choice(pair_pool)
        max_rate = max(min(base_rate * rng.uniform(0.85, 1.25), cross_capacity), 0.5)
        data = max(base_data * data_multiplier * rng.uniform(0.7, 1.25), max_rate * 0.5)
        weight = max(base_weight * weight_multiplier * rng.uniform(0.85, 1.2), 1.0)
        arrival = arrivals[index]
        min_duration = data / max(max_rate, EPS)
        slack = max(min_duration * slack_multiplier, min_duration + 0.2)
        deadline = min(planning_end, arrival + slack)
        if deadline <= arrival + 0.05:
            deadline = min(planning_end, arrival + max(min_duration * 1.05, 0.1))
        if deadline <= arrival:
            deadline = arrival + 0.1
        tasks.append(
            {
                "id": f"{_sanitize_slug(case_name)}_emg_{index + 1:02d}",
                "src": src,
                "dst": dst,
                "arrival": float(arrival),
                "deadline": float(deadline),
                "data": float(data),
                "weight": float(weight),
                "max_rate": float(max_rate),
                "type": "emg",
                "preemption_priority": float(weight),
                "notes": "generated_by=stage2_emergency_validation",
            }
        )
    return tasks


def build_suite_cases(suite: str) -> list[dict[str, Any]]:
    suite_name = str(suite).strip().lower()
    if suite_name == "smoke":
        return [dict(item) for item in DEFAULT_SMOKE_CASES]
    if suite_name in {"small_validation", "small-validation", "formal_small", "formal-small"}:
        return [dict(item) for item in DEFAULT_SMALL_VALIDATION_CASES]
    raise ValueError(f"Unknown suite {suite!r}; expected smoke or small-validation")


def parse_candidate_indices(text: str | None) -> list[int]:
    if text in {None, ""}:
        return [0]
    return [int(part.strip()) for part in str(text).split(",") if part.strip()]


def parse_rho_values(text: str | None, default_rho: float) -> list[dict[str, Any]]:
    if text in {None, ""}:
        return [{"label": "default", "value": float(default_rho), "is_default": True}]
    values: list[dict[str, Any]] = []
    for part in str(text).split(","):
        token = part.strip()
        if not token:
            continue
        if token.lower() == "default":
            values.append({"label": "default", "value": float(default_rho), "is_default": True})
        else:
            rho_value = float(token)
            label = f"rho_{str(token).replace('.', '_')}"
            values.append({"label": label, "value": rho_value, "is_default": abs(rho_value - default_rho) <= EPS})
    if not values:
        values.append({"label": "default", "value": float(default_rho), "is_default": True})
    return values


def load_experiment_spec(
    spec_path: str | Path,
    *,
    default_rho: float,
) -> dict[str, Any]:
    path = Path(spec_path)
    payload = json.loads(path.read_text(encoding="utf-8-sig"))
    candidate_indices = list(payload.get("candidate_indices") or [0])
    raw_rho_values = payload.get("rho_values")
    if raw_rho_values is None:
        rho_values = [{"label": "default", "value": float(default_rho), "is_default": True}]
    else:
        rho_values = []
        for item in raw_rho_values:
            if str(item).lower() == "default":
                rho_values.append({"label": "default", "value": float(default_rho), "is_default": True})
            else:
                rho_value = float(item)
                rho_values.append(
                    {
                        "label": f"rho_{str(item).replace('.', '_')}",
                        "value": rho_value,
                        "is_default": abs(rho_value - default_rho) <= EPS,
                    }
                )
    base_dir = path.parent
    cases: list[dict[str, Any]] = []
    for raw_case in payload.get("cases") or []:
        case = dict(raw_case)
        source = dict(case.get("source") or {})
        if "path" in source and source["path"]:
            source["path"] = str((base_dir / source["path"]).resolve())
        case["source"] = source
        cases.append(case)
    return {
        "suite_name": payload.get("name", path.stem),
        "candidate_indices": candidate_indices,
        "rho_values": rho_values,
        "cases": cases,
    }


def _serialize_task_tasks(tasks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            key: value
            for key, value in task.items()
            if key not in {"task_id", "task_type"}
        }
        for task in tasks
    ]


def _task_completion_summary(
    task_rows: Iterable[Any],
    delivered_by_task: dict[str, float],
    *,
    completion_tolerance_ratio: float,
) -> dict[str, Any]:
    tasks = list(task_rows)
    if not tasks:
        return {
            "count": 0,
            "success_count": 0,
            "failure_count": 0,
            "success_rate": 1.0,
            "weighted_true_completion": 1.0,
            "mean_completion_ratio": 1.0,
            "failed_task_ids": [],
            "completion_by_task": [],
        }
    success_count = 0
    total_weight = 0.0
    weighted_true_completion = 0.0
    completion_sum = 0.0
    failed_task_ids: list[str] = []
    completion_by_task: list[dict[str, Any]] = []
    for task in tasks:
        if hasattr(task, "task_id"):
            task_id = str(task.task_id)
            data = float(task.data)
            weight = float(task.weight)
            task_type = str(task.task_type)
        else:
            task_id = str(task.get("id"))
            data = float(task.get("data"))
            weight = float(task.get("weight"))
            task_type = str(task.get("type"))
        delivered = min(max(delivered_by_task.get(task_id, 0.0), 0.0), data)
        remaining = max(data - delivered, 0.0)
        tolerance = max(completion_tolerance_ratio * data, EPS)
        success = remaining <= tolerance
        completion_ratio = delivered / data if data > EPS else 1.0
        success_count += int(success)
        total_weight += weight
        weighted_true_completion += weight * float(success)
        completion_sum += completion_ratio
        if not success:
            failed_task_ids.append(task_id)
        completion_by_task.append(
            {
                "task_id": task_id,
                "task_type": task_type,
                "data": float(data),
                "weight": float(weight),
                "delivered": float(delivered),
                "remaining": float(remaining),
                "completion_ratio": float(completion_ratio),
                "success": bool(success),
            }
        )
    return {
        "count": len(tasks),
        "success_count": success_count,
        "failure_count": len(tasks) - success_count,
        "success_rate": success_count / len(tasks),
        "weighted_true_completion": weighted_true_completion / max(total_weight, EPS),
        "mean_completion_ratio": completion_sum / len(tasks),
        "failed_task_ids": failed_task_ids,
        "completion_by_task": completion_by_task,
    }


def summarize_task_outcomes(scenario, allocations: list[Allocation]) -> dict[str, Any]:
    delivered_by_task: dict[str, float] = defaultdict(float)
    for allocation in allocations:
        delivered_by_task[str(allocation.task_id)] += float(allocation.delivered)
    completion_tolerance_ratio = max(float(scenario.stage2.completion_tolerance), 0.0)
    tasks_all = list(scenario.tasks)
    tasks_reg = [task for task in tasks_all if task.task_type == "reg"]
    tasks_emg = [task for task in tasks_all if task.task_type == "emg"]
    return {
        "overall": _task_completion_summary(tasks_all, delivered_by_task, completion_tolerance_ratio=completion_tolerance_ratio),
        "reg": _task_completion_summary(tasks_reg, delivered_by_task, completion_tolerance_ratio=completion_tolerance_ratio),
        "emg": _task_completion_summary(tasks_emg, delivered_by_task, completion_tolerance_ratio=completion_tolerance_ratio),
    }


def summarize_baseline_trace(scenario, baseline_trace: Stage1BaselineTrace) -> dict[str, Any]:
    completion_tolerance_ratio = max(float(scenario.stage2.completion_tolerance), 0.0)
    delivered_by_task: dict[str, float] = defaultdict(float)
    for allocation in baseline_trace.allocations:
        delivered_by_task[str(allocation.task_id)] += float(allocation.delivered)
    regular_tasks = [task for task in scenario.tasks if task.task_type == "reg"]
    summary_reg = _task_completion_summary(regular_tasks, delivered_by_task, completion_tolerance_ratio=completion_tolerance_ratio)
    return {
        "summary": dict(baseline_trace.summary),
        "reg": summary_reg,
        "completed": dict(baseline_trace.completed),
        "remaining_end": dict(baseline_trace.remaining_end),
    }


def _normalized_allocations(allocations: list[Allocation], *, task_type: str | None = None) -> list[tuple[Any, ...]]:
    rows: list[tuple[Any, ...]] = []
    for alloc in allocations:
        if task_type is not None and str(alloc.task_type) != task_type:
            continue
        rows.append(
            (
                str(alloc.task_id),
                int(alloc.segment_index),
                str(alloc.path_id),
                tuple(str(edge_id) for edge_id in alloc.edge_ids),
                _stable_round(float(alloc.rate)),
                _stable_round(float(alloc.delivered)),
                alloc.cross_window_id,
                bool(alloc.is_preempted),
            )
        )
    return sorted(rows)


def is_strict_baseline_degenerate(
    *,
    baseline_trace: Stage1BaselineTrace,
    result,
    baseline_reg_completion: float,
) -> bool:
    if result.n_preemptions != 0:
        return False
    if not bool(result.metadata.get("empty_emergency_insert")):
        return False
    if result.metadata.get("cross_window_usage_delta_by_segment"):
        return False
    if abs(float(result.cr_reg) - float(baseline_reg_completion)) > EPS:
        return False
    return _normalized_allocations(result.allocations, task_type="reg") == _normalized_allocations(
        baseline_trace.allocations,
        task_type="reg",
    )


def _selected_candidate_summary(
    *,
    candidate_index: int,
    candidate_info: dict[str, Any] | None,
    selected_candidate_index: int | None,
    selected_candidate_source: str | None,
    plan: list[ScheduledWindow],
    t_pre: float,
) -> dict[str, Any]:
    summary = {
        "requested_candidate_index": candidate_index,
        "stage1_selected_candidate_index": selected_candidate_index,
        "stage1_selected_candidate_source": selected_candidate_source,
        "uses_stage1_selected_plan": candidate_index == selected_candidate_index,
        "window_count": len(plan),
        "gateway_count": gateway_count(plan),
        "activation_count": activation_count(plan, t_pre),
    }
    if candidate_info:
        for key in (
            "fr",
            "mean_completion_ratio",
            "eta_cap",
            "eta_0",
            "hotspot_coverage",
            "hotspot_max_gap",
            "window_count",
            "gateway_count",
            "activation_count",
            "max_cross_gap",
            "cross_active_fraction",
        ):
            if key in candidate_info:
                summary[key] = candidate_info[key]
    return summary


def _load_case_tasks(
    case_spec: dict[str, Any],
    *,
    base_payload: dict[str, Any],
    plan_rows: list[dict[str, Any]],
    seed: int,
) -> list[dict[str, Any]]:
    source = dict(case_spec.get("source") or {})
    mode = str(source.get("mode", "generate")).strip().lower()
    if mode == "generate":
        params = dict(source.get("params") or {})
        params.setdefault("num_emergencies", int(case_spec.get("num_emergencies", 0)))
        params.setdefault("arrival_pattern", case_spec.get("arrival_pattern", "uniform"))
        params.setdefault("deadline_tightness", case_spec.get("deadline_tightness", "medium"))
        params.setdefault("data_scale", case_spec.get("data_scale", "medium"))
        params.setdefault("weight_scale", case_spec.get("weight_scale", "medium"))
        params.setdefault("hotspot_bias", bool(case_spec.get("hotspot_bias", False)))
        return generate_emergency_tasks(
            base_payload=base_payload,
            plan_rows=plan_rows,
            case_name=str(case_spec.get("name", "generated_case")),
            seed=seed,
            num_emergencies=int(params.get("num_emergencies", 0)),
            arrival_pattern=str(params.get("arrival_pattern", "uniform")),
            deadline_tightness=params.get("deadline_tightness", "medium"),
            data_scale=params.get("data_scale", "medium"),
            weight_scale=params.get("weight_scale", "medium"),
            hotspot_bias=bool(params.get("hotspot_bias", False)),
        )
    if mode == "json":
        return load_emergency_tasks_from_json(source["path"])
    if mode == "csv":
        return load_emergency_tasks_from_csv(source["path"])
    if mode == "workbook":
        return load_emergency_tasks_from_workbook(source["path"], source["sheet"])
    raise ValueError(f"Unsupported case source mode {mode!r}")


def _build_effective_scenario_payload(
    *,
    base_payload: dict[str, Any],
    emergency_tasks: list[dict[str, Any]],
    case_name: str,
    case_type: str,
    suite_name: str,
    candidate_index: int,
    rho_value: float,
    rho_label: str,
    seed: int,
) -> dict[str, Any]:
    payload = deepcopy(base_payload)
    regular_tasks = [dict(task) for task in payload.get("tasks", []) if str(task.get("type", "")).lower() == "reg"]
    payload["tasks"] = regular_tasks + [dict(task) for task in emergency_tasks]
    payload.setdefault("stage1", {})
    payload["stage1"]["rho"] = float(rho_value)
    metadata = dict(payload.get("metadata", {}))
    metadata["stage2_emergency_validation"] = {
        "suite_name": suite_name,
        "case_name": case_name,
        "case_type": case_type,
        "candidate_index": int(candidate_index),
        "rho": float(rho_value),
        "rho_label": rho_label,
        "seed": int(seed),
    }
    payload["metadata"] = metadata
    return payload


def _mean(values: Iterable[float]) -> float:
    rows = list(values)
    return sum(rows) / len(rows) if rows else 0.0


def _group_case_metrics(cases: list[dict[str, Any]], key: str) -> dict[str, dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for case in cases:
        grouped[str(case.get(key))].append(case)
    rows: dict[str, dict[str, Any]] = {}
    for label, items in sorted(grouped.items()):
        rows[label] = {
            "case_count": len(items),
            "mean_cr_reg_before": _mean(item["baseline_impact"]["cr_reg_before"] for item in items),
            "mean_cr_reg_after": _mean(item["baseline_impact"]["cr_reg_after"] for item in items),
            "mean_cr_emg": _mean(item["stage2"]["cr_emg"] for item in items),
            "mean_n_preemptions": _mean(item["stage2"]["n_preemptions"] for item in items),
            "mean_elapsed_seconds": _mean(item["stage2"]["elapsed_seconds"] for item in items),
            "mean_emergency_success_rate": _mean(item["task_outcomes"]["stage2"]["emg"]["success_rate"] for item in items),
            "regular_degradation_case_count": sum(int(item["diagnostics"]["regular_completion_rate_dropped"]) for item in items),
        }
    return rows


def infer_validation_findings(case_summaries: list[dict[str, Any]]) -> dict[str, Any]:
    non_empty_cases = [case for case in case_summaries if case["emergency_task_set"]["summary"]["count"] > 0]
    empty_cases = [case for case in case_summaries if case["case_type"] == "empty"]
    hotspot_cases = [case for case in case_summaries if case["case_type"] == "hotspot"]
    non_hotspot_non_empty_cases = [case for case in non_empty_cases if case["case_type"] != "hotspot"]
    rho_groups = _group_case_metrics(non_empty_cases, "rho_label")
    candidate_groups = _group_case_metrics(case_summaries, "candidate_label")

    hotspot_mean = _mean(case["stage2"]["cr_emg"] for case in hotspot_cases)
    non_hotspot_mean = _mean(case["stage2"]["cr_emg"] for case in non_hotspot_non_empty_cases)
    hotspot_delta = hotspot_mean - non_hotspot_mean
    if hotspot_cases and non_hotspot_non_empty_cases:
        if hotspot_delta > 0.05:
            hotspot_assessment = "hotspot_biased_emergencies_show_higher_success"
        elif hotspot_delta < -0.05:
            hotspot_assessment = "hotspot_biased_emergencies_do_not_show_higher_success"
        else:
            hotspot_assessment = "hotspot_effect_is_mixed"
    else:
        hotspot_assessment = "insufficient_data"

    preemption_cases = [case["case_id"] for case in case_summaries if case["diagnostics"]["controlled_preemption_count"] > 0]
    preemption_insufficient_cases = [
        case["case_id"]
        for case in case_summaries
        if case["diagnostics"]["controlled_preemption_count"] > 0
        and case["diagnostics"]["emergency_unfinished_count"] > 0
    ]
    regular_damage_ratio = (
        sum(int(case["diagnostics"]["regular_completion_rate_dropped"]) for case in non_empty_cases) / len(non_empty_cases)
        if non_empty_cases
        else 0.0
    )
    mean_emg_completion = _mean(case["stage2"]["cr_emg"] for case in non_empty_cases)
    if mean_emg_completion < 0.95 and regular_damage_ratio <= 0.2:
        style = "conservative_but_stable"
    elif mean_emg_completion >= 0.9 and regular_damage_ratio > 0.5:
        style = "aggressive_but_regular_damage_visible"
    else:
        style = "balanced_tradeoff"

    return {
        "empty_cases_degenerate_to_baseline": {
            "all_cases": bool(empty_cases) and all(case["baseline_impact"]["degenerates_to_baseline"] for case in empty_cases),
            "case_ids": [case["case_id"] for case in empty_cases],
            "failed_case_ids": [case["case_id"] for case in empty_cases if not case["baseline_impact"]["degenerates_to_baseline"]],
        },
        "hotspot_insertability": {
            "assessment": hotspot_assessment,
            "hotspot_mean_cr_emg": hotspot_mean,
            "non_hotspot_mean_cr_emg": non_hotspot_mean,
            "delta": hotspot_delta,
        },
        "rho_groups": rho_groups,
        "candidate_groups": candidate_groups,
        "controlled_preemption_case_ids": preemption_cases,
        "preemption_still_insufficient_case_ids": preemption_insufficient_cases,
        "stage2_behavior_style": {
            "assessment": style,
            "mean_non_empty_cr_emg": mean_emg_completion,
            "regular_completion_drop_case_ratio": regular_damage_ratio,
        },
    }


def build_summary_markdown(summary: dict[str, Any]) -> str:
    lines = [
        "# Stage2 Emergency Validation Summary",
        "",
        f"- suite_name: `{summary['suite_name']}`",
        f"- scenario: `{summary['scenario_path']}`",
        f"- stage1_result: `{summary['stage1_result_path']}`",
        f"- case_count: `{summary['case_count']}`",
        f"- candidate_indices: `{', '.join(str(item) for item in summary['candidate_indices'])}`",
        f"- rho_labels: `{', '.join(item['label'] for item in summary['rho_values'])}`",
        "",
        "## Key Findings",
        "",
    ]
    findings = summary["findings"]
    empty_info = findings["empty_cases_degenerate_to_baseline"]
    lines.append(f"- Empty-emergency degeneration: `{empty_info['all_cases']}`")
    lines.append(f"- Hotspot assessment: `{findings['hotspot_insertability']['assessment']}`")
    lines.append(f"- Controlled preemption used in cases: `{', '.join(findings['controlled_preemption_case_ids']) or 'none'}`")
    lines.append(
        f"- Preemption still insufficient in cases: `{', '.join(findings['preemption_still_insufficient_case_ids']) or 'none'}`"
    )
    lines.append(f"- Observed stage2 style: `{findings['stage2_behavior_style']['assessment']}`")
    lines.extend(
        [
            "",
            "## By Case Type",
            "",
            "| case_type | count | mean_cr_emg | mean_cr_reg_before | mean_cr_reg_after | mean_preemptions | mean_elapsed_s |",
            "|---|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for label, stats in summary["aggregates"]["by_case_type"].items():
        lines.append(
            f"| {label} | {stats['case_count']} | {stats['mean_cr_emg']:.4f} | {stats['mean_cr_reg_before']:.4f} | {stats['mean_cr_reg_after']:.4f} | {stats['mean_n_preemptions']:.2f} | {stats['mean_elapsed_seconds']:.4f} |"
        )
    if summary["aggregates"]["by_rho"]:
        lines.extend(["", "## By Rho", "", "| rho | count | mean_cr_emg | mean_cr_reg_after | mean_preemptions |", "|---|---:|---:|---:|---:|"])
        for label, stats in summary["aggregates"]["by_rho"].items():
            lines.append(
                f"| {label} | {stats['case_count']} | {stats['mean_cr_emg']:.4f} | {stats['mean_cr_reg_after']:.4f} | {stats['mean_n_preemptions']:.2f} |"
            )
    if summary["aggregates"]["by_candidate"]:
        lines.extend(
            [
                "",
                "## By Candidate",
                "",
                "| candidate | count | mean_cr_emg | mean_cr_reg_after | mean_preemptions |",
                "|---|---:|---:|---:|---:|",
            ]
        )
        for label, stats in summary["aggregates"]["by_candidate"].items():
            lines.append(
                f"| {label} | {stats['case_count']} | {stats['mean_cr_emg']:.4f} | {stats['mean_cr_reg_after']:.4f} | {stats['mean_n_preemptions']:.2f} |"
            )
    lines.extend(
        [
            "",
            "## Cases",
            "",
            "| case_id | case_type | candidate | rho | emg_count | cr_emg | cr_reg_before | cr_reg_after | preemptions | degraded_reg_tasks |",
            "|---|---|---:|---|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for case in summary["cases"]:
        lines.append(
            f"| {case['case_id']} | {case['case_type']} | {case['selected_candidate']['requested_candidate_index']} | {case['rho_label']} | {case['emergency_task_set']['summary']['count']} | {case['stage2']['cr_emg']:.4f} | {case['baseline_impact']['cr_reg_before']:.4f} | {case['baseline_impact']['cr_reg_after']:.4f} | {case['stage2']['n_preemptions']} | {len(case['baseline_impact']['regular_task_degradation'])} |"
        )
    return "\n".join(lines) + "\n"


def build_case_markdown(case_summary: dict[str, Any]) -> str:
    lines = [
        f"# {case_summary['case_id']}",
        "",
        f"- case_type: `{case_summary['case_type']}`",
        f"- candidate_index: `{case_summary['selected_candidate']['requested_candidate_index']}`",
        f"- rho: `{case_summary['rho_label']}` -> `{case_summary['rho']}`",
        f"- baseline_source: `{case_summary['baseline']['baseline_source']}`",
        f"- emergency_count: `{case_summary['emergency_task_set']['summary']['count']}`",
        f"- solver_mode: `{case_summary['stage2']['solver_mode']}`",
        f"- cr_reg_before / after: `{case_summary['baseline_impact']['cr_reg_before']}` -> `{case_summary['baseline_impact']['cr_reg_after']}`",
        f"- cr_emg: `{case_summary['stage2']['cr_emg']}`",
        f"- n_preemptions: `{case_summary['stage2']['n_preemptions']}`",
        f"- regular_completion_rate_dropped: `{case_summary['diagnostics']['regular_completion_rate_dropped']}`",
        f"- strict_baseline_degenerate: `{case_summary['baseline_impact']['degenerates_to_baseline']}`",
        "",
        "## Emergency Diagnostics",
        "",
        f"- direct_success_task_ids: `{', '.join(case_summary['diagnostics']['direct_success_task_ids']) or 'none'}`",
        f"- controlled_preemption_task_ids: `{', '.join(case_summary['diagnostics']['preemption_success_task_ids']) or 'none'}`",
        f"- failed_task_ids: `{', '.join(case_summary['diagnostics']['failed_emergency_task_ids']) or 'none'}`",
        f"- degraded_regular_tasks: `{', '.join(case_summary['baseline_impact']['regular_task_degradation']) or 'none'}`",
        "",
    ]
    return "\n".join(lines)


def run_stage2_emergency_validation(
    *,
    scenario_path: str | Path,
    stage1_result_path: str | Path,
    output_root: str | Path | None = None,
    suite_name: str = "smoke",
    run_name: str | None = None,
    candidate_indices: list[int] | None = None,
    rho_values: list[dict[str, Any]] | None = None,
    cases: list[dict[str, Any]] | None = None,
    seed: int = 7,
) -> dict[str, Any]:
    scenario_path = Path(scenario_path).resolve()
    stage1_result_path = Path(stage1_result_path).resolve()
    output_root = Path(output_root or DEFAULT_OUTPUT_ROOT).resolve()
    stage1_artifacts = load_stage1_artifacts(stage1_result_path)
    base_payload = json.loads(scenario_path.read_text(encoding="utf-8-sig"))
    base_rho = float(base_payload.get("stage1", {}).get("rho", 0.0))
    resolved_candidate_indices = list(candidate_indices or [0])
    resolved_rho_values = list(rho_values or [{"label": "default", "value": base_rho, "is_default": True}])
    resolved_cases = list(cases or build_suite_cases(suite_name))
    run_name = run_name or f"{_sanitize_slug(suite_name)}_{time.strftime('%Y%m%d_%H%M%S')}"
    run_dir = output_root / run_name
    run_dir.mkdir(parents=True, exist_ok=True)

    case_summaries: list[dict[str, Any]] = []
    regular_task_ids = {
        str(task.get("id"))
        for task in base_payload.get("tasks", [])
        if str(task.get("type", "")).lower() == "reg"
    }

    for case_spec in resolved_cases:
        case_name = str(case_spec.get("name", "case"))
        case_type = str(case_spec.get("case_type", case_name))
        case_seed = int(case_spec.get("seed", seed + int(case_spec.get("seed_offset", 0))))

        for candidate_index in resolved_candidate_indices:
            plan, plan_rows, candidate_info, uses_selected_plan = stage1_artifacts.resolve_candidate(candidate_index)
            for rho_variant in resolved_rho_values:
                rho_value = float(rho_variant["value"])
                rho_label = str(rho_variant["label"])
                case_id = "__".join((_sanitize_slug(case_name), f"cand_{candidate_index}", rho_label))
                case_dir = run_dir / case_id
                case_dir.mkdir(parents=True, exist_ok=True)

                emergency_tasks_raw = _load_case_tasks(
                    case_spec,
                    base_payload=base_payload,
                    plan_rows=plan_rows,
                    seed=case_seed,
                )
                emergency_tasks, id_mapping = ensure_unique_task_ids(
                    emergency_tasks_raw,
                    regular_task_ids,
                    prefix=_sanitize_slug(case_name),
                )
                effective_payload = _build_effective_scenario_payload(
                    base_payload=base_payload,
                    emergency_tasks=emergency_tasks,
                    case_name=case_name,
                    case_type=case_type,
                    suite_name=suite_name,
                    candidate_index=candidate_index,
                    rho_value=rho_value,
                    rho_label=rho_label,
                    seed=case_seed,
                )
                effective_scenario_path = case_dir / "effective_scenario.json"
                emergency_task_path = case_dir / "emergency_tasks.json"
                write_json(effective_scenario_path, effective_payload)
                write_json(
                    emergency_task_path,
                    {
                        "case_name": case_name,
                        "case_type": case_type,
                        "seed": case_seed,
                        "task_id_mapping": id_mapping,
                        "tasks": _serialize_task_tasks(emergency_tasks),
                        "summary": summarize_task_set(emergency_tasks),
                    },
                )

                scenario = load_scenario(effective_scenario_path)
                use_stage1_trace = (
                    uses_selected_plan
                    and stage1_artifacts.baseline_trace is not None
                    and abs(float(stage1_artifacts.baseline_trace_rho or 0.0) - float(rho_value)) <= EPS
                )
                provided_baseline_trace = stage1_artifacts.baseline_trace if use_stage1_trace else None
                if provided_baseline_trace is not None:
                    baseline_trace = provided_baseline_trace
                    baseline_source = "stage1_result"
                else:
                    baseline_trace = RegularEvaluator(scenario).baseline_trace(plan, rho=rho_value)
                    baseline_source = "reconstructed_from_stage1"

                result = run_stage2(scenario, plan=plan, baseline_trace=provided_baseline_trace)
                task_outcomes = summarize_task_outcomes(scenario, result.allocations)
                baseline_summary = summarize_baseline_trace(scenario, baseline_trace)
                baseline_cr_reg_before = float(baseline_summary["reg"]["weighted_true_completion"])
                baseline_success_rate_before = float(baseline_summary["reg"]["success_rate"])

                insertion_events = list(result.metadata.get("emergency_insertions") or [])
                direct_success_task_ids = [
                    str(item["task_id"])
                    for item in insertion_events
                    if str(item.get("strategy")) == "direct_insert" and bool(item.get("completed"))
                ]
                preemption_success_task_ids = [
                    str(item["task_id"])
                    for item in insertion_events
                    if str(item.get("strategy")) == "controlled_preemption" and bool(item.get("completed"))
                ]
                failed_emergency_task_ids = list(task_outcomes["emg"]["failed_task_ids"])
                affected_regular_task_ids = sorted(
                    {
                        str(item.get("preempted_task_id"))
                        for item in insertion_events
                        if item.get("preempted_task_id") not in {None, ""}
                    }.union(set(result.metadata.get("regular_tasks_degraded_by_emergency") or []))
                )
                regular_success_rate_after = float(task_outcomes["reg"]["success_rate"])
                regular_completion_rate_dropped = regular_success_rate_after + EPS < baseline_success_rate_before
                strict_degenerate = is_strict_baseline_degenerate(
                    baseline_trace=baseline_trace,
                    result=result,
                    baseline_reg_completion=baseline_cr_reg_before,
                )

                selected_candidate = _selected_candidate_summary(
                    candidate_index=candidate_index,
                    candidate_info=candidate_info,
                    selected_candidate_index=stage1_artifacts.selected_candidate_index,
                    selected_candidate_source=stage1_artifacts.selected_candidate_source,
                    plan=plan,
                    t_pre=float(scenario.stage1.t_pre),
                )

                case_summary = {
                    "case_id": case_id,
                    "case_name": case_name,
                    "case_type": case_type,
                    "description": case_spec.get("description"),
                    "scenario_path": str(scenario_path),
                    "stage1_result_path": str(stage1_result_path),
                    "case_dir": str(case_dir),
                    "seed": case_seed,
                    "rho": rho_value,
                    "rho_label": rho_label,
                    "candidate_label": f"candidate_{candidate_index}",
                    "selected_candidate": selected_candidate,
                    "emergency_task_set": {
                        "source_mode": str((case_spec.get("source") or {}).get("mode", "generate")),
                        "summary": summarize_task_set(emergency_tasks),
                        "task_ids": [str(task["id"]) for task in emergency_tasks],
                    },
                    "baseline": {
                        "baseline_source": baseline_source,
                        "stage2_reported_baseline_source": result.metadata.get("baseline_source"),
                        "selected_plan_scale": {
                            "window_count": len(plan),
                            "gateway_count": gateway_count(plan),
                            "activation_count": activation_count(plan, float(scenario.stage1.t_pre)),
                        },
                        "baseline_summary": baseline_summary["summary"],
                    },
                    "stage2": {
                        "solver_mode": result.solver_mode,
                        "cr_reg": float(result.cr_reg),
                        "cr_emg": float(result.cr_emg),
                        "n_preemptions": int(result.n_preemptions),
                        "u_cross": float(result.u_cross),
                        "u_all": float(result.u_all),
                        "elapsed_seconds": float(result.metadata.get("elapsed_seconds", 0.0)),
                    },
                    "diagnostics": {
                        "emergency_total": int(task_outcomes["emg"]["count"]),
                        "emergency_success_count": int(task_outcomes["emg"]["success_count"]),
                        "emergency_unfinished_count": int(task_outcomes["emg"]["failure_count"]),
                        "controlled_preemption_count": int(result.metadata.get("emergency_insertions_used_preemption_count", 0)),
                        "affected_regular_task_count": len(affected_regular_task_ids),
                        "affected_regular_task_ids": affected_regular_task_ids,
                        "regular_completion_rate_dropped": bool(regular_completion_rate_dropped),
                        "direct_success_task_ids": direct_success_task_ids,
                        "preemption_success_task_ids": preemption_success_task_ids,
                        "failed_emergency_task_ids": failed_emergency_task_ids,
                        "insertion_events": insertion_events,
                    },
                    "baseline_impact": {
                        "cr_reg_before": baseline_cr_reg_before,
                        "cr_reg_after": float(result.cr_reg),
                        "cr_emg": float(result.cr_emg),
                        "regular_task_degradation": list(result.metadata.get("regular_tasks_degraded_by_emergency") or []),
                        "cross_window_usage_delta_by_segment": dict(result.metadata.get("cross_window_usage_delta_by_segment") or {}),
                        "regular_success_rate_before": baseline_success_rate_before,
                        "regular_success_rate_after": regular_success_rate_after,
                        "degenerates_to_baseline": bool(strict_degenerate),
                    },
                    "task_outcomes": {
                        "baseline": baseline_summary,
                        "stage2": task_outcomes,
                    },
                    "paths": {
                        "effective_scenario": str(effective_scenario_path),
                        "emergency_tasks": str(emergency_task_path),
                        "stage2_result": str(case_dir / "stage2_result.json"),
                        "summary_json": str(case_dir / "case_summary.json"),
                        "summary_md": str(case_dir / "case_summary.md"),
                    },
                }
                write_json(case_dir / "stage2_result.json", stage2_result_to_dict(result, float(scenario.stage1.t_pre)))
                write_json(case_dir / "case_summary.json", case_summary)
                (case_dir / "case_summary.md").write_text(build_case_markdown(case_summary), encoding="utf-8")
                case_summaries.append(case_summary)

    summary = {
        "suite_name": suite_name,
        "run_name": run_name,
        "scenario_path": str(scenario_path),
        "stage1_result_path": str(stage1_result_path),
        "run_dir": str(run_dir),
        "generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "case_count": len(case_summaries),
        "candidate_indices": resolved_candidate_indices,
        "rho_values": resolved_rho_values,
        "aggregates": {
            "by_case_type": _group_case_metrics(case_summaries, "case_type"),
            "by_rho": _group_case_metrics(case_summaries, "rho_label"),
            "by_candidate": _group_case_metrics(case_summaries, "candidate_label"),
        },
        "findings": infer_validation_findings(case_summaries),
        "cases": case_summaries,
    }
    write_json(run_dir / "summary.json", summary)
    (run_dir / "summary.md").write_text(build_summary_markdown(summary), encoding="utf-8")
    return summary
