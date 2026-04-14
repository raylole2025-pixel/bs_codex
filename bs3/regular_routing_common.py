from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Any

from .models import Allocation, PathCandidate, ScheduledWindow, Scenario, Segment, Task
from .scenario import active_cross_links, active_intra_links

EPS = 1e-9
REGULAR_BASELINE_MODES = {
    "stage1_greedy",
    "stage1_greedy_repair",
    "rolling_milp",
    "full_milp",
}


@dataclass(frozen=True)
class ScoredPathOption:
    candidate: PathCandidate
    path_key: tuple[str, ...]
    rate: float
    delivered: float
    effective_duration: float
    predicted_cross_load: float


def resolve_regular_baseline_mode(stage2_config) -> str:
    raw_mode = str(getattr(stage2_config, "regular_baseline_mode", "") or "").strip().lower()
    if raw_mode in REGULAR_BASELINE_MODES:
        return raw_mode
    return "stage1_greedy_repair"


def is_regular_repair_enabled(stage2_config, mode: str | None = None) -> bool:
    resolved_mode = mode or resolve_regular_baseline_mode(stage2_config)
    override = getattr(stage2_config, "regular_repair_enabled", None)
    if override is None:
        return resolved_mode == "stage1_greedy_repair"
    return bool(override) and resolved_mode == "stage1_greedy_repair"


def completion_tolerance(scenario: Scenario, task: Task) -> float:
    return max(float(scenario.stage2.completion_tolerance) * max(float(task.data), 0.0), EPS)


def is_task_complete(scenario: Scenario, task: Task, remaining_data: float) -> bool:
    return remaining_data <= completion_tolerance(scenario, task)


def regular_priority_key(task: Task, remaining_data: float, segment_start: float) -> tuple[float, ...]:
    slack = max(float(task.deadline) - float(segment_start), EPS)
    urgency = max(float(remaining_data), 0.0) / slack
    return (-float(task.weight), -urgency, -max(float(remaining_data), 0.0), str(task.task_id))


def cross_link_from_edges(edge_ids: tuple[str, ...], cross_edge_ids: set[str]) -> str | None:
    for edge_id in edge_ids:
        if edge_id in cross_edge_ids:
            return edge_id
    for edge_id in edge_ids:
        if edge_id.startswith("X") or edge_id.startswith("W"):
            return edge_id
    return None


def stage1_style_path_options(
    scenario: Scenario,
    task: Task,
    segment: Segment,
    candidates: list[PathCandidate],
    cap_res: dict[str, float],
    cross_used: dict[str, float],
    remaining_task: float,
    prev_path_key: tuple[str, ...] | None,
    cross_reg_capacity: float,
) -> list[ScoredPathOption]:
    options: list[ScoredPathOption] = []
    for candidate in candidates:
        effective_duration = max(0.0, min(float(segment.end), float(task.deadline) - float(candidate.delay)) - float(segment.start))
        if effective_duration <= EPS:
            continue
        bottleneck = float("inf")
        feasible = True
        for edge_id in candidate.edge_ids:
            available = cap_res.get(edge_id)
            if available is None:
                feasible = False
                break
            bottleneck = min(bottleneck, float(available))
        if not feasible or bottleneck <= EPS:
            continue
        rate = min(float(task.max_rate), bottleneck, max(float(remaining_task), 0.0) / effective_duration)
        if rate <= EPS:
            continue
        delivered = rate * effective_duration
        if delivered <= EPS:
            continue
        cross_link = candidate.cross_window_id
        predicted_cross_load = 0.0
        if cross_link is not None:
            predicted_cross_load = (float(cross_used.get(cross_link, 0.0)) + rate) / max(float(cross_reg_capacity), EPS)
        options.append(
            ScoredPathOption(
                candidate=candidate,
                path_key=tuple(candidate.edge_ids),
                rate=rate,
                delivered=delivered,
                effective_duration=effective_duration,
                predicted_cross_load=predicted_cross_load,
            )
        )

    if not options:
        return []

    max_delivered = max(option.delivered for option in options)
    near_threshold = float(scenario.stage1.eta_x) * max_delivered
    near_options = [option for option in options if option.delivered + EPS >= near_threshold]
    near_options.sort(
        key=lambda option: (
            option.predicted_cross_load,
            float(option.candidate.delay),
            0 if prev_path_key is not None and option.path_key == prev_path_key else 1,
            int(option.candidate.hop_count),
            str(option.candidate.path_id),
        )
    )
    return near_options


def build_regular_schedule_diagnostics(
    scenario: Scenario,
    plan: list[ScheduledWindow],
    segments: list[Segment],
    schedule: dict[tuple[str, int], Allocation],
) -> dict[str, Any]:
    regular_tasks = [task for task in scenario.tasks if task.task_type == "reg"]
    remaining = {task.task_id: float(task.data) for task in regular_tasks}
    prev_cross_link = {task.task_id: None for task in regular_tasks}
    prev_path_key = {task.task_id: None for task in regular_tasks}
    cross_edge_ids = {window.window_id for window in plan}
    cross_reg_capacity = max((1.0 - float(scenario.stage1.rho)) * float(scenario.capacities.cross), 0.0)

    remaining_before_trace: dict[str, dict[int, float]] = {task.task_id: {} for task in regular_tasks}
    remaining_after_trace: dict[str, dict[int, float]] = {task.task_id: {} for task in regular_tasks}
    previous_cross_link_trace: dict[str, dict[int, str | None]] = {task.task_id: {} for task in regular_tasks}
    previous_path_key_trace: dict[str, dict[int, tuple[str, ...] | None]] = {task.task_id: {} for task in regular_tasks}
    selected_cross_link_trace: dict[str, dict[int, str | None]] = {task.task_id: {} for task in regular_tasks}
    selected_path_key_trace: dict[str, dict[int, tuple[str, ...] | None]] = {task.task_id: {} for task in regular_tasks}
    cross_usage_by_segment: dict[int, dict[str, float]] = {}
    segment_metrics: dict[int, dict[str, float | int | list[str]]] = {}

    for segment in segments:
        active_tasks = [
            task
            for task in regular_tasks
            if float(task.arrival) <= float(segment.start) < float(task.deadline)
            and not is_task_complete(scenario, task, remaining.get(task.task_id, 0.0))
        ]
        active_task_ids = [task.task_id for task in active_tasks]
        active_cross = active_cross_links(plan, segment.start)
        cross_usage = {window.window_id: 0.0 for window in active_cross}
        served_this_segment: set[str] = set()
        switch_count = 0

        for task in regular_tasks:
            task_id = task.task_id
            remaining_before_trace[task_id][segment.index] = float(remaining.get(task_id, 0.0))
            previous_cross_link_trace[task_id][segment.index] = prev_cross_link.get(task_id)
            previous_path_key_trace[task_id][segment.index] = prev_path_key.get(task_id)
            selected_cross_link_trace[task_id][segment.index] = None
            selected_path_key_trace[task_id][segment.index] = None

        for task in active_tasks:
            alloc = schedule.get((task.task_id, segment.index))
            remaining_now = float(remaining.get(task.task_id, 0.0))
            if alloc is None or segment.duration <= EPS or remaining_now <= EPS:
                continue
            delivered = min(float(alloc.delivered), remaining_now)
            if delivered <= EPS:
                continue
            actual_rate = delivered / float(segment.duration)
            remaining[task.task_id] = max(0.0, remaining_now - delivered)
            cross_link = cross_link_from_edges(tuple(alloc.edge_ids), cross_edge_ids)
            path_key = tuple(alloc.edge_ids)
            if cross_link is not None and prev_cross_link.get(task.task_id) is not None and cross_link != prev_cross_link[task.task_id]:
                switch_count += 1
            if cross_link is not None:
                cross_usage[cross_link] = float(cross_usage.get(cross_link, 0.0)) + actual_rate
            selected_cross_link_trace[task.task_id][segment.index] = cross_link
            selected_path_key_trace[task.task_id][segment.index] = path_key
            prev_cross_link[task.task_id] = cross_link
            prev_path_key[task.task_id] = path_key
            served_this_segment.add(task.task_id)

        for task in active_tasks:
            if task.task_id not in served_this_segment:
                prev_cross_link[task.task_id] = None
                prev_path_key[task.task_id] = None

        for task in regular_tasks:
            remaining_after_trace[task.task_id][segment.index] = float(remaining.get(task.task_id, 0.0))

        q_values = {
            cross_link: (rate / max(cross_reg_capacity, EPS) if cross_reg_capacity > EPS else 0.0)
            for cross_link, rate in cross_usage.items()
        }
        q_peak = max(q_values.values(), default=0.0)
        util_values = list(q_values.values())
        imbalance = max(util_values) - min(util_values) if len(util_values) >= 2 else (util_values[0] if util_values else 0.0)
        cross_usage_by_segment[segment.index] = cross_usage
        segment_metrics[segment.index] = {
            "q_peak": q_peak,
            "imbalance": imbalance,
            "switch_count": switch_count,
            "active_task_count": len(active_tasks),
            "active_tasks": list(active_task_ids),
            "cross_link_count": len(active_cross),
            "cross_rate_used": sum(cross_usage.values()),
        }

    completed = {
        task.task_id: is_task_complete(scenario, task, remaining.get(task.task_id, 0.0))
        for task in regular_tasks
    }
    return {
        "remaining_before_trace": remaining_before_trace,
        "remaining_after_trace": remaining_after_trace,
        "previous_cross_link_trace": previous_cross_link_trace,
        "previous_path_key_trace": previous_path_key_trace,
        "selected_cross_link_trace": selected_cross_link_trace,
        "selected_path_key_trace": selected_path_key_trace,
        "cross_usage_by_segment": cross_usage_by_segment,
        "segment_metrics": segment_metrics,
        "remaining_end": remaining,
        "completed": completed,
        "completed_task_ids": [task_id for task_id, done in completed.items() if done],
    }


def empty_repair_metadata(completed: dict[str, bool]) -> dict[str, Any]:
    completed_count = sum(1 for done in completed.values() if done)
    return {
        "regular_repair_enabled": False,
        "repair_block_count_considered": 0,
        "repair_block_count_accepted": 0,
        "repair_total_improvement_peak": 0.0,
        "repair_total_improvement_integral": 0.0,
        "baseline_completed_count_before_repair": int(completed_count),
        "baseline_completed_count_after_repair": int(completed_count),
    }
