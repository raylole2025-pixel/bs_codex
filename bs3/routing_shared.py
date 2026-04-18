from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from .models import PathCandidate, Scenario, Segment, Task

EPS = 1e-9


@dataclass(frozen=True)
class ScoredPathOption:
    candidate: PathCandidate
    path_key: tuple[str, ...]
    rate: float
    delivered: float
    effective_duration: float
    predicted_cross_load: float


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


def clamp01(value: float) -> float:
    return min(max(float(value), 0.0), 1.0)


def post_allocation_max_utilization(
    edge_ids: tuple[str, ...],
    capacities: Mapping[str, float],
    free_before: Mapping[str, float],
    rate: float,
) -> float:
    max_utilization = 0.0
    for edge_id in edge_ids:
        capacity = float(capacities.get(edge_id, 0.0))
        if capacity <= EPS:
            continue
        used_after = max(capacity - float(free_before.get(edge_id, 0.0)) + float(rate), 0.0)
        max_utilization = max(max_utilization, used_after / capacity)
    return max_utilization


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
