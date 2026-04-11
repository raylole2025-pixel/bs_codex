from __future__ import annotations

from .models import Allocation, ScheduledWindow, Scenario, Segment
from .regular_routing_common import (
    EPS,
    build_regular_schedule_diagnostics,
    completion_tolerance,
    regular_priority_key,
    stage1_style_path_options,
)
from .scenario import active_cross_links, active_intra_links, generate_candidate_paths


def build_regular_baseline_stage1_greedy(
    scenario: Scenario,
    plan: list[ScheduledWindow],
    segments: list[Segment],
) -> tuple[dict[tuple[str, int], Allocation], dict[str, bool], dict]:
    regular_tasks = [task for task in scenario.tasks if task.task_type == "reg"]
    schedule: dict[tuple[str, int], Allocation] = {}
    if not regular_tasks or not segments:
        return schedule, {}, build_regular_schedule_diagnostics(scenario, plan, segments, schedule)

    remaining = {task.task_id: float(task.data) for task in regular_tasks}
    prev_path_keys = {task.task_id: None for task in regular_tasks}
    cross_reg_capacity = max((1.0 - float(scenario.stage1.rho)) * float(scenario.capacities.cross), 0.0)

    for segment in segments:
        if segment.duration <= EPS:
            continue

        active_tasks = [
            task
            for task in regular_tasks
            if float(task.arrival) <= float(segment.start) < float(task.deadline)
            and remaining.get(task.task_id, 0.0) > completion_tolerance(scenario, task)
        ]
        active_tasks.sort(key=lambda task: regular_priority_key(task, remaining.get(task.task_id, 0.0), segment.start))

        cap_res: dict[str, float] = {}
        for link in active_intra_links(scenario, "A", segment.start):
            cap_res[link.link_id] = float(scenario.capacities.domain_a)
        for link in active_intra_links(scenario, "B", segment.start):
            cap_res[link.link_id] = float(scenario.capacities.domain_b)
        cross_now = active_cross_links(plan, segment.start)
        for window in cross_now:
            cap_res[window.window_id] = cross_reg_capacity
        cross_used = {window.window_id: 0.0 for window in cross_now}
        served_this_segment: set[str] = set()

        for task in active_tasks:
            remaining_task = float(remaining.get(task.task_id, 0.0))
            if remaining_task <= EPS:
                continue
            candidates = generate_candidate_paths(scenario, plan, task, segment, scenario.stage2.k_paths)
            options = stage1_style_path_options(
                scenario=scenario,
                task=task,
                segment=segment,
                candidates=candidates,
                cap_res=cap_res,
                cross_used=cross_used,
                remaining_task=remaining_task,
                prev_path_key=prev_path_keys.get(task.task_id),
                cross_reg_capacity=cross_reg_capacity,
            )
            if not options:
                continue
            selected = options[0]
            schedule[(task.task_id, segment.index)] = Allocation(
                task_id=task.task_id,
                segment_index=segment.index,
                path_id=selected.candidate.path_id,
                edge_ids=selected.candidate.edge_ids,
                rate=selected.rate,
                delivered=selected.delivered,
                task_type=task.task_type,
            )
            for edge_id in selected.candidate.edge_ids:
                if edge_id in cap_res:
                    cap_res[edge_id] = max(0.0, float(cap_res[edge_id]) - selected.rate)
            if selected.candidate.cross_window_id is not None:
                cross_used[selected.candidate.cross_window_id] = float(cross_used.get(selected.candidate.cross_window_id, 0.0)) + selected.rate
            remaining[task.task_id] = max(0.0, remaining_task - selected.delivered)
            prev_path_keys[task.task_id] = selected.path_key
            served_this_segment.add(task.task_id)

        for task in active_tasks:
            if task.task_id not in served_this_segment:
                prev_path_keys[task.task_id] = None

    diagnostics = build_regular_schedule_diagnostics(scenario, plan, segments, schedule)
    return schedule, dict(diagnostics["completed"]), diagnostics
