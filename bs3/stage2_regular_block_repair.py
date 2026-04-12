from __future__ import annotations

from collections import defaultdict
from typing import Any

import pulp

from .models import Allocation, ScheduledWindow, Scenario, Segment, Task
from .regular_routing_common import EPS, build_regular_schedule_diagnostics, stage1_style_path_options
from .scenario import generate_candidate_paths
from .stage2_regular_joint_milp import (
    _CandidateRecord,
    _ObjectiveStage,
    _accept_stage_solution,
    _build_edge_capacities,
    _build_solver,
    _build_task_segments,
    _expr_sum,
)


def repair_regular_baseline_blocks(
    scenario: Scenario,
    plan: list[ScheduledWindow],
    segments: list[Segment],
    baseline_schedule: dict[tuple[str, int], Allocation],
    baseline_diag: dict[str, Any],
) -> tuple[dict[tuple[str, int], Allocation], dict[str, Any]]:
    current_schedule = dict(baseline_schedule)
    current_diag = baseline_diag
    blocks = _select_repair_blocks(scenario, segments, baseline_diag)
    metadata: dict[str, Any] = {
        "regular_repair_enabled": True,
        "repair_block_count_considered": len(blocks),
        "repair_block_count_accepted": 0,
        "repair_total_improvement_peak": 0.0,
        "repair_total_improvement_integral": 0.0,
        "baseline_completed_count_before_repair": sum(1 for done in baseline_diag["completed"].values() if done),
        "baseline_completed_count_after_repair": sum(1 for done in baseline_diag["completed"].values() if done),
        "diagnostics_before": baseline_diag,
        "diagnostics_after": baseline_diag,
    }
    if not blocks:
        return current_schedule, metadata

    for block in blocks:
        block_result = _repair_block(
            scenario=scenario,
            plan=plan,
            segments=segments,
            current_schedule=current_schedule,
            diagnostics=current_diag,
            block=block,
        )
        if not block_result["accepted"]:
            continue
        candidate_schedule = block_result["schedule"]
        candidate_diag = build_regular_schedule_diagnostics(scenario, plan, segments, candidate_schedule)
        current_completed = {task_id for task_id, done in current_diag["completed"].items() if done}
        candidate_completed = {task_id for task_id, done in candidate_diag["completed"].items() if done}
        current_peak, current_integral = _global_objective_from_diag(current_diag, segments)
        candidate_peak, candidate_integral = _global_objective_from_diag(candidate_diag, segments)
        epsilon = float(scenario.stage2.repair_accept_epsilon)
        if not current_completed.issubset(candidate_completed):
            continue
        if candidate_peak + epsilon >= current_peak:
            continue
        if candidate_integral > current_integral + epsilon:
            continue

        current_schedule = candidate_schedule
        current_diag = candidate_diag
        metadata["repair_block_count_accepted"] += 1
        metadata["repair_total_improvement_peak"] += max(current_peak - candidate_peak, 0.0)
        metadata["repair_total_improvement_integral"] += max(current_integral - candidate_integral, 0.0)
        metadata["diagnostics_after"] = current_diag

    metadata["baseline_completed_count_after_repair"] = sum(1 for done in current_diag["completed"].values() if done)
    return current_schedule, metadata


def _segment_score(segment_metrics: dict[str, Any]) -> float:
    return float(segment_metrics.get("q_peak", 0.0)) + float(segment_metrics.get("imbalance", 0.0))


def _select_repair_blocks(
    scenario: Scenario,
    segments: list[Segment],
    diagnostics: dict[str, Any],
) -> list[dict[str, Any]]:
    if not segments:
        return []

    threshold = max(float(scenario.stage2.repair_util_threshold), 0.0)
    min_active = max(int(scenario.stage2.repair_min_active_tasks), 1)
    expand = max(int(scenario.stage2.repair_expand_segments), 0)
    max_len = max(int(scenario.stage2.repair_max_block_segments), 1)
    max_count = max(int(scenario.stage2.repair_block_max_count), 0)
    if max_count <= 0:
        return []

    hot_segments: list[int] = []
    for segment in segments:
        segment_metrics = diagnostics["segment_metrics"].get(segment.index, {})
        if int(segment_metrics.get("active_task_count", 0)) < min_active:
            continue
        if _segment_score(segment_metrics) + EPS < threshold:
            continue
        hot_segments.append(segment.index)

    if not hot_segments:
        return []

    merged_ranges: list[tuple[int, int]] = []
    start = hot_segments[0]
    stop = hot_segments[0]
    for index in hot_segments[1:]:
        if index == stop + 1:
            stop = index
            continue
        merged_ranges.append((start, stop))
        start = stop = index
    merged_ranges.append((start, stop))

    candidates: list[dict[str, Any]] = []
    for raw_start, raw_end in merged_ranges:
        block_start = max(raw_start - expand, 0)
        block_end = min(raw_end + expand, len(segments) - 1)
        if block_end - block_start + 1 > max_len:
            focus = max(
                range(raw_start, raw_end + 1),
                key=lambda idx: _segment_score(diagnostics["segment_metrics"].get(idx, {})),
            )
            half = max_len // 2
            block_start = max(focus - half, 0)
            block_end = min(block_start + max_len - 1, len(segments) - 1)
            block_start = max(block_end - max_len + 1, 0)

        active_task_ids: set[str] = set()
        max_score = 0.0
        for segment_index in range(block_start, block_end + 1):
            segment_metrics = diagnostics["segment_metrics"].get(segment_index, {})
            active_task_ids.update(str(task_id) for task_id in segment_metrics.get("active_tasks", []))
            max_score = max(max_score, _segment_score(segment_metrics))
        if len(active_task_ids) < min_active:
            continue
        candidates.append(
            {
                "start": block_start,
                "end": block_end,
                "score": max_score,
                "active_task_ids": tuple(sorted(active_task_ids)),
            }
        )

    candidates.sort(key=lambda item: (-float(item["score"]), int(item["start"]), int(item["end"])))
    selected: list[dict[str, Any]] = []
    covered: set[int] = set()
    for candidate in candidates:
        block_indices = set(range(int(candidate["start"]), int(candidate["end"]) + 1))
        if covered.intersection(block_indices):
            continue
        selected.append(candidate)
        covered.update(block_indices)
        if len(selected) >= max_count:
            break
    return selected


def _block_objective_from_diag(diagnostics: dict[str, Any], block_segments: list[Segment]) -> tuple[float, float, float]:
    peak = max(float(diagnostics["segment_metrics"].get(segment.index, {}).get("q_peak", 0.0)) for segment in block_segments)
    integral = sum(float(diagnostics["segment_metrics"].get(segment.index, {}).get("q_peak", 0.0)) * float(segment.duration) for segment in block_segments)
    switches = sum(float(diagnostics["segment_metrics"].get(segment.index, {}).get("switch_count", 0.0)) for segment in block_segments)
    return peak, integral, switches


def _global_objective_from_diag(diagnostics: dict[str, Any], segments: list[Segment]) -> tuple[float, float]:
    peak = max((float(diagnostics["segment_metrics"].get(segment.index, {}).get("q_peak", 0.0)) for segment in segments), default=0.0)
    integral = sum(float(diagnostics["segment_metrics"].get(segment.index, {}).get("q_peak", 0.0)) * float(segment.duration) for segment in segments)
    return peak, integral


def _is_strictly_better(candidate: tuple[float, ...], baseline: tuple[float, ...], epsilon: float) -> bool:
    for new_value, base_value in zip(candidate, baseline):
        if new_value < base_value - epsilon:
            return True
        if new_value > base_value + epsilon:
            return False
    return False


def _repair_block(
    scenario: Scenario,
    plan: list[ScheduledWindow],
    segments: list[Segment],
    current_schedule: dict[tuple[str, int], Allocation],
    diagnostics: dict[str, Any],
    block: dict[str, Any],
) -> dict[str, Any]:
    block_segments = segments[int(block["start"]) : int(block["end"]) + 1]
    affected_task_ids = tuple(block["active_task_ids"])
    if not block_segments or len(affected_task_ids) < max(int(scenario.stage2.repair_min_active_tasks), 1):
        return {"accepted": False}

    tasks_by_id = {task.task_id: task for task in scenario.tasks if task.task_type == "reg"}
    affected_tasks = [tasks_by_id[task_id] for task_id in affected_task_ids if task_id in tasks_by_id]
    if len(affected_tasks) < max(int(scenario.stage2.repair_min_active_tasks), 1):
        return {"accepted": False}

    unaffected_task_ids = {
        task_id
        for (task_id, segment_index) in current_schedule
        if task_id not in affected_task_ids and block_segments[0].index <= segment_index <= block_segments[-1].index
    }
    edge_capacities, cross_links_by_segment, reserved_cross = _build_edge_capacities(scenario, plan, block_segments)
    fixed_cross_usage: dict[int, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    for (task_id, segment_index), alloc in current_schedule.items():
        if task_id not in unaffected_task_ids:
            continue
        if segment_index not in edge_capacities:
            continue
        for edge_id in alloc.edge_ids:
            if edge_id in edge_capacities[segment_index]:
                edge_capacities[segment_index][edge_id] = max(0.0, float(edge_capacities[segment_index][edge_id]) - float(alloc.rate))
        for cross_link in cross_links_by_segment.get(segment_index, ()):
            if cross_link in alloc.edge_ids:
                fixed_cross_usage[segment_index][cross_link] += float(alloc.rate)

    task_segments = _build_task_segments(affected_tasks, block_segments)
    candidate_records: dict[tuple[str, int, int], _CandidateRecord] = {}
    segment_candidates: dict[tuple[str, int], list[tuple[str, int, int]]] = defaultdict(list)
    edge_to_candidates: dict[tuple[int, str], list[tuple[str, int, int]]] = defaultdict(list)
    cross_to_candidates: dict[tuple[int, str], list[tuple[str, int, int]]] = defaultdict(list)
    cross_choice_keys: dict[tuple[str, int], dict[str, list[tuple[str, int, int]]]] = defaultdict(lambda: defaultdict(list))

    path_limit = max(int(scenario.stage2.repair_candidate_path_limit), 1)
    task_start_remaining: dict[str, float] = {}
    task_end_remaining_limit: dict[str, float] = {}
    initial_cross_link: dict[str, str | None] = {}

    for task in affected_tasks:
        local_segments = task_segments.get(task.task_id, [])
        if not local_segments:
            continue
        first_segment = local_segments[0]
        task_start_remaining[task.task_id] = float(diagnostics["remaining_before_trace"][task.task_id][first_segment.index])
        task_end_remaining_limit[task.task_id] = float(diagnostics["remaining_after_trace"][task.task_id][block_segments[-1].index])
        initial_cross_link[task.task_id] = diagnostics["previous_cross_link_trace"][task.task_id][first_segment.index]
        for local_index, segment in enumerate(local_segments):
            candidates = generate_candidate_paths(scenario, plan, task, segment, scenario.stage2.k_paths)
            ranked_options = stage1_style_path_options(
                scenario=scenario,
                task=task,
                segment=segment,
                candidates=candidates,
                cap_res=edge_capacities.get(segment.index, {}),
                cross_used=fixed_cross_usage.get(segment.index, {}),
                remaining_task=float(diagnostics["remaining_before_trace"][task.task_id][segment.index]),
                prev_path_key=diagnostics["previous_path_key_trace"][task.task_id][segment.index],
                cross_reg_capacity=reserved_cross,
            )
            for path_index, option in enumerate(ranked_options[:path_limit]):
                rate_upper_bound = min(
                    float(task.max_rate),
                    min((float(edge_capacities[segment.index].get(edge_id, 0.0)) for edge_id in option.candidate.edge_ids), default=0.0),
                )
                if rate_upper_bound <= EPS:
                    continue
                key = (task.task_id, local_index, path_index)
                candidate_records[key] = _CandidateRecord(
                    task_id=task.task_id,
                    local_index=local_index,
                    segment_index=segment.index,
                    path_index=path_index,
                    path=option.candidate,
                    effective_duration=option.effective_duration,
                    rate_upper_bound=rate_upper_bound,
                )
                segment_candidates[(task.task_id, local_index)].append(key)
                for edge_id in option.candidate.edge_ids:
                    edge_to_candidates[(segment.index, edge_id)].append(key)
                if option.candidate.cross_window_id is not None:
                    cross_to_candidates[(segment.index, option.candidate.cross_window_id)].append(key)
                    cross_choice_keys[(task.task_id, local_index)][option.candidate.cross_window_id].append(key)

    if not candidate_records:
        return {"accepted": False}

    model = pulp.LpProblem("stage2_regular_block_repair", pulp.LpMinimize)
    remaining_vars: dict[tuple[str, int], pulp.LpVariable] = {}
    first_switch_vars: dict[str, pulp.LpVariable] = {}
    transition_switch_vars: dict[tuple[str, int], pulp.LpVariable] = {}
    rate_vars = {
        key: pulp.LpVariable(
            f"x_{task_id}_{local_index}_{path_index}",
            lowBound=0.0,
            upBound=record.rate_upper_bound,
        )
        for key, record in candidate_records.items()
        for task_id, local_index, path_index in [key]
    }
    choice_vars = {
        key: pulp.LpVariable(f"z_{task_id}_{local_index}_{path_index}", cat=pulp.LpBinary)
        for key in candidate_records
        for task_id, local_index, path_index in [key]
    }
    q_vars = {
        segment.index: pulp.LpVariable(f"q_{segment.index}", lowBound=0.0, upBound=1.0)
        for segment in block_segments
    }
    q_peak_var = pulp.LpVariable("q_peak_block", lowBound=0.0, upBound=1.0)

    for task in affected_tasks:
        local_segments = task_segments.get(task.task_id, [])
        if not local_segments:
            continue
        start_remaining = float(task_start_remaining.get(task.task_id, task.data))
        for local_index in range(len(local_segments) + 1):
            remaining_vars[(task.task_id, local_index)] = pulp.LpVariable(
                f"R_{task.task_id}_{local_index}",
                lowBound=0.0,
                upBound=max(start_remaining, 0.0),
            )
        first_switch_vars[task.task_id] = pulp.LpVariable(f"u0_{task.task_id}", cat=pulp.LpBinary)
        for local_index in range(1, len(local_segments)):
            transition_switch_vars[(task.task_id, local_index)] = pulp.LpVariable(f"u_{task.task_id}_{local_index}", cat=pulp.LpBinary)

    for task in affected_tasks:
        local_segments = task_segments.get(task.task_id, [])
        if not local_segments:
            continue
        model += remaining_vars[(task.task_id, 0)] == float(task_start_remaining.get(task.task_id, task.data))
        for local_index, segment in enumerate(local_segments):
            keys = segment_candidates.get((task.task_id, local_index), [])
            delivered_terms = [rate_vars[key] * candidate_records[key].effective_duration for key in keys]
            if keys:
                model += pulp.lpSum(choice_vars[key] for key in keys) <= 1.0
                for key in keys:
                    model += rate_vars[key] <= candidate_records[key].rate_upper_bound * choice_vars[key]
                model += _expr_sum(delivered_terms) <= remaining_vars[(task.task_id, local_index)]
            model += remaining_vars[(task.task_id, local_index + 1)] == remaining_vars[(task.task_id, local_index)] - _expr_sum(delivered_terms)
        model += remaining_vars[(task.task_id, len(local_segments))] <= float(task_end_remaining_limit.get(task.task_id, task.data)) + float(scenario.stage2.repair_accept_epsilon)

    for segment in block_segments:
        for edge_id, capacity in edge_capacities.get(segment.index, {}).items():
            keys = edge_to_candidates.get((segment.index, edge_id), [])
            if keys:
                model += pulp.lpSum(rate_vars[key] for key in keys) <= float(capacity)
        model += q_peak_var >= q_vars[segment.index]
        if reserved_cross <= EPS or not cross_links_by_segment.get(segment.index):
            model += q_vars[segment.index] == 0.0
            continue
        for cross_link in cross_links_by_segment.get(segment.index, ()):
            keys = cross_to_candidates.get((segment.index, cross_link), [])
            fixed_usage = float(fixed_cross_usage.get(segment.index, {}).get(cross_link, 0.0))
            model += fixed_usage + pulp.lpSum(rate_vars[key] for key in keys) <= q_vars[segment.index] * float(reserved_cross)

    switch_terms: list = []
    for task in affected_tasks:
        local_segments = task_segments.get(task.task_id, [])
        if not local_segments:
            continue
        first_switch = first_switch_vars[task.task_id]
        switch_terms.append(first_switch)
        preferred = initial_cross_link.get(task.task_id)
        first_choices = cross_choice_keys.get((task.task_id, 0), {})
        if preferred is not None:
            for cross_link, keys in first_choices.items():
                if cross_link == preferred:
                    continue
                model += first_switch >= pulp.lpSum(choice_vars[key] for key in keys)
        for local_index in range(1, len(local_segments)):
            switch_var = transition_switch_vars[(task.task_id, local_index)]
            switch_terms.append(switch_var)
            prev_choices = cross_choice_keys.get((task.task_id, local_index - 1), {})
            curr_choices = cross_choice_keys.get((task.task_id, local_index), {})
            if not prev_choices or not curr_choices:
                continue
            for prev_cross, prev_keys in prev_choices.items():
                for curr_cross, curr_keys in curr_choices.items():
                    if prev_cross == curr_cross:
                        continue
                    model += switch_var >= pulp.lpSum(choice_vars[key] for key in prev_keys) + pulp.lpSum(choice_vars[key] for key in curr_keys) - 1.0

    stages = [
        _ObjectiveStage(name="peak", sense=pulp.LpMinimize, expr=q_peak_var),
        _ObjectiveStage(
            name="integral",
            sense=pulp.LpMinimize,
            expr=_expr_sum([float(segment.duration) * q_vars[segment.index] for segment in block_segments]),
        ),
        _ObjectiveStage(name="switch", sense=pulp.LpMinimize, expr=_expr_sum(switch_terms)),
    ]

    objective_values: dict[str, float] = {}
    time_limit = (
        None
        if scenario.stage2.repair_time_limit_seconds in {None, "", 0, 0.0}
        else float(scenario.stage2.repair_time_limit_seconds)
    )
    for stage in stages:
        solver = _build_solver(scenario, time_limit_seconds=time_limit, log_path=None)
        model.sense = stage.sense
        model.setObjective(stage.expr)
        status_code = model.solve(solver)
        status_name = pulp.LpStatus.get(status_code, str(status_code))
        solution_status_code = getattr(model, "sol_status", None)
        solution_status_name = pulp.LpSolution.get(solution_status_code, str(solution_status_code))
        if not _accept_stage_solution(status_name, solution_status_name, allow_incumbent=True):
            return {"accepted": False}
        optimum = float(pulp.value(stage.expr) or 0.0)
        objective_values[stage.name] = optimum
        model += stage.expr <= optimum + 1e-6

    baseline_objective = _block_objective_from_diag(diagnostics, block_segments)
    candidate_objective = (
        float(objective_values.get("peak", 0.0)),
        float(objective_values.get("integral", 0.0)),
        float(objective_values.get("switch", 0.0)),
    )
    epsilon = float(scenario.stage2.repair_accept_epsilon)
    if not _is_strictly_better(candidate_objective, baseline_objective, epsilon):
        return {"accepted": False}

    updated_schedule = dict(current_schedule)
    block_segment_indices = {segment.index for segment in block_segments}
    for key in list(updated_schedule):
        if key[0] in affected_task_ids and key[1] in block_segment_indices:
            del updated_schedule[key]
    for key, record in candidate_records.items():
        rate_value = float(rate_vars[key].value() or 0.0)
        if rate_value <= EPS:
            continue
        delivered = rate_value * record.effective_duration
        if delivered <= EPS:
            continue
        updated_schedule[(record.task_id, record.segment_index)] = Allocation(
            task_id=record.task_id,
            segment_index=record.segment_index,
            path_id=record.path.path_id,
            edge_ids=record.path.edge_ids,
            rate=rate_value,
            delivered=delivered,
            task_type="reg",
        )

    return {
        "accepted": True,
        "schedule": updated_schedule,
        "improvement_peak": max(baseline_objective[0] - candidate_objective[0], 0.0),
        "improvement_integral": max(baseline_objective[1] - candidate_objective[1], 0.0),
    }
