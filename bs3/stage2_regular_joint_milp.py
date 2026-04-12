from __future__ import annotations

import json
import re
import tempfile
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

import pulp

from .models import Allocation, PathCandidate, ScheduledWindow, Scenario, Segment, Task
from .scenario import active_cross_links, active_intra_links, generate_candidate_paths

EPS = 1e-9


@dataclass(frozen=True)
class _CandidateRecord:
    task_id: str
    local_index: int
    segment_index: int
    path_index: int
    path: PathCandidate
    effective_duration: float
    rate_upper_bound: float


@dataclass(frozen=True)
class _WindowSolveResult:
    schedule: dict[tuple[str, int], Allocation]
    remaining_end: dict[str, float]
    profiling: dict[str, Any] | None = None


@dataclass(frozen=True)
class _ObjectiveStage:
    name: str
    sense: int
    expr: pulp.LpAffineExpression


@dataclass(frozen=True)
class _StageSolveInfo:
    name: str
    status: str
    solution_status: str
    objective_value: float
    elapsed_seconds: float
    relative_gap: float | None
    accepted: bool


def _completion_tolerance(scenario: Scenario, task: Task) -> float:
    return max(float(scenario.stage2.completion_tolerance) * max(float(task.data), 0.0), EPS)


def _effective_duration(segment: Segment, task: Task, path: PathCandidate) -> float:
    usable_end = min(float(segment.end), float(task.deadline) - float(path.delay))
    return max(0.0, usable_end - float(segment.start))


def _expr_sum(terms: list) -> pulp.LpAffineExpression:
    if not terms:
        return pulp.LpAffineExpression()
    return pulp.lpSum(terms)


def _cross_link_from_edges(edge_ids: tuple[str, ...], cross_edge_ids: set[str]) -> str | None:
    for edge_id in edge_ids:
        if edge_id in cross_edge_ids:
            return edge_id
    return None


def _build_edge_capacities(
    scenario: Scenario,
    plan: list[ScheduledWindow],
    segments: list[Segment],
) -> tuple[dict[int, dict[str, float]], dict[int, tuple[str, ...]], float]:
    reserved_cross = max((1.0 - float(scenario.stage1.rho)) * float(scenario.capacities.cross), 0.0)
    edge_caps: dict[int, dict[str, float]] = {}
    cross_links: dict[int, tuple[str, ...]] = {}
    for segment in segments:
        current: dict[str, float] = {}
        for link in active_intra_links(scenario, "A", segment.start):
            current[link.link_id] = float(scenario.capacities.domain_a)
        for link in active_intra_links(scenario, "B", segment.start):
            current[link.link_id] = float(scenario.capacities.domain_b)
        active_x = active_cross_links(plan, segment.start)
        cross_links[segment.index] = tuple(window.window_id for window in active_x)
        for window in active_x:
            current[window.window_id] = reserved_cross
        edge_caps[segment.index] = current
    return edge_caps, cross_links, reserved_cross


def _build_solver(
    scenario: Scenario,
    *,
    time_limit_seconds: float | None = None,
    log_path: str | None = None,
) -> pulp.PULP_CBC_CMD:
    kwargs: dict[str, object] = {"msg": bool(log_path)}
    effective_time_limit = (
        float(time_limit_seconds)
        if time_limit_seconds is not None
        else (
            float(scenario.stage2.milp_time_limit_seconds)
            if scenario.stage2.milp_time_limit_seconds is not None
            else None
        )
    )
    if effective_time_limit is not None:
        kwargs["timeLimit"] = effective_time_limit
    if scenario.stage2.milp_relative_gap is not None:
        kwargs["gapRel"] = float(scenario.stage2.milp_relative_gap)
    if log_path:
        kwargs["logPath"] = str(log_path)
    return pulp.PULP_CBC_CMD(**kwargs)


def _solver_log_path() -> Path:
    return Path(tempfile.gettempdir()) / f"stage2_cbc_{uuid.uuid4().hex}.log"


def _parse_cbc_relative_gap(log_path: Path, stage_sense: int, objective_value: float) -> float | None:
    if not log_path.exists():
        return None
    content = log_path.read_text(encoding="utf-8", errors="ignore")
    match = re.search(r"Gap:\s*([+-]?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)", content)
    if match:
        return abs(float(match.group(1)))
    match = re.search(
        r"gap(?: between best possible and best solution)? is\s*([+-]?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)",
        content,
        flags=re.IGNORECASE,
    )
    if match:
        return abs(float(match.group(1)))
    if "Optimal solution found" in content:
        return 0.0

    objective_match = re.search(r"Objective value:\s*([+-]?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)", content)
    upper_match = re.search(r"Upper bound:\s*([+-]?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)", content)
    lower_match = re.search(r"Lower bound:\s*([+-]?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)", content)
    if objective_match is None:
        return None

    obj = float(objective_match.group(1))
    bound: float | None = None
    if stage_sense == pulp.LpMaximize and upper_match is not None:
        bound = float(upper_match.group(1))
    elif stage_sense == pulp.LpMinimize and lower_match is not None:
        bound = float(lower_match.group(1))
    elif upper_match is not None:
        bound = float(upper_match.group(1))
    elif lower_match is not None:
        bound = float(lower_match.group(1))
    if bound is None:
        return None
    return abs(bound - obj) / max(abs(objective_value), abs(obj), EPS)


def _accept_stage_solution(status_name: str, solution_status_name: str, allow_incumbent: bool) -> bool:
    if status_name == "Optimal" and solution_status_name == "Optimal Solution Found":
        return True
    if not allow_incumbent:
        return False
    return solution_status_name in {"Optimal Solution Found", "Solution Found"} and status_name in {"Optimal", "Not Solved"}


def _solve_lexicographic(
    model: pulp.LpProblem,
    scenario: Scenario,
    stages: list[_ObjectiveStage],
    *,
    mode_label: str,
    allow_incumbent: bool,
    capture_solver_log: bool,
) -> dict[str, Any]:
    values: dict[str, float] = {}
    stage_details: list[dict[str, Any]] = []
    solve_started = time.perf_counter()
    total_time_budget = (
        float(scenario.stage2.milp_time_limit_seconds)
        if scenario.stage2.milp_time_limit_seconds is not None
        else None
    )
    for idx, stage in enumerate(stages, start=1):
        elapsed_before = time.perf_counter() - solve_started
        stage_time_limit = None
        if total_time_budget is not None:
            stage_time_limit = max(total_time_budget - elapsed_before, 0.0)
            if stage_time_limit <= 1e-6:
                break

        log_path = _solver_log_path() if capture_solver_log else None
        solver = _build_solver(
            scenario,
            time_limit_seconds=stage_time_limit,
            log_path=(str(log_path) if log_path is not None else None),
        )
        model.sense = stage.sense
        model.setObjective(stage.expr)
        stage_started = time.perf_counter()
        status_code = model.solve(solver)
        elapsed_seconds = time.perf_counter() - stage_started
        status_name = pulp.LpStatus.get(status_code, str(status_code))
        solution_status_code = getattr(model, "sol_status", None)
        solution_status_name = pulp.LpSolution.get(solution_status_code, str(solution_status_code))
        optimum = float(pulp.value(stage.expr) or 0.0)
        relative_gap = None
        if log_path is not None:
            relative_gap = _parse_cbc_relative_gap(log_path, stage.sense, optimum)
            try:
                log_path.unlink(missing_ok=True)
            except OSError:
                pass
        accepted = _accept_stage_solution(status_name, solution_status_name, allow_incumbent)
        stage_details.append(
            {
                "name": stage.name,
                "status": status_name,
                "solution_status": solution_status_name,
                "objective_value": optimum,
                "elapsed_seconds": elapsed_seconds,
                "relative_gap": relative_gap,
                "accepted": accepted,
                "time_limit_seconds": stage_time_limit,
            }
        )
        if not accepted:
            raise RuntimeError(
                f"Stage2-1 joint MILP {mode_label}-stage-{idx}-{stage.name} solve did not return an acceptable incumbent: "
                f"status={status_name}, solution_status={solution_status_name}"
            )
        values[stage.name] = optimum
        if idx >= len(stages):
            continue
        if stage.sense == pulp.LpMaximize:
            model += stage.expr >= optimum - 1e-6
        else:
            model += stage.expr <= optimum + 1e-6
    if not stage_details:
        raise RuntimeError(f"Stage2-1 joint MILP {mode_label} exhausted its window-level time budget before a feasible solve")

    all_optimal = (
        len(stage_details) == len(stages)
        and all(detail["solution_status"] == "Optimal Solution Found" for detail in stage_details)
    )
    relative_gap = max(
        (float(detail["relative_gap"]) for detail in stage_details if detail["relative_gap"] is not None),
        default=(0.0 if all_optimal else None),
    )
    return {
        "objective_values": values,
        "stage_details": stage_details,
        "overall_status": ("Optimal" if all_optimal else "AcceptedIncumbent"),
        "overall_solution_status": stage_details[-1]["solution_status"],
        "relative_gap": relative_gap,
        "elapsed_seconds": time.perf_counter() - solve_started,
        "completed_stage_count": len(stage_details),
        "planned_stage_count": len(stages),
    }


def _build_task_segments(tasks: list[Task], segments: list[Segment]) -> dict[str, list[Segment]]:
    task_segments: dict[str, list[Segment]] = {}
    for task in tasks:
        task_segments[task.task_id] = [segment for segment in segments if task.arrival <= segment.start < task.deadline]
    return task_segments


def _normalized_milp_mode(mode: str) -> str:
    return "full" if str(mode).strip().lower() == "full" else "rolling"


def _rolling_high_weight_threshold(scenario: Scenario, tasks: list[Task]) -> float:
    configured = scenario.stage2.milp_rolling_high_weight_threshold
    if configured is not None:
        return float(configured)
    weights = sorted(float(task.weight) for task in tasks)
    if not weights:
        return float('inf')
    if abs(weights[0] - weights[-1]) <= EPS:
        return float('inf')
    quartile_rank = max(((3 * len(weights) + 3) // 4) - 1, 0)
    return weights[min(quartile_rank, len(weights) - 1)]


def _rolling_priority_key(task: Task, remaining_data: float, segment: Segment) -> tuple[float, ...]:
    slack = max(float(task.deadline) - float(segment.start), float(segment.duration), EPS)
    remaining_ratio = max(float(remaining_data), 0.0) / max(float(task.data), EPS)
    required_rate = max(float(remaining_data), 0.0) / slack
    return (-float(task.weight), -required_rate, -remaining_ratio, float(task.deadline), float(task.arrival))


def _select_rolling_path_limits(
    scenario: Scenario,
    tasks: list[Task],
    task_segments: dict[str, list[Segment]],
    initial_remaining: dict[str, float],
) -> dict[tuple[str, int], int]:
    base_limit = max(int(scenario.stage2.milp_rolling_path_limit), 1)
    high_limit = max(int(scenario.stage2.milp_rolling_high_path_limit), base_limit)
    promote_budget = max(int(scenario.stage2.milp_rolling_promoted_tasks_per_segment), 0)
    competition_threshold = max(int(scenario.stage2.milp_rolling_high_competition_task_threshold), 1)
    if high_limit <= base_limit or promote_budget <= 0:
        return {}

    high_weight_threshold = _rolling_high_weight_threshold(scenario, tasks)
    per_segment: dict[int, list[tuple[Task, int, Segment, float]]] = defaultdict(list)
    for task in tasks:
        remaining_data = max(float(initial_remaining.get(task.task_id, float(task.data))), 0.0)
        for local_index, segment in enumerate(task_segments.get(task.task_id, [])):
            per_segment[segment.index].append((task, local_index, segment, remaining_data))

    path_limits: dict[tuple[str, int], int] = {}
    for entries in per_segment.values():
        if len(entries) < competition_threshold:
            continue
        ranked = sorted(entries, key=lambda item: _rolling_priority_key(item[0], item[3], item[2]))
        promoted: list[tuple[Task, int, Segment, float]] = [
            item
            for item in ranked
            if float(item[0].weight) + EPS >= high_weight_threshold
        ][:promote_budget]
        promoted_keys = {(item[0].task_id, item[1]) for item in promoted}
        if len(promoted) < promote_budget:
            for item in ranked:
                key = (item[0].task_id, item[1])
                if key in promoted_keys:
                    continue
                promoted.append(item)
                promoted_keys.add(key)
                if len(promoted) >= promote_budget:
                    break
        for task, local_index, _, _ in promoted:
            path_limits[(task.task_id, local_index)] = high_limit
    return path_limits


def _build_candidate_structures(
    scenario: Scenario,
    plan: list[ScheduledWindow],
    task_segments: dict[str, list[Segment]],
    tasks: list[Task],
    edge_capacities: dict[int, dict[str, float]],
    default_path_limit: int | None = None,
    path_limits_by_task_segment: dict[tuple[str, int], int] | None = None,
) -> tuple[
    dict[tuple[str, int, int], _CandidateRecord],
    dict[tuple[str, int], list[tuple[str, int, int]]],
    dict[tuple[int, str], list[tuple[str, int, int]]],
    dict[tuple[int, str], list[tuple[str, int, int]]],
    dict[tuple[str, int], dict[str, list[tuple[str, int, int]]]],
]:
    candidate_records: dict[tuple[str, int, int], _CandidateRecord] = {}
    segment_candidates: dict[tuple[str, int], list[tuple[str, int, int]]] = defaultdict(list)
    edge_to_candidates: dict[tuple[int, str], list[tuple[str, int, int]]] = defaultdict(list)
    cross_to_candidates: dict[tuple[int, str], list[tuple[str, int, int]]] = defaultdict(list)
    cross_choice_keys: dict[tuple[str, int], dict[str, list[tuple[str, int, int]]]] = defaultdict(lambda: defaultdict(list))

    for task in tasks:
        local_segments = task_segments.get(task.task_id, [])
        for local_index, segment in enumerate(local_segments):
            candidates = generate_candidate_paths(scenario, plan, task, segment, scenario.stage2.k_paths)
            effective_limit = default_path_limit
            if path_limits_by_task_segment is not None:
                effective_limit = path_limits_by_task_segment.get((task.task_id, local_index), effective_limit)
            if effective_limit is not None:
                candidates = sorted(
                    candidates,
                    key=lambda path: (float(path.delay), int(path.hop_count), str(path.path_id)),
                )[:effective_limit]
            edge_caps = edge_capacities.get(segment.index, {})
            for path_index, path in enumerate(candidates):
                effective_duration = _effective_duration(segment, task, path)
                if effective_duration <= EPS:
                    continue
                rate_upper_bound = min(
                    float(task.max_rate),
                    min((float(edge_caps.get(edge_id, 0.0)) for edge_id in path.edge_ids), default=0.0),
                )
                if rate_upper_bound <= EPS:
                    continue
                key = (task.task_id, local_index, path_index)
                candidate_records[key] = _CandidateRecord(
                    task_id=task.task_id,
                    local_index=local_index,
                    segment_index=segment.index,
                    path_index=path_index,
                    path=path,
                    effective_duration=effective_duration,
                    rate_upper_bound=rate_upper_bound,
                )
                segment_candidates[(task.task_id, local_index)].append(key)
                for edge_id in path.edge_ids:
                    edge_to_candidates[(segment.index, edge_id)].append(key)
                if path.cross_window_id is not None:
                    cross_to_candidates[(segment.index, path.cross_window_id)].append(key)
                    cross_choice_keys[(task.task_id, local_index)][path.cross_window_id].append(key)

    return (
        candidate_records,
        segment_candidates,
        edge_to_candidates,
        cross_to_candidates,
        cross_choice_keys,
    )


def _solve_regular_window_milp(
    scenario: Scenario,
    plan: list[ScheduledWindow],
    window_segments: list[Segment],
    tasks: list[Task],
    initial_remaining: dict[str, float],
    initial_cross_link: dict[str, str | None],
    milp_mode: str,
    profile_callback: Callable[[dict[str, Any]], None] | None = None,
    profile_context: dict[str, Any] | None = None,
) -> _WindowSolveResult:
    if not tasks or not window_segments:
        return _WindowSolveResult(
            schedule={},
            remaining_end={task.task_id: initial_remaining.get(task.task_id, float(task.data)) for task in tasks},
            profiling={
                "solver_status": "Skipped",
                "solver_solution_status": "Skipped",
                "objective_values": {},
                "stage_details": [],
                "elapsed_seconds": 0.0,
                "relative_gap": None,
                "candidate_path_count": 0,
                "promoted_task_count": 0,
                "promoted_task_segment_count": 0,
                "variable_count": 0,
                "constraint_count": 0,
            },
        )

    mode = _normalized_milp_mode(milp_mode)
    rolling_mode = mode == "rolling"
    edge_capacities, cross_links_by_segment, reserved_cross = _build_edge_capacities(scenario, plan, window_segments)
    task_segments = _build_task_segments(tasks, window_segments)
    rolling_path_limits = (
        _select_rolling_path_limits(scenario, tasks, task_segments, initial_remaining)
        if rolling_mode
        else None
    )
    (
        candidate_records,
        segment_candidates,
        edge_to_candidates,
        cross_to_candidates,
        cross_choice_keys,
    ) = _build_candidate_structures(
        scenario,
        plan,
        task_segments,
        tasks,
        edge_capacities,
        default_path_limit=(scenario.stage2.milp_rolling_path_limit if rolling_mode else None),
        path_limits_by_task_segment=rolling_path_limits,
    )

    model = pulp.LpProblem("stage2_regular_joint_milp_window", pulp.LpMaximize)

    horizon_end = float(window_segments[-1].end)
    due_task_ids = {task.task_id for task in tasks if task.deadline <= horizon_end + EPS}
    allow_incumbent = (
        scenario.stage2.milp_time_limit_seconds is not None or scenario.stage2.milp_relative_gap is not None
    )
    capture_solver_log = allow_incumbent

    remaining_vars: dict[tuple[str, int], pulp.LpVariable] = {}
    first_switch_vars: dict[str, pulp.LpVariable] = {}
    transition_switch_vars: dict[tuple[str, int], pulp.LpVariable] = {}
    completion_vars = {
        task_id: pulp.LpVariable(f"y_{task_id}", cat=pulp.LpBinary)
        for task_id in due_task_ids
    }
    q_vars = {
        segment.index: pulp.LpVariable(f"q_{segment.index}", lowBound=0.0, upBound=1.0)
        for segment in window_segments
        if cross_links_by_segment.get(segment.index)
    }
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

    for task in tasks:
        local_segments = task_segments[task.task_id]
        start_remaining = max(float(initial_remaining.get(task.task_id, float(task.data))), 0.0)
        for local_index in range(len(local_segments) + 1):
            remaining_vars[(task.task_id, local_index)] = pulp.LpVariable(
                f"R_{task.task_id}_{local_index}",
                lowBound=0.0,
                upBound=start_remaining,
            )
        if local_segments:
            first_switch_vars[task.task_id] = pulp.LpVariable(f"u0_{task.task_id}", cat=pulp.LpBinary)
        for local_index in range(1, len(local_segments)):
            transition_switch_vars[(task.task_id, local_index)] = pulp.LpVariable(
                f"u_{task.task_id}_{local_index}",
                cat=pulp.LpBinary,
            )

    completion_expr = _expr_sum(
        [float(task.weight) * completion_vars[task.task_id] for task in tasks if task.task_id in completion_vars]
    )

    progress_terms: list = []
    for task in tasks:
        local_segments = task_segments[task.task_id]
        start_remaining = max(float(initial_remaining.get(task.task_id, float(task.data))), 0.0)
        model += remaining_vars[(task.task_id, 0)] == start_remaining
        for local_index, segment in enumerate(local_segments):
            keys = segment_candidates.get((task.task_id, local_index), [])
            delivered_terms = [rate_vars[key] * candidate_records[key].effective_duration for key in keys]
            if keys:
                model += pulp.lpSum(choice_vars[key] for key in keys) <= 1.0
                for key in keys:
                    model += rate_vars[key] <= candidate_records[key].rate_upper_bound * choice_vars[key]
                model += _expr_sum(delivered_terms) <= remaining_vars[(task.task_id, local_index)]
                if float(task.data) > EPS:
                    urgency = float(task.weight) / (float(task.data) * max(float(task.deadline) - float(segment.start), float(segment.duration), EPS))
                    for key in keys:
                        progress_terms.append(urgency * rate_vars[key] * candidate_records[key].effective_duration)
            model += remaining_vars[(task.task_id, local_index + 1)] == remaining_vars[(task.task_id, local_index)] - _expr_sum(delivered_terms)
        tail_index = len(local_segments)
        if task.task_id in completion_vars:
            model += remaining_vars[(task.task_id, tail_index)] <= _completion_tolerance(scenario, task) + start_remaining * (1.0 - completion_vars[task.task_id])

    for segment in window_segments:
        edge_caps = edge_capacities.get(segment.index, {})
        for edge_id, capacity in edge_caps.items():
            keys = edge_to_candidates.get((segment.index, edge_id), [])
            if not keys:
                continue
            model += pulp.lpSum(rate_vars[key] for key in keys) <= float(capacity)

    if reserved_cross > EPS:
        for segment in window_segments:
            q_var = q_vars.get(segment.index)
            if q_var is None:
                continue
            for cross_link in cross_links_by_segment.get(segment.index, ()):
                keys = cross_to_candidates.get((segment.index, cross_link), [])
                if keys:
                    model += pulp.lpSum(rate_vars[key] for key in keys) <= q_var * reserved_cross
                else:
                    model += q_var >= 0.0

    switch_expr_terms: list = []
    for task in tasks:
        local_segments = task_segments[task.task_id]
        if not local_segments:
            continue
        first_switch = first_switch_vars[task.task_id]
        switch_expr_terms.append(first_switch)
        first_choices = cross_choice_keys.get((task.task_id, 0), {})
        preferred = initial_cross_link.get(task.task_id)
        if preferred is not None:
            for cross_link, keys in first_choices.items():
                if cross_link == preferred:
                    continue
                model += first_switch >= pulp.lpSum(choice_vars[key] for key in keys)
        for local_index in range(1, len(local_segments)):
            switch_var = transition_switch_vars[(task.task_id, local_index)]
            switch_expr_terms.append(switch_var)
            prev_choices = cross_choice_keys.get((task.task_id, local_index - 1), {})
            curr_choices = cross_choice_keys.get((task.task_id, local_index), {})
            if not prev_choices or not curr_choices:
                continue
            for prev_cross, prev_keys in prev_choices.items():
                for curr_cross, curr_keys in curr_choices.items():
                    if prev_cross == curr_cross:
                        continue
                    model += switch_var >= pulp.lpSum(choice_vars[key] for key in prev_keys) + pulp.lpSum(choice_vars[key] for key in curr_keys) - 1.0

    progress_expr = _expr_sum(progress_terms)
    load_expr = _expr_sum([float(segment.duration) * q_vars[segment.index] for segment in window_segments if segment.index in q_vars])
    switch_expr = _expr_sum(switch_expr_terms)
    stages = [_ObjectiveStage(name="completion", sense=pulp.LpMaximize, expr=completion_expr)]
    if rolling_mode:
        stages.append(_ObjectiveStage(name="progress", sense=pulp.LpMaximize, expr=progress_expr))
    stages.append(_ObjectiveStage(name="load_balance", sense=pulp.LpMinimize, expr=load_expr))
    stages.append(_ObjectiveStage(name="switch", sense=pulp.LpMinimize, expr=switch_expr))
    if not rolling_mode:
        stages = [stages[0], stages[-2], stages[-1]]

    profile_base = dict(profile_context or {})
    profile_base.update(
        {
            "candidate_path_count": len(candidate_records),
            "promoted_task_count": len({task_id for task_id, _ in (rolling_path_limits or {}).keys()}),
            "promoted_task_segment_count": len(rolling_path_limits or {}),
            "variable_count": len(model.variables()),
            "constraint_count": len(model.constraints),
            "planned_objective_layers": [stage.name for stage in stages],
        }
    )
    if profile_callback is not None:
        profile_callback(
            {
                **profile_base,
                "window_phase": "started",
                "solver_status": "Running",
                "solver_solution_status": "Running",
                "objective_values": {},
                "stage_details": [],
                "elapsed_seconds": 0.0,
                "relative_gap": None,
            }
        )

    try:
        solve_profile = _solve_lexicographic(
            model,
            scenario,
            stages,
            mode_label=mode,
            allow_incumbent=allow_incumbent,
            capture_solver_log=capture_solver_log,
        )
    except Exception as exc:
        if profile_callback is not None:
            profile_callback(
                {
                    **profile_base,
                    "window_phase": "failed",
                    "solver_status": "Failed",
                    "solver_solution_status": "Failed",
                    "objective_values": {},
                    "stage_details": [],
                    "elapsed_seconds": 0.0,
                    "relative_gap": None,
                    "error": f"{type(exc).__name__}: {exc}",
                }
            )
        raise

    schedule: dict[tuple[str, int], Allocation] = {}
    remaining_end: dict[str, float] = {}
    for key, record in candidate_records.items():
        rate_value = float(rate_vars[key].value() or 0.0)
        if rate_value <= EPS:
            continue
        delivered = rate_value * record.effective_duration
        if delivered <= EPS:
            continue
        schedule[(record.task_id, record.segment_index)] = Allocation(
            task_id=record.task_id,
            segment_index=record.segment_index,
            path_id=record.path.path_id,
            edge_ids=record.path.edge_ids,
            rate=rate_value,
            delivered=delivered,
            task_type="reg",
        )
    for task in tasks:
        tail_index = len(task_segments[task.task_id])
        remaining_end[task.task_id] = float(remaining_vars[(task.task_id, tail_index)].value() or initial_remaining.get(task.task_id, float(task.data)))
    profiling = {
        **profile_base,
        "solver_status": solve_profile["overall_status"],
        "solver_solution_status": solve_profile["overall_solution_status"],
        "objective_values": {
            stage.name: solve_profile["objective_values"].get(stage.name)
            for stage in stages
        },
        "stage_details": solve_profile["stage_details"],
        "elapsed_seconds": solve_profile["elapsed_seconds"],
        "relative_gap": solve_profile["relative_gap"],
        "completed_objective_layers": int(solve_profile["completed_stage_count"]),
    }
    if profile_callback is not None:
        profile_callback({**profiling, "window_phase": "finished"})
    return _WindowSolveResult(schedule=schedule, remaining_end=remaining_end, profiling=profiling)


def _commit_window_prefix(
    scenario: Scenario,
    plan: list[ScheduledWindow],
    segments: list[Segment],
    window_schedule: dict[tuple[str, int], Allocation],
    commit_segment_indices: list[int],
    remaining_by_task: dict[str, float],
    prev_cross_link: dict[str, str | None],
) -> dict[tuple[str, int], Allocation]:
    regular_tasks = [task for task in scenario.tasks if task.task_type == "reg"]
    cross_edge_ids = {window.window_id for window in plan}
    committed_chunk: dict[tuple[str, int], Allocation] = {}
    for segment_index in commit_segment_indices:
        segment = segments[segment_index]
        served_now: set[str] = set()
        for task in regular_tasks:
            alloc = window_schedule.get((task.task_id, segment_index))
            if alloc is None:
                continue
            delivered = min(float(alloc.delivered), max(float(remaining_by_task.get(task.task_id, 0.0)), 0.0))
            if delivered <= EPS:
                continue
            committed = Allocation(
                task_id=alloc.task_id,
                segment_index=alloc.segment_index,
                path_id=alloc.path_id,
                edge_ids=alloc.edge_ids,
                rate=alloc.rate,
                delivered=delivered,
                task_type=alloc.task_type,
                is_preempted=alloc.is_preempted,
            )
            committed_chunk[(task.task_id, segment_index)] = committed
            remaining_by_task[task.task_id] = max(0.0, float(remaining_by_task.get(task.task_id, 0.0)) - delivered)
            prev_cross_link[task.task_id] = _cross_link_from_edges(committed.edge_ids, cross_edge_ids)
            served_now.add(task.task_id)
        for task in regular_tasks:
            if task.arrival <= segment.start < task.deadline and task.task_id not in served_now:
                prev_cross_link[task.task_id] = None
    return committed_chunk


def _rolling_profile_path(scenario: Scenario) -> Path | None:
    configured = scenario.metadata.get("stage2_rolling_profile_path")
    if configured in {None, ""}:
        return None
    return Path(str(configured))


def _append_profile_record(profile_path: Path | None, payload: dict[str, Any]) -> None:
    if profile_path is None:
        return
    profile_path.parent.mkdir(parents=True, exist_ok=True)
    with profile_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False) + "\n")


def build_regular_baseline_full_milp(
    scenario: Scenario,
    plan: list[ScheduledWindow],
    segments: list[Segment],
) -> tuple[dict[tuple[str, int], Allocation], dict[str, bool]]:
    regular_tasks = [task for task in scenario.tasks if task.task_type == "reg"]
    if not regular_tasks:
        return {}, {}
    initial_remaining = {task.task_id: float(task.data) for task in regular_tasks}
    initial_cross_link = {task.task_id: None for task in regular_tasks}
    result = _solve_regular_window_milp(
        scenario=scenario,
        plan=plan,
        window_segments=segments,
        tasks=regular_tasks,
        initial_remaining=initial_remaining,
        initial_cross_link=initial_cross_link,
        milp_mode="full",
    )
    completed = {
        task.task_id: result.remaining_end.get(task.task_id, float(task.data)) <= _completion_tolerance(scenario, task) + 1e-9
        for task in regular_tasks
    }
    return result.schedule, completed


def build_regular_baseline_rolling_milp(
    scenario: Scenario,
    plan: list[ScheduledWindow],
    segments: list[Segment],
) -> tuple[dict[tuple[str, int], Allocation], dict[str, bool]]:
    regular_tasks = [task for task in scenario.tasks if task.task_type == "reg"]
    if not regular_tasks or not segments:
        return {}, {}

    horizon_len = max(int(scenario.stage2.milp_horizon_segments), 1)
    commit_len = max(int(scenario.stage2.milp_commit_segments), 1)
    commit_len = min(commit_len, horizon_len)

    remaining_by_task = {task.task_id: float(task.data) for task in regular_tasks}
    prev_cross_link = {task.task_id: None for task in regular_tasks}
    schedule: dict[tuple[str, int], Allocation] = {}
    profile_rows: list[dict[str, Any]] = []
    scenario.metadata["stage2_rolling_window_profiles"] = profile_rows
    profile_path = _rolling_profile_path(scenario)
    if profile_path is not None and profile_path.exists():
        profile_path.unlink()

    def write_profile(row: dict[str, Any]) -> None:
        _append_profile_record(profile_path, row)
        if row.get("window_phase") in {"finished", "failed", "skipped"}:
            profile_rows.append(dict(row))

    start_pos = 0
    window_index = 0
    while start_pos < len(segments):
        horizon_stop = min(start_pos + horizon_len, len(segments))
        commit_stop = min(start_pos + commit_len, len(segments))
        window_segments = segments[start_pos:horizon_stop]
        if not window_segments:
            break
        window_start = float(window_segments[0].start)
        window_end = float(window_segments[-1].end)
        active_tasks = [
            task
            for task in regular_tasks
            if remaining_by_task.get(task.task_id, 0.0) > _completion_tolerance(scenario, task)
            and task.arrival < window_end - EPS
            and task.deadline > window_start + EPS
        ]
        profile_context = {
            "window_index": window_index,
            "window_segment_start_index": int(window_segments[0].index),
            "window_segment_end_index": int(window_segments[-1].index),
            "window_segment_count": len(window_segments),
            "commit_segment_start_index": int(segments[start_pos].index),
            "commit_segment_end_index": int(segments[commit_stop - 1].index),
            "commit_segment_count": max(commit_stop - start_pos, 0),
            "window_time_start": window_start,
            "window_time_end": window_end,
            "active_task_count": len(active_tasks),
            "due_task_count": sum(1 for task in active_tasks if float(task.deadline) <= window_end + EPS),
        }
        if active_tasks:
            window_result = _solve_regular_window_milp(
                scenario=scenario,
                plan=plan,
                window_segments=window_segments,
                tasks=active_tasks,
                initial_remaining=remaining_by_task,
                initial_cross_link=prev_cross_link,
                milp_mode="rolling",
                profile_callback=write_profile,
                profile_context=profile_context,
            )
            committed_chunk = _commit_window_prefix(
                scenario=scenario,
                plan=plan,
                segments=segments,
                window_schedule=window_result.schedule,
                commit_segment_indices=[segment.index for segment in segments[start_pos:commit_stop]],
                remaining_by_task=remaining_by_task,
                prev_cross_link=prev_cross_link,
            )
            schedule.update(committed_chunk)
        else:
            write_profile(
                {
                    **profile_context,
                    "window_phase": "skipped",
                    "solver_status": "SkippedNoActiveTasks",
                    "solver_solution_status": "SkippedNoActiveTasks",
                    "candidate_path_count": 0,
                    "promoted_task_count": 0,
                    "promoted_task_segment_count": 0,
                    "variable_count": 0,
                    "constraint_count": 0,
                    "planned_objective_layers": [],
                    "completed_objective_layers": 0,
                    "objective_values": {},
                    "stage_details": [],
                    "elapsed_seconds": 0.0,
                    "relative_gap": None,
                }
            )
            for segment in segments[start_pos:commit_stop]:
                for task in regular_tasks:
                    if task.arrival <= segment.start < task.deadline:
                        prev_cross_link[task.task_id] = None
        start_pos = commit_stop
        window_index += 1

    completed = {
        task.task_id: remaining_by_task.get(task.task_id, float(task.data)) <= _completion_tolerance(scenario, task) + 1e-9
        for task in regular_tasks
    }
    return schedule, completed


def build_regular_baseline_joint_milp(
    scenario: Scenario,
    plan: list[ScheduledWindow],
    segments: list[Segment],
) -> tuple[dict[tuple[str, int], Allocation], dict[str, bool]]:
    return build_regular_baseline_full_milp(scenario, plan, segments)


def _edge_overlap_ratio(lhs_edge_ids: tuple[str, ...], rhs_edge_ids: tuple[str, ...]) -> float:
    if not lhs_edge_ids or not rhs_edge_ids:
        return 0.0
    overlap = len(set(lhs_edge_ids).intersection(rhs_edge_ids))
    return overlap / max(min(len(lhs_edge_ids), len(rhs_edge_ids)), 1)


def _select_hotspot_candidates(
    scenario: Scenario,
    task: Task,
    segment: Segment,
    candidates: list[PathCandidate],
    *,
    baseline_edge_ids: tuple[str, ...],
    hot_segment: bool,
    hot_task_segment: bool,
    hot_window_ids: set[str],
    augmented_window_ids: set[str],
) -> list[PathCandidate]:
    if not candidates:
        return []

    base_limit = max(int(scenario.stage2.k_paths), 1)
    hot_limit = max(int(scenario.stage2.hot_path_limit), base_limit, 4)
    limit = hot_limit if hot_segment and hot_task_segment else base_limit
    ranked = sorted(candidates, key=lambda path: (float(path.delay), int(path.hop_count), str(path.path_id)))

    selected: list[PathCandidate] = []
    seen: set[str] = set()

    def take(paths: list[PathCandidate]) -> None:
        for path in paths:
            if path.path_id in seen:
                continue
            selected.append(path)
            seen.add(path.path_id)
            if len(selected) >= limit:
                return

    def take_one(paths: list[PathCandidate]) -> None:
        for path in paths:
            if path.path_id in seen:
                continue
            selected.append(path)
            seen.add(path.path_id)
            return

    baseline_matches = [path for path in ranked if tuple(path.edge_ids) == tuple(baseline_edge_ids)]
    if baseline_matches:
        take_one(baseline_matches)
    take_one(ranked)
    if hot_segment and hot_task_segment:
        low_overlap = sorted(
            ranked,
            key=lambda path: (
                0 if path.cross_window_id is not None and path.cross_window_id not in hot_window_ids else 1,
                _edge_overlap_ratio(tuple(path.edge_ids), tuple(baseline_edge_ids)),
                float(path.delay),
                int(path.hop_count),
                str(path.path_id),
            ),
        )
        relief_paths = [
            path
            for path in sorted(
                ranked,
                key=lambda path: (
                    0 if path.cross_window_id in augmented_window_ids else 1,
                    float(path.delay),
                    int(path.hop_count),
                    str(path.path_id),
                ),
            )
            if path.cross_window_id in augmented_window_ids
        ]
        relief_oriented = sorted(
            ranked,
            key=lambda path: (
                0 if path.cross_window_id in augmented_window_ids else 1,
                0 if path.cross_window_id is not None and path.cross_window_id not in hot_window_ids else 1,
                _edge_overlap_ratio(tuple(path.edge_ids), tuple(baseline_edge_ids)),
                float(path.delay),
                int(path.hop_count),
                str(path.path_id),
            ),
        )
        take_one(relief_paths)
        take_one(low_overlap)
        take(low_overlap)
        take(relief_oriented)
    take(ranked)
    return selected[:limit]


def solve_regular_hotspot_local_milp(
    scenario: Scenario,
    plan: list[ScheduledWindow],
    segments: list[Segment],
    current_schedule: dict[tuple[str, int], Allocation],
    diagnostics: dict[str, Any],
    horizon_segments: list[Segment],
    active_tasks: list[Task],
    hot_segment_indices: set[int],
    hot_task_segments: set[tuple[str, int]],
    hot_window_ids_by_segment: dict[int, tuple[str, ...]],
    augmented_window_ids: set[str],
) -> dict[str, Any]:
    if not horizon_segments or not active_tasks:
        return {"accepted": False, "solver_status": "Skipped"}

    affected_task_ids = {task.task_id for task in active_tasks}
    unaffected_task_ids = {
        task_id
        for (task_id, segment_index) in current_schedule
        if task_id not in affected_task_ids and horizon_segments[0].index <= segment_index <= horizon_segments[-1].index
    }
    edge_capacities, cross_links_by_segment, reserved_cross = _build_edge_capacities(scenario, plan, horizon_segments)
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

    task_segments = _build_task_segments(active_tasks, horizon_segments)
    candidate_records: dict[tuple[str, int, int], _CandidateRecord] = {}
    segment_candidates: dict[tuple[str, int], list[tuple[str, int, int]]] = defaultdict(list)
    edge_to_candidates: dict[tuple[int, str], list[tuple[str, int, int]]] = defaultdict(list)
    cross_to_candidates: dict[tuple[int, str], list[tuple[str, int, int]]] = defaultdict(list)
    cross_choice_keys: dict[tuple[str, int], dict[str, list[tuple[str, int, int]]]] = defaultdict(lambda: defaultdict(list))
    task_start_remaining: dict[str, float] = {}
    task_end_remaining_limit: dict[str, float] = {}
    initial_cross_link: dict[str, str | None] = {}
    baseline_completed = diagnostics.get("completed", {})

    for task in active_tasks:
        local_segments = task_segments.get(task.task_id, [])
        if not local_segments:
            continue
        first_segment = local_segments[0]
        task_start_remaining[task.task_id] = float(diagnostics["remaining_before_trace"][task.task_id].get(first_segment.index, task.data))
        task_end_remaining_limit[task.task_id] = float(diagnostics["remaining_after_trace"][task.task_id].get(horizon_segments[-1].index, task.data))
        initial_cross_link[task.task_id] = diagnostics["previous_cross_link_trace"][task.task_id].get(first_segment.index)
        for local_index, segment in enumerate(local_segments):
            baseline_edge_ids = tuple(diagnostics["selected_path_key_trace"][task.task_id].get(segment.index) or tuple())
            candidates = generate_candidate_paths(scenario, plan, task, segment, max(int(scenario.stage2.hot_path_limit), int(scenario.stage2.k_paths)))
            selected_candidates = _select_hotspot_candidates(
                scenario,
                task,
                segment,
                candidates,
                baseline_edge_ids=baseline_edge_ids,
                hot_segment=segment.index in hot_segment_indices,
                hot_task_segment=(task.task_id, segment.index) in hot_task_segments,
                hot_window_ids=set(hot_window_ids_by_segment.get(segment.index, tuple())),
                augmented_window_ids=augmented_window_ids,
            )
            for path_index, path in enumerate(selected_candidates):
                effective_duration = _effective_duration(segment, task, path)
                if effective_duration <= EPS:
                    continue
                rate_upper_bound = min(
                    float(task.max_rate),
                    min((float(edge_capacities[segment.index].get(edge_id, 0.0)) for edge_id in path.edge_ids), default=0.0),
                )
                if rate_upper_bound <= EPS:
                    continue
                key = (task.task_id, local_index, path_index)
                candidate_records[key] = _CandidateRecord(
                    task_id=task.task_id,
                    local_index=local_index,
                    segment_index=segment.index,
                    path_index=path_index,
                    path=path,
                    effective_duration=effective_duration,
                    rate_upper_bound=rate_upper_bound,
                )
                segment_candidates[(task.task_id, local_index)].append(key)
                for edge_id in path.edge_ids:
                    edge_to_candidates[(segment.index, edge_id)].append(key)
                if path.cross_window_id is not None:
                    cross_to_candidates[(segment.index, path.cross_window_id)].append(key)
                    cross_choice_keys[(task.task_id, local_index)][path.cross_window_id].append(key)

    if not candidate_records:
        return {"accepted": False, "solver_status": "NoCandidates"}

    model = pulp.LpProblem("stage2_regular_hotspot_local_peak", pulp.LpMaximize)
    remaining_vars: dict[tuple[str, int], pulp.LpVariable] = {}
    first_switch_vars: dict[str, pulp.LpVariable] = {}
    transition_switch_vars: dict[tuple[str, int], pulp.LpVariable] = {}
    completion_vars = {
        task.task_id: pulp.LpVariable(f"y_{task.task_id}", cat=pulp.LpBinary)
        for task in active_tasks
        if bool(baseline_completed.get(task.task_id, False))
    }
    q_vars = {
        segment.index: pulp.LpVariable(f"q_{segment.index}", lowBound=0.0, upBound=1.0)
        for segment in horizon_segments
    }
    z_peak = pulp.LpVariable("z_peak_local", lowBound=0.0, upBound=1.0)
    z_window = {
        window_id: pulp.LpVariable(f"zw_{window_id}", cat=pulp.LpBinary)
        for window_id in sorted(augmented_window_ids)
    }
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

    progress_terms: list = []
    for task in active_tasks:
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

        model += remaining_vars[(task.task_id, 0)] == start_remaining
        for local_index, segment in enumerate(local_segments):
            keys = segment_candidates.get((task.task_id, local_index), [])
            delivered_terms = [rate_vars[key] * candidate_records[key].effective_duration for key in keys]
            if keys:
                model += pulp.lpSum(choice_vars[key] for key in keys) <= 1.0
                for key in keys:
                    model += rate_vars[key] <= candidate_records[key].rate_upper_bound * choice_vars[key]
                    if candidate_records[key].path.cross_window_id in z_window:
                        model += choice_vars[key] <= z_window[candidate_records[key].path.cross_window_id]
                        model += rate_vars[key] <= candidate_records[key].rate_upper_bound * z_window[candidate_records[key].path.cross_window_id]
                model += _expr_sum(delivered_terms) <= remaining_vars[(task.task_id, local_index)]
                for key in keys:
                    progress_terms.append(float(task.weight) * rate_vars[key] * candidate_records[key].effective_duration)
            model += remaining_vars[(task.task_id, local_index + 1)] == remaining_vars[(task.task_id, local_index)] - _expr_sum(delivered_terms)
        tail_index = len(local_segments)
        model += remaining_vars[(task.task_id, tail_index)] <= float(task_end_remaining_limit.get(task.task_id, task.data)) + float(scenario.stage2.local_peak_accept_epsilon)
        if task.task_id in completion_vars:
            model += remaining_vars[(task.task_id, tail_index)] <= _completion_tolerance(scenario, task) + start_remaining * (1.0 - completion_vars[task.task_id])

    for segment in horizon_segments:
        for edge_id, capacity in edge_capacities.get(segment.index, {}).items():
            keys = edge_to_candidates.get((segment.index, edge_id), [])
            if keys:
                model += pulp.lpSum(rate_vars[key] for key in keys) <= float(capacity)
        model += z_peak >= q_vars[segment.index]
        if reserved_cross <= EPS or not cross_links_by_segment.get(segment.index):
            model += q_vars[segment.index] == 0.0
            continue
        for cross_link in cross_links_by_segment.get(segment.index, ()):
            keys = cross_to_candidates.get((segment.index, cross_link), [])
            fixed_usage = float(fixed_cross_usage.get(segment.index, {}).get(cross_link, 0.0))
            model += fixed_usage + pulp.lpSum(rate_vars[key] for key in keys) <= q_vars[segment.index] * float(reserved_cross)

    if z_window:
        model += pulp.lpSum(z_window.values()) <= int(scenario.stage2.augment_window_budget)

    switch_terms: list = []
    for task in active_tasks:
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

    completion_expr = _expr_sum([float(task.weight) * completion_vars[task.task_id] for task in active_tasks if task.task_id in completion_vars])
    progress_expr = _expr_sum(progress_terms)
    peak_integral_expr = _expr_sum([float(segment.duration) * q_vars[segment.index] for segment in horizon_segments])
    switch_expr = _expr_sum(switch_terms)
    stages = [
        _ObjectiveStage(name="completion", sense=pulp.LpMaximize, expr=completion_expr),
        _ObjectiveStage(name="progress", sense=pulp.LpMaximize, expr=progress_expr),
        _ObjectiveStage(name="peak", sense=pulp.LpMinimize, expr=z_peak),
        _ObjectiveStage(name="integral", sense=pulp.LpMinimize, expr=peak_integral_expr),
        _ObjectiveStage(name="switch", sense=pulp.LpMinimize, expr=switch_expr),
    ]

    allow_incumbent = scenario.stage2.milp_time_limit_seconds is not None or scenario.stage2.milp_relative_gap is not None
    try:
        solve_profile = _solve_lexicographic(
            model,
            scenario,
            stages,
            mode_label="hotspot_local_peak",
            allow_incumbent=allow_incumbent,
            capture_solver_log=allow_incumbent,
        )
    except Exception as exc:
        return {"accepted": False, "solver_status": f"Failed:{type(exc).__name__}"}

    updated_schedule = dict(current_schedule)
    horizon_segment_indices = {segment.index for segment in horizon_segments}
    for key in list(updated_schedule):
        if key[0] in affected_task_ids and key[1] in horizon_segment_indices:
            del updated_schedule[key]

    used_augmented_windows: set[str] = set()
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
        if record.path.cross_window_id in augmented_window_ids:
            used_augmented_windows.add(str(record.path.cross_window_id))

    return {
        "accepted": True,
        "schedule": updated_schedule,
        "solver_status": solve_profile.get("overall_status", "Accepted"),
        "objective_values": dict(solve_profile.get("objective_values", {})),
        "used_augment_windows": sorted(used_augmented_windows),
    }
