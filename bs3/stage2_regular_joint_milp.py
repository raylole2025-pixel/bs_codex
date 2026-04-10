from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass

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


@dataclass(frozen=True)
class _ObjectiveStage:
    name: str
    sense: int
    expr: pulp.LpAffineExpression


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


def _build_solver(scenario: Scenario) -> pulp.PULP_CBC_CMD:
    kwargs: dict[str, object] = {"msg": False}
    if scenario.stage2.milp_time_limit_seconds is not None:
        kwargs["timeLimit"] = float(scenario.stage2.milp_time_limit_seconds)
    if scenario.stage2.milp_relative_gap is not None:
        kwargs["gapRel"] = float(scenario.stage2.milp_relative_gap)
    return pulp.PULP_CBC_CMD(**kwargs)


def _require_optimal(model: pulp.LpProblem, solver: pulp.LpSolver, stage: str) -> None:
    status_code = model.solve(solver)
    status_name = pulp.LpStatus.get(status_code, str(status_code))
    if status_name != "Optimal":
        raise RuntimeError(f"Stage2-1 joint MILP {stage} solve did not reach optimality: {status_name}")


def _solve_lexicographic(
    model: pulp.LpProblem,
    solver: pulp.LpSolver,
    stages: list[_ObjectiveStage],
    *,
    mode_label: str,
) -> dict[str, float]:
    values: dict[str, float] = {}
    for idx, stage in enumerate(stages, start=1):
        model.sense = stage.sense
        model.setObjective(stage.expr)
        _require_optimal(model, solver, f"{mode_label}-stage-{idx}-{stage.name}")
        optimum = float(pulp.value(stage.expr) or 0.0)
        values[stage.name] = optimum
        if idx >= len(stages):
            continue
        if stage.sense == pulp.LpMaximize:
            model += stage.expr >= optimum - 1e-6
        else:
            model += stage.expr <= optimum + 1e-6
    return values


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
) -> _WindowSolveResult:
    if not tasks or not window_segments:
        return _WindowSolveResult(schedule={}, remaining_end={task.task_id: initial_remaining.get(task.task_id, float(task.data)) for task in tasks})

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

    solver = _build_solver(scenario)
    model = pulp.LpProblem("stage2_regular_joint_milp_window", pulp.LpMaximize)

    horizon_end = float(window_segments[-1].end)
    due_task_ids = {task.task_id for task in tasks if task.deadline <= horizon_end + EPS}

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
    _solve_lexicographic(model, solver, stages, mode_label=mode)

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
    return _WindowSolveResult(schedule=schedule, remaining_end=remaining_end)


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

    start_pos = 0
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
        if active_tasks:
            window_result = _solve_regular_window_milp(
                scenario=scenario,
                plan=plan,
                window_segments=window_segments,
                tasks=active_tasks,
                initial_remaining=remaining_by_task,
                initial_cross_link=prev_cross_link,
                milp_mode="rolling",
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
            for segment in segments[start_pos:commit_stop]:
                for task in regular_tasks:
                    if task.arrival <= segment.start < task.deadline:
                        prev_cross_link[task.task_id] = None
        start_pos = commit_stop

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
    if str(scenario.stage2.milp_mode).lower() == "full":
        return build_regular_baseline_full_milp(scenario, plan, segments)
    return build_regular_baseline_rolling_milp(scenario, plan, segments)
