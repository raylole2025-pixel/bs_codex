from __future__ import annotations

import time
from collections import defaultdict
from dataclasses import dataclass

from .models import Allocation, PathCandidate, ScheduledWindow, Scenario, Segment, Stage2Result, Task
from .regular_routing_common import (
    build_regular_schedule_diagnostics,
    empty_repair_metadata,
    is_regular_repair_enabled,
    resolve_regular_baseline_mode,
)
from .scenario import active_cross_links, active_intra_links, build_segments, compress_segments, generate_candidate_paths
from .stage2_hotspot_relief import run_hotspot_relief
from .stage2_regular_block_repair import repair_regular_baseline_blocks
from .stage2_regular_greedy_baseline import build_regular_baseline_stage1_greedy
from .stage2_regular_joint_milp import (
    build_regular_baseline_full_milp,
    build_regular_baseline_joint_milp,
    build_regular_baseline_rolling_milp,
)

EPS = 1e-9


@dataclass(frozen=True)
class _CapacityState:
    total: dict[str, float]
    regular_cross: dict[str, float]
    edge_kind: dict[str, str]


@dataclass(frozen=True)
class _PlannedAction:
    segment_index: int
    path: PathCandidate | None
    rate: float
    delivered: float


@dataclass(frozen=True)
class _PlanLabel:
    last_cross_link: str | None
    remaining_data: float
    idle_steps: int
    switches: int
    deviations: int
    load_cost: float
    finish_time: float | None
    actions: tuple[_PlannedAction, ...] = ()


@dataclass(frozen=True)
class _TaskPlan:
    actions: tuple[_PlannedAction, ...]
    remaining_data: float
    completed: bool
    finish_time: float | None
    switches: int
    deviations: int
    load_cost: float


class TwoPhaseEventDrivenScheduler:
    def __init__(self, scenario: Scenario) -> None:
        self.scenario = scenario
        self.numeric_tolerance = EPS
        self.completion_tolerance_ratio = max(float(self.scenario.stage2.completion_tolerance), 0.0)
        self.task_by_id = {task.task_id: task for task in self.scenario.tasks}
        self._last_regular_baseline_mode = resolve_regular_baseline_mode(self.scenario.stage2)
        self._last_regular_baseline_source = self._last_regular_baseline_mode
        self._hotspot_relief_active = False
        self._hotspot_relief_metadata: dict[str, object] = {}
        self._hotspot_relief_report: dict[str, object] = {}

    def _task_completion_tolerance(self, task: Task | float) -> float:
        data = float(task.data) if isinstance(task, Task) else float(task)
        return max(self.completion_tolerance_ratio * max(data, 0.0), self.numeric_tolerance)

    def _is_task_complete(self, task: Task, remaining_data: float) -> bool:
        return remaining_data <= self._task_completion_tolerance(task)

    def _ordered_emergencies(self) -> list[Task]:
        emergencies = [task for task in self.scenario.tasks if task.task_type == "emg"]
        emergencies.sort(key=lambda task: (task.arrival, -task.weight, task.deadline, task.task_id))
        return emergencies

    def run(self, plan: list[ScheduledWindow]) -> Stage2Result:
        started_at = time.perf_counter()
        self._hotspot_relief_active = False
        self._hotspot_relief_metadata = {}
        self._hotspot_relief_report = {}
        regular_tasks = [task for task in self.scenario.tasks if task.task_type == "reg"]
        self.scenario.metadata["stage2_rolling_window_profiles"] = []
        self.scenario.metadata.pop("stage2_segment_compression_result", None)
        segments = build_segments(self.scenario, plan, regular_tasks)
        raw_event_segment_count = len(segments)
        hotspot_requested = bool(self.scenario.stage2.hotspot_relief_enabled) and bool(
            getattr(self.scenario.stage2, "closed_loop_relief_enabled", True)
        )
        if self._segment_compression_enabled() and not hotspot_requested:
            segments, compression_result = compress_segments(self.scenario, plan, segments, regular_tasks)
            self.scenario.metadata["stage2_segment_compression_result"] = compression_result
        elif self._segment_compression_enabled() and hotspot_requested:
            self.scenario.metadata["stage2_segment_compression_result"] = {
                "event_segment_compression_disabled_for_hotspot_relief": True,
                "event_segment_count_raw": raw_event_segment_count,
                "event_segment_count_compressed": raw_event_segment_count,
                "event_segment_compression_ratio": 1.0,
            }
        effective_event_segment_count = len(segments)
        baseline_mode = resolve_regular_baseline_mode(self.scenario.stage2)
        if hotspot_requested:
            self._validate_hotspot_relief_mode(baseline_mode)
        baseline_schedule, baseline_completed, baseline_context = self._build_regular_baseline(plan, segments, baseline_mode)
        repair_metadata = empty_repair_metadata(baseline_completed)
        repair_metadata["regular_repair_enabled"] = is_regular_repair_enabled(self.scenario.stage2, baseline_mode)
        if baseline_mode == "stage1_greedy_repair" and repair_metadata["regular_repair_enabled"]:
            baseline_schedule, repair_metadata = repair_regular_baseline_blocks(
                scenario=self.scenario,
                plan=plan,
                segments=segments,
                baseline_schedule=baseline_schedule,
                baseline_diag=baseline_context["diagnostics"],
            )
            baseline_completed = dict(repair_metadata.get("diagnostics_after", baseline_context["diagnostics"]).get("completed", baseline_completed))
            baseline_context["diagnostics"] = repair_metadata.get("diagnostics_after", baseline_context["diagnostics"])
        baseline_diag = baseline_context.get("diagnostics")
        if not isinstance(baseline_diag, dict):
            baseline_diag = build_regular_schedule_diagnostics(self.scenario, plan, segments, baseline_schedule)
            baseline_context["diagnostics"] = baseline_diag
        if hotspot_requested:
            hotspot_result = run_hotspot_relief(
                scenario=self.scenario,
                plan=plan,
                segments=segments,
                baseline_schedule=baseline_schedule,
                baseline_diagnostics=baseline_diag,
            )
            plan = hotspot_result.plan
            segments = hotspot_result.segments
            effective_event_segment_count = len(segments)
            baseline_schedule = hotspot_result.schedule
            baseline_diag = hotspot_result.diagnostics
            baseline_context["diagnostics"] = baseline_diag
            baseline_completed = dict(baseline_diag.get("completed", baseline_completed))
            self._hotspot_relief_metadata = dict(hotspot_result.metadata)
            self._hotspot_relief_report = dict(hotspot_result.report)
            self._hotspot_relief_active = bool(self._hotspot_relief_metadata.get("hot_ranges_considered", 0))
        self._last_regular_baseline_mode = baseline_mode
        if baseline_mode in {"rolling_milp", "full_milp"}:
            self._last_regular_baseline_source = baseline_mode
        elif repair_metadata["regular_repair_enabled"] and baseline_mode == "stage1_greedy_repair":
            self._last_regular_baseline_source = "stage1_greedy_repair"
        else:
            self._last_regular_baseline_source = "stage1_greedy"
        committed: dict[tuple[str, int], Allocation] = dict(baseline_schedule)
        actual_remaining = {task.task_id: float(task.data) for task in self.scenario.tasks}
        prev_cross_link = {task.task_id: None for task in self.scenario.tasks}
        cross_edge_ids = {window.window_id for window in plan}
        allocations: list[Allocation] = []
        edge_usage = defaultdict(float)
        n_preemptions = 0

        cross_denominator = 0.0
        all_denominator = 0.0
        pending_emergencies = self._ordered_emergencies()
        seg_idx = 0

        while seg_idx < len(segments):
            segment = segments[seg_idx]

            if pending_emergencies and segment.start + self.numeric_tolerance < pending_emergencies[0].arrival < segment.end - self.numeric_tolerance:
                segments, baseline_schedule, committed = self._split_segment_if_needed(
                    segments,
                    pending_emergencies[0].arrival,
                    baseline_schedule,
                    committed,
                )
                continue

            while pending_emergencies and abs(pending_emergencies[0].arrival - segment.start) <= self.numeric_tolerance:
                emergency = pending_emergencies.pop(0)
                for boundary in (emergency.deadline,):
                    segments, baseline_schedule, committed = self._split_segment_if_needed(
                        segments,
                        boundary,
                        baseline_schedule,
                        committed,
                    )
                segment = segments[seg_idx]
                n_preemptions += self._insert_emergency_task(
                    emergency=emergency,
                    plan=plan,
                    segments=segments,
                    current_index=seg_idx,
                    committed=committed,
                    actual_remaining=actual_remaining,
                    prev_cross_link=prev_cross_link,
                )

            segment = segments[seg_idx]
            if pending_emergencies and segment.start + self.numeric_tolerance < pending_emergencies[0].arrival < segment.end - self.numeric_tolerance:
                segments, baseline_schedule, committed = self._split_segment_if_needed(
                    segments,
                    pending_emergencies[0].arrival,
                    baseline_schedule,
                    committed,
                )
                continue

            active_a = active_intra_links(self.scenario, "A", segment.start)
            active_b = active_intra_links(self.scenario, "B", segment.start)
            active_x = active_cross_links(plan, segment.start)
            cross_denominator += len(active_x) * self.scenario.capacities.cross * segment.duration
            all_denominator += (
                len(active_a) * self.scenario.capacities.domain_a
                + len(active_b) * self.scenario.capacities.domain_b
                + len(active_x) * self.scenario.capacities.cross
            ) * segment.duration

            served_now: set[str] = set()
            current_allocations = sorted(
                (
                    alloc
                    for (task_id, index), alloc in committed.items()
                    if index == segment.index and not self._is_task_complete(self.task_by_id[task_id], actual_remaining.get(task_id, 0.0))
                ),
                key=lambda alloc: (0 if alloc.task_type == "emg" else 1, alloc.task_id, alloc.path_id),
            )
            for alloc in current_allocations:
                task = self.task_by_id[alloc.task_id]
                if not (task.arrival <= segment.start < task.deadline):
                    continue
                if segment.duration <= self.numeric_tolerance:
                    continue
                max_deliverable = max(actual_remaining[task.task_id], 0.0)
                if self._is_task_complete(task, max_deliverable):
                    continue
                requested_delivered = min(float(alloc.delivered), float(alloc.rate) * segment.duration)
                delivered = min(max_deliverable, requested_delivered)
                if delivered <= self.numeric_tolerance:
                    continue
                rate = delivered / segment.duration
                executed = Allocation(
                    task_id=alloc.task_id,
                    segment_index=alloc.segment_index,
                    path_id=alloc.path_id,
                    edge_ids=alloc.edge_ids,
                    rate=rate,
                    delivered=delivered,
                    task_type=alloc.task_type,
                    is_preempted=alloc.is_preempted,
                )
                allocations.append(executed)
                actual_remaining[task.task_id] = max(0.0, actual_remaining[task.task_id] - delivered)
                served_now.add(task.task_id)
                prev_cross_link[task.task_id] = self._cross_link_from_edges(executed.edge_ids, cross_edge_ids)
                for edge_id in executed.edge_ids:
                    edge_usage[edge_id] += executed.delivered

            for task in self.scenario.tasks:
                if task.arrival <= segment.start < task.deadline and task.task_id not in served_now:
                    prev_cross_link[task.task_id] = None

            seg_idx += 1

        cr_reg = self._weighted_true_completion("reg", actual_remaining)
        cr_emg = self._weighted_true_completion("emg", actual_remaining)
        u_cross = sum(usage for edge_id, usage in edge_usage.items() if edge_id in cross_edge_ids) / cross_denominator if cross_denominator > self.numeric_tolerance else 0.0
        u_all = sum(edge_usage.values()) / all_denominator if all_denominator > self.numeric_tolerance else 0.0
        metadata = self._build_result_metadata(
            event_segment_count=effective_event_segment_count,
            event_segment_count_raw=raw_event_segment_count,
            regular_task_count=len(regular_tasks),
            elapsed_seconds=time.perf_counter() - started_at,
            repair_metadata=repair_metadata,
            plan_window_count=len(plan),
        )

        return Stage2Result(
            plan=plan,
            cr_reg=cr_reg,
            cr_emg=cr_emg,
            n_preemptions=n_preemptions,
            u_cross=u_cross,
            u_all=u_all,
            allocations=allocations,
            solver_mode=self._solver_mode_label(),
            metadata=metadata,
        )

    def _split_segment_if_needed(
        self,
        segments: list[Segment],
        split_time: float,
        *schedules: dict[tuple[str, int], Allocation],
    ) -> tuple:
        if split_time <= self.numeric_tolerance or split_time >= self.scenario.planning_end - self.numeric_tolerance:
            return (segments, *schedules)

        target_index = None
        for idx, segment in enumerate(segments):
            if segment.start + self.numeric_tolerance < split_time < segment.end - self.numeric_tolerance:
                target_index = idx
                break
        if target_index is None:
            return (segments, *schedules)

        old_segment = segments[target_index]
        new_segments: list[Segment] = []
        for idx, segment in enumerate(segments):
            if idx == target_index:
                new_segments.append(Segment(index=len(new_segments), start=segment.start, end=split_time))
                new_segments.append(Segment(index=len(new_segments), start=split_time, end=segment.end))
            else:
                new_segments.append(Segment(index=len(new_segments), start=segment.start, end=segment.end))

        remapped = [self._remap_schedule_for_split(schedule, segments, target_index, split_time) for schedule in schedules]
        return (new_segments, *remapped)

    def _remap_schedule_for_split(
        self,
        schedule: dict[tuple[str, int], Allocation],
        segments: list[Segment],
        target_index: int,
        split_time: float,
    ) -> dict[tuple[str, int], Allocation]:
        old_segment = segments[target_index]
        first_duration = split_time - old_segment.start
        second_duration = old_segment.end - split_time
        remapped: dict[tuple[str, int], Allocation] = {}

        for (task_id, segment_index), alloc in schedule.items():
            if segment_index < target_index:
                remapped[(task_id, segment_index)] = alloc
                continue
            if segment_index > target_index:
                remapped[(task_id, segment_index + 1)] = Allocation(
                    task_id=alloc.task_id,
                    segment_index=alloc.segment_index + 1,
                    path_id=alloc.path_id,
                    edge_ids=alloc.edge_ids,
                    rate=alloc.rate,
                    delivered=alloc.delivered,
                    task_type=alloc.task_type,
                    is_preempted=alloc.is_preempted,
                )
                continue

            for new_alloc in self._split_allocation(alloc, target_index, first_duration, second_duration):
                remapped[(new_alloc.task_id, new_alloc.segment_index)] = new_alloc

        return remapped

    def _split_allocation(
        self,
        alloc: Allocation,
        target_index: int,
        first_duration: float,
        second_duration: float,
    ) -> list[Allocation]:
        if alloc.rate <= self.numeric_tolerance or alloc.delivered <= self.numeric_tolerance:
            return []

        first_delivered = min(float(alloc.delivered), float(alloc.rate) * max(first_duration, 0.0))
        second_delivered = max(float(alloc.delivered) - first_delivered, 0.0)
        result: list[Allocation] = []
        if first_delivered > self.numeric_tolerance:
            result.append(
                Allocation(
                    task_id=alloc.task_id,
                    segment_index=target_index,
                    path_id=f"{alloc.path_id}:split0",
                    edge_ids=alloc.edge_ids,
                    rate=alloc.rate,
                    delivered=first_delivered,
                    task_type=alloc.task_type,
                    is_preempted=alloc.is_preempted,
                )
            )
        if second_delivered > self.numeric_tolerance:
            result.append(
                Allocation(
                    task_id=alloc.task_id,
                    segment_index=target_index + 1,
                    path_id=f"{alloc.path_id}:split1",
                    edge_ids=alloc.edge_ids,
                    rate=alloc.rate,
                    delivered=second_delivered,
                    task_type=alloc.task_type,
                    is_preempted=alloc.is_preempted,
                )
            )
        return result

    def _build_regular_baseline(
        self,
        plan: list[ScheduledWindow],
        segments: list[Segment],
        baseline_mode: str | None = None,
    ) -> tuple[dict[tuple[str, int], Allocation], dict[str, bool], dict]:
        mode = baseline_mode or resolve_regular_baseline_mode(self.scenario.stage2)
        if mode == "stage1_greedy":
            schedule, completed, diagnostics = build_regular_baseline_stage1_greedy(self.scenario, plan, segments)
            return schedule, completed, {"diagnostics": diagnostics}
        if mode == "stage1_greedy_repair":
            schedule, completed, diagnostics = build_regular_baseline_stage1_greedy(self.scenario, plan, segments)
            return schedule, completed, {"diagnostics": diagnostics}
        if mode == "full_milp":
            schedule, completed = build_regular_baseline_full_milp(self.scenario, plan, segments)
            return schedule, completed, {}
        if mode == "rolling_milp":
            schedule, completed = build_regular_baseline_rolling_milp(self.scenario, plan, segments)
            return schedule, completed, {}
        schedule, completed = build_regular_baseline_joint_milp(self.scenario, plan, segments)
        return schedule, completed, {}

    def _solver_mode_label(self) -> str:
        if self._last_regular_baseline_source == "rolling_milp":
            label = "two_phase_event_insert+joint_milp_rolling"
        elif self._last_regular_baseline_source == "full_milp":
            label = "two_phase_event_insert+joint_milp_full"
        elif self._last_regular_baseline_source == "stage1_greedy_repair":
            label = "two_phase_event_insert+stage1_greedy_repair"
        else:
            label = "two_phase_event_insert+stage1_greedy"
        if self._hotspot_relief_active:
            return f"{label}+hotspot_relief_local_peak_milp"
        return label

    def _validate_hotspot_relief_mode(self, baseline_mode: str) -> None:
        if not bool(self.scenario.stage2.hotspot_relief_enabled):
            return
        return

    def _segment_compression_enabled(self) -> bool:
        compression_cfg = self.scenario.metadata.get("stage2_segment_compression")
        return isinstance(compression_cfg, dict) and bool(compression_cfg.get("enabled"))

    def _build_result_metadata(
        self,
        *,
        event_segment_count: int,
        event_segment_count_raw: int,
        regular_task_count: int,
        elapsed_seconds: float,
        repair_metadata: dict[str, object],
        plan_window_count: int,
    ) -> dict[str, float | int | bool | None | str]:
        metadata: dict[str, float | int | bool | None | str | list | dict] = {
            "regular_baseline_mode": self._last_regular_baseline_mode,
            "regular_baseline_source": self._last_regular_baseline_source,
            "solver_mode": self._solver_mode_label(),
            "prefer_milp": bool(self.scenario.stage2.prefer_milp),
            "milp_mode": (
                "rolling"
                if self._last_regular_baseline_source == "rolling_milp"
                else "full"
                if self._last_regular_baseline_source == "full_milp"
                else str(self.scenario.stage2.milp_mode)
            ),
            "milp_horizon_segments": int(self.scenario.stage2.milp_horizon_segments),
            "milp_commit_segments": int(self.scenario.stage2.milp_commit_segments),
            "milp_rolling_path_limit": int(self.scenario.stage2.milp_rolling_path_limit),
            "milp_rolling_high_path_limit": int(self.scenario.stage2.milp_rolling_high_path_limit),
            "milp_rolling_promoted_tasks_per_segment": int(self.scenario.stage2.milp_rolling_promoted_tasks_per_segment),
            "milp_time_limit_seconds": self.scenario.stage2.milp_time_limit_seconds,
            "milp_relative_gap": self.scenario.stage2.milp_relative_gap,
            "hotspot_relief_enabled": bool(self.scenario.stage2.hotspot_relief_enabled),
            "closed_loop_relief_enabled": bool(getattr(self.scenario.stage2, "closed_loop_relief_enabled", True)),
            "regular_repair_enabled": bool(repair_metadata.get("regular_repair_enabled", False)),
            "repair_block_count_considered": int(repair_metadata.get("repair_block_count_considered", 0)),
            "repair_block_count_accepted": int(repair_metadata.get("repair_block_count_accepted", 0)),
            "repair_total_improvement_peak": float(repair_metadata.get("repair_total_improvement_peak", 0.0)),
            "repair_total_improvement_integral": float(repair_metadata.get("repair_total_improvement_integral", 0.0)),
            "baseline_completed_count_before_repair": int(repair_metadata.get("baseline_completed_count_before_repair", 0)),
            "baseline_completed_count_after_repair": int(repair_metadata.get("baseline_completed_count_after_repair", 0)),
            "event_segment_count": int(event_segment_count),
            "event_segment_count_raw": int(event_segment_count_raw),
            "regular_task_count": int(regular_task_count),
            "plan_window_count": int(plan_window_count),
            "elapsed_seconds": float(elapsed_seconds),
        }
        if self._hotspot_relief_metadata:
            metadata.update(dict(self._hotspot_relief_metadata))
        if self._hotspot_relief_report:
            metadata["hotspot_report"] = dict(self._hotspot_relief_report)
        compression_result = self.scenario.metadata.get("stage2_segment_compression_result")
        if isinstance(compression_result, dict):
            metadata.update(dict(compression_result))
        return metadata

    def _build_regular_baseline_sequential(
        self,
        plan: list[ScheduledWindow],
        segments: list[Segment],
    ) -> tuple[dict[tuple[str, int], Allocation], dict[str, bool]]:
        regular_tasks = [task for task in self.scenario.tasks if task.task_type == "reg"]
        capacity_states = {segment.index: self._build_capacity_state(plan, segment) for segment in segments}
        schedule: dict[tuple[str, int], Allocation] = {}
        completed: dict[str, bool] = {}

        for task in sorted(regular_tasks, key=self._baseline_priority):
            task_segments = [segment for segment in segments if task.arrival <= segment.start < task.deadline]
            if not task_segments:
                completed[task.task_id] = False
                continue
            task_plan = self._plan_task(
                plan=plan,
                task=task,
                segments=task_segments,
                capacity_states=capacity_states,
                remaining_data=task.data,
                initial_cross_link=None,
                preferred_cross_links={},
                objective="baseline",
            )
            if task_plan is None:
                completed[task.task_id] = False
                continue
            for action in task_plan.actions:
                if action.path is None or action.rate <= self.numeric_tolerance:
                    continue
                alloc = Allocation(
                    task_id=task.task_id,
                    segment_index=action.segment_index,
                    path_id=action.path.path_id,
                    edge_ids=action.path.edge_ids,
                    rate=action.rate,
                    delivered=action.delivered,
                    task_type=task.task_type,
                )
                schedule[(task.task_id, action.segment_index)] = alloc
                self._apply_action_to_capacity(capacity_states, action, task.task_type)
            completed[task.task_id] = bool(task_plan.completed)

        return schedule, completed

    def _insert_emergency_task(
        self,
        emergency: Task,
        plan: list[ScheduledWindow],
        segments: list[Segment],
        current_index: int,
        committed: dict[tuple[str, int], Allocation],
        actual_remaining: dict[str, float],
        prev_cross_link: dict[str, str | None],
    ) -> int:
        horizon = self._emergency_horizon(segments, current_index, emergency)
        if not horizon:
            return 0

        direct_plan = self._solve_direct_insert(
            task=emergency,
            plan=plan,
            horizon=horizon,
            committed=committed,
            actual_remaining=actual_remaining,
            prev_cross_link=prev_cross_link,
        )
        if direct_plan is not None and direct_plan.completed:
            self._commit_single_task_plan(committed, emergency, horizon, direct_plan.actions)
            return 0

        affected = self._affected_regular_tasks(
            emergency=emergency,
            plan=plan,
            horizon=horizon,
            committed=committed,
            actual_remaining=actual_remaining,
        )
        preemptions, preempted_task_id, released_segments, repaired = self._try_controlled_preemption(
            emergency=emergency,
            affected=affected,
            plan=plan,
            horizon=horizon,
            committed=committed,
            actual_remaining=actual_remaining,
            prev_cross_link=prev_cross_link,
        )
        if repaired is not None:
            for segment_index in released_segments:
                committed.pop((preempted_task_id, segment_index), None)
            self._commit_single_task_plan(committed, emergency, horizon, repaired.actions)
            return preemptions

        if direct_plan is not None:
            self._commit_single_task_plan(committed, emergency, horizon, direct_plan.actions)
        return preemptions

    def _solve_direct_insert(
        self,
        task: Task,
        plan: list[ScheduledWindow],
        horizon: list[Segment],
        committed: dict[tuple[str, int], Allocation],
        actual_remaining: dict[str, float],
        prev_cross_link: dict[str, str | None],
    ) -> _TaskPlan | None:
        capacity_states = self._free_capacity_states(horizon, plan, committed, exclude_tasks=set())
        return self._plan_task(
            plan=plan,
            task=task,
            segments=horizon,
            capacity_states=capacity_states,
            remaining_data=actual_remaining[task.task_id],
            initial_cross_link=prev_cross_link.get(task.task_id),
            preferred_cross_links={},
            objective="emergency",
        )

    def _try_controlled_preemption(
        self,
        emergency: Task,
        affected: list[Task],
        plan: list[ScheduledWindow],
        horizon: list[Segment],
        committed: dict[tuple[str, int], Allocation],
        actual_remaining: dict[str, float],
        prev_cross_link: dict[str, str | None],
    ) -> tuple[int, str | None, set[int], _TaskPlan | None]:
        if not affected:
            return 0, None, set(), None

        corridor = self._candidate_corridor(plan, emergency, horizon)
        candidates = [
            task
            for task in affected
            if task.weight < emergency.weight and self._is_committed_complete(task, committed, actual_remaining)
        ]
        candidates.sort(
            key=lambda task: (
                task.weight,
                -self._task_slack(task, actual_remaining, horizon[0].start),
                -self._task_overlap(task.task_id, committed, corridor),
                task.task_id,
            )
        )

        for task in candidates:
            released_segments = self._conflict_segments(task.task_id, committed, corridor)
            if not released_segments:
                continue

            tentative = {
                key: alloc
                for key, alloc in committed.items()
                if not (key[0] == task.task_id and key[1] in released_segments)
            }
            direct_plan = self._solve_direct_insert(
                task=emergency,
                plan=plan,
                horizon=horizon,
                committed=tentative,
                actual_remaining=actual_remaining,
                prev_cross_link=prev_cross_link,
            )
            if direct_plan is not None and direct_plan.completed:
                return 1, task.task_id, released_segments, direct_plan

        return 0, None, set(), None

    def _plan_task(
        self,
        plan: list[ScheduledWindow],
        task: Task,
        segments: list[Segment],
        capacity_states: dict[int, _CapacityState],
        remaining_data: float,
        initial_cross_link: str | None,
        preferred_cross_links: dict[int, str | None],
        objective: str,
    ) -> _TaskPlan | None:
        completion_tolerance = self._task_completion_tolerance(task)
        if remaining_data <= completion_tolerance:
            return _TaskPlan(actions=tuple(), remaining_data=0.0, completed=True, finish_time=None, switches=0, deviations=0, load_cost=0.0)
        if not segments:
            return None

        path_cache: dict[tuple[str, int], list[PathCandidate]] = {}
        frontier = [
            _PlanLabel(
                last_cross_link=initial_cross_link,
                remaining_data=remaining_data,
                idle_steps=0,
                switches=0,
                deviations=0,
                load_cost=0.0,
                finish_time=None,
                actions=tuple(),
            )
        ]
        partial: list[_PlanLabel] = frontier[:]
        terminals: list[_PlanLabel] = []

        for segment in segments:
            buckets: dict[str | None, list[_PlanLabel]] = defaultdict(list)
            for label in frontier:
                if label.remaining_data <= completion_tolerance:
                    terminals.append(label)
                    continue

                wait_label = _PlanLabel(
                    last_cross_link=None,
                    remaining_data=label.remaining_data,
                    idle_steps=label.idle_steps + 1,
                    switches=label.switches,
                    deviations=label.deviations + self._deviation_penalty(preferred_cross_links.get(segment.index), None),
                    load_cost=label.load_cost,
                    finish_time=label.finish_time,
                    actions=label.actions + (_PlannedAction(segment_index=segment.index, path=None, rate=0.0, delivered=0.0),),
                )
                partial.append(wait_label)
                self._insert_nondominated(buckets[None], wait_label, objective)

                for candidate in self._candidate_paths(plan, task, segment, path_cache):
                    state = capacity_states.get(segment.index)
                    if state is None or segment.duration <= EPS:
                        continue
                    bottleneck = self._path_bottleneck(task, candidate, state)
                    if bottleneck <= self.numeric_tolerance:
                        continue
                    rate = min(task.max_rate, bottleneck, label.remaining_data / segment.duration)
                    if rate <= self.numeric_tolerance:
                        continue
                    delivered = rate * segment.duration
                    remaining_after = max(0.0, label.remaining_data - delivered)
                    cross_link = candidate.cross_window_id
                    switches = label.switches + (
                        1 if cross_link is not None and label.last_cross_link is not None and cross_link != label.last_cross_link else 0
                    )
                    deviations = label.deviations + self._deviation_penalty(preferred_cross_links.get(segment.index), cross_link)
                    load_cost = label.load_cost + self._path_load_cost(segment.index, candidate, rate, capacity_states)
                    finish_time = None
                    if remaining_after <= completion_tolerance:
                        finish_time = segment.start + (label.remaining_data / rate)
                    next_label = _PlanLabel(
                        last_cross_link=cross_link,
                        remaining_data=remaining_after,
                        idle_steps=label.idle_steps,
                        switches=switches,
                        deviations=deviations,
                        load_cost=load_cost,
                        finish_time=finish_time,
                        actions=label.actions + (_PlannedAction(segment_index=segment.index, path=candidate, rate=rate, delivered=delivered),),
                    )
                    partial.append(next_label)
                    if remaining_after <= completion_tolerance and finish_time is not None and finish_time <= task.deadline + self.numeric_tolerance:
                        terminals.append(next_label)
                        continue
                    self._insert_nondominated(buckets[cross_link], next_label, objective)

            frontier = [label for bucket in buckets.values() for label in bucket]
            if not frontier and terminals:
                break

        if terminals:
            best = min(terminals, key=lambda label: self._terminal_key(label, objective))
            return _TaskPlan(
                actions=best.actions,
                remaining_data=max(0.0, best.remaining_data),
                completed=True,
                finish_time=best.finish_time,
                switches=best.switches,
                deviations=best.deviations,
                load_cost=best.load_cost,
            )
        if not partial:
            return None
        best = min(partial, key=lambda label: self._partial_key(label, objective))
        return _TaskPlan(
            actions=best.actions,
            remaining_data=max(0.0, best.remaining_data),
            completed=False,
            finish_time=best.finish_time,
            switches=best.switches,
            deviations=best.deviations,
            load_cost=best.load_cost,
        )

    def _candidate_paths(
        self,
        plan: list[ScheduledWindow],
        task: Task,
        segment: Segment,
        path_cache: dict[tuple[str, int], list[PathCandidate]],
    ) -> list[PathCandidate]:
        key = (task.task_id, segment.index)
        if key not in path_cache:
            path_cache[key] = generate_candidate_paths(
                self.scenario,
                plan,
                task,
                segment,
                self.scenario.stage2.k_paths,
            )
        return path_cache[key]

    def _insert_nondominated(self, bucket: list[_PlanLabel], candidate: _PlanLabel, objective: str) -> None:
        survivors: list[_PlanLabel] = []
        for existing in bucket:
            if self._dominates(existing, candidate):
                return
            if self._dominates(candidate, existing):
                continue
            survivors.append(existing)
        survivors.append(candidate)
        survivors.sort(key=lambda label: self._partial_key(label, objective))
        bucket[:] = survivors[: self.scenario.stage2.effective_label_keep_limit]

    def _dominates(self, lhs: _PlanLabel, rhs: _PlanLabel) -> bool:
        lhs_remaining = self._remaining_key(lhs.remaining_data)
        rhs_remaining = self._remaining_key(rhs.remaining_data)
        no_worse = (
            lhs_remaining <= rhs_remaining + self.numeric_tolerance
            and lhs.switches <= rhs.switches
            and lhs.deviations <= rhs.deviations
            and lhs.load_cost <= rhs.load_cost + self.numeric_tolerance
            and lhs.idle_steps <= rhs.idle_steps
        )
        strictly_better = (
            lhs_remaining < rhs_remaining - self.numeric_tolerance
            or lhs.switches < rhs.switches
            or lhs.deviations < rhs.deviations
            or lhs.load_cost < rhs.load_cost - self.numeric_tolerance
            or lhs.idle_steps < rhs.idle_steps
        )
        return no_worse and strictly_better

    def _remaining_key(self, remaining_data: float) -> float:
        if remaining_data <= self.numeric_tolerance:
            return 0.0
        return remaining_data

    def _terminal_key(self, label: _PlanLabel, objective: str) -> tuple[float, ...]:
        finish_time = label.finish_time if label.finish_time is not None else float("inf")
        if objective == "baseline":
            return (label.load_cost, float(label.switches), finish_time, float(label.idle_steps))
        if objective == "emergency":
            return (float(label.switches), label.load_cost, finish_time, float(label.idle_steps))
        return (float(label.deviations), label.load_cost, float(label.switches), finish_time, float(label.idle_steps))

    def _partial_key(self, label: _PlanLabel, objective: str) -> tuple[float, ...]:
        remaining = self._remaining_key(label.remaining_data)
        if objective == "baseline":
            return (remaining, label.load_cost, float(label.switches), float(label.idle_steps))
        if objective == "emergency":
            return (remaining, float(label.switches), label.load_cost, float(label.idle_steps))
        return (remaining, float(label.deviations), label.load_cost, float(label.switches), float(label.idle_steps))

    def _build_capacity_state(self, plan: list[ScheduledWindow], segment: Segment) -> _CapacityState:
        total: dict[str, float] = {}
        regular_cross: dict[str, float] = {}
        edge_kind: dict[str, str] = {}
        regular_cross_capacity = max((1.0 - self.scenario.stage1.rho) * self.scenario.capacities.cross, 0.0)

        for link in active_intra_links(self.scenario, "A", segment.start):
            total[link.link_id] = self.scenario.capacities.domain_a
            edge_kind[link.link_id] = "A"
        for link in active_intra_links(self.scenario, "B", segment.start):
            total[link.link_id] = self.scenario.capacities.domain_b
            edge_kind[link.link_id] = "B"
        for window in active_cross_links(plan, segment.start):
            total[window.window_id] = self.scenario.capacities.cross
            regular_cross[window.window_id] = regular_cross_capacity
            edge_kind[window.window_id] = "X"
        return _CapacityState(total=total, regular_cross=regular_cross, edge_kind=edge_kind)

    def _free_capacity_states(
        self,
        segments: list[Segment],
        plan: list[ScheduledWindow],
        committed: dict[tuple[str, int], Allocation],
        exclude_tasks: set[str],
    ) -> dict[int, _CapacityState]:
        states = {segment.index: self._build_capacity_state(plan, segment) for segment in segments}
        valid_indices = set(states)
        for (task_id, segment_index), alloc in committed.items():
            if task_id in exclude_tasks or segment_index not in valid_indices:
                continue
            self._apply_existing_allocation(states[segment_index], alloc)
        return states

    def _apply_existing_allocation(self, state: _CapacityState, alloc: Allocation) -> None:
        for edge_id in alloc.edge_ids:
            if edge_id in state.total:
                state.total[edge_id] = max(0.0, state.total[edge_id] - alloc.rate)
            if alloc.task_type == "reg" and state.edge_kind.get(edge_id) == "X" and edge_id in state.regular_cross:
                state.regular_cross[edge_id] = max(0.0, state.regular_cross[edge_id] - alloc.rate)

    def _apply_action_to_capacity(
        self,
        capacity_states: dict[int, _CapacityState],
        action: _PlannedAction,
        task_type: str,
    ) -> None:
        if action.path is None or action.rate <= self.numeric_tolerance:
            return
        state = capacity_states.get(action.segment_index)
        if state is None:
            return
        for edge_id in action.path.edge_ids:
            if edge_id in state.total:
                state.total[edge_id] = max(0.0, state.total[edge_id] - action.rate)
            if task_type == "reg" and state.edge_kind.get(edge_id) == "X" and edge_id in state.regular_cross:
                state.regular_cross[edge_id] = max(0.0, state.regular_cross[edge_id] - action.rate)

    def _apply_actions_to_capacity(
        self,
        capacity_states: dict[int, _CapacityState],
        actions: tuple[_PlannedAction, ...],
        task_type: str,
    ) -> None:
        for action in actions:
            self._apply_action_to_capacity(capacity_states, action, task_type)

    def _path_bottleneck(self, task: Task, path: PathCandidate, state: _CapacityState) -> float:
        if not path.edge_ids:
            return float("inf")
        bottleneck = float("inf")
        for edge_id in path.edge_ids:
            available = state.total.get(edge_id)
            if available is None:
                return 0.0
            if task.task_type == "reg" and state.edge_kind.get(edge_id) == "X":
                available = min(available, state.regular_cross.get(edge_id, 0.0))
            bottleneck = min(bottleneck, available)
        return bottleneck

    def _path_load_cost(
        self,
        segment_index: int,
        path: PathCandidate,
        rate: float,
        capacity_states: dict[int, _CapacityState],
    ) -> float:
        cross_link = path.cross_window_id
        if cross_link is None or self.scenario.capacities.cross <= self.numeric_tolerance:
            return 0.0
        state = capacity_states.get(segment_index)
        if state is None or cross_link not in state.total:
            return 0.0
        used_after = max(self.scenario.capacities.cross - state.total[cross_link] + rate, 0.0)
        return used_after / self.scenario.capacities.cross

    def _deviation_penalty(self, preferred: str | None, selected: str | None) -> int:
        if preferred == selected:
            return 0
        if preferred is None and selected is None:
            return 0
        return 1

    def _commit_single_task_plan(
        self,
        committed: dict[tuple[str, int], Allocation],
        task: Task,
        horizon: list[Segment],
        actions: tuple[_PlannedAction, ...],
    ) -> None:
        self._commit_task_action_map(committed, {task.task_id: actions}, {segment.index for segment in horizon})

    def _commit_task_action_map(
        self,
        committed: dict[tuple[str, int], Allocation],
        action_map: dict[str, tuple[_PlannedAction, ...]],
        segment_indices: set[int],
    ) -> None:
        task_ids = set(action_map)
        for key in list(committed):
            if key[0] in task_ids and key[1] in segment_indices:
                del committed[key]
        for task_id, actions in action_map.items():
            task = self.task_by_id[task_id]
            for action in actions:
                if action.segment_index not in segment_indices or action.path is None or action.rate <= self.numeric_tolerance:
                    continue
                committed[(task_id, action.segment_index)] = Allocation(
                    task_id=task_id,
                    segment_index=action.segment_index,
                    path_id=action.path.path_id,
                    edge_ids=action.path.edge_ids,
                    rate=action.rate,
                    delivered=action.delivered,
                    task_type=task.task_type,
                )

    def _candidate_corridor(
        self,
        plan: list[ScheduledWindow],
        emergency: Task,
        horizon: list[Segment],
    ) -> dict[int, set[str]]:
        corridor: dict[int, set[str]] = {}
        path_cache: dict[tuple[str, int], list[PathCandidate]] = {}
        for segment in horizon:
            edges: set[str] = set()
            for path in self._candidate_paths(plan, emergency, segment, path_cache):
                edges.update(path.edge_ids)
            corridor[segment.index] = edges
        return corridor

    def _affected_regular_tasks(
        self,
        emergency: Task,
        plan: list[ScheduledWindow],
        horizon: list[Segment],
        committed: dict[tuple[str, int], Allocation],
        actual_remaining: dict[str, float],
    ) -> list[Task]:
        corridor = self._candidate_corridor(plan, emergency, horizon)
        affected: list[tuple[int, Task]] = []
        for task in self.scenario.tasks:
            if task.task_type != "reg":
                continue
            if not (task.arrival <= horizon[0].start < task.deadline):
                continue
            if self._is_task_complete(task, actual_remaining.get(task.task_id, 0.0)):
                continue
            overlap = self._task_overlap(task.task_id, committed, corridor)
            if overlap <= 0:
                continue
            affected.append((overlap, task))
        affected.sort(key=lambda item: (-item[0], -item[1].weight, item[1].deadline, item[1].task_id))
        return [task for _, task in affected]

    def _task_overlap(
        self,
        task_id: str,
        committed: dict[tuple[str, int], Allocation],
        corridor: dict[int, set[str]],
    ) -> int:
        overlap = 0
        for (alloc_task_id, segment_index), alloc in committed.items():
            if alloc_task_id != task_id:
                continue
            overlap += len(set(alloc.edge_ids).intersection(corridor.get(segment_index, set())))
        return overlap

    def _is_committed_complete(
        self,
        task: Task,
        committed: dict[tuple[str, int], Allocation],
        actual_remaining: dict[str, float],
    ) -> bool:
        committed_delivery = sum(
            alloc.delivered
            for (task_id, _), alloc in committed.items()
            if task_id == task.task_id
        )
        remaining_after_commit = max(actual_remaining.get(task.task_id, 0.0) - committed_delivery, 0.0)
        return self._is_task_complete(task, remaining_after_commit)

    def _conflict_segments(
        self,
        task_id: str,
        committed: dict[tuple[str, int], Allocation],
        corridor: dict[int, set[str]],
    ) -> set[int]:
        released_segments: set[int] = set()
        for (alloc_task_id, segment_index), alloc in committed.items():
            if alloc_task_id != task_id:
                continue
            if set(alloc.edge_ids).intersection(corridor.get(segment_index, set())):
                released_segments.add(segment_index)
        return released_segments

    def _task_slack(self, task: Task, actual_remaining: dict[str, float], current_time: float) -> float:
        remaining = actual_remaining.get(task.task_id, 0.0)
        return task.deadline - current_time - (remaining / max(task.max_rate, self.numeric_tolerance))

    def _emergency_horizon(self, segments: list[Segment], current_index: int, task: Task) -> list[Segment]:
        return [segment for segment in segments[current_index:] if task.arrival <= segment.start < task.deadline]

    def _baseline_priority(self, task: Task) -> tuple[float, ...]:
        required_rate = task.data / max(task.deadline - task.arrival, self.numeric_tolerance)
        return (-task.weight, task.deadline, task.arrival, -required_rate, task.task_id)

    def _weighted_true_completion(self, task_type: str, remaining: dict[str, float]) -> float:
        tasks = [task for task in self.scenario.tasks if task.task_type == task_type]
        if not tasks:
            return 1.0
        total_weight = sum(task.weight for task in tasks)
        if total_weight <= self.numeric_tolerance:
            return 1.0
        return sum(task.weight * float(self._is_task_complete(task, remaining.get(task.task_id, 0.0))) for task in tasks) / total_weight

    def _cross_link_from_edges(self, edge_ids: tuple[str, ...], cross_edge_ids: set[str]) -> str | None:
        if not cross_edge_ids:
            cross_edge_ids = {window.window_id for window in self.scenario.candidate_windows}
        if cross_edge_ids:
            for edge_id in edge_ids:
                if edge_id in cross_edge_ids:
                    return edge_id
        for edge_id in edge_ids:
            if edge_id.startswith("X") or edge_id.startswith("W"):
                return edge_id
        return None


def run_stage2_two_phase_event_insert(scenario: Scenario, plan: list[ScheduledWindow]) -> Stage2Result:
    return TwoPhaseEventDrivenScheduler(scenario).run(plan)

