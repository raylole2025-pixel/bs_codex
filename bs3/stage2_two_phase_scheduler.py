from __future__ import annotations

import time
from collections import defaultdict
from dataclasses import dataclass

from .models import Allocation, PathCandidate, ScheduledWindow, Scenario, Segment, Stage1BaselineTrace, Stage2Result, Task
from .regular_routing_common import clamp01, cross_link_from_edges, post_allocation_max_utilization
from .scenario import active_cross_links, active_intra_links, build_segments, generate_candidate_paths

EPS = 1e-9
CAPACITY_TIER_RESERVED_ONLY = "reserved_only"
CAPACITY_TIER_BORROW_UNUSED_REGULAR_SHARE = "borrow_unused_regular_share"
CAPACITY_TIER_PREEMPTED = "preempted"
CAPACITY_TIER_BLOCKED = "blocked"
PREEMPTION_WEIGHT_COEFF = 0.35
PREEMPTION_RECOVERY_SLACK_COEFF = 0.30
PREEMPTION_RECOVERABILITY_COEFF = 0.35
PREEMPTION_SCORE_EPS = 1e-6
PREEMPTION_MIN_GAIN_RATIO = 0.05
TASK_RUNTIME_NORMAL = "normal"
TASK_RUNTIME_PREEMPTED_RECOVERABLE = "preempted_recoverable"
TASK_RUNTIME_RECOVERED_PARTIAL = "recovered_partial"
TASK_RUNTIME_RECOVERED_COMPLETE = "recovered_complete"
RECOVERY_START_BASIS_PREEMPTION = "max(last_released_segment_end, emergency_finish_time)"


@dataclass(frozen=True)
class _CapacityState:
    capacity: dict[str, float]
    total_free: dict[str, float]
    regular_cross_free: dict[str, float]
    used_regular_cross: dict[str, float]
    used_emergency_cross: dict[str, float]
    edge_kind: dict[str, str]


@dataclass(frozen=True)
class _PlannedAction:
    segment_index: int
    path: PathCandidate | None
    rate: float
    delivered: float
    tier: int = 0


@dataclass(frozen=True)
class _PlanLabel:
    last_cross_link: str | None
    remaining_data: float
    tier_cost: int
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
    tier_cost: int
    switches: int
    deviations: int
    load_cost: float


@dataclass(frozen=True)
class _PreemptionCandidate:
    task: Task
    score: float
    released_segments: tuple[int, ...]
    released_edge_ids: tuple[str, ...]
    released_cross_window_ids: tuple[str, ...]


@dataclass(frozen=True)
class _PreemptionAttempt:
    candidate: _PreemptionCandidate
    repaired_plan: _TaskPlan | None
    repaired_delivery: float
    emergency_gain: float
    emergency_gain_ratio: float
    victim_recovery_plan: _TaskPlan | None
    victim_recovery_delivery: float
    victim_recovery_completed: bool
    original_finish_time: float | None
    recovery_start_time: float | None
    recovery_basis: str | None
    last_released_segment_end: float | None
    repaired_emergency_finish_time: float | None
    remaining_after_preemption: float


class TwoPhaseEventDrivenScheduler:
    def __init__(self, scenario: Scenario) -> None:
        self.scenario = scenario
        self.numeric_tolerance = EPS
        self.completion_tolerance_ratio = max(float(self.scenario.stage2.completion_tolerance), 0.0)
        self.task_by_id = {task.task_id: task for task in self.scenario.tasks}

    def _task_completion_tolerance(self, task: Task | float) -> float:
        data = float(task.data) if isinstance(task, Task) else float(task)
        return max(self.completion_tolerance_ratio * max(data, 0.0), self.numeric_tolerance)

    def _is_task_complete(self, task: Task, remaining_data: float) -> bool:
        return remaining_data <= self._task_completion_tolerance(task)

    def _ordered_emergencies(self) -> list[Task]:
        emergencies = [task for task in self.scenario.tasks if task.task_type == "emg"]
        emergencies.sort(key=lambda task: (task.arrival, -task.weight, task.deadline, task.task_id))
        return emergencies

    def _resolve_baseline_trace(
        self,
        plan: list[ScheduledWindow],
        baseline_trace: Stage1BaselineTrace | None,
    ) -> tuple[Stage1BaselineTrace, str]:
        if baseline_trace is not None:
            return baseline_trace, "stage1_result"

        from .stage1 import RegularEvaluator

        return RegularEvaluator(self.scenario).baseline_trace(plan, rho=self.scenario.stage1.rho), "reconstructed_from_stage1"

    def _segments_from_baseline_trace(self, baseline_trace: Stage1BaselineTrace, plan: list[ScheduledWindow]) -> list[Segment]:
        rows = sorted(
            (dict(row) for row in baseline_trace.segments),
            key=lambda row: (float(row["start"]), float(row["end"]), int(row["segment_index"])),
        )
        if rows:
            return [
                Segment(
                    index=int(row["segment_index"]),
                    start=float(row["start"]),
                    end=float(row["end"]),
                )
                for row in rows
            ]
        regular_tasks = [task for task in self.scenario.tasks if task.task_type == "reg"]
        return build_segments(self.scenario, plan, regular_tasks)

    def _schedule_from_baseline_trace(
        self,
        baseline_trace: Stage1BaselineTrace,
    ) -> dict[tuple[str, int], Allocation]:
        schedule: dict[tuple[str, int], Allocation] = {}
        for item in baseline_trace.allocations:
            alloc = Allocation(
                task_id=item.task_id,
                segment_index=int(item.segment_index),
                path_id=item.path_id,
                edge_ids=tuple(item.edge_ids),
                rate=float(item.rate),
                delivered=float(item.delivered),
                task_type=item.task_type,
                cross_window_id=item.cross_window_id,
                is_preempted=bool(item.is_preempted),
            )
            schedule[(alloc.task_id, alloc.segment_index)] = alloc
        return schedule

    def run(self, plan: list[ScheduledWindow], baseline_trace: Stage1BaselineTrace | None = None) -> Stage2Result:
        started_at = time.perf_counter()
        regular_tasks = [task for task in self.scenario.tasks if task.task_type == "reg"]
        baseline_trace, baseline_source = self._resolve_baseline_trace(plan, baseline_trace)
        segments = self._segments_from_baseline_trace(baseline_trace, plan)
        raw_event_segment_count = len(segments)
        effective_event_segment_count = len(segments)
        baseline_schedule = self._schedule_from_baseline_trace(baseline_trace)
        baseline_completed = dict(baseline_trace.completed)
        committed: dict[tuple[str, int], Allocation] = dict(baseline_schedule)
        actual_remaining = {task.task_id: float(task.data) for task in self.scenario.tasks}
        prev_cross_link = {task.task_id: None for task in self.scenario.tasks}
        cross_edge_ids = {window.window_id for window in plan}
        allocations: list[Allocation] = []
        edge_usage = defaultdict(float)
        n_preemptions = 0
        insertion_events: list[dict[str, object]] = []
        recovery_events: list[dict[str, object]] = []
        task_runtime_state = {task.task_id: TASK_RUNTIME_NORMAL for task in self.scenario.tasks}
        task_preemption_count = {task.task_id: 0 for task in self.scenario.tasks}
        task_original_committed_finish = {task.task_id: None for task in self.scenario.tasks}
        pending_recovery_tasks: dict[str, dict[str, object]] = {}

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
                event_record = self._insert_emergency_task(
                    emergency=emergency,
                    plan=plan,
                    segments=segments,
                    current_index=seg_idx,
                    committed=committed,
                    actual_remaining=actual_remaining,
                    prev_cross_link=prev_cross_link,
                    task_runtime_state=task_runtime_state,
                    task_preemption_count=task_preemption_count,
                    task_original_committed_finish=task_original_committed_finish,
                    pending_recovery_tasks=pending_recovery_tasks,
                )
                preemptions_before = n_preemptions
                n_preemptions += int(event_record.get("preemptions", 0))
                insertion_events.append(
                    {
                        "task_id": emergency.task_id,
                        "arrival": float(emergency.arrival),
                        "deadline": float(emergency.deadline),
                        "data": float(emergency.data),
                        "weight": float(emergency.weight),
                        "segment_index": int(segment.index),
                        "preemptions_added": int(n_preemptions - preemptions_before),
                        **dict(event_record),
                    }
                )
                preempted_task_id = event_record.get("preempted_task_id")
                if (
                    isinstance(preempted_task_id, str)
                    and preempted_task_id in pending_recovery_tasks
                    and pending_recovery_tasks[preempted_task_id].get("insertion_event_index") is None
                ):
                    pending_recovery_tasks[preempted_task_id]["insertion_event_index"] = len(insertion_events) - 1

            segment = segments[seg_idx]
            if pending_emergencies and segment.start + self.numeric_tolerance < pending_emergencies[0].arrival < segment.end - self.numeric_tolerance:
                segments, baseline_schedule, committed = self._split_segment_if_needed(
                    segments,
                    pending_emergencies[0].arrival,
                    baseline_schedule,
                    committed,
                )
                continue

            recovery_split_time = self._pending_recovery_split_time(segment, pending_recovery_tasks)
            if recovery_split_time is not None:
                segments, baseline_schedule, committed = self._split_segment_if_needed(
                    segments,
                    recovery_split_time,
                    baseline_schedule,
                    committed,
                )
                continue

            ready_recoveries = sorted(
                (
                    info
                    for info in pending_recovery_tasks.values()
                    if not bool(info.get("processed"))
                    and info.get("recovery_start_time") is not None
                    and float(info["recovery_start_time"]) <= segment.start + self.numeric_tolerance
                ),
                key=lambda info: (
                    float(info.get("recovery_start_time", 0.0)),
                    float(info.get("deadline", float("inf"))),
                    -float(self.task_by_id[str(info["task_id"])].weight),
                    str(info["task_id"]),
                ),
            )
            for recovery_info in ready_recoveries:
                recovery_event = self._schedule_recovery_best_effort(
                    task=self.task_by_id[str(recovery_info["task_id"])],
                    recovery_info=recovery_info,
                    plan=plan,
                    segments=segments,
                    committed=committed,
                    actual_remaining=actual_remaining,
                    prev_cross_link=prev_cross_link,
                    task_runtime_state=task_runtime_state,
                )
                pending_recovery_tasks[str(recovery_info["task_id"])]["processed"] = True
                recovery_events.append(recovery_event)
                insertion_event_index = recovery_info.get("insertion_event_index")
                if isinstance(insertion_event_index, int) and 0 <= insertion_event_index < len(insertion_events):
                    insertion_events[insertion_event_index]["victim_recovery_delivery"] = float(recovery_event["delivery"])
                    insertion_events[insertion_event_index]["victim_recovery_completed"] = bool(recovery_event["completed"])

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
                    cross_window_id=alloc.cross_window_id,
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
        baseline_completed_count = sum(1 for done in baseline_completed.values() if done)
        final_regular_remaining = {
            task.task_id: float(actual_remaining.get(task.task_id, 0.0))
            for task in regular_tasks
        }
        final_regular_completed = {
            task.task_id: bool(self._is_task_complete(task, final_regular_remaining[task.task_id]))
            for task in regular_tasks
        }
        final_completed_count = sum(1 for done in final_regular_completed.values() if done)
        degraded_regular_tasks = sorted(
            task_id
            for task_id, done in baseline_completed.items()
            if done and not final_regular_completed.get(task_id, False)
        )
        baseline_cross_usage = {
            int(segment_index): {str(window_id): float(value) for window_id, value in usage.items()}
            for segment_index, usage in baseline_trace.cross_window_usage_by_segment.items()
        }
        final_cross_usage = self._cross_usage_from_allocations(allocations)
        regular_cross_usage = self._cross_usage_from_allocations(allocations, task_type="reg")
        emergency_cross_usage = self._cross_usage_from_allocations(allocations, task_type="emg")
        recovered_regular_tasks = sorted(
            task_id
            for task_id, state in task_runtime_state.items()
            if state in {TASK_RUNTIME_RECOVERED_PARTIAL, TASK_RUNTIME_RECOVERED_COMPLETE}
        )
        preempted_regular_tasks = sorted(
            task.task_id
            for task in regular_tasks
            if int(task_preemption_count.get(task.task_id, 0)) > 0
        )
        recovered_regular_completed_count = sum(
            1
            for task_id in recovered_regular_tasks
            if task_runtime_state.get(task_id) == TASK_RUNTIME_RECOVERED_COMPLETE
        )
        metadata = self._build_result_metadata(
            event_segment_count=effective_event_segment_count,
            event_segment_count_raw=raw_event_segment_count,
            regular_task_count=len(regular_tasks),
            elapsed_seconds=time.perf_counter() - started_at,
            plan_window_count=len(plan),
        )
        metadata.update(
            {
                "baseline_source": baseline_source,
                "baseline_summary": dict(baseline_trace.summary),
                "baseline_completed_count": int(baseline_completed_count),
                "final_regular_completed_count": int(final_completed_count),
                "regular_tasks_degraded_by_emergency": degraded_regular_tasks,
                "regular_remaining_end": final_regular_remaining,
                "baseline_remaining_end": dict(baseline_trace.remaining_end),
                "baseline_regular_allocation_count": len(baseline_trace.allocations),
                "executed_regular_allocation_count": sum(1 for alloc in allocations if alloc.task_type == "reg"),
                "executed_emergency_allocation_count": sum(1 for alloc in allocations if alloc.task_type == "emg"),
                "emergency_insertions": insertion_events,
                "emergency_task_count": len([task for task in self.scenario.tasks if task.task_type == "emg"]),
                "emergency_insertions_count": len(insertion_events),
                "emergency_insertions_used_preemption_count": sum(
                    1 for item in insertion_events if bool(item.get("used_preemption"))
                ),
                "emergency_insertions_direct_count": sum(
                    1 for item in insertion_events if str(item.get("strategy")) == "direct_insert"
                ),
                "controlled_preemption_best_effort_count": sum(
                    1 for item in insertion_events if str(item.get("strategy")) == "controlled_preemption_best_effort"
                ),
                "recovery_events": recovery_events,
                "recovered_regular_tasks": recovered_regular_tasks,
                "recovered_regular_completed_count": int(recovered_regular_completed_count),
                "preempted_regular_tasks": preempted_regular_tasks,
                "empty_emergency_insert": len(insertion_events) == 0,
                "baseline_cross_window_usage_by_segment": baseline_cross_usage,
                "final_cross_window_usage_by_segment": final_cross_usage,
                "regular_cross_window_usage_by_segment": regular_cross_usage,
                "emergency_cross_window_usage_by_segment": emergency_cross_usage,
                "cross_window_usage_delta_by_segment": self._cross_usage_delta(baseline_cross_usage, final_cross_usage),
            }
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
                    cross_window_id=alloc.cross_window_id,
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
                    cross_window_id=alloc.cross_window_id,
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
                    cross_window_id=alloc.cross_window_id,
                    is_preempted=alloc.is_preempted,
                )
            )
        return result

    def _pending_recovery_split_time(
        self,
        segment: Segment,
        pending_recovery_tasks: dict[str, dict[str, object]],
    ) -> float | None:
        split_times = [
            float(info["recovery_start_time"])
            for info in pending_recovery_tasks.values()
            if not bool(info.get("processed"))
            and info.get("recovery_start_time") is not None
            and segment.start + self.numeric_tolerance < float(info["recovery_start_time"]) < segment.end - self.numeric_tolerance
        ]
        if not split_times:
            return None
        return min(split_times)

    def _solver_mode_label(self) -> str:
        return "stage2_emergency_insert"

    def _build_result_metadata(
        self,
        *,
        event_segment_count: int,
        event_segment_count_raw: int,
        regular_task_count: int,
        elapsed_seconds: float,
        plan_window_count: int,
    ) -> dict[str, object]:
        return {
            "stage2_role": "emergency_event_insert_only",
            "solver_mode": self._solver_mode_label(),
            "event_segment_count": int(event_segment_count),
            "event_segment_count_raw": int(event_segment_count_raw),
            "regular_task_count": int(regular_task_count),
            "plan_window_count": int(plan_window_count),
            "elapsed_seconds": float(elapsed_seconds),
        }

    @staticmethod
    def _task_plan_delivery(task_plan: _TaskPlan | None) -> float:
        if task_plan is None:
            return 0.0
        return float(sum(action.delivered for action in task_plan.actions))

    def _emergency_gain(self, repaired_plan: _TaskPlan | None, direct_plan: _TaskPlan | None) -> float:
        return max(0.0, self._task_plan_delivery(repaired_plan) - self._task_plan_delivery(direct_plan))

    def _emergency_gain_ratio(self, task: Task, repaired_plan: _TaskPlan | None, direct_plan: _TaskPlan | None) -> float:
        return self._emergency_gain(repaired_plan, direct_plan) / max(float(task.data), self.numeric_tolerance)

    @staticmethod
    def _plan_capacity_tier(task_plan: _TaskPlan | None) -> str:
        if task_plan is None:
            return CAPACITY_TIER_BLOCKED
        return CAPACITY_TIER_BORROW_UNUSED_REGULAR_SHARE if int(task_plan.tier_cost) > 0 else CAPACITY_TIER_RESERVED_ONLY

    def _task_plan_label(self, task_plan: _TaskPlan | None) -> _PlanLabel:
        if task_plan is None:
            return _PlanLabel(
                last_cross_link=None,
                remaining_data=float("inf"),
                tier_cost=int(1e9),
                idle_steps=int(1e9),
                switches=int(1e9),
                deviations=int(1e9),
                load_cost=float("inf"),
                finish_time=None,
                actions=tuple(),
            )
        last_cross_link = None
        idle_steps = 0
        for action in task_plan.actions:
            if action.path is None or action.delivered <= self.numeric_tolerance:
                idle_steps += 1
                continue
            last_cross_link = action.path.cross_window_id
        return _PlanLabel(
            last_cross_link=last_cross_link,
            remaining_data=float(task_plan.remaining_data),
            tier_cost=int(task_plan.tier_cost),
            idle_steps=int(idle_steps),
            switches=int(task_plan.switches),
            deviations=int(task_plan.deviations),
            load_cost=float(task_plan.load_cost),
            finish_time=task_plan.finish_time,
            actions=task_plan.actions,
        )

    def _is_better_partial_plan(self, lhs: _TaskPlan | None, rhs: _TaskPlan | None) -> bool:
        lhs_delivery = self._task_plan_delivery(lhs)
        rhs_delivery = self._task_plan_delivery(rhs)
        if lhs_delivery > rhs_delivery + self.numeric_tolerance:
            return True
        if lhs_delivery + self.numeric_tolerance < rhs_delivery:
            return False
        lhs_label = self._task_plan_label(lhs)
        rhs_label = self._task_plan_label(rhs)
        lhs_key = (
            self._remaining_key(lhs_label.remaining_data),
            float(lhs_label.tier_cost),
            float(lhs_label.switches),
            lhs_label.load_cost,
            float(lhs_label.idle_steps),
        )
        rhs_key = (
            self._remaining_key(rhs_label.remaining_data),
            float(rhs_label.tier_cost),
            float(rhs_label.switches),
            rhs_label.load_cost,
            float(rhs_label.idle_steps),
        )
        return lhs_key < rhs_key

    def _make_insertion_event(
        self,
        *,
        emergency: Task,
        horizon: list[Segment],
        strategy: str,
        completed: bool,
        capacity_tier: str,
        used_preemption: bool,
        preemptions: int,
        planned_delivery: float,
        direct_plan_delivery: float,
        preempted_task_id: str | None = None,
        released_segments: tuple[int, ...] = (),
        released_edge_ids: tuple[str, ...] = (),
        released_cross_window_ids: tuple[str, ...] = (),
        preemption_score: float | None = None,
        emergency_gain: float = 0.0,
        emergency_gain_ratio: float = 0.0,
        preemption_accepted_by_gain_gate: bool = False,
        preemption_rejected_reason: str | None = None,
        victim_original_finish_time: float | None = None,
        victim_recovery_start_time: float | None = None,
        victim_recovery_basis: str | None = None,
        last_released_segment_end: float | None = None,
        repaired_emergency_finish_time: float | None = None,
        victim_recovery_delivery: float | None = None,
        victim_recovery_completed: bool | None = None,
        best_effort_source: str | None = None,
    ) -> dict[str, object]:
        return {
            "task_id": emergency.task_id,
            "strategy": strategy,
            "completed": bool(completed),
            "capacity_tier": capacity_tier,
            "used_preemption": bool(used_preemption),
            "preemptions": int(preemptions),
            "preempted_task_id": preempted_task_id,
            "released_segments": [int(index) for index in released_segments],
            "released_edge_ids": [str(edge_id) for edge_id in released_edge_ids],
            "released_cross_window_ids": [str(window_id) for window_id in released_cross_window_ids],
            "preemption_score": None if preemption_score is None else float(preemption_score),
            "emergency_gain": float(emergency_gain),
            "emergency_gain_ratio": float(emergency_gain_ratio),
            "preemption_accepted_by_gain_gate": bool(preemption_accepted_by_gain_gate),
            "preemption_rejected_reason": preemption_rejected_reason,
            "horizon_segment_indices": [segment.index for segment in horizon],
            "planned_delivery": float(planned_delivery),
            "direct_plan_delivery": float(direct_plan_delivery),
            "victim_original_finish_time": None if victim_original_finish_time is None else float(victim_original_finish_time),
            "victim_recovery_start_time": None if victim_recovery_start_time is None else float(victim_recovery_start_time),
            "victim_recovery_basis": victim_recovery_basis,
            "last_released_segment_end": None if last_released_segment_end is None else float(last_released_segment_end),
            "repaired_emergency_finish_time": (
                None if repaired_emergency_finish_time is None else float(repaired_emergency_finish_time)
            ),
            "victim_recovery_delivery": None if victim_recovery_delivery is None else float(victim_recovery_delivery),
            "victim_recovery_completed": None if victim_recovery_completed is None else bool(victim_recovery_completed),
            "best_effort_source": best_effort_source,
        }

    def _insert_emergency_task(
        self,
        emergency: Task,
        plan: list[ScheduledWindow],
        segments: list[Segment],
        current_index: int,
        committed: dict[tuple[str, int], Allocation],
        actual_remaining: dict[str, float],
        prev_cross_link: dict[str, str | None],
        task_runtime_state: dict[str, str],
        task_preemption_count: dict[str, int],
        task_original_committed_finish: dict[str, float | None],
        pending_recovery_tasks: dict[str, dict[str, object]],
    ) -> dict[str, object]:
        horizon = self._emergency_horizon(segments, current_index, emergency)
        if not horizon:
            return self._make_insertion_event(
                emergency=emergency,
                horizon=[],
                strategy="blocked",
                completed=False,
                capacity_tier=CAPACITY_TIER_BLOCKED,
                used_preemption=False,
                preemptions=0,
                planned_delivery=0.0,
                direct_plan_delivery=0.0,
            )

        direct_plan = self._solve_direct_insert(
            task=emergency,
            plan=plan,
            horizon=horizon,
            committed=committed,
            actual_remaining=actual_remaining,
            prev_cross_link=prev_cross_link,
        )
        direct_plan_delivery = self._task_plan_delivery(direct_plan)
        corridor = self._candidate_corridor(plan, emergency, horizon, committed)
        affected = self._affected_regular_tasks(
            emergency=emergency,
            horizon=horizon,
            committed=committed,
            actual_remaining=actual_remaining,
            corridor=corridor,
            task_runtime_state=task_runtime_state,
            task_preemption_count=task_preemption_count,
        )
        if direct_plan is not None and direct_plan.completed:
            self._commit_single_task_plan(committed, emergency, horizon, direct_plan.actions)
            return self._make_insertion_event(
                emergency=emergency,
                horizon=horizon,
                strategy="direct_insert",
                completed=True,
                capacity_tier=self._plan_capacity_tier(direct_plan),
                used_preemption=False,
                preemptions=0,
                planned_delivery=direct_plan_delivery,
                direct_plan_delivery=direct_plan_delivery,
            )

        best_complete, best_partial, preemption_rejection = self._try_controlled_preemption(
            emergency=emergency,
            direct_plan=direct_plan,
            affected=affected,
            plan=plan,
            segments=segments,
            horizon=horizon,
            committed=committed,
            actual_remaining=actual_remaining,
            prev_cross_link=prev_cross_link,
            corridor=corridor,
        )
        rejection_event_fields = {
            "emergency_gain": float((preemption_rejection or {}).get("emergency_gain", 0.0)),
            "emergency_gain_ratio": float((preemption_rejection or {}).get("emergency_gain_ratio", 0.0)),
            "preemption_rejected_reason": (preemption_rejection or {}).get("preemption_rejected_reason"),
            "victim_recovery_completed": (preemption_rejection or {}).get("victim_recovery_completed"),
        }

        if best_complete is not None:
            self._activate_preemption_attempt(
                emergency=emergency,
                attempt=best_complete,
                segments=segments,
                horizon=horizon,
                committed=committed,
                actual_remaining=actual_remaining,
                task_runtime_state=task_runtime_state,
                task_preemption_count=task_preemption_count,
                task_original_committed_finish=task_original_committed_finish,
                pending_recovery_tasks=pending_recovery_tasks,
            )
            return self._make_insertion_event(
                emergency=emergency,
                horizon=horizon,
                strategy="controlled_preemption",
                completed=True,
                capacity_tier=CAPACITY_TIER_PREEMPTED,
                used_preemption=True,
                preemptions=1,
                planned_delivery=best_complete.repaired_delivery,
                direct_plan_delivery=direct_plan_delivery,
                preempted_task_id=best_complete.candidate.task.task_id,
                released_segments=best_complete.candidate.released_segments,
                released_edge_ids=best_complete.candidate.released_edge_ids,
                released_cross_window_ids=best_complete.candidate.released_cross_window_ids,
                preemption_score=best_complete.candidate.score,
                emergency_gain=best_complete.emergency_gain,
                emergency_gain_ratio=best_complete.emergency_gain_ratio,
                preemption_accepted_by_gain_gate=True,
                victim_original_finish_time=best_complete.original_finish_time,
                victim_recovery_start_time=best_complete.recovery_start_time,
                victim_recovery_basis=best_complete.recovery_basis,
                last_released_segment_end=best_complete.last_released_segment_end,
                repaired_emergency_finish_time=best_complete.repaired_emergency_finish_time,
            )

        if best_partial is not None and self._is_better_partial_plan(best_partial.repaired_plan, direct_plan):
            self._activate_preemption_attempt(
                emergency=emergency,
                attempt=best_partial,
                segments=segments,
                horizon=horizon,
                committed=committed,
                actual_remaining=actual_remaining,
                task_runtime_state=task_runtime_state,
                task_preemption_count=task_preemption_count,
                task_original_committed_finish=task_original_committed_finish,
                pending_recovery_tasks=pending_recovery_tasks,
            )
            return self._make_insertion_event(
                emergency=emergency,
                horizon=horizon,
                strategy="controlled_preemption_best_effort",
                completed=False,
                capacity_tier=CAPACITY_TIER_PREEMPTED,
                used_preemption=True,
                preemptions=1,
                planned_delivery=best_partial.repaired_delivery,
                direct_plan_delivery=direct_plan_delivery,
                preempted_task_id=best_partial.candidate.task.task_id,
                released_segments=best_partial.candidate.released_segments,
                released_edge_ids=best_partial.candidate.released_edge_ids,
                released_cross_window_ids=best_partial.candidate.released_cross_window_ids,
                preemption_score=best_partial.candidate.score,
                emergency_gain=best_partial.emergency_gain,
                emergency_gain_ratio=best_partial.emergency_gain_ratio,
                preemption_accepted_by_gain_gate=True,
                victim_original_finish_time=best_partial.original_finish_time,
                victim_recovery_start_time=best_partial.recovery_start_time,
                victim_recovery_basis=best_partial.recovery_basis,
                last_released_segment_end=best_partial.last_released_segment_end,
                repaired_emergency_finish_time=best_partial.repaired_emergency_finish_time,
                best_effort_source="preempted",
            )

        if direct_plan is not None and direct_plan_delivery > self.numeric_tolerance:
            self._commit_single_task_plan(committed, emergency, horizon, direct_plan.actions)
            return self._make_insertion_event(
                emergency=emergency,
                horizon=horizon,
                strategy="direct_insert_best_effort",
                completed=False,
                capacity_tier=self._plan_capacity_tier(direct_plan),
                used_preemption=False,
                preemptions=0,
                planned_delivery=direct_plan_delivery,
                direct_plan_delivery=direct_plan_delivery,
                best_effort_source="direct",
                **rejection_event_fields,
            )
        return self._make_insertion_event(
            emergency=emergency,
            horizon=horizon,
            strategy="blocked",
            completed=False,
            capacity_tier=CAPACITY_TIER_BLOCKED,
            used_preemption=False,
            preemptions=0,
            planned_delivery=0.0,
            direct_plan_delivery=direct_plan_delivery,
            **rejection_event_fields,
        )

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
        direct_plan: _TaskPlan | None,
        affected: list[Task],
        plan: list[ScheduledWindow],
        segments: list[Segment],
        horizon: list[Segment],
        committed: dict[tuple[str, int], Allocation],
        actual_remaining: dict[str, float],
        prev_cross_link: dict[str, str | None],
        corridor: dict[int, set[str]],
    ) -> tuple[_PreemptionAttempt | None, _PreemptionAttempt | None, dict[str, object] | None]:
        if not affected:
            return None, None, None

        candidates = self._rank_preemption_candidates(
            emergency=emergency,
            affected=affected,
            plan=plan,
            committed=committed,
            actual_remaining=actual_remaining,
            segments=segments,
            horizon=horizon,
            corridor=corridor,
        )
        best_complete: _PreemptionAttempt | None = None
        best_partial: _PreemptionAttempt | None = None
        best_gain_gate_rejection: dict[str, object] | None = None
        for candidate in candidates:
            if not candidate.released_segments:
                continue

            released_segment_set = set(candidate.released_segments)
            tentative = {
                key: alloc
                for key, alloc in committed.items()
                if not (key[0] == candidate.task.task_id and key[1] in released_segment_set)
            }
            repaired_plan = self._solve_direct_insert(
                task=emergency,
                plan=plan,
                horizon=horizon,
                committed=tentative,
                actual_remaining=actual_remaining,
                prev_cross_link=prev_cross_link,
            )
            repaired_delivery = self._task_plan_delivery(repaired_plan)
            if repaired_plan is None or repaired_delivery <= self.numeric_tolerance:
                continue
            emergency_gain = self._emergency_gain(repaired_plan, direct_plan)
            emergency_gain_ratio = self._emergency_gain_ratio(emergency, repaired_plan, direct_plan)

            original_finish_time = self._original_committed_finish_time(candidate.task.task_id, committed, segments)
            (
                recovery_start_time,
                recovery_basis,
                last_released_segment_end,
                repaired_emergency_finish_time,
            ) = self._resolve_recovery_start_time(
                released_segments=candidate.released_segments,
                segments=segments,
                repaired_plan=repaired_plan,
                horizon=horizon,
            )
            remaining_after_preemption = self._remaining_after_committed(candidate.task, tentative, actual_remaining)

            tentative_after_emergency = dict(tentative)
            self._commit_single_task_plan(tentative_after_emergency, emergency, horizon, repaired_plan.actions)
            victim_recovery_plan = self._plan_recovery_best_effort(
                task=candidate.task,
                recovery_start_time=recovery_start_time,
                remaining_data=remaining_after_preemption,
                plan=plan,
                segments=segments,
                committed=tentative_after_emergency,
                initial_cross_link=None,
            )
            victim_recovery_delivery = self._task_plan_delivery(victim_recovery_plan)
            victim_recovery_completed = self._is_task_complete(
                candidate.task,
                max(remaining_after_preemption - victim_recovery_delivery, 0.0),
            )

            attempt = _PreemptionAttempt(
                candidate=candidate,
                repaired_plan=repaired_plan,
                repaired_delivery=repaired_delivery,
                emergency_gain=emergency_gain,
                emergency_gain_ratio=emergency_gain_ratio,
                victim_recovery_plan=victim_recovery_plan,
                victim_recovery_delivery=victim_recovery_delivery,
                victim_recovery_completed=victim_recovery_completed,
                original_finish_time=original_finish_time,
                recovery_start_time=recovery_start_time,
                recovery_basis=recovery_basis,
                last_released_segment_end=last_released_segment_end,
                repaired_emergency_finish_time=repaired_emergency_finish_time,
                remaining_after_preemption=remaining_after_preemption,
            )
            if repaired_plan.completed:
                if emergency_gain_ratio + self.numeric_tolerance < PREEMPTION_MIN_GAIN_RATIO:
                    rejection = {
                        "preemption_rejected_reason": "gain_ratio_below_threshold",
                        "emergency_gain": float(emergency_gain),
                        "emergency_gain_ratio": float(emergency_gain_ratio),
                        "victim_recovery_completed": bool(victim_recovery_completed),
                    }
                    if best_gain_gate_rejection is None or (
                        float(rejection["emergency_gain_ratio"]) > float(best_gain_gate_rejection["emergency_gain_ratio"]) + self.numeric_tolerance
                        or (
                            abs(
                                float(rejection["emergency_gain_ratio"]) - float(best_gain_gate_rejection["emergency_gain_ratio"])
                            ) <= self.numeric_tolerance
                            and float(rejection["emergency_gain"]) > float(best_gain_gate_rejection["emergency_gain"]) + self.numeric_tolerance
                        )
                    ):
                        best_gain_gate_rejection = rejection
                    continue
                if best_complete is None:
                    best_complete = attempt
                    continue
                current_key = (
                    0 if attempt.victim_recovery_completed else 1,
                    float(attempt.candidate.score),
                    *self._terminal_key(self._task_plan_label(attempt.repaired_plan), "emergency"),
                    -float(attempt.victim_recovery_delivery),
                    attempt.candidate.task.task_id,
                )
                best_key = (
                    0 if best_complete.victim_recovery_completed else 1,
                    float(best_complete.candidate.score),
                    *self._terminal_key(self._task_plan_label(best_complete.repaired_plan), "emergency"),
                    -float(best_complete.victim_recovery_delivery),
                    best_complete.candidate.task.task_id,
                )
                if current_key < best_key:
                    best_complete = attempt
                continue
            if emergency_gain <= self.numeric_tolerance:
                continue
            if emergency_gain_ratio + self.numeric_tolerance < PREEMPTION_MIN_GAIN_RATIO:
                rejection = {
                    "preemption_rejected_reason": "gain_ratio_below_threshold",
                    "emergency_gain": float(emergency_gain),
                    "emergency_gain_ratio": float(emergency_gain_ratio),
                    "victim_recovery_completed": bool(victim_recovery_completed),
                }
                if best_gain_gate_rejection is None or (
                    float(rejection["emergency_gain_ratio"]) > float(best_gain_gate_rejection["emergency_gain_ratio"]) + self.numeric_tolerance
                    or (
                        abs(
                            float(rejection["emergency_gain_ratio"]) - float(best_gain_gate_rejection["emergency_gain_ratio"])
                        ) <= self.numeric_tolerance
                        and float(rejection["emergency_gain"]) > float(best_gain_gate_rejection["emergency_gain"]) + self.numeric_tolerance
                    )
                ):
                    best_gain_gate_rejection = rejection
                continue
            if best_partial is None:
                best_partial = attempt
                continue
            if self._is_better_partial_plan(attempt.repaired_plan, best_partial.repaired_plan):
                best_partial = attempt
                continue
            if not self._is_better_partial_plan(best_partial.repaired_plan, attempt.repaired_plan):
                current_key = (
                    0 if attempt.victim_recovery_completed else 1,
                    float(attempt.candidate.score),
                    -float(attempt.victim_recovery_delivery),
                    attempt.candidate.task.task_id,
                )
                best_key = (
                    0 if best_partial.victim_recovery_completed else 1,
                    float(best_partial.candidate.score),
                    -float(best_partial.victim_recovery_delivery),
                    best_partial.candidate.task.task_id,
                )
                if current_key < best_key:
                    best_partial = attempt

        return best_complete, best_partial, best_gain_gate_rejection

    def _activate_preemption_attempt(
        self,
        *,
        emergency: Task,
        attempt: _PreemptionAttempt,
        segments: list[Segment],
        horizon: list[Segment],
        committed: dict[tuple[str, int], Allocation],
        actual_remaining: dict[str, float],
        task_runtime_state: dict[str, str],
        task_preemption_count: dict[str, int],
        task_original_committed_finish: dict[str, float | None],
        pending_recovery_tasks: dict[str, dict[str, object]],
    ) -> None:
        victim = attempt.candidate.task
        for segment_index in attempt.candidate.released_segments:
            committed.pop((victim.task_id, segment_index), None)
        if attempt.repaired_plan is not None:
            self._commit_single_task_plan(committed, emergency, horizon, attempt.repaired_plan.actions)
        task_runtime_state[victim.task_id] = TASK_RUNTIME_PREEMPTED_RECOVERABLE
        task_preemption_count[victim.task_id] = int(task_preemption_count.get(victim.task_id, 0)) + 1
        if task_original_committed_finish.get(victim.task_id) is None:
            task_original_committed_finish[victim.task_id] = attempt.original_finish_time
        if attempt.recovery_start_time is None or attempt.recovery_start_time >= victim.deadline - self.numeric_tolerance:
            return
        pending_recovery_tasks[victim.task_id] = {
            "task_id": victim.task_id,
            "recovery_start_time": float(attempt.recovery_start_time),
            "recovery_basis": attempt.recovery_basis,
            "last_released_segment_end": (
                None if attempt.last_released_segment_end is None else float(attempt.last_released_segment_end)
            ),
            "repaired_emergency_finish_time": (
                None
                if attempt.repaired_emergency_finish_time is None
                else float(attempt.repaired_emergency_finish_time)
            ),
            "deadline": float(victim.deadline),
            "remaining_after_preemption": float(attempt.remaining_after_preemption),
            "preempted_by": emergency.task_id,
            "released_segments": [int(index) for index in attempt.candidate.released_segments],
            "processed": False,
            "insertion_event_index": None,
        }

    def _released_segment_end_time(
        self,
        released_segments: tuple[int, ...] | list[int],
        segments: list[Segment],
    ) -> float | None:
        if not segments or not released_segments:
            return None
        segment_by_index = {segment.index: segment for segment in segments}
        released_ends = [
            float(segment_by_index[index].end)
            for index in released_segments
            if index in segment_by_index
        ]
        if not released_ends:
            return None
        return max(released_ends)

    def _task_plan_effective_finish_time(
        self,
        task_plan: _TaskPlan | None,
        segments: list[Segment],
        fallback_time: float | None = None,
    ) -> float | None:
        if task_plan is None:
            return fallback_time
        if task_plan.finish_time is not None:
            return float(task_plan.finish_time)
        if not segments:
            return fallback_time
        segment_by_index = {segment.index: segment for segment in segments}
        finish_time = fallback_time
        for action in task_plan.actions:
            if action.delivered <= self.numeric_tolerance:
                continue
            segment = segment_by_index.get(action.segment_index)
            if segment is None:
                continue
            segment_end = float(segment.end)
            finish_time = segment_end if finish_time is None else max(float(finish_time), segment_end)
        return finish_time

    def _resolve_recovery_start_time(
        self,
        *,
        released_segments: tuple[int, ...],
        segments: list[Segment],
        repaired_plan: _TaskPlan | None,
        horizon: list[Segment],
    ) -> tuple[float | None, str | None, float | None, float | None]:
        last_released_segment_end = self._released_segment_end_time(released_segments, segments)
        fallback_time = float(horizon[0].start) if horizon else None
        repaired_emergency_finish_time = self._task_plan_effective_finish_time(
            repaired_plan,
            horizon,
            fallback_time=fallback_time,
        )
        candidates = [
            value
            for value in (last_released_segment_end, repaired_emergency_finish_time)
            if value is not None
        ]
        recovery_start_time = max(candidates) if candidates else fallback_time
        recovery_basis = RECOVERY_START_BASIS_PREEMPTION if recovery_start_time is not None else None
        return recovery_start_time, recovery_basis, last_released_segment_end, repaired_emergency_finish_time

    def _original_committed_finish_time(
        self,
        task_id: str,
        committed: dict[tuple[str, int], Allocation],
        segments: list[Segment],
    ) -> float | None:
        if not segments:
            return None
        segment_by_index = {segment.index: segment for segment in segments}
        last_end: float | None = None
        for (alloc_task_id, segment_index), alloc in committed.items():
            if alloc_task_id != task_id or alloc.delivered <= self.numeric_tolerance:
                continue
            segment = segment_by_index.get(segment_index)
            if segment is None:
                continue
            last_end = segment.end if last_end is None else max(last_end, segment.end)
        return last_end

    def _remaining_after_committed(
        self,
        task: Task,
        committed: dict[tuple[str, int], Allocation],
        actual_remaining: dict[str, float],
    ) -> float:
        committed_delivery = sum(
            float(alloc.delivered)
            for (task_id, _), alloc in committed.items()
            if task_id == task.task_id
        )
        return max(float(actual_remaining.get(task.task_id, 0.0)) - committed_delivery, 0.0)

    def _recovery_horizon(
        self,
        task: Task,
        segments: list[Segment],
        recovery_start_time: float | None,
    ) -> list[Segment]:
        if recovery_start_time is None or recovery_start_time >= task.deadline - self.numeric_tolerance:
            return []
        return [
            segment
            for segment in segments
            if segment.start + self.numeric_tolerance >= recovery_start_time and segment.start < task.deadline - self.numeric_tolerance
        ]

    def _plan_recovery_best_effort(
        self,
        *,
        task: Task,
        recovery_start_time: float | None,
        remaining_data: float,
        plan: list[ScheduledWindow],
        segments: list[Segment],
        committed: dict[tuple[str, int], Allocation],
        initial_cross_link: str | None,
    ) -> _TaskPlan | None:
        recovery_horizon = self._recovery_horizon(task, segments, recovery_start_time)
        if not recovery_horizon:
            return None
        capacity_states = self._free_capacity_states(recovery_horizon, plan, committed, exclude_tasks=set())
        return self._plan_task(
            plan=plan,
            task=task,
            segments=recovery_horizon,
            capacity_states=capacity_states,
            remaining_data=remaining_data,
            initial_cross_link=initial_cross_link,
            preferred_cross_links={},
            objective="recovery",
        )

    def _estimate_recoverability(
        self,
        task: Task,
        recovery_start_time: float | None,
        committed: dict[tuple[str, int], Allocation],
        actual_remaining: dict[str, float],
        plan: list[ScheduledWindow],
        segments: list[Segment],
    ) -> float:
        remaining_after_preemption = self._remaining_after_committed(task, committed, actual_remaining)
        if self._is_task_complete(task, remaining_after_preemption):
            return 1.0
        recovery_plan = self._plan_recovery_best_effort(
            task=task,
            recovery_start_time=recovery_start_time,
            remaining_data=remaining_after_preemption,
            plan=plan,
            segments=segments,
            committed=committed,
            initial_cross_link=None,
        )
        recoverable_delivery = self._task_plan_delivery(recovery_plan)
        return clamp01(recoverable_delivery / max(remaining_after_preemption, self.numeric_tolerance))

    def _schedule_recovery_best_effort(
        self,
        *,
        task: Task,
        recovery_info: dict[str, object],
        plan: list[ScheduledWindow],
        segments: list[Segment],
        committed: dict[tuple[str, int], Allocation],
        actual_remaining: dict[str, float],
        prev_cross_link: dict[str, str | None],
        task_runtime_state: dict[str, str],
    ) -> dict[str, object]:
        recovery_start_time = recovery_info.get("recovery_start_time")
        recovery_start = None if recovery_start_time is None else float(recovery_start_time)
        remaining_before = float(actual_remaining.get(task.task_id, 0.0))
        if self._is_task_complete(task, remaining_before):
            task_runtime_state[task.task_id] = TASK_RUNTIME_RECOVERED_COMPLETE
            return {
                "task_id": task.task_id,
                "recovery_start_time": recovery_start,
                "recovery_basis": recovery_info.get("recovery_basis"),
                "last_released_segment_end": recovery_info.get("last_released_segment_end"),
                "repaired_emergency_finish_time": recovery_info.get("repaired_emergency_finish_time"),
                "delivery": 0.0,
                "completed": True,
                "remaining_after_recovery": 0.0,
                "preempted_by": recovery_info.get("preempted_by"),
                "released_segments": list(recovery_info.get("released_segments") or []),
            }
        recovery_plan = self._plan_recovery_best_effort(
            task=task,
            recovery_start_time=recovery_start,
            remaining_data=remaining_before,
            plan=plan,
            segments=segments,
            committed=committed,
            initial_cross_link=prev_cross_link.get(task.task_id),
        )
        recovery_delivery = self._task_plan_delivery(recovery_plan)
        remaining_after_recovery = max(remaining_before - recovery_delivery, 0.0)
        recovery_completed = self._is_task_complete(task, remaining_after_recovery)
        if recovery_plan is not None and recovery_delivery > self.numeric_tolerance:
            recovery_horizon = self._recovery_horizon(task, segments, recovery_start)
            self._commit_single_task_plan(committed, task, recovery_horizon, recovery_plan.actions)
            task_runtime_state[task.task_id] = (
                TASK_RUNTIME_RECOVERED_COMPLETE if recovery_completed else TASK_RUNTIME_RECOVERED_PARTIAL
            )
        elif recovery_completed:
            task_runtime_state[task.task_id] = TASK_RUNTIME_RECOVERED_COMPLETE
        return {
            "task_id": task.task_id,
            "recovery_start_time": recovery_start,
            "recovery_basis": recovery_info.get("recovery_basis"),
            "last_released_segment_end": recovery_info.get("last_released_segment_end"),
            "repaired_emergency_finish_time": recovery_info.get("repaired_emergency_finish_time"),
            "delivery": float(recovery_delivery),
            "completed": bool(recovery_completed),
            "remaining_after_recovery": float(remaining_after_recovery),
            "preempted_by": recovery_info.get("preempted_by"),
            "released_segments": list(recovery_info.get("released_segments") or []),
        }

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
            return _TaskPlan(
                actions=tuple(),
                remaining_data=0.0,
                completed=True,
                finish_time=None,
                tier_cost=0,
                switches=0,
                deviations=0,
                load_cost=0.0,
            )
        if not segments:
            return None

        path_cache: dict[tuple[str, int], list[PathCandidate]] = {}
        frontier = [
            _PlanLabel(
                last_cross_link=initial_cross_link,
                remaining_data=remaining_data,
                tier_cost=0,
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
                    tier_cost=label.tier_cost,
                    idle_steps=label.idle_steps + 1,
                    switches=label.switches,
                    deviations=label.deviations + self._deviation_penalty(preferred_cross_links.get(segment.index), None),
                    load_cost=label.load_cost,
                    finish_time=label.finish_time,
                    actions=label.actions + (_PlannedAction(segment_index=segment.index, path=None, rate=0.0, delivered=0.0, tier=0),),
                )
                partial.append(wait_label)
                self._insert_nondominated(buckets[None], wait_label, objective)

                for candidate in self._candidate_paths(plan, task, segment, path_cache):
                    state = capacity_states.get(segment.index)
                    if state is None or segment.duration <= EPS:
                        continue
                    capacity_option = self._path_rate_and_tier(task, candidate, state, label.remaining_data, segment.duration)
                    if capacity_option is None:
                        continue
                    rate, tier = capacity_option
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
                        tier_cost=label.tier_cost + tier,
                        idle_steps=label.idle_steps,
                        switches=switches,
                        deviations=deviations,
                        load_cost=load_cost,
                        finish_time=finish_time,
                        actions=label.actions + (_PlannedAction(segment_index=segment.index, path=candidate, rate=rate, delivered=delivered, tier=tier),),
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
                tier_cost=best.tier_cost,
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
            tier_cost=best.tier_cost,
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
            and lhs.tier_cost <= rhs.tier_cost
            and lhs.switches <= rhs.switches
            and lhs.deviations <= rhs.deviations
            and lhs.load_cost <= rhs.load_cost + self.numeric_tolerance
            and lhs.idle_steps <= rhs.idle_steps
        )
        strictly_better = (
            lhs_remaining < rhs_remaining - self.numeric_tolerance
            or lhs.tier_cost < rhs.tier_cost
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
            return (float(label.tier_cost), finish_time, float(label.switches), label.load_cost, float(label.idle_steps))
        if objective == "recovery":
            return (label.load_cost, float(label.switches), float(label.idle_steps), finish_time)
        return (float(label.deviations), label.load_cost, float(label.switches), finish_time, float(label.idle_steps))

    def _partial_key(self, label: _PlanLabel, objective: str) -> tuple[float, ...]:
        remaining = self._remaining_key(label.remaining_data)
        if objective == "baseline":
            return (remaining, label.load_cost, float(label.switches), float(label.idle_steps))
        if objective == "emergency":
            return (remaining, float(label.tier_cost), float(label.switches), label.load_cost, float(label.idle_steps))
        if objective == "recovery":
            return (remaining, label.load_cost, float(label.switches), float(label.idle_steps))
        return (remaining, float(label.deviations), label.load_cost, float(label.switches), float(label.idle_steps))

    def _build_capacity_state(self, plan: list[ScheduledWindow], segment: Segment) -> _CapacityState:
        capacity: dict[str, float] = {}
        total_free: dict[str, float] = {}
        regular_cross_free: dict[str, float] = {}
        used_regular_cross: dict[str, float] = {}
        used_emergency_cross: dict[str, float] = {}
        edge_kind: dict[str, str] = {}
        regular_cross_capacity = max((1.0 - self.scenario.stage1.rho) * self.scenario.capacities.cross, 0.0)

        for link in active_intra_links(self.scenario, "A", segment.start):
            capacity[link.link_id] = self.scenario.capacities.domain_a
            total_free[link.link_id] = self.scenario.capacities.domain_a
            edge_kind[link.link_id] = "A"
        for link in active_intra_links(self.scenario, "B", segment.start):
            capacity[link.link_id] = self.scenario.capacities.domain_b
            total_free[link.link_id] = self.scenario.capacities.domain_b
            edge_kind[link.link_id] = "B"
        for window in active_cross_links(plan, segment.start):
            capacity[window.window_id] = self.scenario.capacities.cross
            total_free[window.window_id] = self.scenario.capacities.cross
            regular_cross_free[window.window_id] = regular_cross_capacity
            used_regular_cross[window.window_id] = 0.0
            used_emergency_cross[window.window_id] = 0.0
            edge_kind[window.window_id] = "X"
        return _CapacityState(
            capacity=capacity,
            total_free=total_free,
            regular_cross_free=regular_cross_free,
            used_regular_cross=used_regular_cross,
            used_emergency_cross=used_emergency_cross,
            edge_kind=edge_kind,
        )

    def _free_capacity_states(
        self,
        segments: list[Segment],
        plan: list[ScheduledWindow],
        committed: dict[tuple[str, int], Allocation],
        exclude_tasks: set[str],
        exclude_task_types: set[str] | None = None,
    ) -> dict[int, _CapacityState]:
        states = {segment.index: self._build_capacity_state(plan, segment) for segment in segments}
        valid_indices = set(states)
        for (task_id, segment_index), alloc in committed.items():
            if (
                task_id in exclude_tasks
                or segment_index not in valid_indices
                or (exclude_task_types and alloc.task_type in exclude_task_types)
            ):
                continue
            self._apply_existing_allocation(states[segment_index], alloc)
        return states

    def _apply_existing_allocation(self, state: _CapacityState, alloc: Allocation) -> None:
        for edge_id in alloc.edge_ids:
            if edge_id in state.total_free:
                state.total_free[edge_id] = max(0.0, state.total_free[edge_id] - alloc.rate)
            if state.edge_kind.get(edge_id) != "X":
                continue
            if alloc.task_type == "reg" and edge_id in state.regular_cross_free:
                state.regular_cross_free[edge_id] = max(0.0, state.regular_cross_free[edge_id] - alloc.rate)
                state.used_regular_cross[edge_id] = float(state.used_regular_cross.get(edge_id, 0.0)) + float(alloc.rate)
            elif alloc.task_type == "emg":
                state.used_emergency_cross[edge_id] = float(state.used_emergency_cross.get(edge_id, 0.0)) + float(alloc.rate)

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
            if edge_id in state.total_free:
                state.total_free[edge_id] = max(0.0, state.total_free[edge_id] - action.rate)
            if state.edge_kind.get(edge_id) != "X":
                continue
            if task_type == "reg" and edge_id in state.regular_cross_free:
                state.regular_cross_free[edge_id] = max(0.0, state.regular_cross_free[edge_id] - action.rate)
                state.used_regular_cross[edge_id] = float(state.used_regular_cross.get(edge_id, 0.0)) + float(action.rate)
            elif task_type == "emg":
                state.used_emergency_cross[edge_id] = float(state.used_emergency_cross.get(edge_id, 0.0)) + float(action.rate)

    def _apply_actions_to_capacity(
        self,
        capacity_states: dict[int, _CapacityState],
        actions: tuple[_PlannedAction, ...],
        task_type: str,
    ) -> None:
        for action in actions:
            self._apply_action_to_capacity(capacity_states, action, task_type)

    def _path_bottleneck_bounds(self, task: Task, path: PathCandidate, state: _CapacityState) -> tuple[float, float]:
        if not path.edge_ids:
            return float("inf"), float("inf")
        total_bottleneck = float("inf")
        reserved_bottleneck = float("inf")
        for edge_id in path.edge_ids:
            total_available = state.total_free.get(edge_id)
            if total_available is None:
                return 0.0, 0.0
            if task.task_type == "reg" and state.edge_kind.get(edge_id) == "X":
                total_available = min(total_available, state.regular_cross_free.get(edge_id, 0.0))
            total_bottleneck = min(total_bottleneck, total_available)

            reserved_available = total_available
            if task.task_type == "emg" and state.edge_kind.get(edge_id) == "X":
                reserved_available = min(total_available, self._reserve_free(edge_id, state))
            reserved_bottleneck = min(reserved_bottleneck, reserved_available)
        return total_bottleneck, reserved_bottleneck

    def _path_rate_and_tier(
        self,
        task: Task,
        path: PathCandidate,
        state: _CapacityState,
        remaining_data: float,
        segment_duration: float,
    ) -> tuple[float, int] | None:
        total_bottleneck, reserved_bottleneck = self._path_bottleneck_bounds(task, path, state)
        if total_bottleneck <= self.numeric_tolerance or segment_duration <= self.numeric_tolerance:
            return None
        rate = min(float(task.max_rate), float(total_bottleneck), max(float(remaining_data), 0.0) / segment_duration)
        if rate <= self.numeric_tolerance:
            return None
        tier = 0
        if task.task_type == "emg" and rate > reserved_bottleneck + self.numeric_tolerance:
            tier = 1
        return rate, tier

    def _reserve_free(self, edge_id: str, state: _CapacityState) -> float:
        capacity = float(state.capacity.get(edge_id, 0.0))
        if capacity <= self.numeric_tolerance:
            return 0.0
        used_emergency = float(state.used_emergency_cross.get(edge_id, 0.0))
        return max(float(self.scenario.stage1.rho) * capacity - used_emergency, 0.0)

    def _path_load_cost(
        self,
        segment_index: int,
        path: PathCandidate,
        rate: float,
        capacity_states: dict[int, _CapacityState],
    ) -> float:
        state = capacity_states.get(segment_index)
        if state is None:
            return 0.0
        return post_allocation_max_utilization(path.edge_ids, state.capacity, state.total_free, rate)

    def _deviation_penalty(self, preferred: str | None, selected: str | None) -> int:
        if preferred == selected:
            return 0
        if preferred is None and selected is None:
            return 0
        return 1

    @staticmethod
    def _cross_usage_from_allocations(allocations: list[Allocation], task_type: str | None = None) -> dict[int, dict[str, float]]:
        usage: dict[int, dict[str, float]] = defaultdict(lambda: defaultdict(float))
        for alloc in allocations:
            if task_type is not None and alloc.task_type != task_type:
                continue
            if alloc.cross_window_id is None:
                continue
            usage[int(alloc.segment_index)][str(alloc.cross_window_id)] += float(alloc.rate)
        return {
            int(segment_index): {window_id: float(value) for window_id, value in per_window.items()}
            for segment_index, per_window in usage.items()
        }

    @staticmethod
    def _cross_usage_delta(
        before: dict[int, dict[str, float]],
        after: dict[int, dict[str, float]],
    ) -> dict[int, dict[str, float]]:
        segment_indices = sorted(set(before).union(after))
        delta: dict[int, dict[str, float]] = {}
        for segment_index in segment_indices:
            window_ids = sorted(set(before.get(segment_index, {})).union(after.get(segment_index, {})))
            segment_delta = {
                window_id: float(after.get(segment_index, {}).get(window_id, 0.0) - before.get(segment_index, {}).get(window_id, 0.0))
                for window_id in window_ids
                if abs(float(after.get(segment_index, {}).get(window_id, 0.0) - before.get(segment_index, {}).get(window_id, 0.0))) > EPS
            }
            if segment_delta:
                delta[segment_index] = segment_delta
        return delta

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
                    cross_window_id=action.path.cross_window_id,
                )

    def _candidate_corridor(
        self,
        plan: list[ScheduledWindow],
        emergency: Task,
        horizon: list[Segment],
        committed: dict[tuple[str, int], Allocation],
    ) -> dict[int, set[str]]:
        corridor: dict[int, set[str]] = {}
        path_cache: dict[tuple[str, int], list[PathCandidate]] = {}
        capacity_states = self._free_capacity_states(
            horizon,
            plan,
            committed,
            exclude_tasks=set(),
            exclude_task_types={"reg"},
        )
        for segment in horizon:
            edges: set[str] = set()
            state = capacity_states.get(segment.index)
            for path in self._near_optimal_candidate_paths(plan, emergency, segment, state, path_cache):
                edges.update(path.edge_ids)
            corridor[segment.index] = edges
        return corridor

    def _near_optimal_candidate_paths(
        self,
        plan: list[ScheduledWindow],
        task: Task,
        segment: Segment,
        state: _CapacityState | None,
        path_cache: dict[tuple[str, int], list[PathCandidate]],
    ) -> list[PathCandidate]:
        if state is None or segment.duration <= self.numeric_tolerance:
            return []
        scored: list[tuple[float, float, int, str, PathCandidate]] = []
        for candidate in self._candidate_paths(plan, task, segment, path_cache):
            capacity_option = self._path_rate_and_tier(task, candidate, state, task.data, segment.duration)
            if capacity_option is None:
                continue
            rate, tier = capacity_option
            delivered = rate * segment.duration
            if delivered <= self.numeric_tolerance:
                continue
            scored.append((delivered, float(candidate.delay), tier, str(candidate.path_id), candidate))
        if not scored:
            return []
        max_delivered = max(item[0] for item in scored)
        near_threshold = float(self.scenario.stage1.eta_x) * max_delivered
        near = [item for item in scored if item[0] + self.numeric_tolerance >= near_threshold]
        near.sort(key=lambda item: (item[2], item[1], int(item[4].hop_count), item[3]))
        return [item[4] for item in near]

    def _affected_regular_tasks(
        self,
        emergency: Task,
        horizon: list[Segment],
        committed: dict[tuple[str, int], Allocation],
        actual_remaining: dict[str, float],
        corridor: dict[int, set[str]],
        task_runtime_state: dict[str, str],
        task_preemption_count: dict[str, int],
    ) -> list[Task]:
        if not horizon:
            return []
        affected: list[tuple[int, Task]] = []
        for task in self.scenario.tasks:
            if task.task_type != "reg":
                continue
            if not self._is_lower_priority_regular(task, emergency):
                continue
            if not (task.arrival < emergency.deadline and task.deadline > emergency.arrival):
                continue
            if task_runtime_state.get(task.task_id) == TASK_RUNTIME_PREEMPTED_RECOVERABLE:
                continue
            if int(task_preemption_count.get(task.task_id, 0)) > 0:
                continue
            if self._is_task_complete(task, actual_remaining.get(task.task_id, 0.0)):
                continue
            overlap = self._task_overlap(task.task_id, committed, corridor)
            if overlap <= 0:
                continue
            affected.append((overlap, task))
        affected.sort(key=lambda item: (-item[0], float(item[1].weight), item[1].deadline, item[1].task_id))
        return [task for _, task in affected]

    def _rank_preemption_candidates(
        self,
        emergency: Task,
        affected: list[Task],
        plan: list[ScheduledWindow],
        committed: dict[tuple[str, int], Allocation],
        actual_remaining: dict[str, float],
        segments: list[Segment],
        horizon: list[Segment],
        corridor: dict[int, set[str]],
    ) -> list[_PreemptionCandidate]:
        ranked: list[_PreemptionCandidate] = []
        for task in affected:
            released_segments = tuple(sorted(self._conflict_segments(task.task_id, committed, corridor)))
            if not released_segments:
                continue
            released_edge_ids, released_cross_window_ids = self._released_allocation_details(
                task.task_id,
                committed,
                set(released_segments),
            )
            score = self._preemption_loss_score(
                task=task,
                emergency=emergency,
                plan=plan,
                segments=segments,
                committed=committed,
                actual_remaining=actual_remaining,
                corridor=corridor,
                horizon=horizon,
            )
            ranked.append(
                _PreemptionCandidate(
                    task=task,
                    score=score,
                    released_segments=released_segments,
                    released_edge_ids=released_edge_ids,
                    released_cross_window_ids=released_cross_window_ids,
                )
            )

        ranked.sort(
            key=lambda candidate: (
                float(candidate.score),
                float(candidate.task.weight),
                -self._task_overlap(candidate.task.task_id, committed, corridor),
                candidate.task.task_id,
            )
        )
        return ranked

    def _preemption_loss_score(
        self,
        task: Task,
        emergency: Task,
        plan: list[ScheduledWindow],
        segments: list[Segment],
        committed: dict[tuple[str, int], Allocation],
        actual_remaining: dict[str, float],
        corridor: dict[int, set[str]],
        horizon: list[Segment],
    ) -> float:
        regular_weights = [float(item.weight) for item in self.scenario.tasks if item.task_type == "reg"]
        max_regular_weight = max(regular_weights, default=1.0)
        normalized_weight = clamp01(float(task.weight) / max(max_regular_weight, self.numeric_tolerance))
        released_segments = self._conflict_segments(task.task_id, committed, corridor)
        tentative_committed = {
            key: alloc
            for key, alloc in committed.items()
            if not (key[0] == task.task_id and key[1] in released_segments)
        }
        original_finish_time = self._original_committed_finish_time(task.task_id, committed, segments)
        recovery_start_time = original_finish_time
        recovery_window = max(float(task.deadline) - float(task.arrival), self.numeric_tolerance)
        recovery_slack = 0.0 if recovery_start_time is None else max(float(task.deadline) - float(recovery_start_time), 0.0)
        normalized_recovery_slack = clamp01(recovery_slack / recovery_window)
        normalized_recoverability = self._estimate_recoverability(
            task=task,
            recovery_start_time=recovery_start_time,
            committed=tentative_committed,
            actual_remaining=actual_remaining,
            plan=plan,
            segments=segments,
        )
        normalized_useful_release = self._normalized_useful_release(
            task_id=task.task_id,
            emergency=emergency,
            committed=committed,
            corridor=corridor,
            horizon=horizon,
        )
        loss = (
            PREEMPTION_WEIGHT_COEFF * normalized_weight
            + PREEMPTION_RECOVERY_SLACK_COEFF * (1.0 - normalized_recovery_slack)
            + PREEMPTION_RECOVERABILITY_COEFF * (1.0 - normalized_recoverability)
        )
        return loss / (PREEMPTION_SCORE_EPS + normalized_useful_release)

    def _normalized_useful_release(
        self,
        task_id: str,
        emergency: Task,
        committed: dict[tuple[str, int], Allocation],
        corridor: dict[int, set[str]],
        horizon: list[Segment],
    ) -> float:
        useful_release = 0.0
        release_reference = min(
            float(emergency.data),
            float(emergency.max_rate) * sum(max(float(segment.duration), 0.0) for segment in horizon),
        )
        if release_reference <= self.numeric_tolerance:
            return 0.0
        remaining_need = release_reference
        for segment in horizon:
            if remaining_need <= self.numeric_tolerance or segment.duration <= self.numeric_tolerance:
                continue
            segment_need_rate = min(float(emergency.max_rate), remaining_need / float(segment.duration))
            segment_release_rate = 0.0
            for (alloc_task_id, segment_index), alloc in committed.items():
                if alloc_task_id != task_id or segment_index != segment.index:
                    continue
                if not set(alloc.edge_ids).intersection(corridor.get(segment.index, set())):
                    continue
                segment_release_rate = max(segment_release_rate, min(float(alloc.rate), segment_need_rate))
            useful_release += segment_release_rate * float(segment.duration)
            remaining_need = max(0.0, remaining_need - segment_need_rate * float(segment.duration))
        return clamp01(useful_release / release_reference)

    def _released_allocation_details(
        self,
        task_id: str,
        committed: dict[tuple[str, int], Allocation],
        released_segments: set[int],
    ) -> tuple[tuple[str, ...], tuple[str, ...]]:
        edge_ids: set[str] = set()
        cross_window_ids: set[str] = set()
        for (alloc_task_id, segment_index), alloc in committed.items():
            if alloc_task_id != task_id or segment_index not in released_segments:
                continue
            edge_ids.update(str(edge_id) for edge_id in alloc.edge_ids)
            if alloc.cross_window_id is not None:
                cross_window_ids.add(str(alloc.cross_window_id))
        return tuple(sorted(edge_ids)), tuple(sorted(cross_window_ids))

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

    def _lowest_regular_weight(self) -> float:
        weights = [float(task.weight) for task in self.scenario.tasks if task.task_type == "reg"]
        return min(weights, default=0.0)

    def _is_lower_priority_regular(self, regular: Task, emergency: Task) -> bool:
        regular_priority = (float(regular.preemption_priority), float(regular.weight))
        emergency_priority = (float(emergency.preemption_priority), float(emergency.weight))
        return regular_priority < emergency_priority

    def _emergency_horizon(self, segments: list[Segment], current_index: int, task: Task) -> list[Segment]:
        return [segment for segment in segments[current_index:] if task.arrival <= segment.start < task.deadline]

    def _weighted_true_completion(self, task_type: str, remaining: dict[str, float]) -> float:
        tasks = [task for task in self.scenario.tasks if task.task_type == task_type]
        if not tasks:
            return 1.0
        total_weight = sum(task.weight for task in tasks)
        if total_weight <= self.numeric_tolerance:
            return 1.0
        return sum(task.weight * float(self._is_task_complete(task, remaining.get(task.task_id, 0.0))) for task in tasks) / total_weight

    def _cross_link_from_edges(self, edge_ids: tuple[str, ...], cross_edge_ids: set[str]) -> str | None:
        effective_cross_edge_ids = cross_edge_ids or {window.window_id for window in self.scenario.candidate_windows}
        return cross_link_from_edges(edge_ids, effective_cross_edge_ids)


def run_stage2_two_phase_event_insert(
    scenario: Scenario,
    plan: list[ScheduledWindow],
    baseline_trace: Stage1BaselineTrace | None = None,
) -> Stage2Result:
    return TwoPhaseEventDrivenScheduler(scenario).run(plan, baseline_trace=baseline_trace)

