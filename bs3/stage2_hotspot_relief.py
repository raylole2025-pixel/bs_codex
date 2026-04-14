from __future__ import annotations

"""Legacy stage2-1 hotspot relief logic.

This module is retained only for historical reference. The default stage2 flow no
longer calls it; stage1 now owns regular baseline generation and stage2 only
handles emergency insertion on top of the exported baseline state.
"""

from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, replace
from statistics import median
import time
from typing import Any

from .models import Allocation, CandidateWindow, ScheduledWindow, Scenario, Segment
from .regular_routing_common import build_regular_schedule_diagnostics, completion_tolerance, is_task_complete
from .scenario import build_segments, generate_candidate_paths
from .stage2_regular_joint_milp import solve_regular_hotspot_local_milp

EPS = 1e-9
_AUGMENT_STAGE_ORDER = {
    "raw_overlap": 0,
    "schedulable": 1,
    "conflict_free": 2,
    "relief_path_ready": 3,
    "shortlisted": 4,
    "selected": 5,
    "applied": 6,
}
_AUGMENT_SELECTION_POLICIES = {"global_score_only", "structural_coverage_first"}
_CLOSED_LOOP_ACTION_MODES = {"reroute_then_augment", "best_global_action"}
_AUGMENT_FUNNEL_STAGE_MEANINGS = {
    "raw_overlap_candidate_count": "Physically overlaps the hot range before any insertion checks.",
    "schedulable_after_t_pre_d_min_count": "Still satisfies the t_pre / d_min timing gate before terminal-conflict rejection.",
    "conflict_free_count": "Can be inserted into the current plan without terminal occupancy conflict.",
    "relief_path_ready_count": "Conflict-free and can generate at least one feasible relief path.",
    "shortlisted_count": "Kept after per-hotspot candidate ranking/truncation.",
    "selected_count": "Chosen by the global augment selection policy within the total budget.",
    "applied_count": "Actually inserted into the plan after the final scheduling pass.",
}


@dataclass(frozen=True)
class CrossSegmentProfileRow:
    segment_index: int
    start: float
    end: float
    duration: float
    q_r: float
    active_selected_cross_count: int
    per_window_util: dict[str, float]
    active_window_ids: tuple[str, ...]
    selected_task_ids: tuple[str, ...]


@dataclass(frozen=True)
class HotRange:
    range_id: str
    start_segment_index: int
    end_segment_index: int
    start: float
    end: float
    max_q_r: float
    q_integral: float
    segment_indices: tuple[int, ...]


@dataclass(frozen=True)
class HotRangeClassification:
    hot_duration: float
    single_link_fraction: float
    median_active_selected: float
    active_selected_count_distribution: tuple[tuple[int, int], ...]
    feasible_alternative_cross_window_count: int
    feasible_alternative_path_count: int
    top_contributing_windows: tuple[str, ...]
    structural: bool
    reroutable: bool
    primary_class: str
    reason: str
    warnings: tuple[str, ...]


@dataclass(frozen=True)
class HotTaskContribution:
    task_id: str
    delivered: float
    weighted_load: float
    segment_indices: tuple[int, ...]
    cross_window_ids: tuple[str, ...]


@dataclass(frozen=True)
class AugmentCandidate:
    range_id: str
    window_id: str
    structural_priority: int
    scheduled_on: float
    scheduled_off: float
    scheduled_duration: float
    overlap_duration: float
    estimated_divertable_rate: float
    feasible_path_count: int
    delay_penalty: float
    switch_penalty: float
    relief_score: float
    contributing_task_ids: tuple[str, ...]


@dataclass(frozen=True)
class HotspotReliefResult:
    plan: list[ScheduledWindow]
    schedule: dict[tuple[str, int], Allocation]
    segments: list[Segment]
    diagnostics: dict[str, Any]
    metadata: dict[str, Any]
    report: dict[str, Any]


def _scheduled_window_from_candidate(window: CandidateWindow) -> ScheduledWindow:
    return ScheduledWindow(
        window_id=window.window_id,
        a=window.a,
        b=window.b,
        start=window.start,
        end=window.end,
        on=window.start,
        off=window.end,
        value=window.value,
        delay=window.delay,
        distance_km=window.distance_km,
    )


def _window_overlap_duration(
    start_a: float,
    end_a: float,
    start_b: float,
    end_b: float,
) -> float:
    return max(min(float(end_a), float(end_b)) - max(float(start_a), float(start_b)), 0.0)


def _active_cross_windows(plan: list[ScheduledWindow], time_point: float) -> list[ScheduledWindow]:
    return [window for window in plan if float(window.on) <= float(time_point) < float(window.off)]


def _allocation_cross_windows(allocation: Allocation, cross_window_ids: set[str]) -> list[str]:
    return [edge_id for edge_id in allocation.edge_ids if edge_id in cross_window_ids]


def _regular_tasks(scenario: Scenario):
    return [task for task in scenario.tasks if task.task_type == "reg"]


def _augment_candidate_debug_record(window: CandidateWindow) -> dict[str, Any]:
    return {
        "window_id": window.window_id,
        "overlap_duration": 0.0,
        "scheduled_on": None,
        "scheduled_off": None,
        "estimated_divertable_rate": 0.0,
        "feasible_path_count": 0,
        "delay_penalty": 0.0,
        "switch_penalty": 0.0,
        "relief_score": 0.0,
        "final_stage_reached": "raw_overlap",
        "rejection_reason": "other",
    }


def _promote_augment_stage(record: dict[str, Any], stage: str) -> None:
    current = str(record.get("final_stage_reached") or "raw_overlap")
    if _AUGMENT_STAGE_ORDER.get(stage, -1) >= _AUGMENT_STAGE_ORDER.get(current, -1):
        record["final_stage_reached"] = stage


def _augment_selection_policy(scenario: Scenario) -> str:
    raw = str(getattr(scenario.stage2, "augment_selection_policy", "global_score_only") or "global_score_only").strip().lower()
    return raw if raw in _AUGMENT_SELECTION_POLICIES else "global_score_only"


def _closed_loop_action_mode(scenario: Scenario) -> str:
    raw = str(getattr(scenario.stage2, "closed_loop_action_mode", "best_global_action") or "best_global_action").strip().lower()
    return raw if raw in _CLOSED_LOOP_ACTION_MODES else "best_global_action"


def _closed_loop_topk_ranges_per_round(scenario: Scenario) -> int:
    configured = getattr(scenario.stage2, "closed_loop_topk_ranges_per_round", scenario.stage2.hotspot_topk_ranges)
    return max(int(configured), 0)


def _closed_loop_topk_candidates_per_range(scenario: Scenario) -> int:
    configured = getattr(
        scenario.stage2,
        "closed_loop_topk_candidates_per_range",
        scenario.stage2.augment_top_windows_per_range,
    )
    return max(int(configured), 0)


def _closed_loop_hard_window_cap(scenario: Scenario) -> int:
    configured_cap = max(int(getattr(scenario.stage2, "closed_loop_max_new_windows", 0)), 0)
    budget_cap = max(int(getattr(scenario.stage2, "augment_window_budget", 0)), 0)
    return min(configured_cap, budget_cap)


def _closed_loop_hard_window_cap_details(scenario: Scenario) -> dict[str, int | str]:
    configured_cap = max(int(getattr(scenario.stage2, "closed_loop_max_new_windows", 0)), 0)
    budget_cap = max(int(getattr(scenario.stage2, "augment_window_budget", 0)), 0)
    effective_cap = min(configured_cap, budget_cap)
    if configured_cap < budget_cap:
        limiter = "closed_loop_max_new_windows"
    elif budget_cap < configured_cap:
        limiter = "augment_window_budget"
    else:
        limiter = "both"
    return {
        "closed_loop_max_new_windows": int(configured_cap),
        "augment_window_budget": int(budget_cap),
        "effective_hard_cap": int(effective_cap),
        "effective_hard_cap_limiter": limiter,
    }


def _augment_candidate_sort_key(candidate: AugmentCandidate) -> tuple[float | int | str, ...]:
    return (
        -int(candidate.structural_priority),
        -float(candidate.relief_score),
        -float(candidate.estimated_divertable_rate),
        -int(candidate.feasible_path_count),
        float(candidate.delay_penalty),
        candidate.window_id,
    )


def _empty_augment_funnel_counts() -> dict[str, int]:
    return {
        "raw_overlap_candidate_count": 0,
        "schedulable_after_t_pre_d_min_count": 0,
        "conflict_free_count": 0,
        "relief_path_ready_count": 0,
        "shortlisted_count": 0,
        "selected_count": 0,
        "applied_count": 0,
    }


def _augment_rejection_breakdown(records: dict[str, dict[str, Any]]) -> dict[str, int]:
    counter: Counter[str] = Counter()
    for record in records.values():
        reason = str(record.get("rejection_reason") or "").strip()
        if reason:
            counter[reason] += 1
    return {reason: int(counter[reason]) for reason in sorted(counter)}


def _hotspot_severity_sort_key(
    hot_range: HotRange | None,
    classification: HotRangeClassification | None,
    range_id: str,
) -> tuple[float | str, ...]:
    return (
        -float(hot_range.max_q_r) if hot_range is not None else 0.0,
        -float(hot_range.q_integral) if hot_range is not None else 0.0,
        -float(classification.hot_duration) if classification is not None else 0.0,
        -float(classification.single_link_fraction) if classification is not None else 0.0,
        range_id,
    )


def _peak_like_threshold_from_peak(q_peak: float) -> float:
    if float(q_peak) <= EPS:
        return 0.995
    return max(0.995, float(q_peak) - 1e-4)


def _peak_like_threshold_from_profile(profile: list[CrossSegmentProfileRow]) -> float:
    q_peak = max((float(row.q_r) for row in profile), default=0.0)
    return _peak_like_threshold_from_peak(q_peak)


def _schedule_candidate_against_plan(
    plan: list[ScheduledWindow],
    window: CandidateWindow,
    *,
    t_pre: float,
    d_min: float,
) -> ScheduledWindow | None:
    result = _schedule_candidate_against_plan_detailed(plan, window, t_pre=t_pre, d_min=d_min)
    scheduled = result.get("scheduled_window")
    return scheduled if isinstance(scheduled, ScheduledWindow) else None


def _schedule_candidate_against_plan_detailed(
    plan: list[ScheduledWindow],
    window: CandidateWindow,
    *,
    t_pre: float,
    d_min: float,
) -> dict[str, Any]:
    occupied_by_node: dict[str, list[tuple[float, float]]] = defaultdict(list)
    for scheduled in sorted(plan, key=lambda item: (float(item.on), float(item.off), item.window_id)):
        occupied = (float(scheduled.on) - float(t_pre), float(scheduled.off))
        occupied_by_node[scheduled.a].append(occupied)
        occupied_by_node[scheduled.b].append(occupied)

    def latest_end_before(node_id: str) -> float:
        latest = -float(t_pre)
        for start, end in occupied_by_node.get(node_id, []):
            if float(start) < float(window.end) and float(end) > latest:
                latest = float(end)
        return latest

    if float(window.end) - float(window.start) + EPS < float(d_min):
        return {
            "scheduled_window": None,
            "pass_t_pre_and_d_min": False,
            "pass_conflict_check": False,
            "real_schedulable": False,
            "rejection_reason": "duration_below_d_min",
            "had_terminal_conflict": False,
            "scheduled_on": None,
            "scheduled_off": None,
        }

    t_on = max(float(window.start), latest_end_before(window.a) + float(t_pre), latest_end_before(window.b) + float(t_pre))
    had_terminal_conflict = float(t_on) > float(window.start) + EPS
    if float(window.end) - float(t_on) + EPS < float(d_min):
        return {
            "scheduled_window": None,
            "pass_t_pre_and_d_min": True,
            "pass_conflict_check": False,
            "real_schedulable": False,
            "rejection_reason": ("unschedulable_after_t_pre" if had_terminal_conflict else "duration_below_d_min"),
            "had_terminal_conflict": had_terminal_conflict,
            "scheduled_on": float(t_on),
            "scheduled_off": float(window.end),
        }
    scheduled_window = ScheduledWindow(
        window_id=window.window_id,
        a=window.a,
        b=window.b,
        start=float(window.start),
        end=float(window.end),
        on=float(t_on),
        off=float(window.end),
        value=window.value,
        delay=float(window.delay),
        distance_km=window.distance_km,
    )
    return {
        "scheduled_window": scheduled_window,
        "pass_t_pre_and_d_min": True,
        "pass_conflict_check": True,
        "real_schedulable": True,
        "rejection_reason": None,
        "had_terminal_conflict": had_terminal_conflict,
        "scheduled_on": float(scheduled_window.on),
        "scheduled_off": float(scheduled_window.off),
    }


def _candidate_paths_via_window(
    scenario: Scenario,
    task,
    segment: Segment,
    window: ScheduledWindow,
    limit: int,
):
    candidates = generate_candidate_paths(
        scenario,
        [window],
        task,
        segment,
        max(limit, 1),
        active_windows=[window],
    )
    return [candidate for candidate in candidates if candidate.cross_window_id == window.window_id]


def build_cross_segment_profile(
    scenario: Scenario,
    plan: list[ScheduledWindow],
    segments: list[Segment],
    schedule: dict[tuple[str, int], Allocation],
) -> list[CrossSegmentProfileRow]:
    reserved_cross_capacity = max((1.0 - float(scenario.stage1.rho)) * float(scenario.capacities.cross), 0.0)
    profile: list[CrossSegmentProfileRow] = []
    for segment in segments:
        active_windows = _active_cross_windows(plan, segment.start)
        active_window_ids = tuple(window.window_id for window in active_windows)
        per_window_rate = {window_id: 0.0 for window_id in active_window_ids}
        selected_task_ids: set[str] = set()
        for (task_id, segment_index), allocation in schedule.items():
            if segment_index != segment.index:
                continue
            for window_id in _allocation_cross_windows(allocation, set(active_window_ids)):
                per_window_rate[window_id] = per_window_rate.get(window_id, 0.0) + float(allocation.rate)
                selected_task_ids.add(task_id)
        if reserved_cross_capacity > EPS:
            per_window_util = {
                window_id: max(float(rate) / reserved_cross_capacity, 0.0)
                for window_id, rate in per_window_rate.items()
            }
        else:
            per_window_util = {window_id: 0.0 for window_id in per_window_rate}
        q_r = max(per_window_util.values(), default=0.0)
        profile.append(
            CrossSegmentProfileRow(
                segment_index=segment.index,
                start=float(segment.start),
                end=float(segment.end),
                duration=float(segment.duration),
                q_r=float(q_r),
                active_selected_cross_count=sum(1 for value in per_window_util.values() if value > EPS),
                per_window_util=per_window_util,
                active_window_ids=active_window_ids,
                selected_task_ids=tuple(sorted(selected_task_ids)),
            )
        )
    return profile


def detect_hot_ranges(
    profile: list[CrossSegmentProfileRow],
    threshold: float,
    topk: int,
) -> list[HotRange]:
    effective_threshold = max(float(threshold), _peak_like_threshold_from_profile(profile))
    hot_rows = [row for row in profile if float(row.q_r) + EPS >= effective_threshold]
    if not hot_rows or topk <= 0:
        return []

    merged_ranges: list[list[CrossSegmentProfileRow]] = []
    current: list[CrossSegmentProfileRow] = [hot_rows[0]]
    for row in hot_rows[1:]:
        if row.segment_index == current[-1].segment_index + 1:
            current.append(row)
            continue
        merged_ranges.append(current)
        current = [row]
    merged_ranges.append(current)

    results: list[HotRange] = []
    for index, rows in enumerate(merged_ranges, start=1):
        results.append(
            HotRange(
                range_id=f"hot_range_{index}",
                start_segment_index=rows[0].segment_index,
                end_segment_index=rows[-1].segment_index,
                start=float(rows[0].start),
                end=float(rows[-1].end),
                max_q_r=max(float(row.q_r) for row in rows),
                q_integral=sum(float(row.q_r) * float(row.duration) for row in rows),
                segment_indices=tuple(row.segment_index for row in rows),
            )
        )
    results.sort(key=lambda item: (-float(item.max_q_r), -float(item.q_integral), float(item.start), item.range_id))
    return results[:topk]


def classify_hot_range(
    profile: list[CrossSegmentProfileRow],
    hot_range: HotRange,
    single_link_fraction_threshold: float,
    *,
    feasible_alternative_cross_window_count: int = 0,
    feasible_alternative_path_count: int = 0,
) -> HotRangeClassification:
    segment_set = set(hot_range.segment_indices)
    rows = [row for row in profile if row.segment_index in segment_set]
    window_integral: dict[str, float] = defaultdict(float)
    active_counts: list[float] = []
    active_count_distribution: Counter[int] = Counter()
    single_link_segments = 0
    hot_duration = 0.0
    for row in rows:
        active_count = int(row.active_selected_cross_count)
        active_counts.append(float(active_count))
        active_count_distribution[active_count] += 1
        hot_duration += float(row.duration)
        if active_count <= 1:
            single_link_segments += 1
        for window_id, util in row.per_window_util.items():
            contribution = float(util) * float(row.duration)
            window_integral[window_id] += contribution
    single_link_fraction = (float(single_link_segments) / float(len(rows))) if rows else 0.0
    median_active_selected = float(median(active_counts)) if active_counts else 0.0
    structural = (
        single_link_fraction + EPS >= 0.9
        or (
            single_link_fraction + EPS >= float(single_link_fraction_threshold)
            and median_active_selected <= 1.0 + EPS
        )
    )
    reroutable = (
        any(active_count >= 2 for active_count in active_count_distribution)
        or int(feasible_alternative_cross_window_count) > 0
        or int(feasible_alternative_path_count) > 0
    )
    warnings: list[str] = []
    if (
        single_link_fraction + EPS >= 0.9
        and median_active_selected <= 1.0 + EPS
        and not structural
    ):
        warnings.append(
            "sanity_warning: single_link_fraction>=0.9 and median_active_selected<=1 but structural=false"
        )
    if structural:
        if single_link_fraction + EPS >= 0.9:
            reason = "single_link_fraction>=0.9"
        else:
            reason = (
                f"single_link_fraction>={float(single_link_fraction_threshold):.3f} "
                "and median_active_selected<=1"
            )
        primary_class = "structural"
    elif reroutable:
        reason = "multiple selected windows or feasible alternative paths exist"
        primary_class = "reroutable"
    else:
        reason = "no multi-selected segment and no feasible alternative cross-window/path candidates"
        primary_class = "blocked"
    top_contributing_windows = tuple(
        window_id
        for window_id, _ in sorted(window_integral.items(), key=lambda item: (-float(item[1]), item[0]))[:3]
    )
    return HotRangeClassification(
        hot_duration=float(hot_duration),
        single_link_fraction=float(single_link_fraction),
        median_active_selected=float(median_active_selected),
        active_selected_count_distribution=tuple(sorted((int(key), int(value)) for key, value in active_count_distribution.items())),
        feasible_alternative_cross_window_count=max(int(feasible_alternative_cross_window_count), 0),
        feasible_alternative_path_count=max(int(feasible_alternative_path_count), 0),
        top_contributing_windows=top_contributing_windows,
        structural=bool(structural),
        reroutable=bool(reroutable),
        primary_class=primary_class,
        reason=reason,
        warnings=tuple(warnings),
    )


def collect_hot_contributing_tasks(
    scenario: Scenario,
    plan: list[ScheduledWindow],
    segments: list[Segment],
    schedule: dict[tuple[str, int], Allocation],
    hot_range: HotRange,
    limit: int,
) -> list[HotTaskContribution]:
    if limit <= 0:
        return []
    cross_window_ids = {window.window_id for window in plan}
    regular_tasks = {task.task_id: task for task in _regular_tasks(scenario)}
    delivered_by_task: dict[str, float] = defaultdict(float)
    weighted_by_task: dict[str, float] = defaultdict(float)
    segments_by_task: dict[str, set[int]] = defaultdict(set)
    windows_by_task: dict[str, set[str]] = defaultdict(set)
    segment_lookup = {segment.index: segment for segment in segments}
    for (task_id, segment_index), allocation in schedule.items():
        if task_id not in regular_tasks or segment_index not in segment_lookup:
            continue
        if segment_index not in set(hot_range.segment_indices):
            continue
        cross_windows = _allocation_cross_windows(allocation, cross_window_ids)
        if not cross_windows:
            continue
        delivered = float(allocation.delivered)
        delivered_by_task[task_id] += delivered
        weighted_by_task[task_id] += delivered * float(regular_tasks[task_id].weight)
        segments_by_task[task_id].add(segment_index)
        windows_by_task[task_id].update(cross_windows)
    ranked = sorted(
        delivered_by_task,
        key=lambda task_id: (-weighted_by_task[task_id], -delivered_by_task[task_id], task_id),
    )
    return [
        HotTaskContribution(
            task_id=task_id,
            delivered=float(delivered_by_task[task_id]),
            weighted_load=float(weighted_by_task[task_id]),
            segment_indices=tuple(sorted(segments_by_task[task_id])),
            cross_window_ids=tuple(sorted(windows_by_task[task_id])),
        )
        for task_id in ranked[:limit]
    ]


def _regular_completion_ratio_from_diagnostics(scenario: Scenario, diagnostics: dict[str, Any]) -> float:
    regular_tasks = _regular_tasks(scenario)
    if not regular_tasks:
        return 1.0
    total_weight = sum(float(task.weight) for task in regular_tasks)
    if total_weight <= EPS:
        return 1.0
    completed = diagnostics.get("completed", {})
    return sum(float(task.weight) * float(bool(completed.get(task.task_id, False))) for task in regular_tasks) / total_weight


def _load_summary_from_profile(profile: list[CrossSegmentProfileRow]) -> dict[str, float | int]:
    q_peak = max((float(row.q_r) for row in profile), default=0.0)
    q_integral = sum(float(row.q_r) * float(row.duration) for row in profile)
    peak_like_threshold = _peak_like_threshold_from_peak(q_peak)
    peak_segment_count = sum(1 for row in profile if float(row.q_r) + EPS >= peak_like_threshold)
    return {
        "q_peak": float(q_peak),
        "q_integral": float(q_integral),
        "peak_like_threshold": float(peak_like_threshold),
        "peak_segment_count": int(peak_segment_count),
    }


def _high_segment_count(profile: list[CrossSegmentProfileRow], threshold: float) -> int:
    effective_threshold = max(float(threshold), 0.0)
    return sum(1 for row in profile if float(row.q_r) + EPS >= effective_threshold)


def _interval_profile_summary(
    profile: list[CrossSegmentProfileRow],
    *,
    start: float,
    end: float,
) -> dict[str, float | int]:
    q_peak = 0.0
    q_integral = 0.0
    covered = 0
    for row in profile:
        overlap = _window_overlap_duration(row.start, row.end, start, end)
        if overlap <= EPS:
            continue
        q_peak = max(q_peak, float(row.q_r))
        q_integral += float(row.q_r) * float(overlap)
        covered += 1
    return {
        "q_peak": float(q_peak),
        "q_integral": float(q_integral),
        "segment_count": int(covered),
    }


def _closed_loop_metrics(
    scenario: Scenario,
    profile: list[CrossSegmentProfileRow],
) -> dict[str, float | int]:
    summary = dict(_load_summary_from_profile(profile))
    summary["high_segment_count"] = _high_segment_count(
        profile,
        threshold=float(scenario.stage2.hotspot_util_threshold),
    )
    return summary


def _closed_loop_delta(before: dict[str, float | int], after: dict[str, float | int]) -> dict[str, float | int]:
    return {
        "delta_q_peak": float(before.get("q_peak", 0.0)) - float(after.get("q_peak", 0.0)),
        "delta_q_integral": float(before.get("q_integral", 0.0)) - float(after.get("q_integral", 0.0)),
        "delta_peak_segment_count": int(before.get("peak_segment_count", 0)) - int(after.get("peak_segment_count", 0)),
        "delta_high_segment_count": int(before.get("high_segment_count", 0)) - int(after.get("high_segment_count", 0)),
    }


def _closed_loop_accepts(
    scenario: Scenario,
    *,
    cr_reg_base: float,
    cr_reg_new: float,
    before_metrics: dict[str, float | int],
    after_metrics: dict[str, float | int],
    before_focus: dict[str, float | int],
    after_focus: dict[str, float | int],
) -> tuple[bool, dict[str, float | int], str | None]:
    delta = _closed_loop_delta(before_metrics, after_metrics)
    delta["delta_focus_q_peak"] = float(before_focus.get("q_peak", 0.0)) - float(after_focus.get("q_peak", 0.0))
    delta["delta_focus_q_integral"] = float(before_focus.get("q_integral", 0.0)) - float(after_focus.get("q_integral", 0.0))
    if float(cr_reg_new) + EPS < float(cr_reg_base):
        return False, delta, "regular_completion_dropped"
    improves_q_peak = float(delta["delta_q_peak"]) + EPS >= float(getattr(scenario.stage2, "closed_loop_min_delta_q_peak", 0.0))
    improves_q_integral = float(delta["delta_q_integral"]) + EPS >= float(
        getattr(scenario.stage2, "closed_loop_min_delta_q_integral", 0.0)
    )
    improves_high_segments = (
        int(delta["delta_high_segment_count"]) >= int(getattr(scenario.stage2, "closed_loop_min_delta_high_segments", 0))
        or int(delta["delta_peak_segment_count"]) >= int(getattr(scenario.stage2, "closed_loop_min_delta_high_segments", 0))
    )
    improves_focus = float(delta["delta_focus_q_peak"]) > EPS or float(delta["delta_focus_q_integral"]) > EPS
    if improves_q_peak or improves_q_integral or improves_high_segments or improves_focus:
        return True, delta, None
    return False, delta, "no_material_load_improvement"


def _relief_accepts(
    *,
    cr_reg_base: float,
    cr_reg_new: float,
    before_summary: dict[str, float | int],
    after_summary: dict[str, float | int],
    epsilon: float,
) -> bool:
    if abs(float(cr_reg_new) - float(cr_reg_base)) > float(epsilon):
        return False
    if float(after_summary["q_peak"]) < float(before_summary["q_peak"]) - epsilon:
        return True
    if int(after_summary["peak_segment_count"]) < int(before_summary["peak_segment_count"]):
        return True
    if float(after_summary["q_integral"]) < float(before_summary["q_integral"]) - epsilon:
        return True
    return False


def _structural_gate_accepts(
    *,
    cr_reg_base: float,
    cr_reg_new: float,
    before_summary: dict[str, float | int],
    after_summary: dict[str, float | int],
    epsilon: float,
) -> bool:
    if float(cr_reg_new) + float(epsilon) < float(cr_reg_base):
        return False
    if int(after_summary["peak_segment_count"]) < int(before_summary["peak_segment_count"]):
        return True
    if float(after_summary["q_integral"]) < float(before_summary["q_integral"]) - epsilon:
        return True
    if float(after_summary["q_peak"]) < float(before_summary["q_peak"]) - epsilon:
        return True
    return False


def _remap_schedule_to_segments(
    old_segments: list[Segment],
    new_segments: list[Segment],
    schedule: dict[tuple[str, int], Allocation],
) -> dict[tuple[str, int], Allocation]:
    old_lookup = {segment.index: segment for segment in old_segments}
    new_schedule: dict[tuple[str, int], Allocation] = {}
    for (task_id, segment_index), allocation in schedule.items():
        old_segment = old_lookup.get(segment_index)
        if old_segment is None or float(allocation.rate) <= EPS or float(allocation.delivered) <= EPS:
            continue
        usable_duration = min(float(allocation.delivered) / max(float(allocation.rate), EPS), float(old_segment.duration))
        usable_end = float(old_segment.start) + usable_duration
        for new_segment in new_segments:
            if float(new_segment.end) <= float(old_segment.start) + EPS:
                continue
            if float(new_segment.start) >= usable_end - EPS:
                break
            if float(new_segment.start) + EPS < float(old_segment.start) or float(new_segment.end) > float(old_segment.end) + EPS:
                continue
            overlap = _window_overlap_duration(new_segment.start, new_segment.end, old_segment.start, usable_end)
            if overlap <= EPS:
                continue
            delivered_piece = min(float(allocation.rate) * overlap, float(allocation.delivered))
            if delivered_piece <= EPS:
                continue
            new_schedule[(task_id, new_segment.index)] = Allocation(
                task_id=allocation.task_id,
                segment_index=new_segment.index,
                path_id=f"{allocation.path_id}:split:{new_segment.index}",
                edge_ids=allocation.edge_ids,
                rate=float(allocation.rate),
                delivered=float(delivered_piece),
                task_type=allocation.task_type,
                is_preempted=allocation.is_preempted,
            )
    return new_schedule


def _candidate_path_exists_via_window(
    scenario: Scenario,
    task,
    segment: Segment,
    window: CandidateWindow,
    limit: int,
) -> bool:
    scheduled = _scheduled_window_from_candidate(window)
    candidates = generate_candidate_paths(
        scenario,
        [scheduled],
        task,
        segment,
        max(limit, 1),
        active_windows=[scheduled],
    )
    return any(candidate.cross_window_id == window.window_id for candidate in candidates)


def _augment_mode(scenario: Scenario) -> str:
    raw_mode = str(scenario.metadata.get("hotspot_augment_mode", "augment_only")).strip().lower()
    return raw_mode if raw_mode in {"augment_only", "swap_if_budgeted"} else "augment_only"


def _structural_repair_gate_enabled(scenario: Scenario) -> bool:
    return bool(scenario.metadata.get("structural_repair_gate_enabled", False))


def _metadata_time_limit_seconds(scenario: Scenario, key: str) -> float | None:
    raw = scenario.metadata.get(key)
    if raw in {None, "", 0, 0.0}:
        return None
    return max(float(raw), 0.0)


def _structural_repair_gate_time_limit_seconds(scenario: Scenario) -> float | None:
    return _metadata_time_limit_seconds(scenario, "structural_repair_gate_time_limit_seconds")


def _hotspot_local_repair_time_limit_seconds(scenario: Scenario) -> float | None:
    return _metadata_time_limit_seconds(scenario, "hotspot_local_repair_time_limit_seconds")


def _hotspot_per_range_time_limit_seconds(scenario: Scenario) -> float | None:
    return _metadata_time_limit_seconds(scenario, "hotspot_per_range_time_limit_seconds")


def _hotspot_total_time_limit_seconds(scenario: Scenario) -> float | None:
    return _metadata_time_limit_seconds(scenario, "hotspot_total_time_limit_seconds")


def _remaining_budget_seconds(limit_seconds: float | None, consumed_seconds: float) -> float | None:
    if limit_seconds is None:
        return None
    return max(float(limit_seconds) - max(float(consumed_seconds), 0.0), 0.0)


def _total_elapsed_seconds(started_at: float) -> float:
    return max(time.perf_counter() - float(started_at), 0.0)


def _effective_solver_time_limit_seconds(
    scenario: Scenario,
    *caps: float | None,
) -> float | None:
    limits: list[float] = []
    base_limit = scenario.stage2.milp_time_limit_seconds
    if base_limit not in {None, "", 0, 0.0}:
        limits.append(max(float(base_limit), 0.0))
    for cap in caps:
        if cap not in {None, "", 0, 0.0}:
            limits.append(max(float(cap), 0.0))
    return min(limits) if limits else None


def _scenario_with_milp_time_limit(scenario: Scenario, time_limit_seconds: float | None) -> Scenario:
    if time_limit_seconds in {None, "", 0, 0.0}:
        return replace(
            scenario,
            stage2=replace(scenario.stage2, milp_time_limit_seconds=None),
        )
    return replace(
        scenario,
        stage2=replace(scenario.stage2, milp_time_limit_seconds=max(float(time_limit_seconds), 0.0)),
    )


def _bounded_skip_result(
    *,
    stage: str,
    total_remaining_seconds: float | None,
    range_remaining_seconds: float | None,
) -> dict[str, Any]:
    if total_remaining_seconds is not None and total_remaining_seconds <= EPS:
        return {
            "attempted": False,
            "accepted": False,
            "rejection_reason": f"{stage}_skipped_total_time_budget_exhausted",
            "local_before_after": None,
            "used_augment_windows": [],
            "detailed_runtime_failure_type": "failed_time_limit_without_incumbent",
            "candidate_solver_status": "SkippedTotalTimeBudget",
            "solver_error": None,
            "schedule": None,
            "objective_values": {},
        }
    if range_remaining_seconds is not None and range_remaining_seconds <= EPS:
        return {
            "attempted": False,
            "accepted": False,
            "rejection_reason": f"{stage}_skipped_range_time_budget_exhausted",
            "local_before_after": None,
            "used_augment_windows": [],
            "detailed_runtime_failure_type": "failed_time_limit_without_incumbent",
            "candidate_solver_status": "SkippedRangeTimeBudget",
            "solver_error": None,
            "schedule": None,
            "objective_values": {},
        }
    return {}


def _fixed_plan_window_count(scenario: Scenario) -> int | None:
    raw = scenario.metadata.get("hotspot_fixed_plan_window_count")
    if raw in {None, ""}:
        return None
    return max(int(raw), 0)


def _collect_augment_candidates(
    scenario: Scenario,
    plan: list[ScheduledWindow],
    segments: list[Segment],
    schedule: dict[tuple[str, int], Allocation],
    hot_ranges: list[HotRange],
    classifications: dict[str, HotRangeClassification],
    contributing_tasks: dict[str, list[HotTaskContribution]],
) -> tuple[list[AugmentCandidate], dict[str, dict[str, Any]]]:
    plan_window_ids = {window.window_id for window in plan}
    tasks_by_id = {task.task_id: task for task in _regular_tasks(scenario)}
    segment_lookup = {segment.index: segment for segment in segments}
    candidates: list[AugmentCandidate] = []
    debug_by_range: dict[str, dict[str, Any]] = {}
    min_overlap = max(float(scenario.stage1.d_min), 300.0)
    for hot_range in hot_ranges:
        contributors = contributing_tasks.get(hot_range.range_id, [])
        structural_priority = 1 if bool(classifications.get(hot_range.range_id) and classifications[hot_range.range_id].structural) else 0
        per_range: list[AugmentCandidate] = []
        segment_set = set(hot_range.segment_indices)
        funnel_counts = _empty_augment_funnel_counts()
        candidate_records: dict[str, dict[str, Any]] = {}
        for window in scenario.candidate_windows:
            if window.window_id in plan_window_ids:
                continue
            record = _augment_candidate_debug_record(window)
            record["delay_penalty"] = float(window.delay) / max(float(scenario.stage1.d_min), 1.0)
            candidate_records[window.window_id] = record
            overlap_duration = _window_overlap_duration(window.start, window.end, hot_range.start, hot_range.end)
            record["overlap_duration"] = float(overlap_duration)
            if overlap_duration <= EPS:
                record["rejection_reason"] = "no_time_overlap_with_hot_range"
                continue
            funnel_counts["raw_overlap_candidate_count"] += 1
            schedule_detail = _schedule_candidate_against_plan_detailed(
                plan,
                window,
                t_pre=float(scenario.stage1.t_pre),
                d_min=float(scenario.stage1.d_min),
            )
            record["scheduled_on"] = schedule_detail.get("scheduled_on")
            record["scheduled_off"] = schedule_detail.get("scheduled_off")
            if not bool(schedule_detail.get("pass_t_pre_and_d_min", False)):
                record["rejection_reason"] = str(schedule_detail.get("rejection_reason") or "other")
                continue
            funnel_counts["schedulable_after_t_pre_d_min_count"] += 1
            _promote_augment_stage(record, "schedulable")
            if not bool(schedule_detail.get("pass_conflict_check", False)):
                record["rejection_reason"] = str(schedule_detail.get("rejection_reason") or "other")
                continue
            funnel_counts["conflict_free_count"] += 1
            _promote_augment_stage(record, "conflict_free")
            scheduled_window = schedule_detail.get("scheduled_window")
            if not isinstance(scheduled_window, ScheduledWindow):
                record["rejection_reason"] = "other"
                continue
            overlap_duration = _window_overlap_duration(scheduled_window.on, scheduled_window.off, hot_range.start, hot_range.end)
            record["overlap_duration"] = float(overlap_duration)
            if overlap_duration + EPS < min_overlap:
                record["rejection_reason"] = "no_time_overlap_with_hot_range"
                continue
            if not contributors:
                record["rejection_reason"] = "no_feasible_relief_path"
                continue
            feasible_task_ids: set[str] = set()
            divertable_rate = 0.0
            feasible_path_count = 0
            switch_penalty = 0.0
            for contribution in contributors:
                task = tasks_by_id.get(contribution.task_id)
                if task is None:
                    continue
                for segment_index in contribution.segment_indices:
                    if segment_index not in segment_set:
                        continue
                    segment = segment_lookup.get(segment_index)
                    allocation = schedule.get((task.task_id, segment_index))
                    if segment is None or allocation is None:
                        continue
                    if _window_overlap_duration(segment.start, segment.end, scheduled_window.on, scheduled_window.off) <= EPS:
                        continue
                    candidate_paths = _candidate_paths_via_window(
                        scenario,
                        task,
                        segment,
                        scheduled_window,
                        max(int(scenario.stage2.hot_path_limit), int(scenario.stage2.k_paths), 1),
                    )
                    if not candidate_paths:
                        continue
                    feasible_task_ids.add(task.task_id)
                    feasible_path_count += len(candidate_paths)
                    divertable_rate += float(allocation.rate)
                    current_cross = next((edge_id for edge_id in allocation.edge_ids if edge_id in contribution.cross_window_ids), None)
                    if current_cross is not None and current_cross != window.window_id:
                        switch_penalty += 1.0
                    break
            if not feasible_task_ids or divertable_rate <= EPS or feasible_path_count <= 0:
                record["estimated_divertable_rate"] = float(divertable_rate)
                record["feasible_path_count"] = int(feasible_path_count)
                record["switch_penalty"] = float(switch_penalty)
                record["rejection_reason"] = "no_feasible_relief_path"
                continue
            funnel_counts["relief_path_ready_count"] += 1
            _promote_augment_stage(record, "relief_path_ready")
            delay_penalty = float(record["delay_penalty"])
            score = overlap_duration * divertable_rate / (1.0 + 0.15 * delay_penalty + 0.05 * switch_penalty)
            if score <= EPS:
                record["estimated_divertable_rate"] = float(divertable_rate)
                record["feasible_path_count"] = int(feasible_path_count)
                record["switch_penalty"] = float(switch_penalty)
                record["rejection_reason"] = "other"
                continue
            record["estimated_divertable_rate"] = float(divertable_rate)
            record["feasible_path_count"] = int(feasible_path_count)
            record["switch_penalty"] = float(switch_penalty)
            record["relief_score"] = float(score)
            record["rejection_reason"] = None
            per_range.append(
                AugmentCandidate(
                    range_id=hot_range.range_id,
                    window_id=window.window_id,
                    structural_priority=int(structural_priority),
                    scheduled_on=float(scheduled_window.on),
                    scheduled_off=float(scheduled_window.off),
                    scheduled_duration=float(scheduled_window.off - scheduled_window.on),
                    overlap_duration=float(overlap_duration),
                    estimated_divertable_rate=float(divertable_rate),
                    feasible_path_count=int(feasible_path_count),
                    delay_penalty=float(delay_penalty),
                    switch_penalty=float(switch_penalty),
                    relief_score=float(score),
                    contributing_task_ids=tuple(sorted(feasible_task_ids)),
                )
            )
        per_range.sort(
            key=_augment_candidate_sort_key
        )
        shortlist = per_range[: _closed_loop_topk_candidates_per_range(scenario)]
        funnel_counts["shortlisted_count"] = len(shortlist)
        shortlisted_ids = {item.window_id for item in shortlist}
        for candidate in shortlist:
            record = candidate_records.get(candidate.window_id)
            if record is None:
                continue
            _promote_augment_stage(record, "shortlisted")
            record["rejection_reason"] = None
        for candidate in per_range:
            if candidate.window_id in shortlisted_ids:
                continue
            record = candidate_records.get(candidate.window_id)
            if record is None:
                continue
            if _AUGMENT_STAGE_ORDER.get(str(record.get("final_stage_reached")), -1) >= _AUGMENT_STAGE_ORDER["relief_path_ready"]:
                record["rejection_reason"] = "dominated_in_ranking"
        candidates.extend(shortlist)
        debug_by_range[hot_range.range_id] = {
            "range_id": hot_range.range_id,
            "funnel_counts": funnel_counts,
            "candidate_records": candidate_records,
            "rejection_breakdown": {},
            "augment_debug_top_candidates": [],
            "fallback_local_swap": {
                "attempted": False,
                "accepted": False,
                "reason": "not_attempted",
            },
        }
    return candidates, debug_by_range


def _select_augment_windows(
    scenario: Scenario,
    plan: list[ScheduledWindow],
    candidates: list[AugmentCandidate],
    *,
    hot_ranges: list[HotRange],
    classifications: dict[str, HotRangeClassification],
) -> list[AugmentCandidate]:
    return _select_augment_windows_with_details(
        scenario,
        plan,
        candidates,
        hot_ranges=hot_ranges,
        classifications=classifications,
    )["selected_candidates"]


def _select_augment_windows_with_details(
    scenario: Scenario,
    plan: list[ScheduledWindow],
    candidates: list[AugmentCandidate],
    *,
    hot_ranges: list[HotRange],
    classifications: dict[str, HotRangeClassification],
) -> dict[str, Any]:
    if not candidates or scenario.stage2.augment_window_budget <= 0:
        return {
            "selected_candidates": [],
            "provisional_reserved_keys": set(),
        }
    policy = _augment_selection_policy(scenario)
    best_by_window: dict[str, AugmentCandidate] = {}
    candidates_by_range: dict[str, list[AugmentCandidate]] = defaultdict(list)
    for candidate in candidates:
        candidates_by_range[candidate.range_id].append(candidate)
        existing = best_by_window.get(candidate.window_id)
        if existing is None or _augment_candidate_sort_key(candidate) < _augment_candidate_sort_key(existing):
            best_by_window[candidate.window_id] = candidate
    for range_candidates in candidates_by_range.values():
        range_candidates.sort(key=_augment_candidate_sort_key)
    ranked = sorted(best_by_window.values(), key=_augment_candidate_sort_key)
    candidate_lookup = {window.window_id: window for window in scenario.candidate_windows}
    selected: list[AugmentCandidate] = []
    working_plan = list(plan)
    selected_window_ids: set[str] = set()
    provisional_reserved_keys: set[tuple[str, str]] = set()
    budget = int(scenario.stage2.augment_window_budget)

    def try_select(candidate: AugmentCandidate) -> bool:
        if len(selected) >= budget or candidate.window_id in selected_window_ids or int(candidate.feasible_path_count) <= 0:
            return False
        base_window = candidate_lookup.get(candidate.window_id)
        if base_window is None:
            return False
        scheduled_window = _schedule_candidate_against_plan(
            working_plan,
            base_window,
            t_pre=float(scenario.stage1.t_pre),
            d_min=float(scenario.stage1.d_min),
        )
        if scheduled_window is None:
            return False
        working_plan.append(scheduled_window)
        selected.append(candidate)
        selected_window_ids.add(candidate.window_id)
        return True

    if policy == "structural_coverage_first":
        hot_range_lookup = {hot_range.range_id: hot_range for hot_range in hot_ranges}
        structural_range_ids = [
            range_id
            for range_id, range_candidates in candidates_by_range.items()
            if range_candidates and bool(classifications.get(range_id) and classifications[range_id].structural)
        ]
        structural_range_ids.sort(
            key=lambda range_id: _hotspot_severity_sort_key(
                hot_range_lookup.get(range_id),
                classifications.get(range_id),
                range_id,
            )
        )
        for range_id in structural_range_ids:
            if len(selected) >= budget:
                break
            for candidate in candidates_by_range.get(range_id, []):
                if try_select(candidate):
                    provisional_reserved_keys.add((candidate.range_id, candidate.window_id))
                    break

    for candidate in ranked:
        if len(selected) >= budget:
            break
        try_select(candidate)
    return {
        "selected_candidates": selected,
        "provisional_reserved_keys": provisional_reserved_keys,
    }


def _apply_augmentation_to_plan(
    scenario: Scenario,
    plan: list[ScheduledWindow],
    schedule: dict[tuple[str, int], Allocation],
    hot_ranges: list[HotRange],
    selected_candidates: list[AugmentCandidate],
) -> tuple[list[ScheduledWindow], list[str], list[str]]:
    if not selected_candidates:
        return list(plan), [], []

    candidate_lookup = {window.window_id: window for window in scenario.candidate_windows}
    selected_window_ids = [candidate.window_id for candidate in selected_candidates]
    working_plan = list(plan)
    added_windows: list[ScheduledWindow] = []
    applied_window_ids: list[str] = []
    for candidate in selected_candidates:
        base_window = candidate_lookup.get(candidate.window_id)
        if base_window is None:
            continue
        scheduled_window = _schedule_candidate_against_plan(
            working_plan,
            base_window,
            t_pre=float(scenario.stage1.t_pre),
            d_min=float(scenario.stage1.d_min),
        )
        if scheduled_window is None:
            continue
        working_plan.append(scheduled_window)
        added_windows.append(scheduled_window)
        applied_window_ids.append(candidate.window_id)
    mode = _augment_mode(scenario)
    fixed_count = _fixed_plan_window_count(scenario)
    if mode != "swap_if_budgeted" or fixed_count is None:
        return list(plan) + added_windows, [], applied_window_ids

    target_count = max(int(fixed_count), 0)
    if len(plan) + len(added_windows) <= target_count:
        return list(plan) + added_windows, [], applied_window_ids

    hot_intervals = [(float(item.start), float(item.end)) for item in hot_ranges]
    usage_by_window: dict[str, float] = defaultdict(float)
    for allocation in schedule.values():
        for edge_id in allocation.edge_ids:
            usage_by_window[edge_id] += float(allocation.delivered)

    removable: list[tuple[float, float, float, str]] = []
    for window in plan:
        overlap_hot = sum(_window_overlap_duration(window.on, window.off, start, end) for start, end in hot_intervals)
        if overlap_hot > EPS:
            continue
        removable.append(
            (
                float(usage_by_window.get(window.window_id, 0.0)),
                float(window.value or 0.0),
                float(window.off - window.on),
                window.window_id,
            )
        )
    removable.sort(key=lambda item: (item[0], item[1], item[2], item[3]))
    need_remove = max(len(plan) + len(added_windows) - target_count, 0)
    removed_ids = {window_id for _, _, _, window_id in removable[:need_remove]}
    kept_plan = [window for window in plan if window.window_id not in removed_ids]
    return kept_plan + added_windows, sorted(removed_ids), applied_window_ids


def _build_hot_range_alternative_diagnostics(
    scenario: Scenario,
    plan: list[ScheduledWindow],
    segments: list[Segment],
    schedule: dict[tuple[str, int], Allocation],
    hot_range: HotRange,
    contributors: list[HotTaskContribution],
    augment_range_debug: dict[str, Any] | None,
) -> dict[str, Any]:
    tasks_by_id = {task.task_id: task for task in _regular_tasks(scenario)}
    segment_lookup = {segment.index: segment for segment in segments}
    plan_window_ids = {window.window_id for window in plan}
    selected_window_alternative_cross_window_ids: set[str] = set()
    selected_window_alternative_path_count = 0
    candidate_limit = max(int(scenario.stage2.hot_path_limit), int(scenario.stage2.k_paths), 6)
    for contribution in contributors:
        task = tasks_by_id.get(contribution.task_id)
        if task is None:
            continue
        for segment_index in contribution.segment_indices:
            segment = segment_lookup.get(segment_index)
            allocation = schedule.get((task.task_id, segment_index))
            if segment is None or allocation is None:
                continue
            current_cross_window_ids = set(_allocation_cross_windows(allocation, plan_window_ids))
            active_selected_window_ids = {window.window_id for window in _active_cross_windows(plan, segment.start)}
            seen_signatures: set[tuple[str | None, tuple[str, ...]]] = set()
            for candidate in generate_candidate_paths(scenario, plan, task, segment, candidate_limit):
                signature = (candidate.cross_window_id, tuple(candidate.edge_ids))
                if signature in seen_signatures:
                    continue
                seen_signatures.add(signature)
                if candidate.cross_window_id is None:
                    continue
                if tuple(candidate.edge_ids) == tuple(allocation.edge_ids):
                    continue
                selected_window_alternative_path_count += 1
                if (
                    candidate.cross_window_id in active_selected_window_ids
                    and candidate.cross_window_id not in current_cross_window_ids
                ):
                    selected_window_alternative_cross_window_ids.add(candidate.cross_window_id)

    augment_candidate_records = (
        dict((augment_range_debug or {}).get("candidate_records") or {})
    )
    augment_ready_records = [
        record
        for record in augment_candidate_records.values()
        if _AUGMENT_STAGE_ORDER.get(str(record.get("final_stage_reached")), -1) >= _AUGMENT_STAGE_ORDER["relief_path_ready"]
    ]
    augment_window_alternative_cross_window_ids = {
        str(record.get("window_id"))
        for record in augment_ready_records
        if record.get("window_id")
    }
    augment_window_alternative_path_count = sum(int(record.get("feasible_path_count", 0) or 0) for record in augment_ready_records)
    feasible_alternative_cross_window_ids = set(selected_window_alternative_cross_window_ids).union(augment_window_alternative_cross_window_ids)
    feasible_alternative_path_count = selected_window_alternative_path_count + augment_window_alternative_path_count
    return {
        "selected_window_alternative_cross_window_ids": tuple(sorted(selected_window_alternative_cross_window_ids)),
        "selected_window_alternative_path_count": int(selected_window_alternative_path_count),
        "augment_window_alternative_cross_window_ids": tuple(sorted(augment_window_alternative_cross_window_ids)),
        "augment_window_alternative_path_count": int(augment_window_alternative_path_count),
        "effective_new_augment_window_count": len(augment_window_alternative_cross_window_ids),
        "feasible_alternative_cross_window_ids": tuple(sorted(feasible_alternative_cross_window_ids)),
        "selected_window_alternative_cross_window_count": len(selected_window_alternative_cross_window_ids),
        "augment_window_alternative_cross_window_count": len(augment_window_alternative_cross_window_ids),
        "feasible_alternative_cross_window_count": len(feasible_alternative_cross_window_ids),
        "feasible_alternative_path_count": int(feasible_alternative_path_count),
    }


def _finalize_augment_debug(
    augment_debug_by_range: dict[str, dict[str, Any]],
    selected_candidates: list[AugmentCandidate],
    applied_window_ids: list[str],
    removed_window_ids: list[str],
) -> None:
    selected_keys = {(candidate.range_id, candidate.window_id) for candidate in selected_candidates}
    applied_window_id_set = set(applied_window_ids)
    applied_keys = {
        (candidate.range_id, candidate.window_id)
        for candidate in selected_candidates
        if candidate.window_id in applied_window_id_set
    }
    removed_window_id_set = set(removed_window_ids)

    for range_id, debug in augment_debug_by_range.items():
        records = dict(debug.get("candidate_records") or {})
        funnel_counts = dict(debug.get("funnel_counts") or {})
        funnel_counts["selected_count"] = 0
        funnel_counts["applied_count"] = 0
        for window_id, record in records.items():
            key = (range_id, window_id)
            if key in selected_keys:
                _promote_augment_stage(record, "selected")
                record["rejection_reason"] = None
                funnel_counts["selected_count"] += 1
            elif (
                _AUGMENT_STAGE_ORDER.get(str(record.get("final_stage_reached")), -1) >= _AUGMENT_STAGE_ORDER["shortlisted"]
                and record.get("rejection_reason") in {None, ""}
            ):
                record["rejection_reason"] = "budget_exhausted"
            if key in applied_keys:
                _promote_augment_stage(record, "applied")
                record["rejection_reason"] = None
                funnel_counts["applied_count"] += 1
            elif key in selected_keys and window_id not in applied_window_id_set:
                record["rejection_reason"] = (
                    "removed_by_swap_rule" if window_id in removed_window_id_set else "terminal_conflict_after_insertion"
                )

        debug["funnel_counts"] = funnel_counts
        debug["rejection_breakdown"] = _augment_rejection_breakdown(records)
        ranked_records = sorted(
            records.values(),
            key=lambda record: (
                -_AUGMENT_STAGE_ORDER.get(str(record.get("final_stage_reached")), -1),
                -float(record.get("relief_score", 0.0) or 0.0),
                -float(record.get("estimated_divertable_rate", 0.0) or 0.0),
                -int(record.get("feasible_path_count", 0) or 0),
                -float(record.get("overlap_duration", 0.0) or 0.0),
                str(record.get("window_id") or ""),
            ),
        )
        debug["augment_debug_top_candidates"] = [
            {
                "window_id": record.get("window_id"),
                "overlap_duration": record.get("overlap_duration"),
                "scheduled_on": record.get("scheduled_on"),
                "scheduled_off": record.get("scheduled_off"),
                "estimated_divertable_rate": record.get("estimated_divertable_rate"),
                "feasible_path_count": record.get("feasible_path_count"),
                "delay_penalty": record.get("delay_penalty"),
                "switch_penalty": record.get("switch_penalty"),
                "relief_score": record.get("relief_score"),
                "final_stage_reached": record.get("final_stage_reached"),
                "rejection_reason": record.get("rejection_reason"),
            }
            for record in ranked_records[:10]
        ]


def _structurally_starved_by_selection_policy(
    *,
    classification: HotRangeClassification,
    funnel_counts: dict[str, Any],
    top_candidates: list[dict[str, Any]],
) -> tuple[bool, str | None]:
    if not bool(classification.structural):
        return False, None
    if int(funnel_counts.get("relief_path_ready_count", 0) or 0) <= 0:
        return False, None
    if int(funnel_counts.get("selected_count", 0) or 0) > 0 or int(funnel_counts.get("applied_count", 0) or 0) > 0:
        return False, None
    reasons = [
        str(candidate.get("rejection_reason") or "").strip()
        for candidate in top_candidates
        if str(candidate.get("rejection_reason") or "").strip()
    ]
    if not reasons:
        return False, None
    dominant_reason, dominant_count = Counter(reasons).most_common(1)[0]
    if dominant_reason not in {"budget_exhausted", "dominated_in_ranking"}:
        return False, dominant_reason
    return dominant_count >= max(1, len(reasons) // 2), dominant_reason


def _profile_subset_summary(
    profile: list[CrossSegmentProfileRow],
    segment_indices: set[int],
) -> dict[str, float | int]:
    rows = [row for row in profile if row.segment_index in segment_indices]
    if not rows:
        return {
            "q_peak": 0.0,
            "q_integral": 0.0,
            "peak_like_threshold": 0.995,
            "peak_segment_count": 0,
        }
    return _load_summary_from_profile(rows)


def _build_structural_gate_scenario(
    scenario: Scenario,
    horizon_length: int,
    *,
    time_limit_seconds: float | None = None,
) -> Scenario:
    gate_time_limit = time_limit_seconds
    if gate_time_limit in {None, 0, 0.0}:
        configured_gate_limit = _structural_repair_gate_time_limit_seconds(scenario)
        if configured_gate_limit in {None, 0, 0.0}:
            gate_time_limit = scenario.stage2.milp_time_limit_seconds
            if gate_time_limit in {None, 0, 0.0}:
                gate_time_limit = 30.0
            else:
                gate_time_limit = min(float(gate_time_limit), 30.0)
        else:
            gate_time_limit = float(configured_gate_limit)
    gate_stage2 = replace(
        scenario.stage2,
        k_paths=min(max(int(scenario.stage2.k_paths), 1), 2),
        hot_path_limit=min(max(int(scenario.stage2.hot_path_limit), 2), 3),
        hot_promoted_tasks_per_segment=min(max(int(scenario.stage2.hot_promoted_tasks_per_segment), 1), 3),
        hotspot_expand_segments=0,
        local_peak_horizon_cap_segments=max(int(horizon_length), 1),
        milp_time_limit_seconds=float(gate_time_limit),
        augment_window_budget=1,
    )
    return replace(
        scenario,
        stage2=gate_stage2,
        metadata={
            **dict(scenario.metadata),
            "structural_repair_gate_active": True,
        },
    )


def _attempt_structural_repair_gate(
    scenario: Scenario,
    plan: list[ScheduledWindow],
    segments: list[Segment],
    current_schedule: dict[tuple[str, int], Allocation],
    current_diagnostics: dict[str, Any],
    current_profile: list[CrossSegmentProfileRow],
    hot_range: HotRange,
    classification: HotRangeClassification,
    contributors: list[HotTaskContribution],
    applied_augment_windows: list[str],
    *,
    time_limit_seconds: float | None = None,
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "attempted": False,
        "accepted": False,
        "rejection_reason": None,
        "local_before_after": None,
        "used_augment_windows": [],
        "detailed_runtime_failure_type": None,
        "candidate_solver_status": "not_attempted",
        "solver_error": None,
        "schedule": None,
        "objective_values": {},
    }
    if not classification.structural or len(applied_augment_windows) != 1:
        return result

    core_horizon = [segment for segment in segments if _window_overlap_duration(segment.start, segment.end, hot_range.start, hot_range.end) > EPS]
    if not core_horizon:
        result["attempted"] = True
        result["rejection_reason"] = "failed_no_candidate_path_after_pruning"
        result["candidate_solver_status"] = "SkippedNoCoreHorizon"
        result["detailed_runtime_failure_type"] = "failed_no_candidate_path_after_pruning"
        return result

    tasks_by_id = {task.task_id: task for task in _regular_tasks(scenario)}
    contributor_task_ids = [item.task_id for item in contributors if item.task_id in tasks_by_id]
    if not contributor_task_ids:
        result["attempted"] = True
        result["rejection_reason"] = "failed_no_candidate_path_after_pruning"
        result["candidate_solver_status"] = "SkippedNoContributors"
        result["detailed_runtime_failure_type"] = "failed_no_candidate_path_after_pruning"
        return result
    active_tasks = [tasks_by_id[task_id] for task_id in contributor_task_ids[: min(len(contributor_task_ids), 6)]]

    hot_window_ids_by_segment = _range_profile_summary(current_profile, hot_range)["hot_window_ids_by_segment"]
    contributor_task_id_set = {task.task_id for task in active_tasks}
    gate_scenario = _build_structural_gate_scenario(
        scenario,
        len(core_horizon),
        time_limit_seconds=time_limit_seconds,
    )
    hot_task_segments = {
        (task_id, segment_index)
        for task_id, segment_index in _hot_task_segments_for_range(
            gate_scenario,
            hot_range,
            current_schedule,
            hot_window_ids_by_segment,
        )
        if task_id in contributor_task_id_set
    }
    if not hot_task_segments:
        hot_task_segments = {
            (task_id, segment.index)
            for task_id in contributor_task_id_set
            for segment in core_horizon
        }

    segment_index_set = {segment.index for segment in core_horizon}
    before_summary = _profile_subset_summary(current_profile, segment_index_set)
    current_cr_reg = _regular_completion_ratio_from_diagnostics(scenario, current_diagnostics)

    result["attempted"] = True
    local_result = solve_regular_hotspot_local_milp(
        scenario=gate_scenario,
        plan=plan,
        segments=segments,
        current_schedule=current_schedule,
        diagnostics=current_diagnostics,
        horizon_segments=core_horizon,
        active_tasks=active_tasks,
        hot_segment_indices=segment_index_set,
        hot_task_segments=hot_task_segments,
        hot_window_ids_by_segment=hot_window_ids_by_segment,
        augmented_window_ids=set(applied_augment_windows),
    )
    result["candidate_solver_status"] = str(local_result.get("solver_status", "unknown"))
    result["detailed_runtime_failure_type"] = local_result.get("detailed_runtime_failure_type")
    result["solver_error"] = local_result.get("solver_error")
    result["objective_values"] = dict(local_result.get("objective_values", {}))
    if not bool(local_result.get("accepted", False)):
        result["rejection_reason"] = str(
            local_result.get("detailed_runtime_failure_type")
            or local_result.get("solver_status")
            or "failed_exception_other"
        )
        return result

    candidate_schedule = dict(local_result["schedule"])
    candidate_diagnostics = build_regular_schedule_diagnostics(scenario, plan, segments, candidate_schedule)
    candidate_profile = build_cross_segment_profile(scenario, plan, segments, candidate_schedule)
    candidate_summary = _profile_subset_summary(candidate_profile, segment_index_set)
    candidate_cr_reg = _regular_completion_ratio_from_diagnostics(scenario, candidate_diagnostics)
    epsilon = float(scenario.stage2.local_peak_accept_epsilon)
    result["local_before_after"] = {
        "before": {
            "cr_reg": float(current_cr_reg),
            **before_summary,
        },
        "after": {
            "cr_reg": float(candidate_cr_reg),
            **candidate_summary,
        },
    }
    used_augment_windows = sorted(local_result.get("used_augment_windows", []))
    if not any(window_id in set(applied_augment_windows) for window_id in used_augment_windows):
        result["rejection_reason"] = "augment_window_not_used_by_gate"
        result["used_augment_windows"] = used_augment_windows
        return result
    if not _structural_gate_accepts(
        cr_reg_base=current_cr_reg,
        cr_reg_new=candidate_cr_reg,
        before_summary=before_summary,
        after_summary=candidate_summary,
        epsilon=epsilon,
    ):
        result["rejection_reason"] = "local_metrics_not_improved"
        return result

    result["accepted"] = True
    result["schedule"] = candidate_schedule
    result["used_augment_windows"] = used_augment_windows
    return result


def _hot_range_segment_indices(segments: list[Segment], hot_range: HotRange) -> list[int]:
    return [
        segment.index
        for segment in segments
        if _window_overlap_duration(segment.start, segment.end, hot_range.start, hot_range.end) > EPS
    ]


def _dominant_window_id(row: CrossSegmentProfileRow) -> str | None:
    if not row.per_window_util:
        return None
    return min(
        row.per_window_util,
        key=lambda window_id: (-float(row.per_window_util[window_id]), window_id),
    )


def _trim_horizon_by_structure(
    segments: list[Segment],
    horizon: list[Segment],
    hot_range: HotRange,
    profile: list[CrossSegmentProfileRow],
    cap_value: int,
) -> list[Segment]:
    if len(horizon) <= cap_value:
        return horizon
    horizon_segment_ids = {segment.index for segment in horizon}
    hot_rows = [
        row
        for row in profile
        if row.segment_index in horizon_segment_ids and row.segment_index in set(hot_range.segment_indices)
    ]
    if not hot_rows:
        return horizon[:cap_value]
    blocks: list[dict[str, float | int]] = []
    block_start = hot_rows[0].segment_index
    block_end = hot_rows[0].segment_index
    block_score = float(hot_rows[0].q_r) * float(hot_rows[0].duration)
    previous_row = hot_rows[0]
    for row in hot_rows[1:]:
        changed = (
            _dominant_window_id(row) != _dominant_window_id(previous_row)
            or int(row.active_selected_cross_count) != int(previous_row.active_selected_cross_count)
            or tuple(row.selected_task_ids) != tuple(previous_row.selected_task_ids)
        )
        if changed:
            blocks.append({"start": block_start, "end": block_end, "score": block_score})
            block_start = row.segment_index
            block_score = 0.0
        block_end = row.segment_index
        block_score += float(row.q_r) * float(row.duration)
        previous_row = row
    blocks.append({"start": block_start, "end": block_end, "score": block_score})
    if len(blocks) <= 1:
        return horizon[:cap_value]

    anchor_index = max(range(len(blocks)), key=lambda index: float(blocks[index]["score"]))
    left = anchor_index
    right = anchor_index

    def current_span_length(left_index: int, right_index: int) -> int:
        return int(blocks[right_index]["end"]) - int(blocks[left_index]["start"]) + 1

    while True:
        best_side: str | None = None
        best_gain = -1.0
        if left > 0:
            left_length = int(blocks[right]["end"]) - int(blocks[left - 1]["start"]) + 1
            if left_length <= cap_value and float(blocks[left - 1]["score"]) > best_gain:
                best_side = "left"
                best_gain = float(blocks[left - 1]["score"])
        if right + 1 < len(blocks):
            right_length = int(blocks[right + 1]["end"]) - int(blocks[left]["start"]) + 1
            if right_length <= cap_value and float(blocks[right + 1]["score"]) > best_gain:
                best_side = "right"
                best_gain = float(blocks[right + 1]["score"])
        if best_side is None:
            break
        if best_side == "left":
            left -= 1
        else:
            right += 1
        if current_span_length(left, right) >= cap_value:
            break

    start_index = int(blocks[left]["start"])
    end_index = int(blocks[right]["end"])
    return [segment for segment in horizon if start_index <= segment.index <= end_index]


def _horizon_segments_for_hot_range(
    scenario: Scenario,
    segments: list[Segment],
    hot_range: HotRange,
    profile: list[CrossSegmentProfileRow] | None = None,
) -> list[Segment]:
    segment_ids = _hot_range_segment_indices(segments, hot_range)
    if not segment_ids:
        return []
    start_idx = max(min(segment_ids) - int(scenario.stage2.hotspot_expand_segments), 0)
    end_idx = min(max(segment_ids) + int(scenario.stage2.hotspot_expand_segments), len(segments) - 1)
    horizon = segments[start_idx : end_idx + 1]
    cap = scenario.stage2.local_peak_horizon_cap_segments
    if cap is None or len(horizon) <= int(cap):
        return horizon
    cap_value = max(int(cap), 1)
    if profile is not None and cap_value < len(horizon):
        structured = _trim_horizon_by_structure(segments, horizon, hot_range, profile, cap_value)
        if structured:
            return structured[:cap_value]
    hot_start = min(segment_ids)
    hot_end = max(segment_ids)
    extra = max(cap_value - (hot_end - hot_start + 1), 0)
    current_start = max(hot_start - (extra // 2), 0)
    current_end = min(current_start + cap_value - 1, len(segments) - 1)
    if current_end < hot_end:
        current_end = hot_end
        current_start = max(current_end - cap_value + 1, 0)
    return segments[current_start : current_end + 1]


def _hot_task_segments_for_range(
    scenario: Scenario,
    hot_range: HotRange,
    schedule: dict[tuple[str, int], Allocation],
    hot_window_ids_by_segment: dict[int, tuple[str, ...]],
) -> set[tuple[str, int]]:
    promoted: set[tuple[str, int]] = set()
    for segment_index in hot_range.segment_indices:
        window_ids = set(hot_window_ids_by_segment.get(segment_index, ()))
        ranked: list[tuple[float, str]] = []
        for (task_id, current_segment_index), allocation in schedule.items():
            if current_segment_index != segment_index:
                continue
            if not any(edge_id in window_ids for edge_id in allocation.edge_ids):
                continue
            ranked.append((float(allocation.delivered), task_id))
        ranked.sort(key=lambda item: (-item[0], item[1]))
        for _, task_id in ranked[: int(scenario.stage2.hot_promoted_tasks_per_segment)]:
            promoted.add((task_id, segment_index))
    return promoted


def _range_profile_summary(profile: list[CrossSegmentProfileRow], hot_range: HotRange) -> dict[str, Any]:
    rows = [row for row in profile if row.segment_index in set(hot_range.segment_indices)]
    hot_window_ids_by_segment = {
        row.segment_index: tuple(
            window_id
            for window_id, util in sorted(row.per_window_util.items(), key=lambda item: (-float(item[1]), item[0]))
            if float(util) > EPS
        )
        for row in rows
    }
    return {
        "range": hot_range,
        "hot_window_ids_by_segment": hot_window_ids_by_segment,
    }


def _active_tasks_for_horizon(
    scenario: Scenario,
    horizon: list[Segment],
    diagnostics: dict[str, Any],
):
    if not horizon:
        return []
    remaining_trace = diagnostics.get("remaining_before_trace", {})
    active_tasks = []
    for task in _regular_tasks(scenario):
        if not any(task.arrival <= segment.start < task.deadline for segment in horizon):
            continue
        task_remaining_before = float(
            (remaining_trace.get(task.task_id, {}) or {}).get(horizon[0].index, task.data)
        )
        if task_remaining_before > completion_tolerance(scenario, task):
            active_tasks.append(task)
    return active_tasks


def _candidate_action_sort_key(action: dict[str, Any], scenario: Scenario) -> tuple[float | int | str, ...]:
    improvement = dict(action.get("improvement") or {})
    priority_bucket = 0
    if _closed_loop_action_mode(scenario) == "reroute_then_augment":
        priority_bucket = 0 if str(action.get("action_type")) == "reroute" else 1
    return (
        int(priority_bucket),
        -float(improvement.get("delta_q_peak", 0.0) or 0.0),
        -int(improvement.get("delta_high_segment_count", 0) or 0),
        -int(improvement.get("delta_peak_segment_count", 0) or 0),
        -float(improvement.get("delta_q_integral", 0.0) or 0.0),
        -float(improvement.get("delta_focus_q_peak", 0.0) or 0.0),
        -float(improvement.get("delta_focus_q_integral", 0.0) or 0.0),
        str(action.get("range_id") or ""),
        str(action.get("window_id") or ""),
    )


def _base_range_report(
    hot_range: HotRange,
    classification: HotRangeClassification,
    *,
    contributions: list[HotTaskContribution],
    alternative_diagnostics: dict[str, Any],
    augment_debug: dict[str, Any],
    sanity_warnings: list[str],
) -> dict[str, Any]:
    return {
        "range_id": hot_range.range_id,
        "start": float(hot_range.start),
        "end": float(hot_range.end),
        "max_q_r": float(hot_range.max_q_r),
        "q_integral": float(hot_range.q_integral),
        "classification": asdict(classification),
        "alternative_diagnostics": dict(alternative_diagnostics),
        "augment_funnel_counts": dict(augment_debug.get("funnel_counts") or {}),
        "augment_rejection_breakdown": dict(augment_debug.get("rejection_breakdown") or {}),
        "augment_debug_top_candidates": list(augment_debug.get("augment_debug_top_candidates") or []),
        "fallback_local_swap": dict(augment_debug.get("fallback_local_swap") or {}),
        "contributing_tasks": [asdict(item) for item in contributions],
        "sanity_warnings": list(sanity_warnings),
        "status": "pending",
        "accepted": False,
        "candidate_action_type": None,
        "candidate_solver_status": "not_attempted",
        "candidate_solver_error": None,
        "rejection_reason": None,
        "detailed_runtime_failure_type": None,
        "selected_augment_windows": [],
        "applied_augment_windows": [],
        "used_augment_windows": [],
        "objective_values": {},
    }


def _evaluate_reroute_action(
    scenario: Scenario,
    *,
    plan: list[ScheduledWindow],
    segments: list[Segment],
    schedule: dict[tuple[str, int], Allocation],
    diagnostics: dict[str, Any],
    profile: list[CrossSegmentProfileRow],
    current_metrics: dict[str, float | int],
    current_cr_reg: float,
    hot_range: HotRange,
    classification: HotRangeClassification,
    total_time_limit_seconds: float | None,
    local_repair_time_limit_seconds: float | None,
    started_at: float,
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "action_type": "reroute",
        "range_id": hot_range.range_id,
        "window_id": None,
        "accepted": False,
        "rejection_reason": None,
        "candidate_solver_status": "not_attempted",
        "candidate_solver_error": None,
        "detailed_runtime_failure_type": None,
        "objective_values": {},
        "used_augment_windows": [],
        "plan": list(plan),
        "segments": list(segments),
    }
    if bool(classification.structural):
        result["rejection_reason"] = "structural_hotspot_prefers_augment"
        result["candidate_solver_status"] = "SkippedStructuralHotspot"
        return result
    if not bool(classification.reroutable):
        result["rejection_reason"] = str(classification.reason)
        result["candidate_solver_status"] = "SkippedNotReroutable"
        return result
    horizon = _horizon_segments_for_hot_range(scenario, segments, hot_range, profile=profile)
    if not horizon:
        result["rejection_reason"] = "hot_range has no local optimization horizon"
        result["candidate_solver_status"] = "SkippedNoHorizon"
        return result
    active_tasks = _active_tasks_for_horizon(scenario, horizon, diagnostics)
    if not active_tasks:
        result["rejection_reason"] = "no active regular tasks in local horizon"
        result["candidate_solver_status"] = "SkippedNoActiveTasks"
        return result
    total_remaining_seconds = _remaining_budget_seconds(
        total_time_limit_seconds,
        _total_elapsed_seconds(started_at),
    )
    skip_result = _bounded_skip_result(
        stage="closed_loop_reroute",
        total_remaining_seconds=total_remaining_seconds,
        range_remaining_seconds=_hotspot_per_range_time_limit_seconds(scenario),
    )
    if skip_result:
        result["rejection_reason"] = str(skip_result.get("rejection_reason"))
        result["candidate_solver_status"] = str(skip_result.get("candidate_solver_status"))
        result["detailed_runtime_failure_type"] = skip_result.get("detailed_runtime_failure_type")
        return result
    hot_window_ids_by_segment = _range_profile_summary(profile, hot_range)["hot_window_ids_by_segment"]
    promoted_task_segments = _hot_task_segments_for_range(
        scenario,
        hot_range,
        schedule,
        hot_window_ids_by_segment,
    )
    local_solver_time_limit = _effective_solver_time_limit_seconds(
        scenario,
        local_repair_time_limit_seconds,
        total_remaining_seconds,
        _hotspot_per_range_time_limit_seconds(scenario),
    )
    local_scenario = _scenario_with_milp_time_limit(scenario, local_solver_time_limit)
    local_result = solve_regular_hotspot_local_milp(
        scenario=local_scenario,
        plan=plan,
        segments=segments,
        current_schedule=schedule,
        diagnostics=diagnostics,
        horizon_segments=horizon,
        active_tasks=active_tasks,
        hot_segment_indices=set(_hot_range_segment_indices(segments, hot_range)),
        hot_task_segments=promoted_task_segments,
        hot_window_ids_by_segment=hot_window_ids_by_segment,
        augmented_window_ids=set(),
    )
    result["candidate_solver_status"] = str(local_result.get("solver_status", "no_candidate_solution"))
    result["candidate_solver_error"] = local_result.get("solver_error")
    result["detailed_runtime_failure_type"] = local_result.get("detailed_runtime_failure_type")
    result["objective_values"] = dict(local_result.get("objective_values", {}))
    if not bool(local_result.get("accepted", False)):
        result["rejection_reason"] = str(
            local_result.get("detailed_runtime_failure_type")
            or local_result.get("solver_status")
            or "no_candidate_solution"
        )
        return result
    candidate_schedule = dict(local_result["schedule"])
    candidate_diagnostics = build_regular_schedule_diagnostics(scenario, plan, segments, candidate_schedule)
    candidate_profile = build_cross_segment_profile(scenario, plan, segments, candidate_schedule)
    candidate_metrics = _closed_loop_metrics(scenario, candidate_profile)
    candidate_cr_reg = _regular_completion_ratio_from_diagnostics(scenario, candidate_diagnostics)
    before_focus = _interval_profile_summary(profile, start=hot_range.start, end=hot_range.end)
    after_focus = _interval_profile_summary(candidate_profile, start=hot_range.start, end=hot_range.end)
    accepted, improvement, rejection_reason = _closed_loop_accepts(
        scenario,
        cr_reg_base=current_cr_reg,
        cr_reg_new=candidate_cr_reg,
        before_metrics=current_metrics,
        after_metrics=candidate_metrics,
        before_focus=before_focus,
        after_focus=after_focus,
    )
    result.update(
        {
            "accepted": bool(accepted),
            "rejection_reason": rejection_reason,
            "schedule": candidate_schedule,
            "diagnostics": candidate_diagnostics,
            "profile": candidate_profile,
            "metrics": candidate_metrics,
            "cr_reg": float(candidate_cr_reg),
            "improvement": improvement,
            "before_focus": before_focus,
            "after_focus": after_focus,
            "used_augment_windows": sorted(local_result.get("used_augment_windows", [])),
        }
    )
    return result


def _evaluate_augment_action(
    scenario: Scenario,
    *,
    plan: list[ScheduledWindow],
    segments: list[Segment],
    schedule: dict[tuple[str, int], Allocation],
    diagnostics: dict[str, Any],
    profile: list[CrossSegmentProfileRow],
    current_metrics: dict[str, float | int],
    current_cr_reg: float,
    hot_range: HotRange,
    classification: HotRangeClassification,
    augment_candidate: AugmentCandidate,
    contributors: list[HotTaskContribution],
    total_time_limit_seconds: float | None,
    gate_time_limit_seconds: float | None,
    started_at: float,
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "action_type": "augment",
        "range_id": hot_range.range_id,
        "window_id": augment_candidate.window_id,
        "accepted": False,
        "rejection_reason": None,
        "candidate_solver_status": "not_attempted",
        "candidate_solver_error": None,
        "detailed_runtime_failure_type": None,
        "objective_values": {},
        "used_augment_windows": [],
    }
    if not bool(classification.structural):
        result["rejection_reason"] = "augment_action_reserved_for_structural_hotspots"
        result["candidate_solver_status"] = "SkippedNonStructuralHotspot"
        return result
    candidate_lookup = {window.window_id: window for window in scenario.candidate_windows}
    base_window = candidate_lookup.get(augment_candidate.window_id)
    if base_window is None:
        result["rejection_reason"] = "candidate_window_missing"
        result["candidate_solver_status"] = "SkippedMissingWindow"
        return result
    scheduled_window = _schedule_candidate_against_plan(
        plan,
        base_window,
        t_pre=float(scenario.stage1.t_pre),
        d_min=float(scenario.stage1.d_min),
    )
    if scheduled_window is None:
        result["rejection_reason"] = "candidate_window_failed_insertion"
        result["candidate_solver_status"] = "SkippedInsertionFailed"
        return result
    augmented_plan = list(plan) + [scheduled_window]
    augmented_segments = build_segments(scenario, augmented_plan, _regular_tasks(scenario))
    augmented_schedule = _remap_schedule_to_segments(segments, augmented_segments, schedule)
    augmented_diagnostics = build_regular_schedule_diagnostics(scenario, augmented_plan, augmented_segments, augmented_schedule)
    augmented_profile = build_cross_segment_profile(scenario, augmented_plan, augmented_segments, augmented_schedule)
    total_remaining_seconds = _remaining_budget_seconds(
        total_time_limit_seconds,
        _total_elapsed_seconds(started_at),
    )
    skip_result = _bounded_skip_result(
        stage="closed_loop_structural_gate",
        total_remaining_seconds=total_remaining_seconds,
        range_remaining_seconds=_hotspot_per_range_time_limit_seconds(scenario),
    )
    if skip_result:
        result["rejection_reason"] = str(skip_result.get("rejection_reason"))
        result["candidate_solver_status"] = str(skip_result.get("candidate_solver_status"))
        result["detailed_runtime_failure_type"] = skip_result.get("detailed_runtime_failure_type")
        return result
    gate_solver_time_limit = _effective_solver_time_limit_seconds(
        scenario,
        gate_time_limit_seconds,
        total_remaining_seconds,
        _hotspot_per_range_time_limit_seconds(scenario),
    )
    gate_result = _attempt_structural_repair_gate(
        scenario,
        augmented_plan,
        augmented_segments,
        augmented_schedule,
        augmented_diagnostics,
        augmented_profile,
        hot_range,
        classification,
        contributors,
        [augment_candidate.window_id],
        time_limit_seconds=gate_solver_time_limit,
    )
    result["candidate_solver_status"] = str(gate_result.get("candidate_solver_status", "not_attempted"))
    result["candidate_solver_error"] = gate_result.get("solver_error")
    result["detailed_runtime_failure_type"] = gate_result.get("detailed_runtime_failure_type")
    result["objective_values"] = dict(gate_result.get("objective_values", {}))
    result["used_augment_windows"] = sorted(gate_result.get("used_augment_windows", []))
    if not bool(gate_result.get("accepted", False)):
        result["rejection_reason"] = str(
            gate_result.get("rejection_reason")
            or gate_result.get("detailed_runtime_failure_type")
            or "structural_gate_rejected"
        )
        return result
    candidate_schedule = dict(gate_result["schedule"])
    candidate_diagnostics = build_regular_schedule_diagnostics(scenario, augmented_plan, augmented_segments, candidate_schedule)
    candidate_profile = build_cross_segment_profile(scenario, augmented_plan, augmented_segments, candidate_schedule)
    candidate_metrics = _closed_loop_metrics(scenario, candidate_profile)
    candidate_cr_reg = _regular_completion_ratio_from_diagnostics(scenario, candidate_diagnostics)
    before_focus = _interval_profile_summary(profile, start=hot_range.start, end=hot_range.end)
    after_focus = _interval_profile_summary(candidate_profile, start=hot_range.start, end=hot_range.end)
    accepted, improvement, rejection_reason = _closed_loop_accepts(
        scenario,
        cr_reg_base=current_cr_reg,
        cr_reg_new=candidate_cr_reg,
        before_metrics=current_metrics,
        after_metrics=candidate_metrics,
        before_focus=before_focus,
        after_focus=after_focus,
    )
    result.update(
        {
            "accepted": bool(accepted),
            "rejection_reason": rejection_reason,
            "plan": augmented_plan,
            "segments": augmented_segments,
            "schedule": candidate_schedule,
            "diagnostics": candidate_diagnostics,
            "profile": candidate_profile,
            "metrics": candidate_metrics,
            "cr_reg": float(candidate_cr_reg),
            "improvement": improvement,
            "before_focus": before_focus,
            "after_focus": after_focus,
        }
    )
    return result


def run_hotspot_relief(
    scenario: Scenario,
    plan: list[ScheduledWindow],
    segments: list[Segment],
    baseline_schedule: dict[tuple[str, int], Allocation],
    baseline_diagnostics: dict[str, Any],
) -> HotspotReliefResult:
    from .stage2_hotspot_relief_closed_loop import run_hotspot_relief_closed_loop

    return run_hotspot_relief_closed_loop(
        scenario=scenario,
        plan=plan,
        segments=segments,
        baseline_schedule=baseline_schedule,
        baseline_diagnostics=baseline_diagnostics,
    )

