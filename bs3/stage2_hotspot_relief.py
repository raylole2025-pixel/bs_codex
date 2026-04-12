from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from statistics import median
from typing import Any

from .models import Allocation, CandidateWindow, ScheduledWindow, Scenario, Segment
from .regular_routing_common import build_regular_schedule_diagnostics, completion_tolerance, is_task_complete
from .scenario import build_segments, generate_candidate_paths
from .stage2_regular_joint_milp import solve_regular_hotspot_local_milp

EPS = 1e-9


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

    t_on = max(float(window.start), latest_end_before(window.a) + float(t_pre), latest_end_before(window.b) + float(t_pre))
    if float(window.end) - float(t_on) + EPS < float(d_min):
        return None
    return ScheduledWindow(
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
) -> list[AugmentCandidate]:
    plan_window_ids = {window.window_id for window in plan}
    tasks_by_id = {task.task_id: task for task in _regular_tasks(scenario)}
    segment_lookup = {segment.index: segment for segment in segments}
    candidates: list[AugmentCandidate] = []
    min_overlap = max(float(scenario.stage1.d_min), 300.0)
    for hot_range in hot_ranges:
        contributors = contributing_tasks.get(hot_range.range_id, [])
        if not contributors:
            continue
        structural_priority = 1 if bool(classifications.get(hot_range.range_id) and classifications[hot_range.range_id].structural) else 0
        per_range: list[AugmentCandidate] = []
        segment_set = set(hot_range.segment_indices)
        for window in scenario.candidate_windows:
            if window.window_id in plan_window_ids:
                continue
            scheduled_window = _schedule_candidate_against_plan(
                plan,
                window,
                t_pre=float(scenario.stage1.t_pre),
                d_min=float(scenario.stage1.d_min),
            )
            if scheduled_window is None:
                continue
            overlap_duration = _window_overlap_duration(scheduled_window.on, scheduled_window.off, hot_range.start, hot_range.end)
            if overlap_duration + EPS < min_overlap:
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
                continue
            delay_penalty = float(window.delay) / max(float(scenario.stage1.d_min), 1.0)
            score = overlap_duration * divertable_rate / (1.0 + 0.15 * delay_penalty + 0.05 * switch_penalty)
            if score <= EPS:
                continue
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
            key=lambda item: (
                -int(item.structural_priority),
                -float(item.relief_score),
                -float(item.estimated_divertable_rate),
                -int(item.feasible_path_count),
                item.window_id,
            )
        )
        candidates.extend(per_range[: max(int(scenario.stage2.augment_top_windows_per_range), 0)])
    return candidates


def _select_augment_windows(
    scenario: Scenario,
    plan: list[ScheduledWindow],
    candidates: list[AugmentCandidate],
) -> list[AugmentCandidate]:
    if not candidates or scenario.stage2.augment_window_budget <= 0:
        return []
    best_by_window: dict[str, AugmentCandidate] = {}
    for candidate in candidates:
        existing = best_by_window.get(candidate.window_id)
        if existing is None or (
            int(candidate.structural_priority),
            float(candidate.relief_score),
            float(candidate.estimated_divertable_rate),
            int(candidate.feasible_path_count),
            -float(candidate.delay_penalty),
            candidate.window_id,
        ) > (
            int(existing.structural_priority),
            float(existing.relief_score),
            float(existing.estimated_divertable_rate),
            int(existing.feasible_path_count),
            -float(existing.delay_penalty),
            existing.window_id,
        ):
            best_by_window[candidate.window_id] = candidate
    ranked = sorted(
        best_by_window.values(),
        key=lambda item: (
            -int(item.structural_priority),
            -float(item.relief_score),
            -float(item.estimated_divertable_rate),
            -int(item.feasible_path_count),
            float(item.delay_penalty),
            item.window_id,
        ),
    )
    candidate_lookup = {window.window_id: window for window in scenario.candidate_windows}
    selected: list[AugmentCandidate] = []
    working_plan = list(plan)
    for candidate in ranked:
        if len(selected) >= int(scenario.stage2.augment_window_budget):
            break
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
        selected.append(candidate)
    return selected


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
    augment_candidates: list[AugmentCandidate],
) -> dict[str, Any]:
    tasks_by_id = {task.task_id: task for task in _regular_tasks(scenario)}
    segment_lookup = {segment.index: segment for segment in segments}
    plan_window_ids = {window.window_id for window in plan}
    selected_alternative_cross_window_ids: set[str] = set()
    selected_alternative_path_count = 0
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
                selected_alternative_path_count += 1
                if (
                    candidate.cross_window_id in active_selected_window_ids
                    and candidate.cross_window_id not in current_cross_window_ids
                ):
                    selected_alternative_cross_window_ids.add(candidate.cross_window_id)

    augment_path_count_by_window: dict[str, int] = {}
    for candidate in augment_candidates:
        if candidate.range_id != hot_range.range_id:
            continue
        augment_path_count_by_window[candidate.window_id] = max(
            augment_path_count_by_window.get(candidate.window_id, 0),
            int(candidate.feasible_path_count),
        )

    feasible_alternative_cross_window_ids = set(selected_alternative_cross_window_ids).union(augment_path_count_by_window)
    feasible_alternative_path_count = selected_alternative_path_count + sum(augment_path_count_by_window.values())
    return {
        "selected_alternative_cross_window_ids": tuple(sorted(selected_alternative_cross_window_ids)),
        "augmentable_cross_window_ids": tuple(sorted(augment_path_count_by_window)),
        "feasible_alternative_cross_window_ids": tuple(sorted(feasible_alternative_cross_window_ids)),
        "selected_alternative_path_count": int(selected_alternative_path_count),
        "augment_alternative_path_count": int(sum(augment_path_count_by_window.values())),
        "feasible_alternative_cross_window_count": len(feasible_alternative_cross_window_ids),
        "feasible_alternative_path_count": int(feasible_alternative_path_count),
    }


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


def run_hotspot_relief(
    scenario: Scenario,
    plan: list[ScheduledWindow],
    segments: list[Segment],
    baseline_schedule: dict[tuple[str, int], Allocation],
    baseline_diagnostics: dict[str, Any],
) -> HotspotReliefResult:
    initial_profile = build_cross_segment_profile(scenario, plan, segments, baseline_schedule)
    initial_summary = _load_summary_from_profile(initial_profile)
    hot_ranges = detect_hot_ranges(
        initial_profile,
        threshold=float(scenario.stage2.hotspot_util_threshold),
        topk=int(scenario.stage2.hotspot_topk_ranges),
    )
    if not hot_ranges:
        metadata = {
            "hotspot_relief_enabled": True,
            "structural_hot_range_count": 0,
            "reroutable_hot_range_count": 0,
            "hot_ranges_considered": 0,
            "augment_candidates_considered": 0,
            "augment_windows_selected": 0,
            "augment_windows_added": 0,
            "selected_augment_windows": [],
            "applied_augment_windows": [],
            "q_peak_before": float(initial_summary["q_peak"]),
            "q_peak_after": float(initial_summary["q_peak"]),
            "peak_like_threshold_before": float(initial_summary["peak_like_threshold"]),
            "peak_like_threshold_after": float(initial_summary["peak_like_threshold"]),
            "peak_segment_count_before": int(initial_summary["peak_segment_count"]),
            "peak_segment_count_after": int(initial_summary["peak_segment_count"]),
            "q_integral_before": float(initial_summary["q_integral"]),
            "q_integral_after": float(initial_summary["q_integral"]),
            "sanity_warning_count": 0,
        }
        report = {
            "hot_ranges": [],
            "structural_bottleneck": [],
            "structural_candidate_pruned": [],
            "blocked_no_feasible_reroute": [],
            "reroutable_candidate_pruned": [],
            "improved_after_augmentation": [],
            "reroute_improved": [],
            "augment_candidates": [],
            "selected_augment_windows": [],
            "applied_augment_windows": [],
            "removed_plan_windows": [],
            "sanity_warnings": [],
            "before_after": {
                "before": initial_summary,
                "after": initial_summary,
            },
        }
        return HotspotReliefResult(
            plan=list(plan),
            schedule=dict(baseline_schedule),
            segments=list(segments),
            diagnostics=baseline_diagnostics,
            metadata=metadata,
            report=report,
        )

    initial_classifications = {
        hot_range.range_id: classify_hot_range(
            initial_profile,
            hot_range,
            single_link_fraction_threshold=float(scenario.stage2.hotspot_single_link_fraction_threshold),
        )
        for hot_range in hot_ranges
    }
    contributions = {
        hot_range.range_id: collect_hot_contributing_tasks(
            scenario,
            plan,
            segments,
            baseline_schedule,
            hot_range,
            limit=int(scenario.stage2.hotspot_top_tasks_per_range),
        )
        for hot_range in hot_ranges
    }
    augment_candidates = _collect_augment_candidates(
        scenario,
        plan,
        segments,
        baseline_schedule,
        hot_ranges,
        initial_classifications,
        contributions,
    )
    alternative_diagnostics = {
        hot_range.range_id: _build_hot_range_alternative_diagnostics(
            scenario,
            plan,
            segments,
            baseline_schedule,
            hot_range,
            contributions.get(hot_range.range_id, []),
            augment_candidates,
        )
        for hot_range in hot_ranges
    }
    classifications = {
        hot_range.range_id: classify_hot_range(
            initial_profile,
            hot_range,
            single_link_fraction_threshold=float(scenario.stage2.hotspot_single_link_fraction_threshold),
            feasible_alternative_cross_window_count=int(
                alternative_diagnostics[hot_range.range_id]["feasible_alternative_cross_window_count"]
            ),
            feasible_alternative_path_count=int(
                alternative_diagnostics[hot_range.range_id]["feasible_alternative_path_count"]
            ),
        )
        for hot_range in hot_ranges
    }
    sanity_warnings = [
        {
            "range_id": hot_range.range_id,
            "warning": warning,
        }
        for hot_range in hot_ranges
        for warning in classifications[hot_range.range_id].warnings
    ]
    selected_augment_candidates = _select_augment_windows(scenario, plan, augment_candidates)
    selected_augment_window_ids = [candidate.window_id for candidate in selected_augment_candidates]
    augmented_plan, removed_window_ids, applied_augment_window_ids = _apply_augmentation_to_plan(
        scenario,
        plan,
        baseline_schedule,
        hot_ranges,
        selected_augment_candidates,
    )
    augmented_segments = build_segments(scenario, augmented_plan, _regular_tasks(scenario))
    current_schedule = _remap_schedule_to_segments(segments, augmented_segments, baseline_schedule)
    current_diagnostics = build_regular_schedule_diagnostics(scenario, augmented_plan, augmented_segments, current_schedule)
    current_profile = build_cross_segment_profile(scenario, augmented_plan, augmented_segments, current_schedule)
    current_summary = _load_summary_from_profile(current_profile)
    current_cr_reg = _regular_completion_ratio_from_diagnostics(scenario, current_diagnostics)
    baseline_aug_summary = dict(current_summary)
    baseline_aug_cr_reg = float(current_cr_reg)
    regular_tasks = _regular_tasks(scenario)
    selected_augment_window_ids_set = set(selected_augment_window_ids)
    applied_augment_window_ids_set = set(applied_augment_window_ids)

    range_reports: list[dict[str, Any]] = []
    for hot_range in hot_ranges:
        classification = classifications[hot_range.range_id]
        selected_augment_for_range = sorted(
            candidate.window_id
            for candidate in selected_augment_candidates
            if candidate.range_id == hot_range.range_id and candidate.window_id in selected_augment_window_ids_set
        )
        applied_augment_for_range = sorted(
            candidate.window_id
            for candidate in augment_candidates
            if candidate.range_id == hot_range.range_id and candidate.window_id in applied_augment_window_ids_set
        )
        default_status = "structural_candidate_pruned" if classification.structural else "reroutable_candidate_pruned"
        range_profile = _range_profile_summary(current_profile, hot_range)
        hot_window_ids_by_segment = range_profile["hot_window_ids_by_segment"]
        promoted_task_segments = _hot_task_segments_for_range(
            scenario,
            hot_range,
            current_schedule,
            hot_window_ids_by_segment,
        )
        horizon = _horizon_segments_for_hot_range(scenario, augmented_segments, hot_range, profile=current_profile)
        range_report: dict[str, Any] = {
            "range_id": hot_range.range_id,
            "start": float(hot_range.start),
            "end": float(hot_range.end),
            "max_q_r": float(hot_range.max_q_r),
            "q_integral": float(hot_range.q_integral),
            "classification": asdict(classification),
            "alternative_diagnostics": alternative_diagnostics[hot_range.range_id],
            "contributing_tasks": [asdict(item) for item in contributions[hot_range.range_id]],
            "selected_augment_windows": selected_augment_for_range,
            "applied_augment_windows": applied_augment_for_range,
            "status": default_status,
            "accepted": False,
            "used_augment_windows": [],
            "candidate_solver_status": "not_attempted",
            "rejection_reason": None,
        }
        if classification.structural and not applied_augment_for_range:
            range_report["status"] = "structural_bottleneck"
            range_report["candidate_solver_status"] = "skipped_no_applied_augment"
            range_report["rejection_reason"] = "structural hotspot has no applied augment window"
            range_reports.append(range_report)
            continue

        if not classification.structural and not classification.reroutable:
            range_report["status"] = "blocked_no_feasible_reroute"
            range_report["candidate_solver_status"] = "skipped_not_reroutable"
            range_report["rejection_reason"] = str(classification.reason)
            range_reports.append(range_report)
            continue

        if not horizon:
            range_report["candidate_solver_status"] = "skipped_no_horizon"
            range_report["rejection_reason"] = "hot range has no local optimization horizon"
            range_reports.append(range_report)
            continue

        active_tasks = [
            task
            for task in regular_tasks
            if any(task.arrival <= segment.start < task.deadline for segment in horizon)
            and float(current_diagnostics["remaining_before_trace"][task.task_id].get(horizon[0].index, task.data)) > completion_tolerance(scenario, task)
        ]
        if not active_tasks:
            range_report["candidate_solver_status"] = "skipped_no_active_tasks"
            range_report["rejection_reason"] = "no active regular tasks in local horizon"
            range_reports.append(range_report)
            continue

        local_result = solve_regular_hotspot_local_milp(
            scenario=scenario,
            plan=augmented_plan,
            segments=augmented_segments,
            current_schedule=current_schedule,
            diagnostics=current_diagnostics,
            horizon_segments=horizon,
            active_tasks=active_tasks,
            hot_segment_indices=set(_hot_range_segment_indices(augmented_segments, hot_range)),
            hot_task_segments=promoted_task_segments,
            hot_window_ids_by_segment=hot_window_ids_by_segment,
            augmented_window_ids=applied_augment_window_ids_set,
        )
        if not local_result.get("accepted", False):
            range_report["candidate_solver_status"] = str(local_result.get("solver_status", "no_candidate_solution"))
            range_report["rejection_reason"] = str(local_result.get("solver_status", "no_candidate_solution"))
            range_reports.append(range_report)
            continue

        candidate_schedule = dict(local_result["schedule"])
        candidate_diagnostics = build_regular_schedule_diagnostics(scenario, augmented_plan, augmented_segments, candidate_schedule)
        candidate_profile = build_cross_segment_profile(scenario, augmented_plan, augmented_segments, candidate_schedule)
        candidate_summary = _load_summary_from_profile(candidate_profile)
        candidate_cr_reg = _regular_completion_ratio_from_diagnostics(scenario, candidate_diagnostics)
        epsilon = float(scenario.stage2.local_peak_accept_epsilon)
        if _relief_accepts(
            cr_reg_base=current_cr_reg,
            cr_reg_new=candidate_cr_reg,
            before_summary=current_summary,
            after_summary=candidate_summary,
            epsilon=epsilon,
        ):
            current_schedule = candidate_schedule
            current_diagnostics = candidate_diagnostics
            current_profile = candidate_profile
            current_summary = candidate_summary
            current_cr_reg = candidate_cr_reg
            range_report["accepted"] = True
            range_report["status"] = (
                "improved_after_augmentation"
                if local_result.get("used_augment_windows") or applied_augment_for_range
                else "reroute_improved"
            )
            range_report["used_augment_windows"] = sorted(local_result.get("used_augment_windows", []))
            range_report["candidate_solver_status"] = str(local_result.get("solver_status", "accepted"))
            range_report["objective_values"] = dict(local_result.get("objective_values", {}))
        else:
            range_report["candidate_solver_status"] = "rejected_by_global_acceptance"
            range_report["rejection_reason"] = "completion/peak/integral acceptance test not improved"
            range_report["candidate_before_after"] = {
                "before": {
                    "cr_reg": float(current_cr_reg),
                    **current_summary,
                },
                "after": {
                    "cr_reg": float(candidate_cr_reg),
                    **candidate_summary,
                },
            }
        range_reports.append(range_report)

    metadata = {
        "hotspot_relief_enabled": True,
        "structural_hot_range_count": sum(1 for item in classifications.values() if item.structural),
        "reroutable_hot_range_count": sum(1 for item in classifications.values() if item.reroutable),
        "hot_ranges_considered": len(hot_ranges),
        "augment_candidates_considered": len(augment_candidates),
        "augment_windows_selected": len(selected_augment_window_ids),
        "augment_windows_added": len(applied_augment_window_ids),
        "selected_augment_windows": list(selected_augment_window_ids),
        "applied_augment_windows": sorted(applied_augment_window_ids_set),
        "q_peak_before": float(baseline_aug_summary["q_peak"]),
        "q_peak_after": float(current_summary["q_peak"]),
        "peak_like_threshold_before": float(baseline_aug_summary["peak_like_threshold"]),
        "peak_like_threshold_after": float(current_summary["peak_like_threshold"]),
        "peak_segment_count_before": int(baseline_aug_summary["peak_segment_count"]),
        "peak_segment_count_after": int(current_summary["peak_segment_count"]),
        "q_integral_before": float(baseline_aug_summary["q_integral"]),
        "q_integral_after": float(current_summary["q_integral"]),
        "sanity_warning_count": len(sanity_warnings),
    }
    no_accepted_improvement = len(
        [item for item in range_reports if item["status"] in {"improved_after_augmentation", "reroute_improved"}]
    ) == 0
    stage1_peak_rescore_hook = {
        "recommended": (no_accepted_improvement and len(hot_ranges) > 0),
        "reason": (
            "no_accepted_hotspot_relief_improvement"
            if no_accepted_improvement
            else "accepted_hotspot_relief_improvement_exists"
        ),
    }
    metadata["stage1_peak_rescore_hook_recommended"] = bool(stage1_peak_rescore_hook["recommended"])
    report = {
        "hot_ranges": range_reports,
        "structural_bottleneck": [item for item in range_reports if item["status"] == "structural_bottleneck"],
        "structural_candidate_pruned": [item for item in range_reports if item["status"] == "structural_candidate_pruned"],
        "blocked_no_feasible_reroute": [item for item in range_reports if item["status"] == "blocked_no_feasible_reroute"],
        "reroutable_candidate_pruned": [item for item in range_reports if item["status"] == "reroutable_candidate_pruned"],
        "improved_after_augmentation": [item for item in range_reports if item["status"] == "improved_after_augmentation"],
        "reroute_improved": [item for item in range_reports if item["status"] == "reroute_improved"],
        "augment_candidates": [asdict(item) for item in augment_candidates],
        "selected_augment_windows": selected_augment_window_ids,
        "applied_augment_windows": sorted(applied_augment_window_ids_set),
        "removed_plan_windows": removed_window_ids,
        "stage1_peak_rescore_hook": stage1_peak_rescore_hook,
        "sanity_warnings": sanity_warnings,
        "before_after": {
            "before": {
                "cr_reg": float(baseline_aug_cr_reg),
                **baseline_aug_summary,
            },
            "after": {
                "cr_reg": float(current_cr_reg),
                **current_summary,
            },
        },
    }
    return HotspotReliefResult(
        plan=augmented_plan,
        schedule=current_schedule,
        segments=augmented_segments,
        diagnostics=current_diagnostics,
        metadata=metadata,
        report=report,
    )
