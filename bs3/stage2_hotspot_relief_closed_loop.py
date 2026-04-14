from __future__ import annotations

"""Legacy closed-loop stage2-1 logic kept out of the default pipeline."""

from collections import defaultdict
import time
from typing import Any

from .models import Allocation, ScheduledWindow, Scenario, Segment
from .regular_routing_common import build_regular_schedule_diagnostics
from .stage2_hotspot_relief import (
    EPS,
    _AUGMENT_FUNNEL_STAGE_MEANINGS,
    _augment_selection_policy,
    _base_range_report,
    _build_hot_range_alternative_diagnostics,
    _closed_loop_action_mode,
    _closed_loop_hard_window_cap_details,
    _closed_loop_hard_window_cap,
    _closed_loop_metrics,
    _closed_loop_topk_ranges_per_round,
    _collect_augment_candidates,
    _evaluate_augment_action,
    _evaluate_reroute_action,
    _hotspot_local_repair_time_limit_seconds,
    _hotspot_per_range_time_limit_seconds,
    _hotspot_total_time_limit_seconds,
    _regular_completion_ratio_from_diagnostics,
    _structural_repair_gate_enabled,
    _structural_repair_gate_time_limit_seconds,
    _total_elapsed_seconds,
    build_cross_segment_profile,
    classify_hot_range,
    collect_hot_contributing_tasks,
    detect_hot_ranges,
    HotspotReliefResult,
)


def run_hotspot_relief_closed_loop(
    scenario: Scenario,
    plan: list[ScheduledWindow],
    segments: list[Segment],
    baseline_schedule: dict[tuple[str, int], Allocation],
    baseline_diagnostics: dict[str, Any],
) -> HotspotReliefResult:
    started_at = time.perf_counter()
    total_time_limit_seconds = _hotspot_total_time_limit_seconds(scenario)
    gate_time_limit_seconds = _structural_repair_gate_time_limit_seconds(scenario)
    local_repair_time_limit_seconds = _hotspot_local_repair_time_limit_seconds(scenario)

    current_plan = list(plan)
    current_segments = list(segments)
    current_schedule = dict(baseline_schedule)
    current_diagnostics = (
        dict(baseline_diagnostics)
        if isinstance(baseline_diagnostics, dict)
        else build_regular_schedule_diagnostics(scenario, current_plan, current_segments, current_schedule)
    )
    current_profile = build_cross_segment_profile(scenario, current_plan, current_segments, current_schedule)
    current_metrics = _closed_loop_metrics(scenario, current_profile)
    current_cr_reg = _regular_completion_ratio_from_diagnostics(scenario, current_diagnostics)
    initial_metrics = dict(current_metrics)
    initial_cr_reg = float(current_cr_reg)

    round_reports: list[dict[str, Any]] = []
    flat_range_reports: list[dict[str, Any]] = []
    all_augment_candidates: list[dict[str, Any]] = []
    selected_augment_windows: list[str] = []
    applied_augment_windows: list[str] = []
    closed_loop_actions: list[dict[str, Any]] = []
    sanity_warnings: list[dict[str, Any]] = []
    bounded_time_budget_skipped_range_ids: list[str] = []
    total_hot_ranges_considered = 0
    total_augment_candidates_considered = 0
    initial_structural_hot_range_count = 0
    initial_reroutable_hot_range_count = 0
    stop_reason = "not_started"
    max_rounds = max(int(getattr(scenario.stage2, "closed_loop_max_rounds", 0)), 0)
    hard_new_window_cap = _closed_loop_hard_window_cap(scenario)
    hard_window_cap_details = _closed_loop_hard_window_cap_details(scenario)
    new_windows_added = 0

    while True:
        round_index = len(round_reports) + 1
        if max_rounds and len(round_reports) >= max_rounds:
            stop_reason = "max_rounds_reached"
            break
        if total_time_limit_seconds is not None and _total_elapsed_seconds(started_at) > float(total_time_limit_seconds) + EPS:
            stop_reason = "total_time_budget_exhausted"
            break

        round_started_at = time.perf_counter()
        round_before_metrics = dict(current_metrics)
        round_before_cr_reg = float(current_cr_reg)
        hot_ranges = detect_hot_ranges(
            current_profile,
            threshold=float(scenario.stage2.hotspot_util_threshold),
            topk=_closed_loop_topk_ranges_per_round(scenario),
        )
        if not hot_ranges:
            stop_reason = "target_threshold_satisfied"
            break

        total_hot_ranges_considered += len(hot_ranges)
        initial_classifications = {
            hot_range.range_id: classify_hot_range(
                current_profile,
                hot_range,
                single_link_fraction_threshold=float(scenario.stage2.hotspot_single_link_fraction_threshold),
            )
            for hot_range in hot_ranges
        }
        contributions = {
            hot_range.range_id: collect_hot_contributing_tasks(
                scenario,
                current_plan,
                current_segments,
                current_schedule,
                hot_range,
                limit=int(scenario.stage2.hotspot_top_tasks_per_range),
            )
            for hot_range in hot_ranges
        }
        augment_candidates, augment_debug_by_range = _collect_augment_candidates(
            scenario,
            current_plan,
            current_segments,
            current_schedule,
            hot_ranges,
            initial_classifications,
            contributions,
        )
        total_augment_candidates_considered += len(augment_candidates)
        all_augment_candidates.extend(item.__dict__ for item in augment_candidates)
        alternative_diagnostics = {
            hot_range.range_id: _build_hot_range_alternative_diagnostics(
                scenario,
                current_plan,
                current_segments,
                current_schedule,
                hot_range,
                contributions.get(hot_range.range_id, []),
                augment_debug_by_range.get(hot_range.range_id),
            )
            for hot_range in hot_ranges
        }
        classifications = {
            hot_range.range_id: classify_hot_range(
                current_profile,
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
        if round_index == 1:
            initial_structural_hot_range_count = sum(1 for item in classifications.values() if item.structural)
            initial_reroutable_hot_range_count = sum(1 for item in classifications.values() if item.reroutable)
        round_sanity_warnings = [
            {
                "round_index": int(round_index),
                "range_id": hot_range.range_id,
                "warning": warning,
            }
            for hot_range in hot_ranges
            for warning in classifications[hot_range.range_id].warnings
        ]
        sanity_warnings.extend(round_sanity_warnings)
        augment_candidates_by_range: dict[str, list[Any]] = defaultdict(list)
        for candidate in augment_candidates:
            augment_candidates_by_range[candidate.range_id].append(candidate)

        range_reports: list[dict[str, Any]] = []
        evaluated_actions: list[dict[str, Any]] = []
        accepted_actions: list[dict[str, Any]] = []

        for hot_range in hot_ranges:
            classification = classifications[hot_range.range_id]
            warnings_for_range = [
                item["warning"]
                for item in round_sanity_warnings
                if item["range_id"] == hot_range.range_id
            ]
            range_report = _base_range_report(
                hot_range,
                classification,
                contributions=contributions.get(hot_range.range_id, []),
                alternative_diagnostics=alternative_diagnostics[hot_range.range_id],
                augment_debug=augment_debug_by_range.get(hot_range.range_id) or {},
                sanity_warnings=warnings_for_range,
            )
            range_report["round_index"] = int(round_index)
            range_report["candidate_available_not_selected"] = False
            range_report["candidate_actions"] = []
            range_candidates = list(augment_candidates_by_range.get(hot_range.range_id, []))
            range_report["selected_augment_windows"] = [candidate.window_id for candidate in range_candidates]

            if bool(classification.structural):
                range_report["candidate_action_type"] = "augment"
                if new_windows_added >= hard_new_window_cap:
                    range_report["status"] = "structural_candidate_pruned"
                    range_report["candidate_solver_status"] = "SkippedHardWindowCap"
                    range_report["rejection_reason"] = "closed_loop_new_window_hard_cap_reached"
                elif not range_candidates:
                    range_report["status"] = "structural_bottleneck"
                    range_report["candidate_solver_status"] = "SkippedNoAugmentCandidates"
                    range_report["rejection_reason"] = "structural hotspot has no viable augment window"
                else:
                    for candidate in range_candidates:
                        action = _evaluate_augment_action(
                            scenario,
                            plan=current_plan,
                            segments=current_segments,
                            schedule=current_schedule,
                            diagnostics=current_diagnostics,
                            profile=current_profile,
                            current_metrics=current_metrics,
                            current_cr_reg=current_cr_reg,
                            hot_range=hot_range,
                            classification=classification,
                            augment_candidate=candidate,
                            contributors=contributions.get(hot_range.range_id, []),
                            total_time_limit_seconds=total_time_limit_seconds,
                            gate_time_limit_seconds=gate_time_limit_seconds,
                            started_at=started_at,
                        )
                        action_summary = {
                            "round_index": int(round_index),
                            "range_id": hot_range.range_id,
                            "action_type": "augment",
                            "window_id": candidate.window_id,
                            "accepted_candidate": bool(action.get("accepted", False)),
                            "rejection_reason": action.get("rejection_reason"),
                            "candidate_solver_status": action.get("candidate_solver_status"),
                            "candidate_solver_error": action.get("candidate_solver_error"),
                            "detailed_runtime_failure_type": action.get("detailed_runtime_failure_type"),
                            "improvement": dict(action.get("improvement") or {}),
                            "objective_values": dict(action.get("objective_values") or {}),
                        }
                        evaluated_actions.append(action_summary)
                        range_report["candidate_actions"].append(action_summary)
                        if bool(action.get("accepted", False)):
                            accepted_actions.append(action)
                        else:
                            range_report["candidate_solver_status"] = str(action.get("candidate_solver_status"))
                            range_report["candidate_solver_error"] = action.get("candidate_solver_error")
                            range_report["detailed_runtime_failure_type"] = action.get("detailed_runtime_failure_type")
                            range_report["rejection_reason"] = action.get("rejection_reason")
            elif bool(classification.reroutable):
                range_report["candidate_action_type"] = "reroute"
                action = _evaluate_reroute_action(
                    scenario,
                    plan=current_plan,
                    segments=current_segments,
                    schedule=current_schedule,
                    diagnostics=current_diagnostics,
                    profile=current_profile,
                    current_metrics=current_metrics,
                    current_cr_reg=current_cr_reg,
                    hot_range=hot_range,
                    classification=classification,
                    total_time_limit_seconds=total_time_limit_seconds,
                    local_repair_time_limit_seconds=local_repair_time_limit_seconds,
                    started_at=started_at,
                )
                action_summary = {
                    "round_index": int(round_index),
                    "range_id": hot_range.range_id,
                    "action_type": "reroute",
                    "window_id": None,
                    "accepted_candidate": bool(action.get("accepted", False)),
                    "rejection_reason": action.get("rejection_reason"),
                    "candidate_solver_status": action.get("candidate_solver_status"),
                    "candidate_solver_error": action.get("candidate_solver_error"),
                    "detailed_runtime_failure_type": action.get("detailed_runtime_failure_type"),
                    "improvement": dict(action.get("improvement") or {}),
                    "objective_values": dict(action.get("objective_values") or {}),
                }
                evaluated_actions.append(action_summary)
                range_report["candidate_actions"].append(action_summary)
                if bool(action.get("accepted", False)):
                    accepted_actions.append(action)
                else:
                    range_report["status"] = (
                        "bounded_time_budget_skipped"
                        if "time_budget" in str(action.get("rejection_reason") or "")
                        else "reroutable_candidate_pruned"
                    )
                    range_report["candidate_solver_status"] = str(action.get("candidate_solver_status"))
                    range_report["candidate_solver_error"] = action.get("candidate_solver_error")
                    range_report["detailed_runtime_failure_type"] = action.get("detailed_runtime_failure_type")
                    range_report["rejection_reason"] = action.get("rejection_reason")
            else:
                range_report["status"] = "blocked_no_feasible_reroute"
                range_report["candidate_action_type"] = "reroute"
                range_report["candidate_solver_status"] = "SkippedNotReroutable"
                range_report["rejection_reason"] = str(classification.reason)

            range_reports.append(range_report)

        def action_sort_key(action: dict[str, Any]) -> tuple[float | int | str, ...]:
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

        chosen_action = min(accepted_actions, key=action_sort_key) if accepted_actions else None
        round_after_metrics = dict(round_before_metrics)
        round_after_cr_reg = float(round_before_cr_reg)
        chosen_action_summary = None

        if chosen_action is None:
            stop_reason = "no_acceptable_action"
            for range_report in range_reports:
                if range_report["status"] == "pending":
                    candidate_available = any(
                        bool(item.get("accepted_candidate", False))
                        for item in range_report.get("candidate_actions", [])
                    )
                    range_report["candidate_available_not_selected"] = bool(candidate_available)
                    range_report["status"] = (
                        "candidate_available_not_selected"
                        if candidate_available
                        else (
                            "structural_candidate_pruned"
                            if range_report["candidate_action_type"] == "augment"
                            else "reroutable_candidate_pruned"
                        )
                    )
                    if not range_report["rejection_reason"]:
                        range_report["rejection_reason"] = (
                            "better_global_action_selected"
                            if candidate_available
                            else (
                                "no_structural_augment_candidate_passed_validation"
                                if range_report["candidate_action_type"] == "augment"
                                else "no_reroute_candidate_passed_validation"
                            )
                        )
                if range_report["status"] == "bounded_time_budget_skipped" and range_report["range_id"] not in bounded_time_budget_skipped_range_ids:
                    bounded_time_budget_skipped_range_ids.append(range_report["range_id"])
        else:
            current_plan = list(chosen_action["plan"])
            current_segments = list(chosen_action["segments"])
            current_schedule = dict(chosen_action["schedule"])
            current_diagnostics = dict(chosen_action["diagnostics"])
            current_profile = list(chosen_action["profile"])
            current_metrics = dict(chosen_action["metrics"])
            current_cr_reg = float(chosen_action["cr_reg"])
            round_after_metrics = dict(current_metrics)
            round_after_cr_reg = float(current_cr_reg)
            if str(chosen_action.get("action_type")) == "augment":
                selected_augment_windows.append(str(chosen_action["window_id"]))
                applied_augment_windows.append(str(chosen_action["window_id"]))
                new_windows_added += 1
            chosen_action_summary = {
                "round_index": int(round_index),
                "action_type": str(chosen_action.get("action_type")),
                "range_id": str(chosen_action.get("range_id")),
                "window_id": chosen_action.get("window_id"),
                "used_augment_windows": list(chosen_action.get("used_augment_windows") or []),
                "improvement": dict(chosen_action.get("improvement") or {}),
                "objective_values": dict(chosen_action.get("objective_values") or {}),
            }
            closed_loop_actions.append(chosen_action_summary)
            for range_report in range_reports:
                if range_report["range_id"] == str(chosen_action.get("range_id")):
                    range_report["accepted"] = True
                    range_report["status"] = (
                        "improved_after_augmentation"
                        if str(chosen_action.get("action_type")) == "augment"
                        else "reroute_improved"
                    )
                    range_report["applied_augment_windows"] = (
                        [str(chosen_action["window_id"])]
                        if str(chosen_action.get("action_type")) == "augment" and chosen_action.get("window_id")
                        else []
                    )
                    range_report["used_augment_windows"] = list(chosen_action.get("used_augment_windows") or [])
                    range_report["candidate_solver_status"] = str(chosen_action.get("candidate_solver_status"))
                    range_report["candidate_solver_error"] = chosen_action.get("candidate_solver_error")
                    range_report["detailed_runtime_failure_type"] = chosen_action.get("detailed_runtime_failure_type")
                    range_report["objective_values"] = dict(chosen_action.get("objective_values") or {})
                    range_report["improvement"] = dict(chosen_action.get("improvement") or {})
                elif range_report["status"] == "pending":
                    candidate_available = any(
                        bool(item.get("accepted_candidate", False))
                        for item in range_report.get("candidate_actions", [])
                    )
                    range_report["candidate_available_not_selected"] = bool(candidate_available)
                    range_report["status"] = (
                        "candidate_available_not_selected"
                        if candidate_available
                        else (
                            "structural_candidate_pruned"
                            if range_report["candidate_action_type"] == "augment"
                            else "reroutable_candidate_pruned"
                        )
                    )
                    range_report["rejection_reason"] = (
                        "better_global_action_selected"
                        if candidate_available
                        else (
                            range_report.get("rejection_reason")
                            or (
                                "no_structural_augment_candidate_passed_validation"
                                if range_report["candidate_action_type"] == "augment"
                                else "no_reroute_candidate_passed_validation"
                            )
                        )
                    )

        round_report = {
            "round_index": int(round_index),
            "before": {"cr_reg": float(round_before_cr_reg), **round_before_metrics},
            "after": {"cr_reg": float(round_after_cr_reg), **round_after_metrics},
            "hot_range_ids": [hot_range.range_id for hot_range in hot_ranges],
            "evaluated_actions": evaluated_actions,
            "range_reports": range_reports,
            "chosen_action": chosen_action_summary,
            "new_windows_added_total": int(new_windows_added),
            "elapsed_seconds": float(_total_elapsed_seconds(round_started_at)),
            "stop_reason": (None if chosen_action is not None else stop_reason),
        }
        round_reports.append(round_report)
        flat_range_reports.extend(range_reports)
        if chosen_action is None:
            break

    did_finish_within_bound = (
        True if total_time_limit_seconds is None else _total_elapsed_seconds(started_at) <= float(total_time_limit_seconds) + EPS
    )
    stage1_peak_rescore_hook = {
        "recommended": (len(closed_loop_actions) == 0 and total_hot_ranges_considered > 0),
        "reason": (
            "no_accepted_hotspot_relief_improvement"
            if len(closed_loop_actions) == 0
            else "accepted_hotspot_relief_improvement_exists"
        ),
    }
    metadata = {
        "hotspot_relief_enabled": True,
        "closed_loop_relief_enabled": True,
        "closed_loop_action_mode": _closed_loop_action_mode(scenario),
        "closed_loop_max_rounds": max_rounds,
        "closed_loop_max_new_windows": int(getattr(scenario.stage2, "closed_loop_max_new_windows", 0)),
        "closed_loop_rounds_completed": len(round_reports),
        "closed_loop_actions_accepted": len(closed_loop_actions),
        "closed_loop_actions_considered": sum(len(item["evaluated_actions"]) for item in round_reports),
        "closed_loop_new_windows_added": int(new_windows_added),
        "closed_loop_new_window_hard_cap": int(hard_window_cap_details["effective_hard_cap"]),
        "closed_loop_new_window_hard_cap_limiter": str(hard_window_cap_details["effective_hard_cap_limiter"]),
        "closed_loop_new_window_hard_cap_components": dict(hard_window_cap_details),
        "closed_loop_stop_reason": stop_reason,
        "structural_hot_range_count": int(initial_structural_hot_range_count),
        "reroutable_hot_range_count": int(initial_reroutable_hot_range_count),
        "hot_ranges_considered": int(total_hot_ranges_considered),
        "augment_candidates_considered": int(total_augment_candidates_considered),
        "augment_windows_selected": len(selected_augment_windows),
        "augment_windows_added": len(applied_augment_windows),
        "augment_selection_policy": _augment_selection_policy(scenario),
        "structural_repair_gate_enabled": _structural_repair_gate_enabled(scenario),
        "structural_hotspot_starvation_count": 0,
        "structural_hotspot_starvation_range_ids": [],
        "released_provisional_augment_windows": [],
        "reallocated_augment_windows_after_release": [],
        "selected_augment_windows": list(selected_augment_windows),
        "applied_augment_windows": list(applied_augment_windows),
        "q_peak_before": float(initial_metrics["q_peak"]),
        "q_peak_after": float(current_metrics["q_peak"]),
        "peak_like_threshold_before": float(initial_metrics["peak_like_threshold"]),
        "peak_like_threshold_after": float(current_metrics["peak_like_threshold"]),
        "peak_segment_count_before": int(initial_metrics["peak_segment_count"]),
        "peak_segment_count_after": int(current_metrics["peak_segment_count"]),
        "high_segment_count_before": int(initial_metrics["high_segment_count"]),
        "high_segment_count_after": int(current_metrics["high_segment_count"]),
        "q_integral_before": float(initial_metrics["q_integral"]),
        "q_integral_after": float(current_metrics["q_integral"]),
        "sanity_warning_count": len(sanity_warnings),
        "elapsed_seconds": float(_total_elapsed_seconds(started_at)),
        "did_finish_within_bound": did_finish_within_bound,
        "hotspot_total_time_limit_seconds": total_time_limit_seconds,
        "hotspot_per_range_time_limit_seconds": _hotspot_per_range_time_limit_seconds(scenario),
        "structural_repair_gate_time_limit_seconds": gate_time_limit_seconds,
        "hotspot_local_repair_time_limit_seconds": local_repair_time_limit_seconds,
        "bounded_time_budget_skipped_range_ids": list(dict.fromkeys(bounded_time_budget_skipped_range_ids)),
        "stage1_peak_rescore_hook_recommended": bool(stage1_peak_rescore_hook["recommended"]),
    }
    report = {
        "hot_ranges": flat_range_reports,
        "structural_bottleneck": [item for item in flat_range_reports if item["status"] == "structural_bottleneck"],
        "structural_candidate_pruned": [item for item in flat_range_reports if item["status"] == "structural_candidate_pruned"],
        "bounded_time_budget_skipped": [item for item in flat_range_reports if item["status"] == "bounded_time_budget_skipped"],
        "blocked_no_feasible_reroute": [item for item in flat_range_reports if item["status"] == "blocked_no_feasible_reroute"],
        "reroutable_candidate_pruned": [item for item in flat_range_reports if item["status"] == "reroutable_candidate_pruned"],
        "improved_after_augmentation": [item for item in flat_range_reports if item["status"] == "improved_after_augmentation"],
        "reroute_improved": [item for item in flat_range_reports if item["status"] == "reroute_improved"],
        "augment_candidates": all_augment_candidates,
        "selected_augment_windows": list(selected_augment_windows),
        "applied_augment_windows": list(applied_augment_windows),
        "removed_plan_windows": [],
        "augment_selection_policy": _augment_selection_policy(scenario),
        "closed_loop_action_mode": _closed_loop_action_mode(scenario),
        "structural_repair_gate_enabled": _structural_repair_gate_enabled(scenario),
        "released_provisional_augment_windows": [],
        "reallocated_augment_windows_after_release": [],
        "closed_loop_new_window_hard_cap": int(hard_window_cap_details["effective_hard_cap"]),
        "closed_loop_new_window_hard_cap_limiter": str(hard_window_cap_details["effective_hard_cap_limiter"]),
        "closed_loop_new_window_hard_cap_components": dict(hard_window_cap_details),
        "augment_funnel_stage_meanings": dict(_AUGMENT_FUNNEL_STAGE_MEANINGS),
        "structural_hotspot_starvation": [],
        "stage1_peak_rescore_hook": stage1_peak_rescore_hook,
        "sanity_warnings": sanity_warnings,
        "elapsed_seconds": float(_total_elapsed_seconds(started_at)),
        "did_finish_within_bound": did_finish_within_bound,
        "hotspot_total_time_limit_seconds": total_time_limit_seconds,
        "hotspot_per_range_time_limit_seconds": _hotspot_per_range_time_limit_seconds(scenario),
        "structural_repair_gate_time_limit_seconds": gate_time_limit_seconds,
        "hotspot_local_repair_time_limit_seconds": local_repair_time_limit_seconds,
        "bounded_time_budget_skipped_range_ids": list(dict.fromkeys(bounded_time_budget_skipped_range_ids)),
        "closed_loop_actions": closed_loop_actions,
        "rounds": round_reports,
        "before_after": {
            "before": {"cr_reg": float(initial_cr_reg), **initial_metrics},
            "after": {"cr_reg": float(current_cr_reg), **current_metrics},
        },
    }
    return HotspotReliefResult(
        plan=current_plan,
        schedule=current_schedule,
        segments=current_segments,
        diagnostics=current_diagnostics,
        metadata=metadata,
        report=report,
    )
