from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, replace
from datetime import datetime
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from bs3.models import ScheduledWindow
from bs3.scenario import load_scenario
from bs3.stage2 import run_stage2


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def _load_stage1_plan(stage1_result_path: Path, candidate_index: int) -> list[ScheduledWindow]:
    payload = json.loads(stage1_result_path.read_text(encoding="utf-8"))
    candidates = payload.get("best_feasible") or []
    if candidates:
        plan_rows = candidates[candidate_index]["plan"]
    else:
        population_best = payload.get("population_best")
        if not population_best:
            raise ValueError(f"No plan found in {stage1_result_path}")
        plan_rows = population_best["plan"]
    return [ScheduledWindow(**row) for row in plan_rows]


def _load_plan_json(path: Path) -> list[ScheduledWindow]:
    return [ScheduledWindow(**row) for row in json.loads(path.read_text(encoding="utf-8"))]


def _result_to_dict(result) -> dict[str, Any]:
    data = asdict(result)
    data["plan"] = [asdict(window) for window in result.plan]
    data["allocations"] = [asdict(item) for item in result.allocations]
    return data


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Stage2 hotspot relief on a fixed Stage1 plan.")
    parser.add_argument("--scenario-path", required=True, help="Scenario JSON path")
    parser.add_argument("--stage1-result-path", help="Stage1 result JSON used to load best_feasible plan")
    parser.add_argument("--fixed-plan-path", help="Existing fixed plan JSON (list[ScheduledWindow])")
    parser.add_argument("--candidate-index", type=int, default=0, help="best_feasible plan index when --stage1-result-path is used")
    parser.add_argument("--output-dir", default=None, help="Output directory")
    parser.add_argument(
        "--baseline-mode",
        choices=["stage1_greedy", "stage1_greedy_repair", "full_milp", "rolling_milp"],
        default="stage1_greedy_repair",
        help="Stage2 regular-task baseline mode; default keeps the hotspot-relief mainline on stage1_greedy_repair",
    )
    parser.add_argument("--augment-mode", choices=["augment_only", "swap_if_budgeted"], default="augment_only")
    parser.add_argument("--fixed-window-count", type=int, default=None, help="Used only with --augment-mode=swap_if_budgeted")
    parser.add_argument("--milp-time-limit-seconds", type=float, default=None, help="Optional CBC time limit for local MILP")
    parser.add_argument("--milp-relative-gap", type=float, default=None, help="Optional CBC relative gap for local MILP")
    parser.add_argument("--hotspot-total-time-limit-seconds", type=float, default=None, help="Hard bound for the full hotspot-relief pass")
    parser.add_argument("--hotspot-per-range-time-limit-seconds", type=float, default=None, help="Hard bound per hotspot range across gate + local repair")
    parser.add_argument("--structural-repair-gate-time-limit-seconds", type=float, default=None, help="Hard bound for the structural repair gate")
    parser.add_argument("--hotspot-local-repair-time-limit-seconds", type=float, default=None, help="Hard bound for each local hotspot repair MILP")
    parser.add_argument("--hotspot-topk-ranges", type=int, default=None, help="Override hotspot_topk_ranges")
    parser.add_argument("--local-peak-horizon-cap-segments", type=int, default=None, help="Override local_peak_horizon_cap_segments")
    parser.add_argument("--hot-path-limit", type=int, default=None, help="Override hot_path_limit")
    parser.add_argument("--augment-top-windows-per-range", type=int, default=None, help="Override augment_top_windows_per_range")
    parser.add_argument("--augment-window-budget", type=int, default=None, help="Override augment_window_budget")
    parser.add_argument("--closed-loop-max-rounds", type=int, default=None, help="Override closed_loop_max_rounds")
    parser.add_argument("--closed-loop-max-new-windows", type=int, default=None, help="Override closed_loop_max_new_windows")
    parser.add_argument("--closed-loop-topk-ranges-per-round", type=int, default=None, help="Override closed_loop_topk_ranges_per_round")
    parser.add_argument("--closed-loop-topk-candidates-per-range", type=int, default=None, help="Override closed_loop_topk_candidates_per_range")
    parser.add_argument(
        "--closed-loop-action-mode",
        choices=["reroute_then_augment", "best_global_action"],
        default=None,
        help="Override closed_loop_action_mode",
    )
    parser.add_argument(
        "--augment-selection-policy",
        choices=["global_score_only", "structural_coverage_first"],
        default=None,
        help="Override augment selection policy",
    )
    parser.add_argument(
        "--enable-structural-repair-gate",
        action="store_true",
        help="Enable the very-small structural repair gate for provisional structural augment slots",
    )
    args = parser.parse_args()

    scenario_path = Path(args.scenario_path)
    output_dir = (
        Path(args.output_dir)
        if args.output_dir
        else Path(__file__).resolve().parents[1] / "results" / "generated" / "stage2_hotspot_relief" / datetime.now().strftime("%Y%m%d_%H%M%S")
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    if args.fixed_plan_path:
        input_plan = _load_plan_json(Path(args.fixed_plan_path))
        plan_source = str(Path(args.fixed_plan_path))
    elif args.stage1_result_path:
        input_plan = _load_stage1_plan(Path(args.stage1_result_path), args.candidate_index)
        plan_source = str(Path(args.stage1_result_path))
    else:
        raise ValueError("Either --fixed-plan-path or --stage1-result-path is required")

    scenario = load_scenario(scenario_path)
    effective_milp_mode = "rolling" if args.baseline_mode == "rolling_milp" else "full"
    effective_regular_mode = args.baseline_mode
    prefer_milp = effective_regular_mode in {"full_milp", "rolling_milp"}
    scenario = replace(
        scenario,
        stage2=replace(
            scenario.stage2,
            prefer_milp=prefer_milp,
            milp_mode=effective_milp_mode,
            regular_baseline_mode=effective_regular_mode,
            hotspot_relief_enabled=True,
            closed_loop_relief_enabled=True,
            fail_if_milp_disabled=False,
            milp_time_limit_seconds=(
                float(args.milp_time_limit_seconds)
                if args.milp_time_limit_seconds is not None
                else scenario.stage2.milp_time_limit_seconds
            ),
            milp_relative_gap=(
                float(args.milp_relative_gap)
                if args.milp_relative_gap is not None
                else scenario.stage2.milp_relative_gap
            ),
            hotspot_topk_ranges=(
                int(args.hotspot_topk_ranges)
                if args.hotspot_topk_ranges is not None
                else scenario.stage2.hotspot_topk_ranges
            ),
            local_peak_horizon_cap_segments=(
                int(args.local_peak_horizon_cap_segments)
                if args.local_peak_horizon_cap_segments is not None
                else scenario.stage2.local_peak_horizon_cap_segments
            ),
            hot_path_limit=(
                int(args.hot_path_limit)
                if args.hot_path_limit is not None
                else scenario.stage2.hot_path_limit
            ),
            augment_top_windows_per_range=(
                int(args.augment_top_windows_per_range)
                if args.augment_top_windows_per_range is not None
                else scenario.stage2.augment_top_windows_per_range
            ),
            augment_window_budget=(
                int(args.augment_window_budget)
                if args.augment_window_budget is not None
                else scenario.stage2.augment_window_budget
            ),
            closed_loop_max_rounds=(
                int(args.closed_loop_max_rounds)
                if args.closed_loop_max_rounds is not None
                else scenario.stage2.closed_loop_max_rounds
            ),
            closed_loop_max_new_windows=(
                int(args.closed_loop_max_new_windows)
                if args.closed_loop_max_new_windows is not None
                else scenario.stage2.closed_loop_max_new_windows
            ),
            closed_loop_topk_ranges_per_round=(
                int(args.closed_loop_topk_ranges_per_round)
                if args.closed_loop_topk_ranges_per_round is not None
                else scenario.stage2.closed_loop_topk_ranges_per_round
            ),
            closed_loop_topk_candidates_per_range=(
                int(args.closed_loop_topk_candidates_per_range)
                if args.closed_loop_topk_candidates_per_range is not None
                else scenario.stage2.closed_loop_topk_candidates_per_range
            ),
            closed_loop_action_mode=(
                str(args.closed_loop_action_mode)
                if args.closed_loop_action_mode is not None
                else scenario.stage2.closed_loop_action_mode
            ),
            augment_selection_policy=(
                str(args.augment_selection_policy)
                if args.augment_selection_policy is not None
                else scenario.stage2.augment_selection_policy
            ),
        ),
        metadata={
            **dict(scenario.metadata),
            "hotspot_augment_mode": args.augment_mode,
            "structural_repair_gate_enabled": bool(args.enable_structural_repair_gate),
            **(
                {"hotspot_total_time_limit_seconds": float(args.hotspot_total_time_limit_seconds)}
                if args.hotspot_total_time_limit_seconds is not None
                else {}
            ),
            **(
                {"hotspot_per_range_time_limit_seconds": float(args.hotspot_per_range_time_limit_seconds)}
                if args.hotspot_per_range_time_limit_seconds is not None
                else {}
            ),
            **(
                {"structural_repair_gate_time_limit_seconds": float(args.structural_repair_gate_time_limit_seconds)}
                if args.structural_repair_gate_time_limit_seconds is not None
                else {}
            ),
            **(
                {"hotspot_local_repair_time_limit_seconds": float(args.hotspot_local_repair_time_limit_seconds)}
                if args.hotspot_local_repair_time_limit_seconds is not None
                else {}
            ),
            **(
                {"hotspot_fixed_plan_window_count": int(args.fixed_window_count)}
                if args.fixed_window_count is not None
                else {}
            ),
        },
    )

    result = run_stage2(scenario, input_plan)
    hotspot_report = dict(result.metadata.get("hotspot_report") or {})
    before_after = dict(hotspot_report.get("before_after") or {})
    unresolved_structural_source = list(hotspot_report.get("structural_bottleneck", [])) + list(hotspot_report.get("structural_candidate_pruned", []))
    unresolved_structural_hotspots = [
        {
            "range_id": item.get("range_id"),
            "augment_funnel_counts": item.get("augment_funnel_counts", {}),
            "structurally_starved_by_selection_policy": item.get("structurally_starved_by_selection_policy", False),
            "top_rejected_augment_candidates": [
                candidate
                for candidate in item.get("augment_debug_top_candidates", [])
                if candidate.get("rejection_reason")
            ][:10],
            "selected_augment_windows": item.get("selected_augment_windows", []),
            "applied_augment_windows": item.get("applied_augment_windows", []),
            "structural_repair_gate_attempted": item.get("structural_repair_gate_attempted", False),
            "structural_repair_gate_accepted": item.get("structural_repair_gate_accepted", False),
            "structural_repair_gate_rejection_reason": item.get("structural_repair_gate_rejection_reason"),
            "structural_repair_gate_local_before_after": item.get("structural_repair_gate_local_before_after"),
            "structural_repair_gate_solver_status": item.get("structural_repair_gate_solver_status"),
            "structural_repair_gate_solver_error": item.get("structural_repair_gate_solver_error"),
            "released_provisional_augment_windows": item.get("released_provisional_augment_windows", []),
            "reallocated_augment_windows_after_release": item.get("reallocated_augment_windows_after_release", []),
            "detailed_runtime_failure_type": item.get("detailed_runtime_failure_type"),
            "fallback_local_swap": item.get("fallback_local_swap", {}),
            "rejection_reason": item.get("rejection_reason"),
        }
        for item in unresolved_structural_source
    ]
    hot_range_5 = next((item for item in hotspot_report.get("hot_ranges", []) if item.get("range_id") == "hot_range_5"), {})
    structural_hotspot_starvation_range_ids = list(result.metadata.get("structural_hotspot_starvation_range_ids", []))
    summary = {
        "scenario_path": str(scenario_path),
        "plan_source": plan_source,
        "solver_mode": result.solver_mode,
        "baseline_mode": effective_regular_mode,
        "prefer_milp": result.metadata.get("prefer_milp"),
        "closed_loop_action_mode": result.metadata.get("closed_loop_action_mode"),
        "closed_loop_rounds_completed": result.metadata.get("closed_loop_rounds_completed"),
        "closed_loop_stop_reason": result.metadata.get("closed_loop_stop_reason"),
        "closed_loop_new_windows_added": result.metadata.get("closed_loop_new_windows_added"),
        "closed_loop_new_window_hard_cap": result.metadata.get("closed_loop_new_window_hard_cap"),
        "closed_loop_new_window_hard_cap_limiter": result.metadata.get("closed_loop_new_window_hard_cap_limiter"),
        "closed_loop_new_window_hard_cap_components": result.metadata.get("closed_loop_new_window_hard_cap_components"),
        "augment_selection_policy": scenario.stage2.augment_selection_policy,
        "structural_repair_gate_enabled": bool(args.enable_structural_repair_gate),
        "elapsed_seconds": result.metadata.get("elapsed_seconds"),
        "did_finish_within_bound": result.metadata.get("did_finish_within_bound"),
        "hotspot_total_time_limit_seconds": result.metadata.get("hotspot_total_time_limit_seconds"),
        "hotspot_per_range_time_limit_seconds": result.metadata.get("hotspot_per_range_time_limit_seconds"),
        "structural_repair_gate_time_limit_seconds": result.metadata.get("structural_repair_gate_time_limit_seconds"),
        "hotspot_local_repair_time_limit_seconds": result.metadata.get("hotspot_local_repair_time_limit_seconds"),
        "plan_window_count_before": len(input_plan),
        "plan_window_count_after": len(result.plan),
        "cr_reg_before": before_after.get("before", {}).get("cr_reg"),
        "cr_reg_after": before_after.get("after", {}).get("cr_reg"),
        "q_peak_before": result.metadata.get("q_peak_before"),
        "q_peak_after": result.metadata.get("q_peak_after"),
        "peak_like_threshold_before": result.metadata.get("peak_like_threshold_before"),
        "peak_like_threshold_after": result.metadata.get("peak_like_threshold_after"),
        "peak_segment_count_before": result.metadata.get("peak_segment_count_before"),
        "peak_segment_count_after": result.metadata.get("peak_segment_count_after"),
        "q_integral_before": result.metadata.get("q_integral_before"),
        "q_integral_after": result.metadata.get("q_integral_after"),
        "selected_augment_windows": hotspot_report.get("selected_augment_windows", []),
        "applied_augment_windows": hotspot_report.get("applied_augment_windows", []),
        "augment_top_windows_per_range": scenario.stage2.augment_top_windows_per_range,
        "augment_window_budget": scenario.stage2.augment_window_budget,
        "structural_hotspot_starvation_count": int(result.metadata.get("structural_hotspot_starvation_count", 0) or 0),
        "structural_hotspot_starvation_range_ids": structural_hotspot_starvation_range_ids,
        "released_provisional_augment_windows": hotspot_report.get("released_provisional_augment_windows", []),
        "reallocated_augment_windows_after_release": hotspot_report.get("reallocated_augment_windows_after_release", []),
        "bounded_time_budget_skipped": {
            "count": len(hotspot_report.get("bounded_time_budget_skipped", [])),
            "range_ids": [item["range_id"] for item in hotspot_report.get("bounded_time_budget_skipped", [])],
        },
        "fixed_plan_structural_bottleneck": {
            "count": len(hotspot_report.get("structural_bottleneck", [])),
            "range_ids": [item["range_id"] for item in hotspot_report.get("structural_bottleneck", [])],
        },
        "structural_candidate_pruned": {
            "count": len(hotspot_report.get("structural_candidate_pruned", [])),
            "range_ids": [item["range_id"] for item in hotspot_report.get("structural_candidate_pruned", [])],
        },
        "blocked_no_feasible_reroute": {
            "count": len(hotspot_report.get("blocked_no_feasible_reroute", [])),
            "range_ids": [item["range_id"] for item in hotspot_report.get("blocked_no_feasible_reroute", [])],
        },
        "reroutable_but_candidate_pruned": {
            "count": len(hotspot_report.get("reroutable_candidate_pruned", [])),
            "range_ids": [item["range_id"] for item in hotspot_report.get("reroutable_candidate_pruned", [])],
        },
        "improved_after_augmentation": {
            "count": len(hotspot_report.get("improved_after_augmentation", [])),
            "range_ids": [item["range_id"] for item in hotspot_report.get("improved_after_augmentation", [])],
        },
        "reroute_improved": {
            "count": len(hotspot_report.get("reroute_improved", [])),
            "range_ids": [item["range_id"] for item in hotspot_report.get("reroute_improved", [])],
        },
        "unresolved_structural_hotspots": unresolved_structural_hotspots,
        "hot_range_5_selected_applied_status": {
            "selected_augment_windows": hot_range_5.get("selected_augment_windows", []),
            "applied_augment_windows": hot_range_5.get("applied_augment_windows", []),
            "status": hot_range_5.get("status"),
            "structurally_starved_by_selection_policy": hot_range_5.get("structurally_starved_by_selection_policy", False),
            "structural_repair_gate_attempted": hot_range_5.get("structural_repair_gate_attempted", False),
            "structural_repair_gate_accepted": hot_range_5.get("structural_repair_gate_accepted", False),
            "structural_repair_gate_rejection_reason": hot_range_5.get("structural_repair_gate_rejection_reason"),
            "structural_repair_gate_local_before_after": hot_range_5.get("structural_repair_gate_local_before_after"),
            "structural_repair_gate_solver_status": hot_range_5.get("structural_repair_gate_solver_status"),
            "structural_repair_gate_solver_error": hot_range_5.get("structural_repair_gate_solver_error"),
            "released_provisional_augment_windows": hot_range_5.get("released_provisional_augment_windows", []),
            "reallocated_augment_windows_after_release": hot_range_5.get("reallocated_augment_windows_after_release", []),
            "detailed_runtime_failure_type": hot_range_5.get("detailed_runtime_failure_type"),
            "elapsed_seconds": hot_range_5.get("elapsed_seconds"),
            "did_finish_within_range_bound": hot_range_5.get("did_finish_within_range_bound"),
        },
        "structural_repair_gate_attempted": hot_range_5.get("structural_repair_gate_attempted", False),
        "structural_repair_gate_accepted": hot_range_5.get("structural_repair_gate_accepted", False),
        "structural_repair_gate_rejection_reason": hot_range_5.get("structural_repair_gate_rejection_reason"),
        "structural_repair_gate_local_before_after": hot_range_5.get("structural_repair_gate_local_before_after"),
        "detailed_runtime_failure_type": hot_range_5.get("detailed_runtime_failure_type"),
        "hot_range_5_top_candidate_rejection_reasons": [
            candidate.get("rejection_reason")
            for candidate in hot_range_5.get("augment_debug_top_candidates", [])
            if candidate.get("rejection_reason")
        ],
        "fallback_local_swap_attempted": any(
            bool(item.get("fallback_local_swap", {}).get("attempted"))
            for item in hotspot_report.get("hot_ranges", [])
        ),
        "hot_range_outcomes": [
            {
                "range_id": item.get("range_id"),
                "status": item.get("status"),
                "accepted": item.get("accepted"),
                "classification": item.get("classification"),
                "alternative_diagnostics": item.get("alternative_diagnostics"),
                "augment_funnel_counts": item.get("augment_funnel_counts"),
                "augment_rejection_breakdown": item.get("augment_rejection_breakdown"),
                "augment_debug_top_candidates": item.get("augment_debug_top_candidates"),
                "structurally_starved_by_selection_policy": item.get("structurally_starved_by_selection_policy"),
                "dominant_top_candidate_rejection_reason": item.get("dominant_top_candidate_rejection_reason"),
                "structural_repair_gate_attempted": item.get("structural_repair_gate_attempted"),
                "structural_repair_gate_accepted": item.get("structural_repair_gate_accepted"),
                "structural_repair_gate_rejection_reason": item.get("structural_repair_gate_rejection_reason"),
                "structural_repair_gate_local_before_after": item.get("structural_repair_gate_local_before_after"),
                "structural_repair_gate_solver_status": item.get("structural_repair_gate_solver_status"),
                "structural_repair_gate_solver_error": item.get("structural_repair_gate_solver_error"),
                "released_provisional_augment_windows": item.get("released_provisional_augment_windows"),
                "reallocated_augment_windows_after_release": item.get("reallocated_augment_windows_after_release"),
                "detailed_runtime_failure_type": item.get("detailed_runtime_failure_type"),
                "elapsed_seconds": item.get("elapsed_seconds"),
                "did_finish_within_range_bound": item.get("did_finish_within_range_bound"),
                "fallback_local_swap": item.get("fallback_local_swap"),
                "selected_augment_windows": item.get("selected_augment_windows"),
                "applied_augment_windows": item.get("applied_augment_windows"),
                "used_augment_windows": item.get("used_augment_windows"),
                "candidate_solver_status": item.get("candidate_solver_status"),
                "candidate_solver_error": item.get("candidate_solver_error"),
                "rejection_reason": item.get("rejection_reason"),
            }
            for item in hotspot_report.get("hot_ranges", [])
        ],
    }

    result_payload = {
        "scenario_path": str(scenario_path),
        "plan_source": plan_source,
        "input_plan_window_ids": [window.window_id for window in input_plan],
        "result": _result_to_dict(result),
    }
    _write_json(output_dir / "result.json", result_payload)
    _write_json(output_dir / "result_summary.json", summary)
    _write_json(output_dir / "hotspot_report.json", hotspot_report)
    _write_json(output_dir / "before_after_load_summary.json", before_after)
    print(json.dumps(summary, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
