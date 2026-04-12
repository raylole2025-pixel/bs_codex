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
        choices=["stage1_greedy", "full_milp", "rolling_milp"],
        default="stage1_greedy",
        help="Stage2 regular-task baseline mode; default keeps the hotspot-relief mainline on stage1_greedy",
    )
    parser.add_argument("--augment-mode", choices=["augment_only", "swap_if_budgeted"], default="augment_only")
    parser.add_argument("--fixed-window-count", type=int, default=None, help="Used only with --augment-mode=swap_if_budgeted")
    parser.add_argument("--milp-time-limit-seconds", type=float, default=None, help="Optional CBC time limit for local MILP")
    parser.add_argument("--milp-relative-gap", type=float, default=None, help="Optional CBC relative gap for local MILP")
    parser.add_argument("--hotspot-topk-ranges", type=int, default=None, help="Override hotspot_topk_ranges")
    parser.add_argument("--local-peak-horizon-cap-segments", type=int, default=None, help="Override local_peak_horizon_cap_segments")
    parser.add_argument("--hot-path-limit", type=int, default=None, help="Override hot_path_limit")
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
    effective_milp_mode = "full"
    effective_regular_mode = args.baseline_mode
    prefer_milp = effective_regular_mode != "stage1_greedy"
    scenario = replace(
        scenario,
        stage2=replace(
            scenario.stage2,
            prefer_milp=prefer_milp,
            milp_mode=effective_milp_mode,
            regular_baseline_mode=effective_regular_mode,
            hotspot_relief_enabled=True,
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
        ),
        metadata={
            **dict(scenario.metadata),
            "hotspot_augment_mode": args.augment_mode,
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
    summary = {
        "scenario_path": str(scenario_path),
        "plan_source": plan_source,
        "solver_mode": result.solver_mode,
        "baseline_mode": effective_regular_mode,
        "prefer_milp": result.metadata.get("prefer_milp"),
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
        "hot_range_outcomes": [
            {
                "range_id": item.get("range_id"),
                "status": item.get("status"),
                "accepted": item.get("accepted"),
                "classification": item.get("classification"),
                "selected_augment_windows": item.get("selected_augment_windows"),
                "applied_augment_windows": item.get("applied_augment_windows"),
                "used_augment_windows": item.get("used_augment_windows"),
                "candidate_solver_status": item.get("candidate_solver_status"),
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
