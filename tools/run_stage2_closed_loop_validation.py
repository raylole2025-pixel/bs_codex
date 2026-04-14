from __future__ import annotations

import argparse
import json
import sys
from dataclasses import replace
from datetime import datetime
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from bs3.regular_routing_common import (
    build_regular_schedule_diagnostics,
    empty_repair_metadata,
    is_regular_repair_enabled,
    resolve_regular_baseline_mode,
)
from bs3.scenario import build_segments, load_scenario
from bs3.stage2 import run_stage2
from bs3.stage2_hotspot_relief import (
    _closed_loop_hard_window_cap_details,
    _closed_loop_metrics,
    _regular_completion_ratio_from_diagnostics,
    build_cross_segment_profile,
    detect_hot_ranges,
)
from bs3.stage2_regular_block_repair import repair_regular_baseline_blocks
from bs3.stage2_two_phase_scheduler import TwoPhaseEventDrivenScheduler
from run_stage2_closed_loop_experiments import (
    DEFAULT_AUGMENT_WINDOW_BUDGET,
    DEFAULT_BASELINE_MODE,
    DEFAULT_CLOSED_LOOP_ACTION_MODE,
    DEFAULT_CLOSED_LOOP_MAX_NEW_WINDOWS,
    DEFAULT_CLOSED_LOOP_MAX_ROUNDS,
    DEFAULT_CLOSED_LOOP_TOPK_CANDIDATES_PER_RANGE,
    DEFAULT_CLOSED_LOOP_TOPK_RANGES_PER_ROUND,
    DEFAULT_HOTSPOT_LOCAL_REPAIR_TIME_LIMIT_SECONDS,
    DEFAULT_HOTSPOT_PER_RANGE_TIME_LIMIT_SECONDS,
    DEFAULT_HOTSPOT_TOPK_RANGES,
    DEFAULT_HOTSPOT_TOTAL_TIME_LIMIT_SECONDS,
    DEFAULT_MILP_TIME_LIMIT_SECONDS,
    DEFAULT_STRUCTURAL_REPAIR_GATE_TIME_LIMIT_SECONDS,
    _all_rounds_single_action,
    _find_default_scenario,
    _find_default_stage1_result,
    _load_stage1_plan,
    _result_to_dict,
    _round_recompute_observed,
    _round_summary,
    _scenario_payload_without_runtime_cache,
    _write_json,
)


DEFAULT_MATRIX_ROUNDS = (3, 5)
DEFAULT_MATRIX_WINDOWS = (1, 2, 3)
BASELINE_CONTROL_MAX_ROUNDS = 5
BASELINE_CONTROL_MAX_NEW_WINDOWS = 3
BASELINE_CONTROL_AUGMENT_WINDOW_BUDGET = 3


def _build_effective_scenario(
    base_scenario,
    *,
    experiment_name: str,
    baseline_mode: str,
    closed_loop_enabled: bool,
    closed_loop_action_mode: str,
    closed_loop_max_rounds: int,
    closed_loop_max_new_windows: int,
    augment_window_budget: int,
    hotspot_topk_ranges: int,
    closed_loop_topk_ranges_per_round: int,
    closed_loop_topk_candidates_per_range: int,
    milp_time_limit_seconds: float | None,
    hotspot_total_time_limit_seconds: float | None,
    hotspot_per_range_time_limit_seconds: float | None,
    structural_repair_gate_time_limit_seconds: float | None,
    hotspot_local_repair_time_limit_seconds: float | None,
):
    return replace(
        base_scenario,
        stage2=replace(
            base_scenario.stage2,
            prefer_milp=False,
            regular_baseline_mode=str(baseline_mode),
            hotspot_relief_enabled=True,
            closed_loop_relief_enabled=bool(closed_loop_enabled),
            fail_if_milp_disabled=False,
            closed_loop_action_mode=str(closed_loop_action_mode),
            closed_loop_max_rounds=max(int(closed_loop_max_rounds), 0),
            closed_loop_max_new_windows=max(int(closed_loop_max_new_windows), 0),
            augment_window_budget=max(int(augment_window_budget), 0),
            hotspot_topk_ranges=max(int(hotspot_topk_ranges), 0),
            closed_loop_topk_ranges_per_round=max(int(closed_loop_topk_ranges_per_round), 0),
            closed_loop_topk_candidates_per_range=max(int(closed_loop_topk_candidates_per_range), 0),
            milp_time_limit_seconds=(
                None if milp_time_limit_seconds in {None, 0, 0.0} else float(milp_time_limit_seconds)
            ),
        ),
        metadata={
            **dict(base_scenario.metadata),
            "experiment_name": experiment_name,
            "structural_repair_gate_enabled": True,
            "hotspot_total_time_limit_seconds": (
                None if hotspot_total_time_limit_seconds in {None, 0, 0.0} else float(hotspot_total_time_limit_seconds)
            ),
            "hotspot_per_range_time_limit_seconds": (
                None if hotspot_per_range_time_limit_seconds in {None, 0, 0.0} else float(hotspot_per_range_time_limit_seconds)
            ),
            "structural_repair_gate_time_limit_seconds": (
                None
                if structural_repair_gate_time_limit_seconds in {None, 0, 0.0}
                else float(structural_repair_gate_time_limit_seconds)
            ),
            "hotspot_local_repair_time_limit_seconds": (
                None
                if hotspot_local_repair_time_limit_seconds in {None, 0, 0.0}
                else float(hotspot_local_repair_time_limit_seconds)
            ),
        },
    )


def _compute_stage1_baseline_profile_summary(scenario, input_plan) -> dict[str, Any]:
    scheduler = TwoPhaseEventDrivenScheduler(scenario)
    regular_tasks = [task for task in scenario.tasks if task.task_type == "reg"]
    segments = build_segments(scenario, input_plan, regular_tasks)
    baseline_mode = resolve_regular_baseline_mode(scenario.stage2)
    baseline_schedule, baseline_completed, baseline_context = scheduler._build_regular_baseline(
        input_plan,
        segments,
        baseline_mode,
    )
    repair_metadata = empty_repair_metadata(baseline_completed)
    repair_metadata["regular_repair_enabled"] = is_regular_repair_enabled(scenario.stage2, baseline_mode)
    if baseline_mode == "stage1_greedy_repair" and repair_metadata["regular_repair_enabled"]:
        baseline_schedule, repair_metadata = repair_regular_baseline_blocks(
            scenario=scenario,
            plan=input_plan,
            segments=segments,
            baseline_schedule=baseline_schedule,
            baseline_diag=baseline_context["diagnostics"],
        )
        baseline_context["diagnostics"] = repair_metadata.get("diagnostics_after", baseline_context["diagnostics"])
    diagnostics = baseline_context.get("diagnostics")
    if not isinstance(diagnostics, dict):
        diagnostics = build_regular_schedule_diagnostics(scenario, input_plan, segments, baseline_schedule)
    profile = build_cross_segment_profile(scenario, input_plan, segments, baseline_schedule)
    metrics = _closed_loop_metrics(scenario, profile)
    cr_reg = _regular_completion_ratio_from_diagnostics(scenario, diagnostics)
    hot_ranges = detect_hot_ranges(
        profile,
        threshold=float(scenario.stage2.hotspot_util_threshold),
        topk=max(int(scenario.stage2.hotspot_topk_ranges), 0),
    )
    return {
        "segments": segments,
        "diagnostics": diagnostics,
        "metrics": metrics,
        "cr_reg": float(cr_reg),
        "hot_range_ids": [hot_range.range_id for hot_range in hot_ranges],
    }


def _accepted_action_sequence(round_summaries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "round_index": int(item["round_index"]),
            "action_type": item["chosen_action"]["action_type"],
            "range_id": item["chosen_action"]["range_id"],
            "window_id": item["chosen_action"]["window_id"],
        }
        for item in round_summaries
        if item.get("chosen_action", {}).get("action_type")
    ]


def _range_head_changes(round_summaries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    changes: list[dict[str, Any]] = []
    for previous, current in zip(round_summaries, round_summaries[1:]):
        previous_top = next(iter(previous.get("hot_range_ids") or []), None)
        current_top = next(iter(current.get("hot_range_ids") or []), None)
        changes.append(
            {
                "from_round": int(previous.get("round_index", 0)),
                "to_round": int(current.get("round_index", 0)),
                "previous_top_range_id": previous_top,
                "current_top_range_id": current_top,
                "changed": previous_top != current_top,
            }
        )
    return changes


def _improvement_against_baseline(reference: dict[str, Any], experiment: dict[str, Any]) -> dict[str, Any]:
    q_peak_delta = float(reference["q_peak_after"]) - float(experiment["q_peak_after"])
    q_integral_delta = float(reference["q_integral_after"]) - float(experiment["q_integral_after"])
    high_segment_delta = int(reference["high_segment_count_after"]) - int(experiment["high_segment_count_after"])
    return {
        "q_peak_delta_vs_baseline": q_peak_delta,
        "q_integral_delta_vs_baseline": q_integral_delta,
        "high_segment_count_delta_vs_baseline": high_segment_delta,
        "improves_any_load_metric_vs_baseline": (
            q_peak_delta > 0.0 or q_integral_delta > 0.0 or high_segment_delta > 0
        ),
    }


def _summarize_closed_loop_experiment(
    *,
    name: str,
    scenario,
    scenario_path: Path,
    stage1_result_path: Path,
    input_plan,
    result,
    closed_loop_enabled: bool,
) -> dict[str, Any]:
    hard_cap_components = dict(result.metadata.get("closed_loop_new_window_hard_cap_components") or {})
    if not hard_cap_components:
        hard_cap_components = _closed_loop_hard_window_cap_details(scenario)
    if closed_loop_enabled:
        hotspot_report = dict(result.metadata.get("hotspot_report") or {})
        before_after = dict(hotspot_report.get("before_after") or {})
        rounds = list(hotspot_report.get("rounds") or [])
        round_summaries = [_round_summary(item) for item in rounds]
        summary = {
            "experiment_name": name,
            "scenario_path": str(scenario_path),
            "stage1_result_path": str(stage1_result_path),
            "baseline_source": result.metadata.get("regular_baseline_source"),
            "solver_mode": result.solver_mode,
            "closed_loop_enabled": True,
            "closed_loop_rounds_completed": int(result.metadata.get("closed_loop_rounds_completed") or 0),
            "closed_loop_actions_accepted": int(result.metadata.get("closed_loop_actions_accepted") or 0),
            "closed_loop_new_windows_added": int(result.metadata.get("closed_loop_new_windows_added") or 0),
            "closed_loop_stop_reason": result.metadata.get("closed_loop_stop_reason"),
            "closed_loop_new_window_hard_cap": int(result.metadata.get("closed_loop_new_window_hard_cap") or hard_cap_components.get("effective_hard_cap", 0)),
            "closed_loop_new_window_hard_cap_limiter": result.metadata.get("closed_loop_new_window_hard_cap_limiter") or hard_cap_components.get("effective_hard_cap_limiter"),
            "closed_loop_new_window_hard_cap_components": hard_cap_components,
            "accepted_action_sequence": _accepted_action_sequence(round_summaries),
            "q_peak_before": result.metadata.get("q_peak_before"),
            "q_peak_after": result.metadata.get("q_peak_after"),
            "q_integral_before": result.metadata.get("q_integral_before"),
            "q_integral_after": result.metadata.get("q_integral_after"),
            "high_segment_count_before": result.metadata.get("high_segment_count_before"),
            "high_segment_count_after": result.metadata.get("high_segment_count_after"),
            "cr_reg_before": before_after.get("before", {}).get("cr_reg"),
            "cr_reg_after": before_after.get("after", {}).get("cr_reg"),
            "elapsed_seconds": float(result.metadata.get("elapsed_seconds") or 0.0),
            "round_summaries": round_summaries,
            "validation_checks": {
                "single_action_per_round": _all_rounds_single_action(rounds),
                "round_recompute_chain_is_consistent": _round_recompute_observed(rounds),
                "range_head_changes": _range_head_changes(round_summaries),
            },
        }
        return summary

    baseline_summary = _compute_stage1_baseline_profile_summary(scenario, input_plan)
    metrics = baseline_summary["metrics"]
    summary = {
        "experiment_name": name,
        "scenario_path": str(scenario_path),
        "stage1_result_path": str(stage1_result_path),
        "baseline_source": result.metadata.get("regular_baseline_source"),
        "solver_mode": result.solver_mode,
        "closed_loop_enabled": False,
        "closed_loop_rounds_completed": 0,
        "closed_loop_actions_accepted": 0,
        "closed_loop_new_windows_added": 0,
        "closed_loop_stop_reason": "closed_loop_disabled",
        "closed_loop_new_window_hard_cap": int(hard_cap_components.get("effective_hard_cap", 0)),
        "closed_loop_new_window_hard_cap_limiter": hard_cap_components.get("effective_hard_cap_limiter"),
        "closed_loop_new_window_hard_cap_components": hard_cap_components,
        "accepted_action_sequence": [],
        "q_peak_before": metrics["q_peak"],
        "q_peak_after": metrics["q_peak"],
        "q_integral_before": metrics["q_integral"],
        "q_integral_after": metrics["q_integral"],
        "high_segment_count_before": metrics["high_segment_count"],
        "high_segment_count_after": metrics["high_segment_count"],
        "cr_reg_before": baseline_summary["cr_reg"],
        "cr_reg_after": baseline_summary["cr_reg"],
        "elapsed_seconds": float(result.metadata.get("elapsed_seconds") or 0.0),
        "round_summaries": [],
        "validation_checks": {
            "single_action_per_round": True,
            "round_recompute_chain_is_consistent": True,
            "range_head_changes": [],
            "baseline_hot_range_ids": baseline_summary["hot_range_ids"],
        },
    }
    return summary


def _experiment_markdown(summary: dict[str, Any]) -> list[str]:
    lines = [
        f"### {summary['experiment_name']}",
        "",
        f"- baseline_source: `{summary['baseline_source']}`",
        f"- solver_mode: `{summary['solver_mode']}`",
        f"- closed_loop_rounds_completed: `{summary['closed_loop_rounds_completed']}`",
        f"- closed_loop_actions_accepted: `{summary['closed_loop_actions_accepted']}`",
        f"- closed_loop_new_windows_added: `{summary['closed_loop_new_windows_added']}`",
        f"- closed_loop_stop_reason: `{summary['closed_loop_stop_reason']}`",
        f"- closed_loop_new_window_hard_cap: `{summary['closed_loop_new_window_hard_cap']}`",
        f"- q_peak_before / after: `{summary['q_peak_before']}` -> `{summary['q_peak_after']}`",
        f"- q_integral_before / after: `{summary['q_integral_before']}` -> `{summary['q_integral_after']}`",
        f"- high_segment_count_before / after: `{summary['high_segment_count_before']}` -> `{summary['high_segment_count_after']}`",
        f"- elapsed_seconds: `{summary['elapsed_seconds']}`",
        "",
        "Accepted actions:",
    ]
    actions = summary.get("accepted_action_sequence") or []
    if not actions:
        lines.append("- none")
    else:
        for action in actions:
            lines.append(
                f"- round {action['round_index']}: {action['action_type']} range={action['range_id']} window={action['window_id']}"
            )
    if summary.get("round_summaries"):
        lines.extend(["", "Round summaries:"])
        for item in summary["round_summaries"]:
            lines.extend(
                [
                    f"- round {item['round_index']}: action={item['chosen_action']['action_type']} range={item['chosen_action']['range_id']} window={item['chosen_action']['window_id']}",
                    f"  hot_range_ids={item.get('hot_range_ids')}",
                    f"  q_peak={item['q_peak_before']} -> {item['q_peak_after']}",
                    f"  q_integral={item['q_integral_before']} -> {item['q_integral_after']}",
                    f"  high_segment_count={item['high_segment_count_before']} -> {item['high_segment_count_after']}",
                ]
            )
    lines.append("")
    return lines


def _overall_markdown(summary: dict[str, Any]) -> str:
    lines = [
        "# Stage2 Closed-Loop Validation Summary",
        "",
        f"- scenario_path: `{summary['scenario_path']}`",
        f"- stage1_result_path: `{summary['stage1_result_path']}`",
        f"- experiment_count: `{len(summary['experiments'])}`",
        "",
        "## Analysis",
        "",
        f"- baseline_experiment: `{summary['analysis']['baseline_experiment_name']}`",
        f"- all_closed_loop_runs_single_action_per_round: `{summary['analysis']['all_closed_loop_runs_single_action_per_round']}`",
        f"- all_closed_loop_runs_recompute_consistent: `{summary['analysis']['all_closed_loop_runs_recompute_consistent']}`",
        f"- q_peak_improved_experiments: `{summary['analysis']['q_peak_improved_experiments']}`",
        f"- integral_only_improved_experiments: `{summary['analysis']['integral_only_improved_experiments']}`",
        f"- stop_reason_counts: `{summary['analysis']['stop_reason_counts']}`",
        f"- stable_improvement_vs_baseline_count: `{summary['analysis']['stable_improvement_vs_baseline_count']}`",
        "",
        "## Experiments",
        "",
    ]
    for experiment in summary["experiments"]:
        lines.extend(_experiment_markdown(experiment))
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a small closed-loop validation matrix against a fixed Stage1 plan.")
    parser.add_argument("--scenario", default=None, help="Scenario JSON path")
    parser.add_argument("--stage1-result", default=None, help="Stage1 result JSON used to load the fixed plan")
    parser.add_argument("--candidate-index", type=int, default=0, help="best_feasible plan index when loading the Stage1 result")
    parser.add_argument("--output-root", default=None, help="Output directory")
    parser.add_argument("--baseline-mode", choices=["stage1_greedy", "stage1_greedy_repair"], default=DEFAULT_BASELINE_MODE)
    parser.add_argument("--closed-loop-action-mode", choices=["reroute_then_augment", "best_global_action"], default=DEFAULT_CLOSED_LOOP_ACTION_MODE)
    parser.add_argument("--hotspot-topk-ranges", type=int, default=DEFAULT_HOTSPOT_TOPK_RANGES)
    parser.add_argument("--closed-loop-topk-ranges-per-round", type=int, default=DEFAULT_CLOSED_LOOP_TOPK_RANGES_PER_ROUND)
    parser.add_argument("--closed-loop-topk-candidates-per-range", type=int, default=DEFAULT_CLOSED_LOOP_TOPK_CANDIDATES_PER_RANGE)
    parser.add_argument("--milp-time-limit-seconds", type=float, default=DEFAULT_MILP_TIME_LIMIT_SECONDS)
    parser.add_argument("--hotspot-total-time-limit-seconds", type=float, default=DEFAULT_HOTSPOT_TOTAL_TIME_LIMIT_SECONDS)
    parser.add_argument("--hotspot-per-range-time-limit-seconds", type=float, default=DEFAULT_HOTSPOT_PER_RANGE_TIME_LIMIT_SECONDS)
    parser.add_argument("--structural-repair-gate-time-limit-seconds", type=float, default=DEFAULT_STRUCTURAL_REPAIR_GATE_TIME_LIMIT_SECONDS)
    parser.add_argument("--hotspot-local-repair-time-limit-seconds", type=float, default=DEFAULT_HOTSPOT_LOCAL_REPAIR_TIME_LIMIT_SECONDS)
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    scenario_path = Path(args.scenario) if args.scenario else _find_default_scenario(repo_root)
    stage1_result_path = Path(args.stage1_result) if args.stage1_result else _find_default_stage1_result(repo_root)
    output_root = (
        Path(args.output_root)
        if args.output_root
        else repo_root / "results" / "generated" / "stage2_closed_loop_validation" / datetime.now().strftime("%Y%m%d_%H%M%S")
    )
    output_root.mkdir(parents=True, exist_ok=True)

    base_scenario = load_scenario(scenario_path)
    input_plan = _load_stage1_plan(stage1_result_path, args.candidate_index)
    _write_json(output_root / "selected_stage1_plan.json", [window.__dict__ for window in input_plan])

    experiment_specs = [
        {
            "name": "baseline_control",
            "closed_loop_enabled": False,
            "closed_loop_max_rounds": BASELINE_CONTROL_MAX_ROUNDS,
            "closed_loop_max_new_windows": BASELINE_CONTROL_MAX_NEW_WINDOWS,
            "augment_window_budget": BASELINE_CONTROL_AUGMENT_WINDOW_BUDGET,
        }
    ]
    for max_rounds in DEFAULT_MATRIX_ROUNDS:
        for max_new_windows in DEFAULT_MATRIX_WINDOWS:
            experiment_specs.append(
                {
                    "name": f"r{max_rounds}_w{max_new_windows}",
                    "closed_loop_enabled": True,
                    "closed_loop_max_rounds": int(max_rounds),
                    "closed_loop_max_new_windows": int(max_new_windows),
                    "augment_window_budget": int(max_new_windows),
                }
            )

    summaries: list[dict[str, Any]] = []
    for spec in experiment_specs:
        experiment_output = output_root / spec["name"]
        experiment_output.mkdir(parents=True, exist_ok=True)
        scenario = _build_effective_scenario(
            base_scenario,
            experiment_name=spec["name"],
            baseline_mode=str(args.baseline_mode),
            closed_loop_enabled=bool(spec["closed_loop_enabled"]),
            closed_loop_action_mode=str(args.closed_loop_action_mode),
            closed_loop_max_rounds=int(spec["closed_loop_max_rounds"]),
            closed_loop_max_new_windows=int(spec["closed_loop_max_new_windows"]),
            augment_window_budget=int(spec["augment_window_budget"]),
            hotspot_topk_ranges=max(int(args.hotspot_topk_ranges), 0),
            closed_loop_topk_ranges_per_round=max(int(args.closed_loop_topk_ranges_per_round), 0),
            closed_loop_topk_candidates_per_range=max(int(args.closed_loop_topk_candidates_per_range), 0),
            milp_time_limit_seconds=args.milp_time_limit_seconds,
            hotspot_total_time_limit_seconds=args.hotspot_total_time_limit_seconds,
            hotspot_per_range_time_limit_seconds=args.hotspot_per_range_time_limit_seconds,
            structural_repair_gate_time_limit_seconds=args.structural_repair_gate_time_limit_seconds,
            hotspot_local_repair_time_limit_seconds=args.hotspot_local_repair_time_limit_seconds,
        )
        result = run_stage2(scenario, list(input_plan))
        summary = _summarize_closed_loop_experiment(
            name=spec["name"],
            scenario=scenario,
            scenario_path=scenario_path,
            stage1_result_path=stage1_result_path,
            input_plan=input_plan,
            result=result,
            closed_loop_enabled=bool(spec["closed_loop_enabled"]),
        )
        summary["configured_closed_loop_max_rounds"] = int(spec["closed_loop_max_rounds"])
        summary["configured_closed_loop_max_new_windows"] = int(spec["closed_loop_max_new_windows"])
        summary["configured_augment_window_budget"] = int(spec["augment_window_budget"])
        _write_json(experiment_output / "effective_scenario.json", _scenario_payload_without_runtime_cache(scenario))
        _write_json(experiment_output / "stage2_result.json", _result_to_dict(result))
        _write_json(experiment_output / "summary.json", summary)
        (experiment_output / "summary.md").write_text("\n".join(_experiment_markdown(summary)), encoding="utf-8")
        summaries.append(summary)
        print(
            json.dumps(
                {
                    "experiment_name": summary["experiment_name"],
                    "closed_loop_stop_reason": summary["closed_loop_stop_reason"],
                    "closed_loop_actions_accepted": summary["closed_loop_actions_accepted"],
                    "q_integral_before": summary["q_integral_before"],
                    "q_integral_after": summary["q_integral_after"],
                    "elapsed_seconds": summary["elapsed_seconds"],
                },
                ensure_ascii=False,
            )
        )

    baseline_reference = next(item for item in summaries if item["experiment_name"] == "baseline_control")
    closed_loop_summaries = [item for item in summaries if item["closed_loop_enabled"]]
    for item in closed_loop_summaries:
        item["vs_baseline"] = _improvement_against_baseline(baseline_reference, item)

    q_peak_improved = [
        item["experiment_name"]
        for item in closed_loop_summaries
        if float(item["q_peak_after"]) + 1e-9 < float(item["q_peak_before"])
    ]
    integral_only_improved = [
        item["experiment_name"]
        for item in closed_loop_summaries
        if float(item["q_peak_after"]) >= float(item["q_peak_before"]) - 1e-9
        and (
            float(item["q_integral_after"]) + 1e-9 < float(item["q_integral_before"])
            or int(item["high_segment_count_after"]) < int(item["high_segment_count_before"])
        )
    ]
    stop_reason_counts: dict[str, int] = {}
    for item in closed_loop_summaries:
        reason = str(item["closed_loop_stop_reason"])
        stop_reason_counts[reason] = stop_reason_counts.get(reason, 0) + 1
    diminishing_returns: dict[str, list[dict[str, Any]]] = {}
    for max_rounds in DEFAULT_MATRIX_ROUNDS:
        runs = [
            item
            for item in closed_loop_summaries
            if item["configured_closed_loop_max_rounds"] == int(max_rounds)
        ]
        diminishing_returns[f"rounds_{max_rounds}"] = [
            {
                "max_new_windows": int(item["configured_closed_loop_max_new_windows"]),
                "accepted_actions": int(item["closed_loop_actions_accepted"]),
                "new_windows_added": int(item["closed_loop_new_windows_added"]),
                "q_peak_delta": float(item["q_peak_before"]) - float(item["q_peak_after"]),
                "q_integral_delta": float(item["q_integral_before"]) - float(item["q_integral_after"]),
                "high_segment_count_delta": int(item["high_segment_count_before"]) - int(item["high_segment_count_after"]),
                "stop_reason": item["closed_loop_stop_reason"],
            }
            for item in sorted(runs, key=lambda payload: int(payload["configured_closed_loop_max_new_windows"]))
        ]

    overall_summary = {
        "scenario_path": str(scenario_path),
        "stage1_result_path": str(stage1_result_path),
        "experiments": summaries,
        "analysis": {
            "baseline_experiment_name": baseline_reference["experiment_name"],
            "all_closed_loop_runs_single_action_per_round": all(
                bool(item["validation_checks"]["single_action_per_round"]) for item in closed_loop_summaries
            ),
            "all_closed_loop_runs_recompute_consistent": all(
                bool(item["validation_checks"]["round_recompute_chain_is_consistent"]) for item in closed_loop_summaries
            ),
            "q_peak_improved_experiments": q_peak_improved,
            "integral_only_improved_experiments": integral_only_improved,
            "stop_reason_counts": stop_reason_counts,
            "range_head_changed_after_first_acceptance": {
                item["experiment_name"]: next(
                    (
                        bool(change["changed"])
                        for change in item["validation_checks"]["range_head_changes"]
                        if int(change["from_round"]) == 1
                    ),
                    None,
                )
                for item in closed_loop_summaries
            },
            "stable_improvement_vs_baseline_count": sum(
                1 for item in closed_loop_summaries if item["vs_baseline"]["improves_any_load_metric_vs_baseline"]
            ),
            "stable_improvement_vs_baseline_experiments": [
                item["experiment_name"]
                for item in closed_loop_summaries
                if item["vs_baseline"]["improves_any_load_metric_vs_baseline"]
            ],
            "diminishing_returns_by_round_limit": diminishing_returns,
        },
    }

    _write_json(output_root / "summary.json", overall_summary)
    (output_root / "summary.md").write_text(_overall_markdown(overall_summary), encoding="utf-8")
    print(json.dumps({"output_root": str(output_root), "experiment_count": len(summaries)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
