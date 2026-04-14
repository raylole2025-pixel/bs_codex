from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from bs3.scenario import REMOVED_STAGE1_FIELDS, REMOVED_STAGE2_FIELDS, build_segments, load_scenario
from bs3.stage2 import run_stage2
from bs3.stage2_hotspot_relief import build_cross_segment_profile, detect_hot_ranges
from bs3.regular_routing_common import (
    build_regular_schedule_diagnostics,
    empty_repair_metadata,
    is_regular_repair_enabled,
    resolve_regular_baseline_mode,
)
from bs3.stage2_regular_block_repair import repair_regular_baseline_blocks
from bs3.stage2_two_phase_scheduler import TwoPhaseEventDrivenScheduler
from run_stage2_closed_loop_validation import (
    DEFAULT_BASELINE_MODE,
    _build_effective_scenario,
    _improvement_against_baseline,
    _summarize_closed_loop_experiment,
)
from run_stage2_closed_loop_experiments import (
    DEFAULT_CLOSED_LOOP_ACTION_MODE,
    DEFAULT_CLOSED_LOOP_TOPK_CANDIDATES_PER_RANGE,
    DEFAULT_CLOSED_LOOP_TOPK_RANGES_PER_ROUND,
    DEFAULT_HOTSPOT_LOCAL_REPAIR_TIME_LIMIT_SECONDS,
    DEFAULT_HOTSPOT_PER_RANGE_TIME_LIMIT_SECONDS,
    DEFAULT_HOTSPOT_TOPK_RANGES,
    DEFAULT_HOTSPOT_TOTAL_TIME_LIMIT_SECONDS,
    DEFAULT_MILP_TIME_LIMIT_SECONDS,
    DEFAULT_STRUCTURAL_REPAIR_GATE_TIME_LIMIT_SECONDS,
    _load_stage1_plan,
    _result_to_dict,
    _scenario_payload_without_runtime_cache,
    _write_json,
)


DEFAULT_CLOSED_LOOP_MAX_ROUNDS = 3
DEFAULT_CLOSED_LOOP_MAX_NEW_WINDOWS = 2
DEFAULT_AUGMENT_WINDOW_BUDGET = 2


def _sanitize_token(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9]+", "_", value.strip())
    cleaned = cleaned.strip("_")
    return cleaned or "case"


def _file_sha256_prefix(path: Path, prefix_len: int = 8) -> str:
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    return digest[:prefix_len]


def _discover_stage1_cases(repo_root: Path, max_candidates_per_result: int | None) -> list[dict[str, Any]]:
    results_root = repo_root / "results"
    stage1_results = sorted(
        path
        for path in results_root.rglob("*stage1_result.json")
        if "generated" not in {part.lower() for part in path.parts}
    )
    cases: list[dict[str, Any]] = []
    for stage1_result_path in stage1_results:
        scenario_matches = sorted(stage1_result_path.parent.glob("*scenario_weighted.json"))
        if not scenario_matches:
            continue
        scenario_path = scenario_matches[0]
        payload = json.loads(stage1_result_path.read_text(encoding="utf-8"))
        best_feasible = payload.get("best_feasible") or []
        if best_feasible:
            candidate_indices = list(range(len(best_feasible)))
        elif payload.get("population_best"):
            candidate_indices = [0]
        else:
            continue
        if max_candidates_per_result is not None:
            candidate_indices = candidate_indices[: max(int(max_candidates_per_result), 0)]
        stage1_run_token = _sanitize_token(stage1_result_path.parent.name)
        scenario_hash = _file_sha256_prefix(scenario_path)
        stage1_hash = _file_sha256_prefix(stage1_result_path)
        for candidate_index in candidate_indices:
            case_name = f"{stage1_run_token}__scn_{scenario_hash}__res_{stage1_hash}__cand_{candidate_index}"
            cases.append(
                {
                    "case_name": case_name,
                    "stage1_run_token": stage1_run_token,
                    "scenario_hash": scenario_hash,
                    "stage1_hash": stage1_hash,
                    "scenario_path": scenario_path,
                    "stage1_result_path": stage1_result_path,
                    "candidate_index": int(candidate_index),
                }
            )
    return cases


def _load_scenario_compat(path: Path, *, sanitized_dir: Path) -> tuple[Any, dict[str, Any]]:
    try:
        return load_scenario(path), {
            "source_path": str(path),
            "sanitized_copy_path": None,
            "removed_stage1_fields": [],
            "removed_stage2_fields": [],
        }
    except ValueError as exc:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
        stage1_cfg = dict(payload.get("stage1", {}) or {})
        stage2_cfg = dict(payload.get("stage2", {}) or {})
        removed_stage1 = sorted(key for key in REMOVED_STAGE1_FIELDS if key in stage1_cfg)
        removed_stage2 = sorted(key for key in REMOVED_STAGE2_FIELDS if key in stage2_cfg)
        if not removed_stage1 and not removed_stage2:
            raise exc
        for key in removed_stage1:
            stage1_cfg.pop(key, None)
        for key in removed_stage2:
            stage2_cfg.pop(key, None)
        payload["stage1"] = stage1_cfg
        payload["stage2"] = stage2_cfg
        sanitized_dir.mkdir(parents=True, exist_ok=True)
        sanitized_path = sanitized_dir / "source_scenario_sanitized.json"
        sanitized_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
        return load_scenario(sanitized_path), {
            "source_path": str(path),
            "sanitized_copy_path": str(sanitized_path),
            "removed_stage1_fields": removed_stage1,
            "removed_stage2_fields": removed_stage2,
        }


def _compute_full_baseline_hot_range_diagnostics(scenario, input_plan) -> dict[str, Any]:
    scheduler = TwoPhaseEventDrivenScheduler(scenario)
    regular_tasks = [task for task in scenario.tasks if task.task_type == "reg"]
    segments = build_segments(scenario, input_plan, regular_tasks)
    baseline_mode = resolve_regular_baseline_mode(scenario.stage2)
    schedule, completed, context = scheduler._build_regular_baseline(input_plan, segments, baseline_mode)
    repair_metadata = empty_repair_metadata(completed)
    repair_metadata["regular_repair_enabled"] = is_regular_repair_enabled(scenario.stage2, baseline_mode)
    if baseline_mode == "stage1_greedy_repair" and repair_metadata["regular_repair_enabled"]:
        schedule, repair_metadata = repair_regular_baseline_blocks(
            scenario=scenario,
            plan=input_plan,
            segments=segments,
            baseline_schedule=schedule,
            baseline_diag=context["diagnostics"],
        )
        context["diagnostics"] = repair_metadata.get("diagnostics_after", context["diagnostics"])
    diagnostics = context.get("diagnostics")
    if not isinstance(diagnostics, dict):
        diagnostics = build_regular_schedule_diagnostics(scenario, input_plan, segments, schedule)
    profile = build_cross_segment_profile(scenario, input_plan, segments, schedule)
    hot_ranges = detect_hot_ranges(
        profile,
        threshold=float(scenario.stage2.hotspot_util_threshold),
        topk=max(len(profile), 1),
    )
    return {
        "baseline_hot_range_count_full": len(hot_ranges),
        "baseline_hot_range_ids_full": [hot_range.range_id for hot_range in hot_ranges],
    }


def _closed_loop_round_top_reordered(summary: dict[str, Any]) -> bool | None:
    for change in summary.get("validation_checks", {}).get("range_head_changes", []):
        if int(change.get("from_round", 0)) == 1:
            return bool(change.get("changed"))
    return None


def _is_first_round_single_action_saturation(summary: dict[str, Any]) -> bool:
    if not bool(summary.get("closed_loop_enabled")):
        return False
    if int(summary.get("closed_loop_actions_accepted", 0)) != 1:
        return False
    if str(summary.get("closed_loop_stop_reason")) != "no_acceptable_action":
        return False
    rounds = list(summary.get("round_summaries") or [])
    if len(rounds) < 2:
        return False
    first_action = rounds[0].get("chosen_action", {}).get("action_type")
    second_action = rounds[1].get("chosen_action", {}).get("action_type")
    return bool(first_action) and not bool(second_action)


def _coverage_diagnostics(closed_loop_summary: dict[str, Any], baseline_summary: dict[str, Any]) -> dict[str, Any]:
    closed_loop_stage2_result = closed_loop_summary["_stage2_result_dict"]
    hotspot_report = dict(closed_loop_stage2_result.get("metadata", {}).get("hotspot_report") or {})
    rounds = list(hotspot_report.get("rounds") or [])
    topk_ranges = int(closed_loop_summary["config"]["hotspot_topk_ranges"])
    topk_candidates = int(closed_loop_summary["config"]["closed_loop_topk_candidates_per_range"])
    baseline_hot_range_count_full = len(baseline_summary.get("hot_range_ids") or [])
    hot_range_head_reordered = _closed_loop_round_top_reordered(closed_loop_summary)
    range_topk_truncation = baseline_hot_range_count_full > topk_ranges

    structural_candidate_limit_hit_rounds: list[int] = []
    structural_candidate_limit_hits: list[dict[str, Any]] = []
    for round_payload in rounds:
        round_index = int(round_payload.get("round_index", 0) or 0)
        for range_report in round_payload.get("range_reports") or []:
            classification = dict(range_report.get("classification") or {})
            if not bool(classification.get("structural")):
                continue
            funnel = dict(range_report.get("augment_funnel_counts") or {})
            ready_count = int(funnel.get("relief_path_ready_count", 0) or 0)
            if ready_count > topk_candidates:
                structural_candidate_limit_hit_rounds.append(round_index)
                structural_candidate_limit_hits.append(
                    {
                        "round_index": round_index,
                        "range_id": range_report.get("range_id"),
                        "relief_path_ready_count": ready_count,
                        "candidate_limit": topk_candidates,
                    }
                )
    return {
        "baseline_hot_range_count_full": baseline_hot_range_count_full,
        "configured_hotspot_topk_ranges": topk_ranges,
        "configured_closed_loop_topk_candidates_per_range": topk_candidates,
        "range_topk_truncation_possible": bool(range_topk_truncation),
        "structural_candidate_limit_hit": bool(structural_candidate_limit_hits),
        "structural_candidate_limit_hit_rounds": sorted(set(structural_candidate_limit_hit_rounds)),
        "structural_candidate_limit_hits": structural_candidate_limit_hits,
        "head_reordered_after_first_acceptance": hot_range_head_reordered,
    }


def _case_markdown(case_summary: dict[str, Any]) -> list[str]:
    baseline = case_summary["baseline_control"]
    closed_loop = case_summary["closed_loop"]
    coverage = case_summary["coverage_diagnostics"]
    lines = [
        f"### {case_summary['case_name']}",
        "",
        f"- scenario_path: `{case_summary['scenario_path']}`",
        f"- stage1_result_path: `{case_summary['stage1_result_path']}`",
        f"- candidate_index: `{case_summary['candidate_index']}`",
        f"- baseline_source: `{closed_loop['baseline_source']}`",
        f"- baseline_solver_mode: `{baseline['solver_mode']}`",
        f"- closed_loop_solver_mode: `{closed_loop['solver_mode']}`",
        f"- closed_loop_rounds_completed: `{closed_loop['closed_loop_rounds_completed']}`",
        f"- closed_loop_actions_accepted: `{closed_loop['closed_loop_actions_accepted']}`",
        f"- closed_loop_new_windows_added: `{closed_loop['closed_loop_new_windows_added']}`",
        f"- closed_loop_stop_reason: `{closed_loop['closed_loop_stop_reason']}`",
        f"- q_peak_before / after: `{closed_loop['q_peak_before']}` -> `{closed_loop['q_peak_after']}`",
        f"- q_integral_before / after: `{closed_loop['q_integral_before']}` -> `{closed_loop['q_integral_after']}`",
        f"- high_segment_count_before / after: `{closed_loop['high_segment_count_before']}` -> `{closed_loop['high_segment_count_after']}`",
        f"- cr_reg_before / after: `{closed_loop['cr_reg_before']}` -> `{closed_loop['cr_reg_after']}`",
        f"- elapsed_seconds: `{closed_loop['elapsed_seconds']}`",
        f"- single_action_per_round: `{closed_loop['validation_checks']['single_action_per_round']}`",
        f"- round_recompute_chain_is_consistent: `{closed_loop['validation_checks']['round_recompute_chain_is_consistent']}`",
        f"- head_reordered_after_first_acceptance: `{coverage['head_reordered_after_first_acceptance']}`",
        f"- range_topk_truncation_possible: `{coverage['range_topk_truncation_possible']}`",
        f"- structural_candidate_limit_hit: `{coverage['structural_candidate_limit_hit']}`",
        "",
        "Accepted actions:",
    ]
    actions = closed_loop.get("accepted_action_sequence") or []
    if not actions:
        lines.append("- none")
    else:
        for action in actions:
            lines.append(
                f"- round {action['round_index']}: {action['action_type']} range={action['range_id']} window={action['window_id']}"
            )
    if closed_loop.get("round_summaries"):
        lines.extend(["", "Round summaries:"])
        for item in closed_loop["round_summaries"]:
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
    analysis = summary["analysis"]
    lines = [
        "# Stage2 Closed-Loop Generalization Validation",
        "",
        f"- case_count: `{summary['case_count']}`",
        f"- unique_scenario_count: `{summary['unique_scenario_count']}`",
        f"- unique_stage1_result_count: `{summary['unique_stage1_result_count']}`",
        f"- discovered_regular_tasksets: `{summary['data_inventory']['discovered_regular_tasksets']}`",
        f"- notes: `{summary['data_inventory']['notes']}`",
        "",
        "## Analysis",
        "",
        f"- all_cases_baseline_source_stage1_greedy_repair: `{analysis['all_cases_baseline_source_stage1_greedy_repair']}`",
        f"- all_closed_loop_runs_single_action_per_round: `{analysis['all_closed_loop_runs_single_action_per_round']}`",
        f"- all_closed_loop_runs_recompute_consistent: `{analysis['all_closed_loop_runs_recompute_consistent']}`",
        f"- first_round_single_action_saturation_count: `{analysis['first_round_single_action_saturation_count']}`",
        f"- q_peak_improved_case_count: `{analysis['q_peak_improved_case_count']}`",
        f"- integral_or_highseg_only_case_count: `{analysis['integral_or_highseg_only_case_count']}`",
        f"- stop_reason_counts: `{analysis['stop_reason_counts']}`",
        f"- improvement_vs_baseline_case_count: `{analysis['improvement_vs_baseline_case_count']}`",
        f"- range_topk_truncation_possible_case_count: `{analysis['range_topk_truncation_possible_case_count']}`",
        f"- structural_candidate_limit_hit_case_count: `{analysis['structural_candidate_limit_hit_case_count']}`",
        "",
        "## Cases",
        "",
    ]
    for case_summary in summary["cases"]:
        lines.extend(_case_markdown(case_summary))
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description="Run broader closed-loop stability/generalization validation across discovered Stage1 plans.")
    parser.add_argument("--output-root", default=None, help="Output directory")
    parser.add_argument("--max-candidates-per-result", type=int, default=None, help="Optional limit on best_feasible plans per Stage1 result")
    parser.add_argument("--baseline-mode", choices=["stage1_greedy", "stage1_greedy_repair"], default=DEFAULT_BASELINE_MODE)
    parser.add_argument("--closed-loop-action-mode", choices=["reroute_then_augment", "best_global_action"], default=DEFAULT_CLOSED_LOOP_ACTION_MODE)
    parser.add_argument("--closed-loop-max-rounds", type=int, default=DEFAULT_CLOSED_LOOP_MAX_ROUNDS)
    parser.add_argument("--closed-loop-max-new-windows", type=int, default=DEFAULT_CLOSED_LOOP_MAX_NEW_WINDOWS)
    parser.add_argument("--augment-window-budget", type=int, default=DEFAULT_AUGMENT_WINDOW_BUDGET)
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
    output_root = (
        Path(args.output_root)
        if args.output_root
        else repo_root / "results" / "generated" / "stage2_closed_loop_generalization_validation"
    )
    output_root.mkdir(parents=True, exist_ok=True)

    cases = _discover_stage1_cases(repo_root, args.max_candidates_per_result)
    if not cases:
        raise FileNotFoundError("No non-generated Stage1 result + scenario_weighted pairs found under results/")

    summary_cases: list[dict[str, Any]] = []
    for case in cases:
        case_root = output_root / case["case_name"]
        baseline_root = case_root / "baseline_control"
        closed_loop_root = case_root / "closed_loop"
        baseline_root.mkdir(parents=True, exist_ok=True)
        closed_loop_root.mkdir(parents=True, exist_ok=True)

        input_plan = _load_stage1_plan(case["stage1_result_path"], case["candidate_index"])

        base_scenario, scenario_compat = _load_scenario_compat(case["scenario_path"], sanitized_dir=case_root)
        baseline_scenario = _build_effective_scenario(
            base_scenario,
            experiment_name=f"{case['case_name']}_baseline_control",
            baseline_mode=str(args.baseline_mode),
            closed_loop_enabled=False,
            closed_loop_action_mode=str(args.closed_loop_action_mode),
            closed_loop_max_rounds=max(int(args.closed_loop_max_rounds), 0),
            closed_loop_max_new_windows=max(int(args.closed_loop_max_new_windows), 0),
            augment_window_budget=max(int(args.augment_window_budget), 0),
            hotspot_topk_ranges=max(int(args.hotspot_topk_ranges), 0),
            closed_loop_topk_ranges_per_round=max(int(args.closed_loop_topk_ranges_per_round), 0),
            closed_loop_topk_candidates_per_range=max(int(args.closed_loop_topk_candidates_per_range), 0),
            milp_time_limit_seconds=args.milp_time_limit_seconds,
            hotspot_total_time_limit_seconds=args.hotspot_total_time_limit_seconds,
            hotspot_per_range_time_limit_seconds=args.hotspot_per_range_time_limit_seconds,
            structural_repair_gate_time_limit_seconds=args.structural_repair_gate_time_limit_seconds,
            hotspot_local_repair_time_limit_seconds=args.hotspot_local_repair_time_limit_seconds,
        )
        baseline_result = run_stage2(baseline_scenario, list(input_plan))
        baseline_summary = _summarize_closed_loop_experiment(
            name="baseline_control",
            scenario=baseline_scenario,
            scenario_path=case["scenario_path"],
            stage1_result_path=case["stage1_result_path"],
            input_plan=input_plan,
            result=baseline_result,
            closed_loop_enabled=False,
        )
        baseline_summary["config"] = {
            "hotspot_topk_ranges": int(args.hotspot_topk_ranges),
            "closed_loop_topk_ranges_per_round": int(args.closed_loop_topk_ranges_per_round),
            "closed_loop_topk_candidates_per_range": int(args.closed_loop_topk_candidates_per_range),
        }
        baseline_summary["_stage2_result_dict"] = _result_to_dict(baseline_result)
        _write_json(baseline_root / "effective_scenario.json", _scenario_payload_without_runtime_cache(baseline_scenario))
        _write_json(baseline_root / "stage2_result.json", baseline_summary["_stage2_result_dict"])
        _write_json(baseline_root / "summary.json", {k: v for k, v in baseline_summary.items() if not k.startswith("_")})

        closed_loop_scenario = _build_effective_scenario(
            base_scenario,
            experiment_name=f"{case['case_name']}_closed_loop",
            baseline_mode=str(args.baseline_mode),
            closed_loop_enabled=True,
            closed_loop_action_mode=str(args.closed_loop_action_mode),
            closed_loop_max_rounds=max(int(args.closed_loop_max_rounds), 0),
            closed_loop_max_new_windows=max(int(args.closed_loop_max_new_windows), 0),
            augment_window_budget=max(int(args.augment_window_budget), 0),
            hotspot_topk_ranges=max(int(args.hotspot_topk_ranges), 0),
            closed_loop_topk_ranges_per_round=max(int(args.closed_loop_topk_ranges_per_round), 0),
            closed_loop_topk_candidates_per_range=max(int(args.closed_loop_topk_candidates_per_range), 0),
            milp_time_limit_seconds=args.milp_time_limit_seconds,
            hotspot_total_time_limit_seconds=args.hotspot_total_time_limit_seconds,
            hotspot_per_range_time_limit_seconds=args.hotspot_per_range_time_limit_seconds,
            structural_repair_gate_time_limit_seconds=args.structural_repair_gate_time_limit_seconds,
            hotspot_local_repair_time_limit_seconds=args.hotspot_local_repair_time_limit_seconds,
        )
        closed_loop_result = run_stage2(closed_loop_scenario, list(input_plan))
        closed_loop_summary = _summarize_closed_loop_experiment(
            name="closed_loop",
            scenario=closed_loop_scenario,
            scenario_path=case["scenario_path"],
            stage1_result_path=case["stage1_result_path"],
            input_plan=input_plan,
            result=closed_loop_result,
            closed_loop_enabled=True,
        )
        closed_loop_summary["config"] = {
            "hotspot_topk_ranges": int(args.hotspot_topk_ranges),
            "closed_loop_topk_ranges_per_round": int(args.closed_loop_topk_ranges_per_round),
            "closed_loop_topk_candidates_per_range": int(args.closed_loop_topk_candidates_per_range),
        }
        closed_loop_summary["_stage2_result_dict"] = _result_to_dict(closed_loop_result)
        _write_json(closed_loop_root / "effective_scenario.json", _scenario_payload_without_runtime_cache(closed_loop_scenario))
        _write_json(closed_loop_root / "stage2_result.json", closed_loop_summary["_stage2_result_dict"])
        _write_json(closed_loop_root / "summary.json", {k: v for k, v in closed_loop_summary.items() if not k.startswith("_")})

        baseline_profile_diagnostics = _compute_full_baseline_hot_range_diagnostics(baseline_scenario, input_plan)
        coverage = _coverage_diagnostics(closed_loop_summary, baseline_profile_diagnostics)

        case_summary = {
            "case_name": case["case_name"],
            "scenario_hash": case["scenario_hash"],
            "stage1_hash": case["stage1_hash"],
            "stage1_run_token": case["stage1_run_token"],
            "scenario_path": str(case["scenario_path"]),
            "stage1_result_path": str(case["stage1_result_path"]),
            "candidate_index": int(case["candidate_index"]),
            "scenario_compat": scenario_compat,
            "baseline_control": {k: v for k, v in baseline_summary.items() if not k.startswith("_")},
            "closed_loop": {k: v for k, v in closed_loop_summary.items() if not k.startswith("_")},
            "improvement_vs_baseline": _improvement_against_baseline(baseline_summary, closed_loop_summary),
            "coverage_diagnostics": coverage,
            "first_round_single_action_saturation": _is_first_round_single_action_saturation(closed_loop_summary),
        }
        (case_root / "summary.md").write_text("\n".join(_case_markdown(case_summary)), encoding="utf-8")
        _write_json(case_root / "summary.json", case_summary)
        summary_cases.append(case_summary)
        print(
            json.dumps(
                {
                    "case_name": case_summary["case_name"],
                    "candidate_index": case_summary["candidate_index"],
                    "stop_reason": case_summary["closed_loop"]["closed_loop_stop_reason"],
                    "actions_accepted": case_summary["closed_loop"]["closed_loop_actions_accepted"],
                    "q_peak_before": case_summary["closed_loop"]["q_peak_before"],
                    "q_peak_after": case_summary["closed_loop"]["q_peak_after"],
                    "q_integral_before": case_summary["closed_loop"]["q_integral_before"],
                    "q_integral_after": case_summary["closed_loop"]["q_integral_after"],
                },
                ensure_ascii=False,
            )
        )

    q_peak_improved_cases = [
        case["case_name"]
        for case in summary_cases
        if float(case["closed_loop"]["q_peak_after"]) + 1e-9 < float(case["closed_loop"]["q_peak_before"])
    ]
    integral_or_highseg_only_cases = [
        case["case_name"]
        for case in summary_cases
        if float(case["closed_loop"]["q_peak_after"]) >= float(case["closed_loop"]["q_peak_before"]) - 1e-9
        and (
            float(case["closed_loop"]["q_integral_after"]) + 1e-9 < float(case["closed_loop"]["q_integral_before"])
            or int(case["closed_loop"]["high_segment_count_after"]) < int(case["closed_loop"]["high_segment_count_before"])
        )
    ]
    stop_reason_counts: dict[str, int] = {}
    for case in summary_cases:
        reason = str(case["closed_loop"]["closed_loop_stop_reason"])
        stop_reason_counts[reason] = stop_reason_counts.get(reason, 0) + 1

    overall_summary = {
        "case_count": len(summary_cases),
        "unique_scenario_count": len({case["scenario_hash"] for case in summary_cases}),
        "unique_stage1_result_count": len({case["stage1_hash"] for case in summary_cases}),
        "fixed_config": {
            "regular_baseline_mode": str(args.baseline_mode),
            "closed_loop_relief_enabled": True,
            "hotspot_relief_enabled": True,
            "closed_loop_action_mode": str(args.closed_loop_action_mode),
            "closed_loop_max_rounds": int(args.closed_loop_max_rounds),
            "closed_loop_max_new_windows": int(args.closed_loop_max_new_windows),
            "augment_window_budget": int(args.augment_window_budget),
            "hotspot_topk_ranges": int(args.hotspot_topk_ranges),
            "closed_loop_topk_ranges_per_round": int(args.closed_loop_topk_ranges_per_round),
            "closed_loop_topk_candidates_per_range": int(args.closed_loop_topk_candidates_per_range),
        },
        "data_inventory": {
            "discovered_regular_tasksets": ["normal72x_v2_regular_tasks_adjusted"],
            "discovered_stage1_result_files": sorted({case["stage1_result_path"] for case in summary_cases}),
            "scenario_compatibility_cases": [
                {
                    "case_name": case["case_name"],
                    "source_path": case["scenario_compat"]["source_path"],
                    "sanitized_copy_path": case["scenario_compat"]["sanitized_copy_path"],
                    "removed_stage1_fields": case["scenario_compat"]["removed_stage1_fields"],
                    "removed_stage2_fields": case["scenario_compat"]["removed_stage2_fields"],
                }
                for case in summary_cases
                if case["scenario_compat"]["removed_stage1_fields"] or case["scenario_compat"]["removed_stage2_fields"]
            ],
            "notes": (
                "Repository provided one formal regular-task corpus under results/, with two distinct "
                "scenario_weighted + stage1_result snapshots and five best_feasible candidate plans each. "
                "No second formal regular-task corpus with paired Stage1 outputs was found under results/."
            ),
        },
        "cases": summary_cases,
        "analysis": {
            "all_cases_baseline_source_stage1_greedy_repair": all(
                case["closed_loop"]["baseline_source"] == "stage1_greedy_repair" for case in summary_cases
            ),
            "all_closed_loop_runs_single_action_per_round": all(
                bool(case["closed_loop"]["validation_checks"]["single_action_per_round"]) for case in summary_cases
            ),
            "all_closed_loop_runs_recompute_consistent": all(
                bool(case["closed_loop"]["validation_checks"]["round_recompute_chain_is_consistent"]) for case in summary_cases
            ),
            "first_round_single_action_saturation_count": sum(
                1 for case in summary_cases if case["first_round_single_action_saturation"]
            ),
            "first_round_single_action_saturation_cases": [
                case["case_name"] for case in summary_cases if case["first_round_single_action_saturation"]
            ],
            "q_peak_improved_case_count": len(q_peak_improved_cases),
            "q_peak_improved_cases": q_peak_improved_cases,
            "integral_or_highseg_only_case_count": len(integral_or_highseg_only_cases),
            "integral_or_highseg_only_cases": integral_or_highseg_only_cases,
            "stop_reason_counts": stop_reason_counts,
            "improvement_vs_baseline_case_count": sum(
                1 for case in summary_cases if case["improvement_vs_baseline"]["improves_any_load_metric_vs_baseline"]
            ),
            "improvement_vs_baseline_cases": [
                case["case_name"]
                for case in summary_cases
                if case["improvement_vs_baseline"]["improves_any_load_metric_vs_baseline"]
            ],
            "range_topk_truncation_possible_case_count": sum(
                1 for case in summary_cases if case["coverage_diagnostics"]["range_topk_truncation_possible"]
            ),
            "range_topk_truncation_possible_cases": [
                case["case_name"]
                for case in summary_cases
                if case["coverage_diagnostics"]["range_topk_truncation_possible"]
            ],
            "structural_candidate_limit_hit_case_count": sum(
                1 for case in summary_cases if case["coverage_diagnostics"]["structural_candidate_limit_hit"]
            ),
            "structural_candidate_limit_hit_cases": [
                case["case_name"]
                for case in summary_cases
                if case["coverage_diagnostics"]["structural_candidate_limit_hit"]
            ],
            "head_reordered_after_first_acceptance_case_count": sum(
                1
                for case in summary_cases
                if case["coverage_diagnostics"]["head_reordered_after_first_acceptance"] is True
            ),
            "head_reordered_after_first_acceptance_cases": [
                case["case_name"]
                for case in summary_cases
                if case["coverage_diagnostics"]["head_reordered_after_first_acceptance"] is True
            ],
        },
    }

    _write_json(output_root / "summary.json", overall_summary)
    (output_root / "summary.md").write_text(_overall_markdown(overall_summary), encoding="utf-8")
    print(json.dumps({"output_root": str(output_root), "case_count": len(summary_cases)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
