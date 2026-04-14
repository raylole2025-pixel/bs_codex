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
from bs3.scenario import load_scenario, scenario_to_dict
from bs3.stage2 import run_stage2


DEFAULT_BASELINE_MODE = "stage1_greedy_repair"
DEFAULT_CLOSED_LOOP_ACTION_MODE = "best_global_action"
DEFAULT_CLOSED_LOOP_MAX_ROUNDS = 3
DEFAULT_CLOSED_LOOP_MAX_NEW_WINDOWS = 2
DEFAULT_AUGMENT_WINDOW_BUDGET = 2
DEFAULT_HOTSPOT_TOPK_RANGES = 5
DEFAULT_CLOSED_LOOP_TOPK_RANGES_PER_ROUND = 5
DEFAULT_CLOSED_LOOP_TOPK_CANDIDATES_PER_RANGE = 3
DEFAULT_MILP_TIME_LIMIT_SECONDS = 30.0
DEFAULT_HOTSPOT_TOTAL_TIME_LIMIT_SECONDS = 180.0
DEFAULT_HOTSPOT_PER_RANGE_TIME_LIMIT_SECONDS = 60.0
DEFAULT_STRUCTURAL_REPAIR_GATE_TIME_LIMIT_SECONDS = 20.0
DEFAULT_HOTSPOT_LOCAL_REPAIR_TIME_LIMIT_SECONDS = 30.0


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


def _result_to_dict(result) -> dict[str, Any]:
    data = asdict(result)
    data["plan"] = [asdict(window) for window in result.plan]
    data["allocations"] = [asdict(item) for item in result.allocations]
    return data


def _scenario_payload_without_runtime_cache(scenario) -> dict[str, Any]:
    payload = scenario_to_dict(scenario)
    payload["metadata"] = dict(payload.get("metadata", {}))
    payload["metadata"].pop("_runtime_cache", None)
    return payload


def _find_default_scenario(repo_root: Path) -> Path:
    matches = sorted(repo_root.rglob("normal72x_v2_regular_tasks_adjusted_scenario_weighted.json"))
    if not matches:
        raise FileNotFoundError("normal72x_v2_regular_tasks_adjusted_scenario_weighted.json not found")
    preferred = [path for path in matches if "stage1_20260411_seed7" in str(path)]
    return preferred[0] if preferred else matches[0]


def _find_default_stage1_result(repo_root: Path) -> Path:
    matches = sorted(repo_root.rglob("normal72x_v2_regular_tasks_adjusted_stage1_result.json"))
    if not matches:
        raise FileNotFoundError("normal72x_v2_regular_tasks_adjusted_stage1_result.json not found")
    preferred = [path for path in matches if "stage1_20260411_seed7" in str(path)]
    return preferred[0] if preferred else matches[0]


def _round_summary(round_payload: dict[str, Any]) -> dict[str, Any]:
    before = dict(round_payload.get("before") or {})
    after = dict(round_payload.get("after") or {})
    chosen_action = dict(round_payload.get("chosen_action") or {})
    return {
        "round_index": int(round_payload.get("round_index", 0) or 0),
        "hot_range_ids": list(round_payload.get("hot_range_ids") or []),
        "chosen_action": {
            "action_type": chosen_action.get("action_type"),
            "range_id": chosen_action.get("range_id"),
            "window_id": chosen_action.get("window_id"),
        },
        "q_peak_before": before.get("q_peak"),
        "q_peak_after": after.get("q_peak"),
        "q_peak_delta": (
            None
            if before.get("q_peak") is None or after.get("q_peak") is None
            else float(before["q_peak"]) - float(after["q_peak"])
        ),
        "q_integral_before": before.get("q_integral"),
        "q_integral_after": after.get("q_integral"),
        "q_integral_delta": (
            None
            if before.get("q_integral") is None or after.get("q_integral") is None
            else float(before["q_integral"]) - float(after["q_integral"])
        ),
        "high_segment_count_before": before.get("high_segment_count"),
        "high_segment_count_after": after.get("high_segment_count"),
        "high_segment_count_delta": (
            None
            if before.get("high_segment_count") is None or after.get("high_segment_count") is None
            else int(before["high_segment_count"]) - int(after["high_segment_count"])
        ),
    }


def _all_rounds_single_action(rounds: list[dict[str, Any]]) -> bool:
    return all(
        (round_payload.get("chosen_action") is None)
        or isinstance(round_payload.get("chosen_action"), dict)
        for round_payload in rounds
    )


def _round_recompute_observed(rounds: list[dict[str, Any]]) -> bool:
    if len(rounds) <= 1:
        return True
    for previous, current in zip(rounds, rounds[1:]):
        previous_after = dict(previous.get("after") or {})
        current_before = dict(current.get("before") or {})
        keys = ("q_peak", "q_integral", "high_segment_count", "peak_segment_count")
        if any(previous_after.get(key) != current_before.get(key) for key in keys):
            return False
    return True


def _summary_to_markdown(summary: dict[str, Any]) -> str:
    lines = [
        "# Stage2 Closed Loop Summary",
        "",
        f"- scenario_path: `{summary['scenario_path']}`",
        f"- stage1_result_path: `{summary['stage1_result_path']}`",
        f"- baseline_source: `{summary['baseline_source']}`",
        f"- solver_mode: `{summary['solver_mode']}`",
        f"- closed_loop_rounds_completed: `{summary['closed_loop_rounds_completed']}`",
        f"- closed_loop_actions_accepted: `{summary['closed_loop_actions_accepted']}`",
        f"- closed_loop_new_windows_added: `{summary['closed_loop_new_windows_added']}`",
        f"- closed_loop_stop_reason: `{summary['closed_loop_stop_reason']}`",
        f"- effective_hard_cap: `{summary['closed_loop_new_window_hard_cap']}`",
        f"- hard_cap_limiter: `{summary['closed_loop_new_window_hard_cap_limiter']}`",
        f"- q_peak_before / after: `{summary['q_peak_before']}` -> `{summary['q_peak_after']}`",
        f"- q_integral_before / after: `{summary['q_integral_before']}` -> `{summary['q_integral_after']}`",
        f"- high_segment_count_before / after: `{summary['high_segment_count_before']}` -> `{summary['high_segment_count_after']}`",
        "",
        "## Rounds",
        "",
    ]
    for round_summary in summary["round_summaries"]:
        lines.extend(
            [
                f"- round {round_summary['round_index']}: action={round_summary['chosen_action']['action_type']} range={round_summary['chosen_action']['range_id']} window={round_summary['chosen_action']['window_id']}",
                f"  q_peak: {round_summary['q_peak_before']} -> {round_summary['q_peak_after']}",
                f"  q_integral: {round_summary['q_integral_before']} -> {round_summary['q_integral_after']}",
                f"  high_segment_count: {round_summary['high_segment_count_before']} -> {round_summary['high_segment_count_after']}",
            ]
        )
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Stage2 closed-loop experiments on a fixed Stage1 plan.")
    parser.add_argument("--scenario", default=None, help="Scenario JSON path")
    parser.add_argument("--stage1-result", default=None, help="Stage1 result JSON used to load the fixed plan")
    parser.add_argument("--candidate-index", type=int, default=0, help="best_feasible plan index when loading the Stage1 result")
    parser.add_argument("--output-root", default=None, help="Output directory")
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
    scenario_path = Path(args.scenario) if args.scenario else _find_default_scenario(repo_root)
    stage1_result_path = Path(args.stage1_result) if args.stage1_result else _find_default_stage1_result(repo_root)
    output_root = (
        Path(args.output_root)
        if args.output_root
        else repo_root / "results" / "generated" / "stage2_closed_loop_experiments" / datetime.now().strftime("%Y%m%d_%H%M%S")
    )
    output_root.mkdir(parents=True, exist_ok=True)

    scenario = load_scenario(scenario_path)
    input_plan = _load_stage1_plan(stage1_result_path, args.candidate_index)
    scenario = replace(
        scenario,
        stage2=replace(
            scenario.stage2,
            prefer_milp=False,
            regular_baseline_mode=str(args.baseline_mode),
            hotspot_relief_enabled=True,
            closed_loop_relief_enabled=True,
            fail_if_milp_disabled=False,
            closed_loop_action_mode=str(args.closed_loop_action_mode),
            closed_loop_max_rounds=max(int(args.closed_loop_max_rounds), 0),
            closed_loop_max_new_windows=max(int(args.closed_loop_max_new_windows), 0),
            augment_window_budget=max(int(args.augment_window_budget), 0),
            hotspot_topk_ranges=max(int(args.hotspot_topk_ranges), 0),
            closed_loop_topk_ranges_per_round=max(int(args.closed_loop_topk_ranges_per_round), 0),
            closed_loop_topk_candidates_per_range=max(int(args.closed_loop_topk_candidates_per_range), 0),
            milp_time_limit_seconds=(None if args.milp_time_limit_seconds in {None, 0, 0.0} else float(args.milp_time_limit_seconds)),
        ),
        metadata={
            **dict(scenario.metadata),
            "experiment_name": "stage2_closed_loop_smoke",
            "structural_repair_gate_enabled": True,
            "hotspot_total_time_limit_seconds": (
                None if args.hotspot_total_time_limit_seconds in {None, 0, 0.0} else float(args.hotspot_total_time_limit_seconds)
            ),
            "hotspot_per_range_time_limit_seconds": (
                None if args.hotspot_per_range_time_limit_seconds in {None, 0, 0.0} else float(args.hotspot_per_range_time_limit_seconds)
            ),
            "structural_repair_gate_time_limit_seconds": (
                None
                if args.structural_repair_gate_time_limit_seconds in {None, 0, 0.0}
                else float(args.structural_repair_gate_time_limit_seconds)
            ),
            "hotspot_local_repair_time_limit_seconds": (
                None if args.hotspot_local_repair_time_limit_seconds in {None, 0, 0.0} else float(args.hotspot_local_repair_time_limit_seconds)
            ),
        },
    )

    result = run_stage2(scenario, input_plan)
    hotspot_report = dict(result.metadata.get("hotspot_report") or {})
    before_after = dict(hotspot_report.get("before_after") or {})
    rounds = list(hotspot_report.get("rounds") or [])
    round_summaries = [_round_summary(item) for item in rounds]
    hard_cap_components = dict(result.metadata.get("closed_loop_new_window_hard_cap_components") or {})
    summary = {
        "scenario_path": str(scenario_path),
        "stage1_result_path": str(stage1_result_path),
        "baseline_mode": result.metadata.get("regular_baseline_mode"),
        "baseline_source": result.metadata.get("regular_baseline_source"),
        "solver_mode": result.solver_mode,
        "closed_loop_action_mode": result.metadata.get("closed_loop_action_mode"),
        "closed_loop_rounds_completed": result.metadata.get("closed_loop_rounds_completed"),
        "closed_loop_actions_accepted": result.metadata.get("closed_loop_actions_accepted"),
        "closed_loop_new_windows_added": result.metadata.get("closed_loop_new_windows_added"),
        "closed_loop_stop_reason": result.metadata.get("closed_loop_stop_reason"),
        "closed_loop_new_window_hard_cap": result.metadata.get("closed_loop_new_window_hard_cap"),
        "closed_loop_new_window_hard_cap_limiter": result.metadata.get("closed_loop_new_window_hard_cap_limiter"),
        "closed_loop_new_window_hard_cap_components": hard_cap_components,
        "cr_reg_before": before_after.get("before", {}).get("cr_reg"),
        "cr_reg_after": before_after.get("after", {}).get("cr_reg"),
        "q_peak_before": result.metadata.get("q_peak_before"),
        "q_peak_after": result.metadata.get("q_peak_after"),
        "q_integral_before": result.metadata.get("q_integral_before"),
        "q_integral_after": result.metadata.get("q_integral_after"),
        "high_segment_count_before": result.metadata.get("high_segment_count_before"),
        "high_segment_count_after": result.metadata.get("high_segment_count_after"),
        "elapsed_seconds": result.metadata.get("elapsed_seconds"),
        "milp_time_limit_seconds": scenario.stage2.milp_time_limit_seconds,
        "hotspot_total_time_limit_seconds": result.metadata.get("hotspot_total_time_limit_seconds"),
        "hotspot_per_range_time_limit_seconds": result.metadata.get("hotspot_per_range_time_limit_seconds"),
        "structural_repair_gate_time_limit_seconds": result.metadata.get("structural_repair_gate_time_limit_seconds"),
        "hotspot_local_repair_time_limit_seconds": result.metadata.get("hotspot_local_repair_time_limit_seconds"),
        "round_summaries": round_summaries,
        "smoke_checks": {
            "baseline_is_stage1_greedy_repair": result.metadata.get("regular_baseline_source") == "stage1_greedy_repair",
            "single_action_per_round": _all_rounds_single_action(rounds),
            "round_recompute_chain_is_consistent": _round_recompute_observed(rounds),
            "hard_cap_has_hidden_conflict": (
                hard_cap_components.get("closed_loop_max_new_windows") != hard_cap_components.get("augment_window_budget")
            ),
        },
    }

    _write_json(output_root / "selected_stage1_plan.json", [asdict(window) for window in input_plan])
    _write_json(output_root / "effective_scenario.json", _scenario_payload_without_runtime_cache(scenario))
    _write_json(output_root / "stage2_result.json", _result_to_dict(result))
    _write_json(output_root / "summary.json", summary)
    (output_root / "summary.md").write_text(_summary_to_markdown(summary), encoding="utf-8")
    print(json.dumps(summary, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
