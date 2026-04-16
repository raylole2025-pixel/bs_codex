from __future__ import annotations

import argparse
import json
import sys
import time
from copy import deepcopy
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from bs3.scenario import load_scenario
from bs3.stage1 import RegularEvaluator
from bs3.stage2 import run_stage2
from tools.stage2_emergency_validation_lib import (
    DEFAULT_OUTPUT_ROOT,
    load_stage1_artifacts,
    stage2_result_to_dict,
    summarize_task_outcomes,
    write_json,
)


def _sanitize_slug(value: str) -> str:
    chars: list[str] = []
    for ch in str(value).strip().lower():
        if ch.isalnum():
            chars.append(ch)
        else:
            chars.append("_")
    slug = "".join(chars).strip("_")
    while "__" in slug:
        slug = slug.replace("__", "_")
    return slug or "stage2_smoke"


def _load_task_rows(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8-sig"))
    if isinstance(payload, dict):
        raw_tasks = payload.get("tasks", payload.get("items", payload.get("emergencies", [])))
    else:
        raw_tasks = payload
    if not isinstance(raw_tasks, list):
        raise ValueError(f"Task JSON {path} must contain a task list")
    return [dict(item) for item in raw_tasks]


def _task_type_count(tasks: list[dict[str, Any]], task_type: str) -> int:
    target = task_type.strip().lower()
    return sum(1 for item in tasks if str(item.get("type", "")).strip().lower() == target)


def _build_run_summary(
    *,
    base_scenario_path: Path,
    stage1_result_path: Path,
    task_json_path: Path,
    effective_scenario_path: Path,
    task_rows: list[dict[str, Any]],
    selected_candidate: dict[str, Any],
    result: Any,
    task_outcomes: dict[str, Any],
) -> dict[str, Any]:
    insertion_events = list(result.metadata.get("emergency_insertions") or [])
    strategy_counts: dict[str, int] = {}
    for event in insertion_events:
        strategy = str(event.get("strategy", "unknown"))
        strategy_counts[strategy] = strategy_counts.get(strategy, 0) + 1
    return {
        "files_used": {
            "stage1_result_file": str(stage1_result_path),
            "base_scenario_file": str(base_scenario_path),
            "task_json_file": str(task_json_path),
            "effective_scenario_file": str(effective_scenario_path),
        },
        "input_counts": {
            "task_total": len(task_rows),
            "regular_task_count": _task_type_count(task_rows, "reg"),
            "emergency_task_count": _task_type_count(task_rows, "emg"),
        },
        "selected_stage1_candidate": selected_candidate,
        "stage2_metrics": {
            "cr_reg": float(result.cr_reg),
            "cr_emg": float(result.cr_emg),
            "n_preemptions": int(result.n_preemptions),
            "u_cross": float(result.u_cross),
            "u_all": float(result.u_all),
            "solver_mode": str(result.solver_mode),
        },
        "strategy_counts": strategy_counts,
        "preempted_regular_tasks": list(result.metadata.get("preempted_regular_tasks") or []),
        "recovery_event_count": len(result.metadata.get("recovery_events") or []),
        "recovered_regular_tasks": list(result.metadata.get("recovered_regular_tasks") or []),
        "recovered_regular_completed_count": int(result.metadata.get("recovered_regular_completed_count", 0)),
        "task_outcomes": task_outcomes,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run Stage2 on a mixed regular+emergency smoke task JSON with a fixed Stage1 plan/baseline trace."
    )
    parser.add_argument("--base-scenario", required=True, help="Base scenario JSON used for topology/windows/capacities")
    parser.add_argument("--stage1-result", required=True, help="Stage1 result JSON that contains selected_plan/baseline_trace")
    parser.add_argument("--task-json", required=True, help="Mixed task JSON (regular + emergency)")
    parser.add_argument("--candidate-index", type=int, default=0, help="Stage1 candidate index to replay")
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT), help="Output root directory")
    parser.add_argument("--run-name", default=None, help="Optional output directory name")
    args = parser.parse_args()

    base_scenario_path = Path(args.base_scenario).resolve()
    stage1_result_path = Path(args.stage1_result).resolve()
    task_json_path = Path(args.task_json).resolve()
    output_root = Path(args.output_root).resolve()
    output_root.mkdir(parents=True, exist_ok=True)

    timestamp = time.strftime("%Y%m%d_%H%M%S")
    default_run_name = f"{_sanitize_slug(task_json_path.stem)}_{timestamp}"
    run_name = args.run_name or default_run_name
    run_dir = output_root / run_name
    run_dir.mkdir(parents=True, exist_ok=True)

    base_payload = json.loads(base_scenario_path.read_text(encoding="utf-8-sig"))
    task_rows = _load_task_rows(task_json_path)
    combined_payload = deepcopy(base_payload)
    combined_payload["tasks"] = task_rows
    combined_payload.setdefault("metadata", {})
    combined_payload["metadata"]["fixed_stage1_smoke"] = {
        "task_json_file": str(task_json_path),
        "stage1_result_file": str(stage1_result_path),
        "candidate_index": int(args.candidate_index),
        "run_name": run_name,
    }

    effective_scenario_path = run_dir / "effective_scenario.json"
    write_json(effective_scenario_path, combined_payload)

    stage1_artifacts = load_stage1_artifacts(stage1_result_path)
    plan, plan_rows, candidate_info, uses_selected_plan = stage1_artifacts.resolve_candidate(args.candidate_index)
    baseline_trace = stage1_artifacts.baseline_trace
    if baseline_trace is None:
        scenario_for_baseline = load_scenario(base_scenario_path)
        baseline_trace = RegularEvaluator(scenario_for_baseline).baseline_trace(plan, rho=scenario_for_baseline.stage1.rho)

    write_json(
        run_dir / "selected_stage1_plan.json",
        {
            "candidate_index": int(args.candidate_index),
            "uses_stage1_selected_plan": bool(uses_selected_plan),
            "candidate_info": candidate_info,
            "plan": plan_rows,
        },
    )

    scenario = load_scenario(effective_scenario_path)
    result = run_stage2(scenario, plan=plan, baseline_trace=baseline_trace)
    task_outcomes = summarize_task_outcomes(scenario, result.allocations)

    stage2_result_path = run_dir / "stage2_result.json"
    write_json(stage2_result_path, stage2_result_to_dict(result, float(scenario.stage1.t_pre)))

    run_summary = _build_run_summary(
        base_scenario_path=base_scenario_path,
        stage1_result_path=stage1_result_path,
        task_json_path=task_json_path,
        effective_scenario_path=effective_scenario_path,
        task_rows=task_rows,
        selected_candidate={
            "candidate_index": int(args.candidate_index),
            "uses_stage1_selected_plan": bool(uses_selected_plan),
            "candidate_info": candidate_info,
        },
        result=result,
        task_outcomes=task_outcomes,
    )
    run_summary_path = run_dir / "run_summary.json"
    write_json(run_summary_path, run_summary)

    print(
        json.dumps(
            {
                "run_dir": str(run_dir),
                "effective_scenario_json": str(effective_scenario_path),
                "stage2_result_json": str(stage2_result_path),
                "run_summary_json": str(run_summary_path),
            },
            indent=2,
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
