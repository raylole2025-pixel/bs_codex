from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
import traceback
from collections import defaultdict
from copy import deepcopy
from dataclasses import asdict, replace
from datetime import datetime
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from bs3.models import Scenario, ScheduledWindow, Stage2Result, Task
from bs3.scenario import build_segments, load_scenario, scenario_to_dict
from bs3.stage2 import run_stage2


DEFAULT_FULL_OUTER_TIMEOUT_SECONDS = 900.0


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def _load_plan(stage1_result_path: Path, candidate_index: int = 0) -> list[ScheduledWindow]:
    payload = json.loads(stage1_result_path.read_text(encoding="utf-8"))
    best = payload.get("best_feasible", [])
    if best:
        plan_rows = best[candidate_index]["plan"]
    else:
        plan_rows = payload["population_best"]["plan"]
    return [ScheduledWindow(**row) for row in plan_rows]


def _save_plan(plan: list[ScheduledWindow], path: Path) -> None:
    _write_json(path, [asdict(window) for window in plan])


def _load_plan_json(path: Path) -> list[ScheduledWindow]:
    return [ScheduledWindow(**row) for row in json.loads(path.read_text(encoding="utf-8"))]


def _task_summary(scenario: Scenario, result: Stage2Result) -> dict[str, Any]:
    delivered_by_task: dict[str, float] = defaultdict(float)
    for allocation in result.allocations:
        delivered_by_task[allocation.task_id] += float(allocation.delivered)

    per_type: dict[str, dict[str, float | int]] = defaultdict(
        lambda: {
            "count": 0,
            "success_count": 0,
            "success_rate": 0.0,
        }
    )
    total_count = len(scenario.tasks)
    total_success = 0

    for task in scenario.tasks:
        tolerance = max(float(scenario.stage2.completion_tolerance) * max(float(task.data), 0.0), 1e-9)
        remaining = max(float(task.data) - delivered_by_task.get(task.task_id, 0.0), 0.0)
        success = remaining <= tolerance + 1e-9
        stats = per_type[task.task_type]
        stats["count"] += 1
        if success:
            stats["success_count"] += 1
            total_success += 1

    for stats in per_type.values():
        count = int(stats["count"])
        stats["success_rate"] = float(stats["success_count"]) / count if count else 1.0

    return {
        "success_counts": {
            "total": total_success,
            "reg": int(per_type.get("reg", {}).get("success_count", 0)),
            "emg": int(per_type.get("emg", {}).get("success_count", 0)),
        },
        "success_rates": {
            "total": (total_success / total_count) if total_count else 1.0,
            "reg": float(per_type.get("reg", {}).get("success_rate", 1.0)),
            "emg": float(per_type.get("emg", {}).get("success_rate", 1.0)),
        },
    }


def _result_payload(scenario: Scenario, result: Stage2Result, elapsed_seconds: float) -> dict[str, Any]:
    summary = _task_summary(scenario, result)
    return {
        "success": True,
        "elapsed_seconds": elapsed_seconds,
        "solver_mode": result.solver_mode,
        "metadata": dict(result.metadata or {}),
        "success_counts": summary["success_counts"],
        "success_rates": summary["success_rates"],
        "cr_reg": result.cr_reg,
        "cr_emg": result.cr_emg,
        "u_cross": result.u_cross,
        "u_all": result.u_all,
        "n_preemptions": result.n_preemptions,
        "allocations_count": len(result.allocations),
    }


def _build_experiment_row(spec: dict[str, Any], result_payload: dict[str, Any]) -> dict[str, Any]:
    metadata = dict(result_payload.get("metadata") or {})
    return {
        "taskset_name": spec["taskset_name"],
        "mode": spec["mode"],
        "params": spec["params"],
        "success": bool(result_payload.get("success", False)),
        "elapsed_seconds": result_payload.get("elapsed_seconds"),
        "success_counts": result_payload.get("success_counts"),
        "success_rates": result_payload.get("success_rates"),
        "cr_reg": result_payload.get("cr_reg"),
        "u_cross": result_payload.get("u_cross"),
        "u_all": result_payload.get("u_all"),
        "n_preemptions": result_payload.get("n_preemptions"),
        "event_segment_count": metadata.get("event_segment_count", spec.get("event_segment_count")),
        "regular_task_count": metadata.get("regular_task_count", spec.get("regular_task_count")),
        "solver_mode": result_payload.get("solver_mode"),
        "metadata": metadata,
        "result_file": result_payload.get("result_file"),
        "error": result_payload.get("error"),
    }


def _materialize_scenario(
    base_scenario: Scenario,
    *,
    tasks: list[Task],
    planning_end: float | None,
    stage2_updates: dict[str, Any],
    experiment_name: str,
) -> Scenario:
    metadata = deepcopy(dict(base_scenario.metadata))
    metadata.pop("_runtime_cache", None)
    metadata["experiment_name"] = experiment_name
    updated_stage2 = replace(base_scenario.stage2, **stage2_updates)
    return replace(
        base_scenario,
        tasks=list(tasks),
        planning_end=(float(planning_end) if planning_end is not None else float(base_scenario.planning_end)),
        stage2=updated_stage2,
        metadata=metadata,
    )


def _scenario_payload_without_runtime_cache(scenario: Scenario) -> dict[str, Any]:
    payload = scenario_to_dict(scenario)
    payload["metadata"] = dict(payload.get("metadata", {}))
    payload["metadata"].pop("_runtime_cache", None)
    return payload


def _half_range_cutoff(tasks: list[Task], planning_end: float) -> float:
    max_time = max([float(planning_end)] + [float(task.arrival) for task in tasks] + [float(task.deadline) for task in tasks])
    min_time = min([0.0] + [float(task.arrival) for task in tasks])
    return min_time + 0.5 * (max_time - min_time)


def _run_worker(spec_path: Path) -> int:
    spec = json.loads(spec_path.read_text(encoding="utf-8"))
    scenario = load_scenario(spec["scenario_path"])
    plan = _load_plan_json(Path(spec["plan_path"]))
    result_path = Path(spec["result_path"])
    started = time.perf_counter()
    try:
        result = run_stage2(scenario, plan)
        payload = _result_payload(scenario, result, time.perf_counter() - started)
        payload["result_file"] = str(result_path)
    except Exception as exc:
        payload = {
            "success": False,
            "elapsed_seconds": time.perf_counter() - started,
            "error": f"{type(exc).__name__}: {exc}",
            "traceback": traceback.format_exc(),
            "result_file": str(result_path),
        }
    _write_json(result_path, payload)
    return 0 if payload.get("success") else 1


def _run_subprocess(script_path: Path, spec_path: Path, *, timeout_seconds: float | None) -> tuple[bool, str | None]:
    try:
        subprocess.run(
            [sys.executable, str(script_path), "--worker", str(spec_path)],
            check=True,
            timeout=timeout_seconds,
        )
        return True, None
    except subprocess.TimeoutExpired:
        return False, f"TimeoutExpired: outer timeout {timeout_seconds}s"
    except subprocess.CalledProcessError as exc:
        return False, f"CalledProcessError: exit_code={exc.returncode}"


def _markdown_table(rows: list[dict[str, Any]]) -> str:
    headers = [
        "taskset_name",
        "mode",
        "success",
        "elapsed_seconds",
        "cr_reg",
        "u_cross",
        "u_all",
        "n_preemptions",
        "event_segment_count",
    ]
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
    ]
    for row in rows:
        values = []
        for key in headers:
            value = row.get(key)
            if isinstance(value, float):
                values.append(f"{value:.6f}")
            else:
                values.append(str(value))
        lines.append("| " + " | ".join(values) + " |")
    return "\n".join(lines)


def _find_default_scenario(repo_root: Path) -> Path:
    matches = sorted(repo_root.rglob("normal72x_v2_regular_tasks_adjusted_scenario_weighted.json"))
    if not matches:
        raise FileNotFoundError("normal72x_v2_regular_tasks_adjusted_scenario_weighted.json not found")
    return matches[0]


def _find_default_stage1_result(repo_root: Path) -> Path:
    candidates = [
        repo_root / "outputs" / "active" / "stage1_validation_tmp" / "normal72x_seed13_no_runtime_limit_result.json",
        repo_root / "outputs" / "active" / "stage1_validation_tmp" / "normal72x_seed7_no_runtime_limit_result.json",
    ]
    for path in candidates:
        if path.exists():
            return path
    raise FileNotFoundError("No default stage1 result found for normal72x_v2_regular_tasks_adjusted")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Stage2-1 rolling/full MILP experiment suites.")
    parser.add_argument("--worker", type=str, default=None, help="Internal worker mode; path to spec JSON")
    parser.add_argument("--scenario", type=str, default=None, help="Weighted scenario JSON")
    parser.add_argument("--stage1-result", type=str, default=None, help="Stage1 result JSON used to load the plan")
    parser.add_argument("--output-root", type=str, default=None, help="Experiment output directory root")
    parser.add_argument("--full-outer-timeout", type=float, default=DEFAULT_FULL_OUTER_TIMEOUT_SECONDS)
    parser.add_argument("--rolling-outer-timeout", type=float, default=None)
    parser.add_argument("--groups", nargs="*", default=["A", "B"], help="Experiment groups to run: A and/or B")
    parser.add_argument("--skip-existing", action="store_true", help="Reuse existing result JSON files when present")
    args = parser.parse_args()

    if args.worker:
        raise SystemExit(_run_worker(Path(args.worker)))

    repo_root = Path(__file__).resolve().parents[1]
    scenario_path = Path(args.scenario) if args.scenario else _find_default_scenario(repo_root)
    stage1_result_path = Path(args.stage1_result) if args.stage1_result else _find_default_stage1_result(repo_root)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_root = (
        Path(args.output_root)
        if args.output_root
        else repo_root / "outputs" / "active" / "stage2_milp_experiments" / f"normal72x_v2_regular_tasks_adjusted_{timestamp}"
    )
    output_root.mkdir(parents=True, exist_ok=True)

    base_scenario = load_scenario(scenario_path)
    plan = _load_plan(stage1_result_path)
    plan_path = output_root / "selected_stage1_plan.json"
    _save_plan(plan, plan_path)

    rolling_specs = [
        ("A_a", {"milp_horizon_segments": 16, "milp_commit_segments": 8, "milp_rolling_path_limit": 1, "milp_rolling_high_path_limit": 2, "milp_rolling_promoted_tasks_per_segment": 2}),
        ("A_b", {"milp_horizon_segments": 20, "milp_commit_segments": 10, "milp_rolling_path_limit": 1, "milp_rolling_high_path_limit": 2, "milp_rolling_promoted_tasks_per_segment": 2}),
        ("A_c", {"milp_horizon_segments": 24, "milp_commit_segments": 8, "milp_rolling_path_limit": 1, "milp_rolling_high_path_limit": 2, "milp_rolling_promoted_tasks_per_segment": 3}),
    ]

    regular_tasks = sorted(
        [task for task in base_scenario.tasks if task.task_type == "reg"],
        key=lambda task: (float(task.arrival), float(task.deadline), task.task_id),
    )
    all_rows: list[dict[str, Any]] = []

    def run_spec(
        spec_name: str,
        scenario_obj: Scenario,
        *,
        taskset_name: str,
        mode: str,
        params: dict[str, Any],
        timeout_seconds: float | None,
    ) -> dict[str, Any]:
        scenario_file = output_root / "scenarios" / f"{spec_name}.json"
        result_file = output_root / "results" / f"{spec_name}.json"
        spec_file = output_root / "specs" / f"{spec_name}.json"
        payload = _scenario_payload_without_runtime_cache(scenario_obj)
        _write_json(scenario_file, payload)
        event_segment_count = len(build_segments(scenario_obj, plan, [task for task in scenario_obj.tasks if task.task_type == "reg"]))
        spec = {
            "name": spec_name,
            "scenario_path": str(scenario_file),
            "plan_path": str(plan_path),
            "result_path": str(result_file),
            "taskset_name": taskset_name,
            "mode": mode,
            "params": params,
            "event_segment_count": event_segment_count,
            "regular_task_count": sum(1 for task in scenario_obj.tasks if task.task_type == "reg"),
        }
        _write_json(spec_file, spec)
        if args.skip_existing and result_file.exists():
            result_payload = json.loads(result_file.read_text(encoding="utf-8"))
        else:
            success, error_text = _run_subprocess(Path(__file__), spec_file, timeout_seconds=timeout_seconds)
            if result_file.exists():
                result_payload = json.loads(result_file.read_text(encoding="utf-8"))
            else:
                result_payload = {
                    "success": False,
                    "elapsed_seconds": timeout_seconds,
                    "error": error_text or "No result file produced",
                    "result_file": str(result_file),
                }
            if not success and "error" not in result_payload:
                result_payload["error"] = error_text
        row = _build_experiment_row(spec, result_payload)
        all_rows.append(row)
        return row

    groups = {item.upper() for item in args.groups}

    if "A" in groups:
        for label, updates in rolling_specs:
            scenario_obj = _materialize_scenario(
                base_scenario,
                tasks=regular_tasks,
                planning_end=None,
                stage2_updates={
                    "prefer_milp": True,
                    "milp_mode": "rolling",
                    "milp_horizon_segments": updates["milp_horizon_segments"],
                    "milp_commit_segments": updates["milp_commit_segments"],
                    "milp_rolling_path_limit": updates["milp_rolling_path_limit"],
                    "milp_rolling_high_path_limit": updates["milp_rolling_high_path_limit"],
                    "milp_rolling_promoted_tasks_per_segment": updates["milp_rolling_promoted_tasks_per_segment"],
                    "milp_time_limit_seconds": None,
                },
                experiment_name=label,
            )
            run_spec(
                label,
                scenario_obj,
                taskset_name="normal72x_v2_regular_tasks_adjusted",
                mode="rolling",
                params=updates | {"milp_time_limit_seconds": None},
                timeout_seconds=args.rolling_outer_timeout,
            )

    if "B" in groups:
        for limit in (24, 36, 48):
            subset = regular_tasks[:limit]
            full_label = f"B_full_front{limit}"
            full_scenario = _materialize_scenario(
                base_scenario,
                tasks=subset,
                planning_end=None,
                stage2_updates={
                    "prefer_milp": True,
                    "milp_mode": "full",
                    "milp_time_limit_seconds": None,
                },
                experiment_name=full_label,
            )
            row = run_spec(
                full_label,
                full_scenario,
                taskset_name=f"normal72x_v2_regular_tasks_adjusted_front{limit}_reg",
                mode="full",
                params={"task_limit": limit, "half_range": False, "milp_time_limit_seconds": None},
                timeout_seconds=float(args.full_outer_timeout),
            )
            if row["success"]:
                continue

            cutoff = _half_range_cutoff(subset, base_scenario.planning_end)
            half_tasks = [task for task in subset if float(task.arrival) < cutoff]
            half_label = f"{full_label}_half_range"
            half_scenario = _materialize_scenario(
                base_scenario,
                tasks=half_tasks,
                planning_end=cutoff,
                stage2_updates={
                    "prefer_milp": True,
                    "milp_mode": "full",
                    "milp_time_limit_seconds": None,
                },
                experiment_name=half_label,
            )
            run_spec(
                half_label,
                half_scenario,
                taskset_name=f"normal72x_v2_regular_tasks_adjusted_front{limit}_reg_half_range",
                mode="full",
                params={"task_limit": limit, "half_range": True, "planning_end": cutoff, "milp_time_limit_seconds": None},
                timeout_seconds=float(args.full_outer_timeout),
            )

    summary = {
        "scenario": str(scenario_path),
        "stage1_result": str(stage1_result_path),
        "plan_file": str(plan_path),
        "rolling_outer_timeout_seconds": args.rolling_outer_timeout,
        "full_outer_timeout_seconds": float(args.full_outer_timeout),
        "experiments": all_rows,
    }
    summary_path = output_root / "summary.json"
    markdown_path = output_root / "summary.md"
    _write_json(summary_path, summary)
    markdown_path.write_text(_markdown_table(all_rows), encoding="utf-8")
    print(json.dumps(summary, indent=2, ensure_ascii=False))
    print(f"summary_json={summary_path}")
    print(f"summary_md={markdown_path}")


if __name__ == "__main__":
    main()
