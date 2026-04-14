from __future__ import annotations

import argparse
import json
import sys
from copy import deepcopy
from dataclasses import asdict
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from bs3.models import Allocation, ScheduledWindow, Stage1BaselineTrace, Stage2Config
from bs3.scenario import load_scenario
from bs3.stage1 import activation_count, gateway_count
from bs3.stage2 import run_stage2
from apps.run_stage1_workbook_batch import read_task_sets, task_stats, workbook_task_to_payload

EPS = 1e-9
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "results" / "generated" / "stage2_taskset_runs"


def _float(value: Any, default: float = 0.0) -> float:
    if value in {None, ""}:
        return default
    return float(value)


def load_stage1_artifacts(stage1_result_path: Path, candidate_index: int = 0) -> tuple[list[ScheduledWindow], Stage1BaselineTrace | None]:
    payload = json.loads(stage1_result_path.read_text(encoding="utf-8"))
    if "stage1" in payload and isinstance(payload["stage1"], dict):
        payload = payload["stage1"]
    plan_rows = payload.get("selected_plan") or []
    if not plan_rows:
        candidates = payload.get("best_feasible") or []
        if not candidates:
            raise ValueError(f"No selected_plan or best_feasible candidates found in {stage1_result_path}")
        if candidate_index < 0 or candidate_index >= len(candidates):
            raise IndexError(f"candidate_index {candidate_index} out of range for {stage1_result_path}")
        plan_rows = candidates[candidate_index].get("plan") or []
    if not plan_rows:
        raise ValueError(f"No selected_plan found in {stage1_result_path}")
    plan = [
        ScheduledWindow(
            window_id=str(item["window_id"]),
            a=str(item["a"]),
            b=str(item["b"]),
            start=float(item["start"]),
            end=float(item["end"]),
            on=float(item["on"]),
            off=float(item["off"]),
            value=item.get("value"),
            delay=_float(item.get("delay"), 0.0),
            distance_km=item.get("distance_km"),
        )
        for item in plan_rows
    ]
    baseline_trace = None
    baseline_trace_file = payload.get("baseline_trace_file")
    baseline_trace_payload = payload.get("baseline_trace")
    if baseline_trace_file:
        trace_path = Path(baseline_trace_file)
        if not trace_path.is_absolute():
            trace_path = stage1_result_path.parent / trace_path
        trace_payload = json.loads(trace_path.read_text(encoding="utf-8"))
    else:
        trace_payload = baseline_trace_payload
    if isinstance(trace_payload, dict):
        baseline_trace = Stage1BaselineTrace(
            rho=float(trace_payload.get("rho", 0.0)),
            segments=list(trace_payload.get("segments") or []),
            allocations=[
                Allocation(
                    task_id=str(item["task_id"]),
                    segment_index=int(item["segment_index"]),
                    path_id=str(item["path_id"]),
                    edge_ids=tuple(item.get("edge_ids") or ()),
                    rate=float(item["rate"]),
                    delivered=float(item["delivered"]),
                    task_type=str(item["task_type"]),
                    cross_window_id=item.get("cross_window_id"),
                    is_preempted=bool(item.get("is_preempted", False)),
                )
                for item in (trace_payload.get("allocations") or [])
            ],
            task_states=list(trace_payload.get("task_states") or []),
            window_states=list(trace_payload.get("window_states") or []),
            remaining_before_by_segment={
                str(task_id): {int(index): float(value) for index, value in values.items()}
                for task_id, values in (trace_payload.get("remaining_before_by_segment") or {}).items()
            },
            remaining_after_by_segment={
                str(task_id): {int(index): float(value) for index, value in values.items()}
                for task_id, values in (trace_payload.get("remaining_after_by_segment") or {}).items()
            },
            remaining_end={str(task_id): float(value) for task_id, value in (trace_payload.get("remaining_end") or {}).items()},
            completed={str(task_id): bool(value) for task_id, value in (trace_payload.get("completed") or {}).items()},
            cross_window_usage_by_segment={
                int(segment_index): {str(window_id): float(value) for window_id, value in usage.items()}
                for segment_index, usage in (trace_payload.get("cross_window_usage_by_segment") or {}).items()
            },
            available_cross_capacity_by_segment={
                int(segment_index): {str(window_id): float(value) for window_id, value in usage.items()}
                for segment_index, usage in (trace_payload.get("available_cross_capacity_by_segment") or {}).items()
            },
            occupied_cross_windows_by_segment={
                int(segment_index): list(window_ids)
                for segment_index, window_ids in (trace_payload.get("occupied_cross_windows_by_segment") or {}).items()
            },
            active_cross_windows_by_segment={
                int(segment_index): list(window_ids)
                for segment_index, window_ids in (trace_payload.get("active_cross_windows_by_segment") or {}).items()
            },
            active_intra_edges_by_segment={
                int(segment_index): {str(domain): list(edge_ids) for domain, edge_ids in values.items()}
                for segment_index, values in (trace_payload.get("active_intra_edges_by_segment") or {}).items()
            },
            available_intra_capacity_by_segment={
                int(segment_index): {str(edge_id): float(value) for edge_id, value in values.items()}
                for segment_index, values in (trace_payload.get("available_intra_capacity_by_segment") or {}).items()
            },
            summary=dict(trace_payload.get("summary") or {}),
        )
    return plan, baseline_trace


def build_scenario_payload(
    base_payload: dict[str, Any],
    workbook_path: Path,
    sheet_name: str,
    tasks: list[dict[str, Any]],
    args: argparse.Namespace,
) -> dict[str, Any]:
    payload = deepcopy(base_payload)
    payload["tasks"] = tasks
    metadata = dict(payload.get("metadata", {}))
    metadata.update(
        {
            "name": f"stage2-taskset-{sheet_name}",
            "source": workbook_path.name,
            "taskset_workbook": str(workbook_path),
            "taskset_sheet": sheet_name,
            "task_units": {"data": "Mb", "rate": "Mbps"},
            "runner": "apps/run_stage2_workbook_sheet.py",
        }
    )
    payload["metadata"] = metadata

    stage2_cfg = dict(payload.get("stage2", {}))
    effective_k_paths = args.k_paths if args.k_paths is not None else stage2_cfg.get("k_paths", 2)
    default_stage2 = asdict(
        Stage2Config(
            k_paths=effective_k_paths,
            completion_tolerance=stage2_cfg.get("completion_tolerance", 1e-6),
        )
    )
    default_stage2.update(stage2_cfg)
    default_stage2["k_paths"] = effective_k_paths
    default_stage2["completion_tolerance"] = stage2_cfg.get("completion_tolerance", 1e-6)
    payload["stage2"] = default_stage2
    return payload


def stage2_to_dict(result, t_pre: float) -> dict[str, Any]:
    data = asdict(result)
    data["gateway_count"] = gateway_count(result.plan)
    data["activation_count"] = activation_count(result.plan, t_pre)
    data["plan"] = [asdict(window) for window in result.plan]
    data["allocations"] = [asdict(item) for item in result.allocations]
    return data


def summarize_result(scenario, result) -> dict[str, Any]:
    delivered = {task.task_id: 0.0 for task in scenario.tasks}
    for alloc in result.allocations:
        delivered[alloc.task_id] = delivered.get(alloc.task_id, 0.0) + alloc.delivered

    per_type: dict[str, dict[str, Any]] = {}
    success_total = 0
    task_rows: list[dict[str, Any]] = []

    for task in scenario.tasks:
        delivered_amount = min(delivered.get(task.task_id, 0.0), task.data)
        remaining = max(task.data - delivered_amount, 0.0)
        success = remaining <= max(float(scenario.stage2.completion_tolerance) * float(task.data), EPS)
        completion_ratio = delivered_amount / task.data if task.data > EPS else 1.0
        bucket = per_type.setdefault(
            task.task_type,
            {"count": 0, "success_count": 0, "success_rate": 0.0, "mean_completion_ratio": 0.0},
        )
        bucket["count"] += 1
        bucket["success_count"] += int(success)
        bucket["mean_completion_ratio"] += completion_ratio
        success_total += int(success)
        task_rows.append(
            {
                "task_id": task.task_id,
                "task_type": task.task_type,
                "arrival": task.arrival,
                "deadline": task.deadline,
                "data": task.data,
                "delivered": delivered_amount,
                "remaining": remaining,
                "completion_ratio": completion_ratio,
                "success": success,
            }
        )

    for stats in per_type.values():
        if stats["count"] > 0:
            stats["success_rate"] = stats["success_count"] / stats["count"]
            stats["mean_completion_ratio"] /= stats["count"]

    total_count = len(scenario.tasks)
    return {
        "task_counts": {
            "total": total_count,
            "reg": per_type.get("reg", {}).get("count", 0),
            "emg": per_type.get("emg", {}).get("count", 0),
        },
        "success_counts": {
            "total": success_total,
            "reg": per_type.get("reg", {}).get("success_count", 0),
            "emg": per_type.get("emg", {}).get("success_count", 0),
        },
        "success_rates": {
            "total": success_total / total_count if total_count else 1.0,
            "reg": per_type.get("reg", {}).get("success_rate", 1.0),
            "emg": per_type.get("emg", {}).get("success_rate", 1.0),
        },
        "mean_completion_ratio": {
            "reg": per_type.get("reg", {}).get("mean_completion_ratio", 1.0),
            "emg": per_type.get("emg", {}).get("mean_completion_ratio", 1.0),
        },
        "solver_metrics": {
            "cr_reg": result.cr_reg,
            "cr_emg": result.cr_emg,
            "n_preemptions": result.n_preemptions,
            "u_cross": result.u_cross,
            "u_all": result.u_all,
            "solver_mode": result.solver_mode,
            **dict(result.metadata or {}),
        },
        "tasks": task_rows,
    }


def write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run stage2 emergency insertion using stage1 selected_plan + baseline_trace.")
    parser.add_argument("--workbook", required=True, help="Path to task workbook (.xlsx)")
    parser.add_argument("--sheet", required=True, help="Workbook sheet name to run")
    parser.add_argument("--base-scenario", required=True, help="Scenario JSON to use as the topology/delay template")
    parser.add_argument("--stage1-result", required=True, help="Stage1 result JSON containing selected_plan and baseline_trace_file")
    parser.add_argument("--candidate-index", type=int, default=0, help="Fallback best_feasible candidate index when selected_plan is absent")
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT), help="Output directory root")
    parser.add_argument("--k-paths", type=int, default=None)
    args = parser.parse_args()

    workbook_path = Path(args.workbook)
    base_scenario_path = Path(args.base_scenario)
    stage1_result_path = Path(args.stage1_result)
    output_root = Path(args.output_root)
    set_dir = output_root / args.sheet
    set_dir.mkdir(parents=True, exist_ok=True)

    _, task_sets = read_task_sets(workbook_path)
    if args.sheet not in task_sets:
        raise KeyError(f"Sheet {args.sheet} not found in {workbook_path}")

    workbook_rows = task_sets[args.sheet]
    tasks = [workbook_task_to_payload(row) for row in workbook_rows]
    stats = task_stats(tasks)
    plan, baseline_trace = load_stage1_artifacts(stage1_result_path, candidate_index=args.candidate_index)
    base_payload = json.loads(base_scenario_path.read_text(encoding="utf-8-sig"))

    scenario_payload = build_scenario_payload(base_payload, workbook_path, args.sheet, tasks, args)
    scenario_path = set_dir / f"{args.sheet}_scenario_stage2_input.json"
    write_json(scenario_path, scenario_payload)

    scenario_run_path = set_dir / f"{args.sheet}_stage2_scenario.json"
    write_json(scenario_run_path, scenario_payload)
    scenario = load_scenario(scenario_run_path)
    result = run_stage2(scenario, plan=plan, baseline_trace=baseline_trace)
    metrics = summarize_result(scenario, result)
    payload = {
        "sheet": args.sheet,
        "workbook": str(workbook_path),
        "base_scenario": str(base_scenario_path),
        "stage1_result": str(stage1_result_path),
        "candidate_index": args.candidate_index,
        "task_stats": stats,
        "metrics": metrics,
        "stage2_result": stage2_to_dict(result, scenario.stage1.t_pre),
    }
    result_path = set_dir / f"{args.sheet}_stage2_result.json"
    write_json(result_path, payload)

    summary = {
        "sheet": args.sheet,
        "workbook": str(workbook_path),
        "base_scenario": str(base_scenario_path),
        "stage1_result": str(stage1_result_path),
        "candidate_index": args.candidate_index,
        "task_stats": stats,
        "result_file": str(result_path),
        "success_rates": metrics["success_rates"],
        "success_counts": metrics["success_counts"],
        "solver_metrics": metrics["solver_metrics"],
    }
    summary_path = set_dir / f"{args.sheet}_stage2_summary.json"
    write_json(summary_path, summary)
    print(json.dumps(summary, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
