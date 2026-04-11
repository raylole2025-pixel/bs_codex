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
from bs3.scenario import build_segments, compress_segments, load_scenario, scenario_to_dict
from bs3.stage2 import run_stage2


DEFAULT_FULL_OUTER_TIMEOUT_SECONDS = 900.0
DEFAULT_ROLLING_OUTER_TIMEOUT_SECONDS = 10800.0
DEFAULT_AC_DIAG_OUTER_TIMEOUT_SECONDS = 7200.0


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


def _find_default_scenario(repo_root: Path) -> Path:
    matches = sorted(repo_root.rglob("normal72x_v2_regular_tasks_adjusted_scenario_weighted.json"))
    if not matches:
        raise FileNotFoundError("normal72x_v2_regular_tasks_adjusted_scenario_weighted.json not found")
    return matches[0]


def _find_default_stage1_result(repo_root: Path) -> Path:
    matches = sorted(repo_root.rglob("normal72x_v2_regular_tasks_adjusted_stage1_result.json"))
    if not matches:
        raise FileNotFoundError("No default stage1 result found for normal72x_v2_regular_tasks_adjusted")
    return matches[0]


def _materialize_scenario(
    base_scenario: Scenario,
    *,
    tasks: list[Task],
    planning_end: float | None,
    stage2_updates: dict[str, Any],
    experiment_name: str,
    metadata_updates: dict[str, Any] | None = None,
) -> Scenario:
    metadata = deepcopy(dict(base_scenario.metadata))
    metadata.pop("_runtime_cache", None)
    metadata["experiment_name"] = experiment_name
    if metadata_updates:
        metadata.update(metadata_updates)
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


def _collapse_profile_rows(profile_path: Path | None) -> dict[str, Any]:
    if profile_path is None or not profile_path.exists():
        return {
            "profile_path": (str(profile_path) if profile_path is not None else None),
            "record_count": 0,
            "window_count": 0,
            "windows": [],
            "slowest_windows": [],
            "unfinished_windows": [],
        }

    records = [
        json.loads(line)
        for line in profile_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    latest_by_window: dict[int, dict[str, Any]] = {}
    for record in records:
        latest_by_window[int(record["window_index"])] = record
    windows = [latest_by_window[index] for index in sorted(latest_by_window)]
    slowest_windows = sorted(
        [row for row in windows if float(row.get("elapsed_seconds") or 0.0) > 0.0],
        key=lambda row: float(row.get("elapsed_seconds") or 0.0),
        reverse=True,
    )[:5]
    unfinished_windows = [
        row for row in windows
        if str(row.get("window_phase")) not in {"finished", "skipped"}
    ]
    return {
        "profile_path": str(profile_path),
        "record_count": len(records),
        "window_count": len(windows),
        "windows": windows,
        "slowest_windows": slowest_windows,
        "unfinished_windows": unfinished_windows,
    }


def _write_collapsed_profile(summary: dict[str, Any], output_path: Path) -> None:
    _write_json(output_path, summary)


def _build_experiment_row(spec: dict[str, Any], result_payload: dict[str, Any], profile_summary: dict[str, Any]) -> dict[str, Any]:
    metadata = dict(result_payload.get("metadata") or {})
    row = {
        "name": spec["name"],
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
        "prefer_milp": metadata.get("prefer_milp"),
        "milp_mode": metadata.get("milp_mode", spec.get("mode")),
        "milp_horizon_segments": metadata.get("milp_horizon_segments"),
        "milp_commit_segments": metadata.get("milp_commit_segments"),
        "milp_rolling_path_limit": metadata.get("milp_rolling_path_limit"),
        "milp_rolling_high_path_limit": metadata.get("milp_rolling_high_path_limit"),
        "milp_rolling_promoted_tasks_per_segment": metadata.get("milp_rolling_promoted_tasks_per_segment"),
        "milp_time_limit_seconds": metadata.get("milp_time_limit_seconds"),
        "milp_relative_gap": metadata.get("milp_relative_gap"),
        "event_segment_count": metadata.get("event_segment_count", spec.get("event_segment_count")),
        "event_segment_count_raw": metadata.get("event_segment_count_raw", spec.get("event_segment_count_raw")),
        "event_segment_count_compressed": metadata.get("event_segment_count_compressed", spec.get("event_segment_count_compressed")),
        "regular_task_count": metadata.get("regular_task_count", spec.get("regular_task_count")),
        "source_regular_task_count": spec.get("source_regular_task_count"),
        "solver_mode": result_payload.get("solver_mode"),
        "metadata": metadata,
        "result_file": result_payload.get("result_file"),
        "error": result_payload.get("error"),
        "rolling_profile_path": spec.get("profile_path"),
        "rolling_profile_collapsed_path": spec.get("profile_collapsed_path"),
        "rolling_profile_record_count": profile_summary.get("record_count"),
        "rolling_profile_window_count": profile_summary.get("window_count"),
        "slowest_windows": profile_summary.get("slowest_windows"),
        "unfinished_windows": profile_summary.get("unfinished_windows"),
    }
    return row


def _markdown_table(rows: list[dict[str, Any]]) -> str:
    headers = [
        "name",
        "taskset_name",
        "mode",
        "milp_mode",
        "milp_horizon_segments",
        "milp_commit_segments",
        "milp_time_limit_seconds",
        "milp_relative_gap",
        "success",
        "elapsed_seconds",
        "cr_reg",
        "u_cross",
        "u_all",
        "n_preemptions",
        "event_segment_count",
        "regular_task_count",
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


def _top_window_summary(row: dict[str, Any]) -> dict[str, Any]:
    slowest = list(row.get("slowest_windows") or [])
    unfinished = list(row.get("unfinished_windows") or [])
    return {
        "name": row["name"],
        "slowest_windows": slowest[:3],
        "unfinished_windows": unfinished[:3],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Stage2-1 rolling robustness and full-compression diagnostics.")
    parser.add_argument("--worker", type=str, default=None, help="Internal worker mode; path to spec JSON")
    parser.add_argument("--scenario", type=str, default=None, help="Weighted scenario JSON")
    parser.add_argument("--stage1-result", type=str, default=None, help="Stage1 result JSON used to load the plan")
    parser.add_argument("--output-root", type=str, default=None, help="Experiment output directory root")
    parser.add_argument("--rolling-outer-timeout", type=float, default=DEFAULT_ROLLING_OUTER_TIMEOUT_SECONDS)
    parser.add_argument("--ac-diag-outer-timeout", type=float, default=DEFAULT_AC_DIAG_OUTER_TIMEOUT_SECONDS)
    parser.add_argument("--full-outer-timeout", type=float, default=DEFAULT_FULL_OUTER_TIMEOUT_SECONDS)
    parser.add_argument("--groups", nargs="*", default=["rolling", "ac_diag", "b_diag", "compress"], help="Experiment groups to run")
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
        else repo_root / "results" / "generated" / "stage2_rolling_robustness" / f"normal72x_v2_regular_tasks_adjusted_{timestamp}"
    )
    output_root.mkdir(parents=True, exist_ok=True)

    base_scenario = load_scenario(scenario_path)
    plan = _load_plan(stage1_result_path)
    plan_path = output_root / "selected_stage1_plan.json"
    _save_plan(plan, plan_path)

    regular_tasks = sorted(
        [task for task in base_scenario.tasks if task.task_type == "reg"],
        key=lambda task: (float(task.arrival), float(task.deadline), task.task_id),
    )

    groups = {item.lower() for item in args.groups}
    all_rows: list[dict[str, Any]] = []

    def run_spec(
        spec_name: str,
        scenario_obj: Scenario,
        *,
        taskset_name: str,
        mode: str,
        params: dict[str, Any],
        timeout_seconds: float | None,
        extra_spec: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        scenario_file = output_root / "scenarios" / f"{spec_name}.json"
        result_file = output_root / "results" / f"{spec_name}.json"
        spec_file = output_root / "specs" / f"{spec_name}.json"
        profile_path = output_root / "profiles" / f"{spec_name}.jsonl"
        collapsed_path = output_root / "profiles" / f"{spec_name}_collapsed.json"

        payload = _scenario_payload_without_runtime_cache(scenario_obj)
        _write_json(scenario_file, payload)

        regular_subset = [task for task in scenario_obj.tasks if task.task_type == "reg"]
        event_segments = build_segments(scenario_obj, plan, regular_subset)
        spec: dict[str, Any] = {
            "name": spec_name,
            "scenario_path": str(scenario_file),
            "plan_path": str(plan_path),
            "result_path": str(result_file),
            "taskset_name": taskset_name,
            "mode": mode,
            "params": params,
            "event_segment_count": len(event_segments),
            "event_segment_count_raw": len(event_segments),
            "regular_task_count": len(regular_subset),
            "source_regular_task_count": params.get("source_regular_task_count", len(regular_subset)),
            "profile_path": str(profile_path),
            "profile_collapsed_path": str(collapsed_path),
        }
        if extra_spec:
            spec.update(extra_spec)
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

        profile_summary = _collapse_profile_rows(profile_path)
        _write_collapsed_profile(profile_summary, collapsed_path)
        row = _build_experiment_row(spec, result_payload, profile_summary)
        all_rows.append(row)
        return row

    if "rolling" in groups:
        rolling_specs = [
            (
                "R1",
                {
                    "milp_horizon_segments": 16,
                    "milp_commit_segments": 8,
                    "milp_rolling_path_limit": 1,
                    "milp_rolling_high_path_limit": 2,
                    "milp_rolling_promoted_tasks_per_segment": 2,
                    "milp_time_limit_seconds": None,
                    "milp_relative_gap": None,
                },
            ),
            (
                "R2",
                {
                    "milp_horizon_segments": 20,
                    "milp_commit_segments": 10,
                    "milp_rolling_path_limit": 1,
                    "milp_rolling_high_path_limit": 2,
                    "milp_rolling_promoted_tasks_per_segment": 2,
                    "milp_time_limit_seconds": None,
                    "milp_relative_gap": None,
                },
            ),
            (
                "R3",
                {
                    "milp_horizon_segments": 20,
                    "milp_commit_segments": 10,
                    "milp_rolling_path_limit": 1,
                    "milp_rolling_high_path_limit": 2,
                    "milp_rolling_promoted_tasks_per_segment": 2,
                    "milp_time_limit_seconds": 60.0,
                    "milp_relative_gap": 0.05,
                },
            ),
            (
                "R4",
                {
                    "milp_horizon_segments": 20,
                    "milp_commit_segments": 10,
                    "milp_rolling_path_limit": 1,
                    "milp_rolling_high_path_limit": 2,
                    "milp_rolling_promoted_tasks_per_segment": 1,
                    "milp_time_limit_seconds": 60.0,
                    "milp_relative_gap": 0.05,
                },
            ),
        ]
        for label, updates in rolling_specs:
            metadata_updates = {
                "stage2_rolling_profile_path": str((output_root / "profiles" / f"{label}.jsonl").resolve()),
            }
            scenario_obj = _materialize_scenario(
                base_scenario,
                tasks=regular_tasks,
                planning_end=None,
                stage2_updates={"prefer_milp": True, "milp_mode": "rolling", **updates},
                experiment_name=label,
                metadata_updates=metadata_updates,
            )
            run_spec(
                label,
                scenario_obj,
                taskset_name="normal72x_v2_regular_tasks_adjusted",
                mode="rolling",
                params={**updates, "source_regular_task_count": len(regular_tasks)},
                timeout_seconds=float(args.rolling_outer_timeout),
            )

    if "ac_diag" in groups:
        label = "A_c_diag"
        updates = {
            "milp_horizon_segments": 24,
            "milp_commit_segments": 8,
            "milp_rolling_path_limit": 1,
            "milp_rolling_high_path_limit": 2,
            "milp_rolling_promoted_tasks_per_segment": 3,
            "milp_time_limit_seconds": None,
            "milp_relative_gap": None,
        }
        metadata_updates = {
            "stage2_rolling_profile_path": str((output_root / "profiles" / f"{label}.jsonl").resolve()),
        }
        scenario_obj = _materialize_scenario(
            base_scenario,
            tasks=regular_tasks,
            planning_end=None,
            stage2_updates={"prefer_milp": True, "milp_mode": "rolling", **updates},
            experiment_name=label,
            metadata_updates=metadata_updates,
        )
        run_spec(
            label,
            scenario_obj,
            taskset_name="normal72x_v2_regular_tasks_adjusted",
            mode="rolling",
            params={**updates, "source_regular_task_count": len(regular_tasks)},
            timeout_seconds=float(args.ac_diag_outer_timeout),
        )

    b_chain_rows: list[dict[str, Any]] = []
    if "b_diag" in groups or "compress" in groups:
        for limit in (24, 36, 48):
            subset = regular_tasks[:limit]
            cutoff = _half_range_cutoff(subset, base_scenario.planning_end)
            half_tasks = [task for task in subset if float(task.arrival) < cutoff]
            full_scenario = _materialize_scenario(
                base_scenario,
                tasks=subset,
                planning_end=None,
                stage2_updates={"prefer_milp": True, "milp_mode": "full", "milp_time_limit_seconds": None},
                experiment_name=f"B_full_front{limit}",
            )
            half_scenario = _materialize_scenario(
                base_scenario,
                tasks=half_tasks,
                planning_end=cutoff,
                stage2_updates={"prefer_milp": True, "milp_mode": "full", "milp_time_limit_seconds": None},
                experiment_name=f"B_full_front{limit}_half_range",
            )
            b_chain_rows.append(
                {
                    "source_regular_task_count": limit,
                    "full_regular_task_count": len(subset),
                    "half_range_regular_task_count": len(half_tasks),
                    "half_range_taskset_name": f"normal72x_v2_regular_tasks_adjusted_front{limit}_reg_half_range_effective{len(half_tasks)}",
                    "half_range_cutoff": cutoff,
                    "full_event_segment_count": len(build_segments(full_scenario, plan, subset)),
                    "half_range_event_segment_count": len(build_segments(half_scenario, plan, half_tasks)),
                    "b6_root_cause": (
                        "half_range scenario intentionally filters out tasks with arrival >= cutoff; old taskset_name hid that effective truncation"
                        if limit == 48 and len(half_tasks) != limit
                        else None
                    ),
                }
            )

    compression_row: dict[str, Any] | None = None
    if "compress" in groups:
        subset = regular_tasks[:24]
        cutoff = _half_range_cutoff(subset, base_scenario.planning_end)
        half_tasks = [task for task in subset if float(task.arrival) < cutoff]
        compression_base = _materialize_scenario(
            base_scenario,
            tasks=half_tasks,
            planning_end=cutoff,
            stage2_updates={"prefer_milp": True, "milp_mode": "full", "milp_time_limit_seconds": None},
            experiment_name="C_full_front24_half_range_compressed",
        )
        raw_segments = build_segments(compression_base, plan, half_tasks)
        compressed_segments, compression_stats = compress_segments(compression_base, plan, raw_segments, half_tasks)
        metadata_updates = {
            "stage2_segment_compression": {"enabled": True},
        }
        scenario_obj = _materialize_scenario(
            base_scenario,
            tasks=half_tasks,
            planning_end=cutoff,
            stage2_updates={"prefer_milp": True, "milp_mode": "full", "milp_time_limit_seconds": None},
            experiment_name="C_full_front24_half_range_compressed",
            metadata_updates=metadata_updates,
        )
        compression_row = run_spec(
            "C_full_front24_half_range_compressed",
            scenario_obj,
            taskset_name=f"normal72x_v2_regular_tasks_adjusted_front24_reg_half_range_effective{len(half_tasks)}_compressed",
            mode="full",
            params={
                "task_limit": 24,
                "half_range": True,
                "planning_end": cutoff,
                "source_regular_task_count": 24,
                "effective_regular_task_count": len(half_tasks),
                "segment_compression": True,
                "milp_time_limit_seconds": None,
            },
            timeout_seconds=float(args.full_outer_timeout),
            extra_spec={
                "event_segment_count_raw": len(raw_segments),
                "event_segment_count_compressed": len(compressed_segments),
                "segment_compression_stats": compression_stats,
            },
        )

    analysis = {
        "rolling_window_hotspots": [
            _top_window_summary(row)
            for row in all_rows
            if row["mode"] == "rolling"
        ],
        "b_chain_diagnostic": b_chain_rows,
        "compression_experiment_name": (compression_row["name"] if compression_row else None),
    }

    summary = {
        "scenario": str(scenario_path),
        "stage1_result": str(stage1_result_path),
        "plan_file": str(plan_path),
        "rolling_outer_timeout_seconds": float(args.rolling_outer_timeout),
        "ac_diag_outer_timeout_seconds": float(args.ac_diag_outer_timeout),
        "full_outer_timeout_seconds": float(args.full_outer_timeout),
        "experiments": all_rows,
        "b_chain_diagnostic": b_chain_rows,
        "analysis": analysis,
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
