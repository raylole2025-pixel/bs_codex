from __future__ import annotations

import argparse
import copy
import json
import sys
import time
from collections import defaultdict
from dataclasses import asdict, replace
from datetime import datetime
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from bs3.models import (
    CandidateWindow,
    CapacityConfig,
    GAConfig,
    Scenario,
    ScheduledWindow,
    Stage1Config,
    Stage2Config,
    Task,
    TemporalLink,
)
from bs3.scenario import load_scenario, validate_scenario
from bs3.stage2 import run_stage2


R4_CONFIG = {
    "prefer_milp": True,
    "milp_mode": "rolling",
    "milp_horizon_segments": 20,
    "milp_commit_segments": 10,
    "milp_rolling_path_limit": 1,
    "milp_rolling_high_path_limit": 2,
    "milp_rolling_promoted_tasks_per_segment": 1,
    "milp_time_limit_seconds": 60.0,
    "milp_relative_gap": 0.05,
}


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def _write_markdown(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _find_default_scenario(repo_root: Path) -> Path:
    matches = sorted(repo_root.rglob("normal72x_v2_regular_tasks_adjusted_scenario_weighted.json"))
    if not matches:
        raise FileNotFoundError("normal72x_v2_regular_tasks_adjusted_scenario_weighted.json not found")
    return matches[0]


def _load_plan(stage1_result_path: Path, candidate_index: int) -> list[ScheduledWindow]:
    payload = json.loads(stage1_result_path.read_text(encoding="utf-8"))
    candidates = list(payload.get("best_feasible") or [])
    if not candidates:
        raise ValueError(f"No best_feasible candidates in {stage1_result_path}")
    if candidate_index >= len(candidates):
        raise IndexError(f"candidate_index {candidate_index} out of range for {stage1_result_path}")
    return [ScheduledWindow(**row) for row in candidates[candidate_index]["plan"]]


def _task_summary(scenario: Scenario, result) -> dict[str, Any]:
    delivered_by_task: dict[str, float] = defaultdict(float)
    for allocation in result.allocations:
        delivered_by_task[allocation.task_id] += float(allocation.delivered)

    total_reg = 0
    success_reg = 0
    total_all = len(scenario.tasks)
    success_all = 0
    tolerance_ratio = max(float(scenario.stage2.completion_tolerance), 0.0)
    for task in scenario.tasks:
        tolerance = max(tolerance_ratio * max(float(task.data), 0.0), 1e-9)
        remaining = max(float(task.data) - delivered_by_task.get(task.task_id, 0.0), 0.0)
        success = remaining <= tolerance + 1e-9
        if task.task_type == "reg":
            total_reg += 1
            if success:
                success_reg += 1
        if success:
            success_all += 1

    return {
        "success_counts": {
            "reg": success_reg,
            "total": success_all,
        },
        "success_rates": {
            "reg": (success_reg / total_reg) if total_reg else 1.0,
            "total": (success_all / total_all) if total_all else 1.0,
        },
    }


def _analyze_allocations(result) -> dict[str, Any]:
    by_task: dict[str, list] = defaultdict(list)
    for alloc in result.allocations:
        by_task[alloc.task_id].append(alloc)
    switched_task_ids: list[str] = []
    paused_task_ids: list[str] = []
    task_details: dict[str, dict[str, Any]] = {}
    for task_id, rows in by_task.items():
        rows.sort(key=lambda item: (int(item.segment_index), str(item.path_id)))
        path_ids = [str(item.path_id) for item in rows]
        segment_indices = [int(item.segment_index) for item in rows]
        if len(set(path_ids)) > 1:
            switched_task_ids.append(task_id)
        if any((segment_indices[idx] - segment_indices[idx - 1]) > 1 for idx in range(1, len(segment_indices))):
            paused_task_ids.append(task_id)
        task_details[task_id] = {
            "segment_indices": segment_indices,
            "path_ids": path_ids,
            "delivered": [float(item.delivered) for item in rows],
            "is_preempted_flags": [bool(item.is_preempted) for item in rows],
        }
    return {
        "allocation_count": len(result.allocations),
        "preempted_allocation_count": sum(1 for alloc in result.allocations if alloc.is_preempted),
        "any_preempted_allocation": any(alloc.is_preempted for alloc in result.allocations),
        "switched_task_ids": sorted(switched_task_ids),
        "paused_task_ids": sorted(paused_task_ids),
        "task_details": task_details,
    }


def _run_stage2_case(
    name: str,
    scenario: Scenario,
    plan: list[ScheduledWindow],
    *,
    metadata: dict[str, Any] | None = None,
    output_path: Path | None = None,
) -> dict[str, Any]:
    started = time.perf_counter()
    result = run_stage2(scenario, plan)
    elapsed = time.perf_counter() - started
    summary = _task_summary(scenario, result)
    row = {
        "name": name,
        "success": True,
        "elapsed_seconds": elapsed,
        "solver_mode": result.solver_mode,
        "cr_reg": result.cr_reg,
        "cr_emg": result.cr_emg,
        "u_cross": result.u_cross,
        "u_all": result.u_all,
        "n_preemptions": int(result.n_preemptions),
        "success_counts": summary["success_counts"],
        "success_rates": summary["success_rates"],
        "metadata": dict(result.metadata or {}),
        "allocation_analysis": _analyze_allocations(result),
    }
    if metadata:
        row.update(metadata)
    if output_path is not None:
        _write_json(output_path, row)
    return row


def _scenario_from_payload(payload: dict[str, Any]) -> Scenario:
    scenario = Scenario(
        node_domain={
            **{node: "A" for node in payload["nodes"]["A"]},
            **{node: "B" for node in payload["nodes"]["B"]},
        },
        intra_links=[
            TemporalLink(
                link_id=item["id"],
                u=item["u"],
                v=item["v"],
                domain=item["domain"],
                start=float(item["start"]),
                end=float(item["end"]),
                delay=float(item.get("delay", 0.0)),
                weight=float(item.get("weight", 1.0)),
            )
            for item in payload["intra_domain_links"]
        ],
        candidate_windows=[
            CandidateWindow(
                window_id=item["id"],
                a=item["a"],
                b=item["b"],
                start=float(item["start"]),
                end=float(item["end"]),
                value=item.get("value"),
                delay=float(item.get("delay", 0.0)),
            )
            for item in payload["candidate_windows"]
        ],
        tasks=[
            Task(
                task_id=item["id"],
                src=item["src"],
                dst=item["dst"],
                arrival=float(item["arrival"]),
                deadline=float(item["deadline"]),
                data=float(item["data"]),
                weight=float(item["weight"]),
                max_rate=float(item["max_rate"]),
                task_type=item["type"],
                preemption_priority=float(item.get("preemption_priority", item["weight"])),
            )
            for item in payload["tasks"]
        ],
        capacities=CapacityConfig(
            domain_a=float(payload["capacities"]["A"]),
            domain_b=float(payload["capacities"]["B"]),
            cross=float(payload["capacities"]["X"]),
        ),
        stage1=Stage1Config(
            rho=float(payload["stage1"]["rho"]),
            t_pre=float(payload["stage1"]["t_pre"]),
            d_min=float(payload["stage1"]["d_min"]),
            ga=GAConfig(),
        ),
        stage2=Stage2Config(**payload["stage2"]),
        planning_end=float(payload["planning_end"]),
        metadata=copy.deepcopy(payload.get("metadata", {})),
    )
    validate_scenario(scenario)
    return scenario


def _build_regular_preemption_probe() -> tuple[dict[str, Any], list[ScheduledWindow]]:
    payload = {
        "metadata": {"name": "regular-routing-switch-probe"},
        "planning_end": 8.0,
        "nodes": {"A": ["A1", "A2"], "B": ["B1", "B2"]},
        "capacities": {"A": 4.0, "B": 4.0, "X": 4.0},
        "stage1": {"rho": 0.0, "t_pre": 1.0, "d_min": 1.0},
        "stage2": {
            "k_paths": 2,
            "completion_tolerance": 1e-6,
            **R4_CONFIG,
        },
        "intra_domain_links": [
            {"id": "A12", "u": "A1", "v": "A2", "domain": "A", "start": 0.0, "end": 8.0, "delay": 0.0},
            {"id": "B12", "u": "B1", "v": "B2", "domain": "B", "start": 0.0, "end": 8.0, "delay": 0.0},
        ],
        "candidate_windows": [
            {"id": "X1", "a": "A1", "b": "B1", "start": 0.0, "end": 4.0, "delay": 0.0},
            {"id": "X2", "a": "A2", "b": "B2", "start": 4.0, "end": 8.0, "delay": 0.0},
        ],
        "tasks": [
            {"id": "R_base", "src": "A1", "dst": "B2", "arrival": 0.0, "deadline": 8.0, "data": 20.0, "weight": 1.0, "max_rate": 4.0, "type": "reg"},
            {"id": "R_hot1", "src": "A1", "dst": "B2", "arrival": 2.0, "deadline": 8.0, "data": 8.0, "weight": 6.0, "max_rate": 4.0, "type": "reg"},
        ],
    }
    plan = [
        ScheduledWindow(window_id="X1", a="A1", b="B1", start=0.0, end=4.0, on=0.0, off=4.0, delay=0.0),
        ScheduledWindow(window_id="X2", a="A2", b="B2", start=4.0, end=8.0, on=4.0, off=8.0, delay=0.0),
    ]
    return payload, plan


def _build_emergency_preemption_probe() -> tuple[dict[str, Any], list[ScheduledWindow]]:
    payload = {
        "metadata": {"name": "emergency-controlled-preemption-probe"},
        "planning_end": 8.0,
        "nodes": {"A": ["A1"], "B": ["B1"]},
        "capacities": {"A": 10.0, "B": 10.0, "X": 4.0},
        "stage1": {"rho": 0.0, "t_pre": 1.0, "d_min": 1.0},
        "stage2": {
            "k_paths": 1,
            "completion_tolerance": 1e-6,
            **R4_CONFIG,
        },
        "intra_domain_links": [],
        "candidate_windows": [
            {"id": "X1", "a": "A1", "b": "B1", "start": 0.0, "end": 8.0, "delay": 0.0},
        ],
        "tasks": [
            {"id": "R1", "src": "A1", "dst": "B1", "arrival": 0.0, "deadline": 8.0, "data": 16.0, "weight": 1.0, "max_rate": 4.0, "type": "reg"},
            {"id": "E1", "src": "A1", "dst": "B1", "arrival": 2.0, "deadline": 4.0, "data": 8.0, "weight": 10.0, "max_rate": 4.0, "type": "emg"},
        ],
    }
    plan = [
        ScheduledWindow(window_id="X1", a="A1", b="B1", start=0.0, end=8.0, on=0.0, off=8.0, delay=0.0),
    ]
    return payload, plan


def _regular_probe_status(regular_row: dict[str, Any], emergency_row: dict[str, Any]) -> dict[str, Any]:
    regular_analysis = regular_row["allocation_analysis"]
    emergency_analysis = emergency_row["allocation_analysis"]
    if (
        regular_row["n_preemptions"] == 0
        and not regular_analysis["any_preempted_allocation"]
        and regular_analysis["switched_task_ids"]
        and emergency_row["n_preemptions"] > 0
    ):
        status = "unavailable_in_practice"
        evidence = [
            "regular rolling probe exhibited a route switch but kept n_preemptions=0 and all Allocation.is_preempted=False",
            "stage2 scheduler increments n_preemptions only in the emergency insertion branch",
            "controlled emergency probe produced n_preemptions=1, so a narrow preemption path exists outside rolling regular MILP",
        ]
    elif regular_row["n_preemptions"] > 0 or regular_analysis["any_preempted_allocation"]:
        status = "actively_used"
        evidence = [
            "regular rolling probe produced observable preemption counts or allocation flags",
        ]
    else:
        status = "unused_but_available"
        evidence = [
            "no observable preemption in the probe, but the emergency branch did not demonstrate controlled preemption either",
        ]
    return {
        "status": status,
        "evidence": evidence,
        "regular_probe": {
            "n_preemptions": regular_row["n_preemptions"],
            "switched_task_ids": regular_analysis["switched_task_ids"],
            "paused_task_ids": regular_analysis["paused_task_ids"],
            "preempted_allocation_count": regular_analysis["preempted_allocation_count"],
        },
        "emergency_probe": {
            "n_preemptions": emergency_row["n_preemptions"],
            "preempted_allocation_count": emergency_analysis["preempted_allocation_count"],
        },
        "code_evidence": [
            "bs3/stage2_two_phase_scheduler.py:89 initializes n_preemptions and only adds to it via _insert_emergency_task() at line 118",
            "bs3/stage2_regular_joint_milp.py:666 creates regular allocations without ever setting is_preempted=True",
        ],
    }


def _stability_markdown(rows: list[dict[str, Any]]) -> str:
    headers = [
        "name",
        "stage1_label",
        "success_counts.reg",
        "cr_reg",
        "u_cross",
        "u_all",
        "n_preemptions",
        "elapsed_seconds",
    ]
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
    ]
    for row in rows:
        values = [
            row["name"],
            row["stage1_label"],
            str(row["success_counts"]["reg"]),
            f"{row['cr_reg']:.6f}",
            f"{row['u_cross']:.6f}",
            f"{row['u_all']:.6f}",
            str(row["n_preemptions"]),
            f"{row['elapsed_seconds']:.6f}",
        ]
        lines.append("| " + " | ".join(values) + " |")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Final Stage2-1 validation for R4 stability and preemption status.")
    parser.add_argument("--scenario", type=str, default=None, help="Path to weighted scenario JSON")
    parser.add_argument("--output-root", type=str, default=None, help="Output directory root")
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    scenario_path = Path(args.scenario) if args.scenario else _find_default_scenario(repo_root)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_root = (
        Path(args.output_root)
        if args.output_root
        else repo_root / "outputs" / "active" / "stage2_finalization_validation" / f"normal72x_v2_regular_tasks_adjusted_{timestamp}"
    )
    output_root.mkdir(parents=True, exist_ok=True)

    scenario = load_scenario(scenario_path)
    stability_specs = [
        {
            "name": "R4_seed7_best0",
            "stage1_label": "seed7#0",
            "stage1_result": repo_root / "outputs" / "active" / "stage1_validation_tmp" / "normal72x_seed7_no_runtime_limit_result.json",
            "candidate_index": 0,
        },
        {
            "name": "R4_seed7_alt1",
            "stage1_label": "seed7#1",
            "stage1_result": repo_root / "outputs" / "active" / "stage1_validation_tmp" / "normal72x_seed7_no_runtime_limit_result.json",
            "candidate_index": 1,
        },
        {
            "name": "R4_seed13_best0",
            "stage1_label": "seed13#0",
            "stage1_result": repo_root / "outputs" / "active" / "stage1_validation_tmp" / "normal72x_seed13_no_runtime_limit_result.json",
            "candidate_index": 0,
        },
        {
            "name": "R4_seed13_alt1",
            "stage1_label": "seed13#1",
            "stage1_result": repo_root / "outputs" / "active" / "stage1_validation_tmp" / "normal72x_seed13_no_runtime_limit_result.json",
            "candidate_index": 1,
        },
    ]

    stability_rows: list[dict[str, Any]] = []
    for spec in stability_specs:
        plan = _load_plan(Path(spec["stage1_result"]), spec["candidate_index"])
        scenario_r4 = replace(
            scenario,
            stage2=replace(scenario.stage2, **R4_CONFIG),
            metadata={**dict(scenario.metadata), "experiment_name": spec["name"]},
        )
        row = _run_stage2_case(
            spec["name"],
            scenario_r4,
            plan,
            metadata={
                "stage1_label": spec["stage1_label"],
                "stage1_result": str(Path(spec["stage1_result"]).resolve()),
                "candidate_index": int(spec["candidate_index"]),
                "r4_config": dict(R4_CONFIG),
            },
            output_path=output_root / "results" / f"{spec['name']}.json",
        )
        stability_rows.append(row)

    regular_payload, regular_plan = _build_regular_preemption_probe()
    _write_json(output_root / "diagnostics" / "regular_probe_scenario.json", regular_payload)
    regular_row = _run_stage2_case(
        "regular_probe",
        _scenario_from_payload(regular_payload),
        regular_plan,
        metadata={"probe_type": "regular_route_switch"},
        output_path=output_root / "results" / "regular_probe.json",
    )

    emergency_payload, emergency_plan = _build_emergency_preemption_probe()
    _write_json(output_root / "diagnostics" / "emergency_probe_scenario.json", emergency_payload)
    emergency_row = _run_stage2_case(
        "emergency_probe",
        _scenario_from_payload(emergency_payload),
        emergency_plan,
        metadata={"probe_type": "emergency_controlled_preemption"},
        output_path=output_root / "results" / "emergency_probe.json",
    )

    preemption_status = _regular_probe_status(regular_row, emergency_row)

    reg_success_values = [int(row["success_counts"]["reg"]) for row in stability_rows]
    cr_reg_values = [float(row["cr_reg"]) for row in stability_rows]
    u_cross_values = [float(row["u_cross"]) for row in stability_rows]
    u_all_values = [float(row["u_all"]) for row in stability_rows]
    elapsed_values = [float(row["elapsed_seconds"]) for row in stability_rows]
    stability_summary = {
        "run_count": len(stability_rows),
        "success_counts_reg_range": [min(reg_success_values), max(reg_success_values)],
        "cr_reg_range": [min(cr_reg_values), max(cr_reg_values)],
        "u_cross_range": [min(u_cross_values), max(u_cross_values)],
        "u_all_range": [min(u_all_values), max(u_all_values)],
        "elapsed_seconds_range": [min(elapsed_values), max(elapsed_values)],
    }

    summary = {
        "scenario": str(scenario_path.resolve()),
        "r4_config": dict(R4_CONFIG),
        "stability_runs": stability_rows,
        "stability_summary": stability_summary,
        "preemption_regular_probe": regular_row,
        "preemption_emergency_probe": emergency_row,
        "preemption_status": preemption_status,
    }
    summary_path = output_root / "summary.json"
    _write_json(summary_path, summary)

    markdown = [
        "# R4 Stability",
        "",
        _stability_markdown(stability_rows),
        "",
        "# Preemption Status",
        "",
        f"- status: `{preemption_status['status']}`",
    ]
    for item in preemption_status["evidence"]:
        markdown.append(f"- {item}")
    markdown_path = output_root / "summary.md"
    _write_markdown(markdown_path, "\n".join(markdown))

    print(json.dumps(summary, indent=2, ensure_ascii=False))
    print(f"summary_json={summary_path}")
    print(f"summary_md={markdown_path}")


if __name__ == "__main__":
    main()
