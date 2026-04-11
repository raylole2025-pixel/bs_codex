from __future__ import annotations

import copy
import unittest

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
from bs3.scenario import build_segments, validate_scenario
from bs3.stage2 import run_stage2
from bs3.stage2_regular_joint_milp import _build_task_segments, _select_rolling_path_limits


BASE_PAYLOAD = {
    "metadata": {"name": "stage2-baseline-insert-test"},
    "planning_end": 6.0,
    "nodes": {
        "A": ["A1", "A2"],
        "B": ["B1", "B2"],
    },
    "capacities": {"A": 10.0, "B": 10.0, "X": 10.0},
    "stage1": {
        "rho": 0.2,
        "t_pre": 1.0,
        "d_min": 1.0,
    },
    "stage2": {
        "k_paths": 2,
        "completion_tolerance": 1e-6,
        "prefer_milp": True,
    },
    "intra_domain_links": [
        {"id": "A12", "u": "A1", "v": "A2", "domain": "A", "start": 0.0, "end": 6.0, "delay": 0.1},
        {"id": "B12", "u": "B1", "v": "B2", "domain": "B", "start": 0.0, "end": 6.0, "delay": 0.1},
    ],
    "candidate_windows": [
        {"id": "X1", "a": "A1", "b": "B1", "start": 0.0, "end": 6.0, "delay": 0.1},
        {"id": "X2", "a": "A2", "b": "B2", "start": 0.0, "end": 6.0, "delay": 0.1},
    ],
    "tasks": [],
}

PLAN = [
    ScheduledWindow(window_id="X1", a="A1", b="B1", start=0.0, end=6.0, on=0.0, off=6.0, delay=0.1),
    ScheduledWindow(window_id="X2", a="A2", b="B2", start=0.0, end=6.0, on=0.0, off=6.0, delay=0.1),
]


def _load_payload(payload: dict) -> Scenario:
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
                weight=float(item.get("weight", item.get("delay", 1.0) or 1.0)),
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
        stage2=Stage2Config(
            k_paths=int(payload["stage2"]["k_paths"]),
            completion_tolerance=float(payload["stage2"]["completion_tolerance"]),
            prefer_milp=bool(payload["stage2"].get("prefer_milp", True)),
            milp_mode=str(payload["stage2"].get("milp_mode", "rolling")),
            milp_horizon_segments=int(payload["stage2"].get("milp_horizon_segments", 16)),
            milp_commit_segments=int(payload["stage2"].get("milp_commit_segments", 8)),
            milp_rolling_path_limit=int(payload["stage2"].get("milp_rolling_path_limit", 1)),
            milp_rolling_high_path_limit=int(payload["stage2"].get("milp_rolling_high_path_limit", 2)),
            milp_rolling_high_weight_threshold=(
                None if payload["stage2"].get("milp_rolling_high_weight_threshold") is None else float(payload["stage2"]["milp_rolling_high_weight_threshold"])
            ),
            milp_rolling_high_competition_task_threshold=int(payload["stage2"].get("milp_rolling_high_competition_task_threshold", 8)),
            milp_rolling_promoted_tasks_per_segment=int(payload["stage2"].get("milp_rolling_promoted_tasks_per_segment", 2)),
        ),
        planning_end=float(payload["planning_end"]),
        metadata=copy.deepcopy(payload.get("metadata", {})),
    )
    validate_scenario(scenario)
    return scenario


def _task_allocation(result, task_id: str):
    return [alloc for alloc in result.allocations if alloc.task_id == task_id]


class Stage2BaselineInsertTests(unittest.TestCase):
    def test_stage2_reports_joint_milp_metadata(self) -> None:
        payload = copy.deepcopy(BASE_PAYLOAD)
        payload["tasks"] = [
            {
                "id": "R1",
                "src": "A1",
                "dst": "B1",
                "arrival": 0.0,
                "deadline": 4.0,
                "data": 4.0,
                "weight": 1.0,
                "max_rate": 2.0,
                "type": "reg",
            }
        ]
        scenario = _load_payload(payload)
        result = run_stage2(scenario, PLAN)
        self.assertEqual(result.solver_mode, "two_phase_event_insert+joint_milp_rolling")
        self.assertTrue(result.metadata["prefer_milp"])
        self.assertEqual(result.metadata["milp_mode"], "rolling")
        self.assertEqual(result.metadata["regular_task_count"], 1)

    def test_regular_baseline_uses_only_reserved_cross_slice(self) -> None:
        payload = copy.deepcopy(BASE_PAYLOAD)
        payload["tasks"] = [
            {
                "id": "R1",
                "src": "A1",
                "dst": "B1",
                "arrival": 0.0,
                "deadline": 2.0,
                "data": 10.0,
                "weight": 3.0,
                "max_rate": 10.0,
                "type": "reg",
            },
            {
                "id": "R2",
                "src": "A1",
                "dst": "B1",
                "arrival": 0.0,
                "deadline": 2.0,
                "data": 20.0,
                "weight": 3.0,
                "max_rate": 10.0,
                "type": "reg",
            }
        ]
        scenario = _load_payload(payload)
        result = run_stage2(scenario, PLAN)
        allocations = result.allocations
        self.assertTrue(allocations)
        reserved_cross = (1.0 - scenario.stage1.rho) * scenario.capacities.cross
        self.assertTrue(all(alloc.rate <= reserved_cross + 1e-9 for alloc in allocations))
        self.assertLess(result.cr_reg, 1.0)

    def test_rolling_path_limit_requires_high_competition_segment(self) -> None:
        payload = copy.deepcopy(BASE_PAYLOAD)
        payload["planning_end"] = 1.0
        payload["stage1"]["rho"] = 0.0
        payload["stage2"].update(
            {
                "milp_rolling_path_limit": 1,
                "milp_rolling_high_path_limit": 2,
                "milp_rolling_high_competition_task_threshold": 4,
                "milp_rolling_promoted_tasks_per_segment": 2,
            }
        )
        payload["candidate_windows"] = [
            {"id": "X1", "a": "A1", "b": "B1", "start": 0.0, "end": 1.0, "delay": 0.0},
            {"id": "X2", "a": "A2", "b": "B2", "start": 0.0, "end": 1.0, "delay": 0.0},
        ]
        payload["tasks"] = [
            {
                "id": "R_hi",
                "src": "A1",
                "dst": "B1",
                "arrival": 0.0,
                "deadline": 1.0,
                "data": 4.0,
                "weight": 3.0,
                "max_rate": 10.0,
                "type": "reg",
            },
            {
                "id": "R_mid",
                "src": "A2",
                "dst": "B2",
                "arrival": 0.0,
                "deadline": 1.0,
                "data": 4.0,
                "weight": 2.0,
                "max_rate": 10.0,
                "type": "reg",
            },
            {
                "id": "R_lo",
                "src": "A1",
                "dst": "B1",
                "arrival": 0.0,
                "deadline": 1.0,
                "data": 4.0,
                "weight": 1.0,
                "max_rate": 10.0,
                "type": "reg",
            },
        ]
        scenario = _load_payload(payload)
        local_plan = [
            ScheduledWindow(window_id="X1", a="A1", b="B1", start=0.0, end=1.0, on=0.0, off=1.0, delay=0.0),
            ScheduledWindow(window_id="X2", a="A2", b="B2", start=0.0, end=1.0, on=0.0, off=1.0, delay=0.0),
        ]
        tasks = [task for task in scenario.tasks if task.task_type == "reg"]
        segments = build_segments(scenario, local_plan, tasks)
        task_segments = _build_task_segments(tasks, segments)
        limits = _select_rolling_path_limits(
            scenario,
            tasks,
            task_segments,
            {task.task_id: float(task.data) for task in tasks},
        )
        self.assertEqual(limits, {})

    def test_rolling_path_limit_only_promotes_budgeted_top_tasks(self) -> None:
        payload = copy.deepcopy(BASE_PAYLOAD)
        payload["planning_end"] = 1.0
        payload["capacities"] = {"A": 10.0, "B": 10.0, "X": 10.0}
        payload["stage1"]["rho"] = 0.0
        payload["stage2"].update(
            {
                "milp_rolling_path_limit": 1,
                "milp_rolling_high_path_limit": 2,
                "milp_rolling_high_competition_task_threshold": 3,
                "milp_rolling_promoted_tasks_per_segment": 2,
            }
        )
        payload["candidate_windows"] = [
            {"id": "X1", "a": "A1", "b": "B1", "start": 0.0, "end": 1.0, "delay": 0.0},
            {"id": "X2", "a": "A2", "b": "B2", "start": 0.0, "end": 1.0, "delay": 0.0},
        ]
        payload["tasks"] = [
            {
                "id": "R_hi",
                "src": "A1",
                "dst": "B1",
                "arrival": 0.0,
                "deadline": 1.0,
                "data": 4.0,
                "weight": 3.0,
                "max_rate": 10.0,
                "type": "reg",
            },
            {
                "id": "R_mid_early",
                "src": "A2",
                "dst": "B2",
                "arrival": 0.0,
                "deadline": 1.0,
                "data": 4.0,
                "weight": 2.0,
                "max_rate": 10.0,
                "type": "reg",
            },
            {
                "id": "R_mid_late",
                "src": "A1",
                "dst": "B1",
                "arrival": 0.0,
                "deadline": 1.0,
                "data": 1.0,
                "weight": 2.0,
                "max_rate": 1.0,
                "type": "reg",
            },
        ]
        scenario = _load_payload(payload)
        local_plan = [
            ScheduledWindow(window_id="X1", a="A1", b="B1", start=0.0, end=1.0, on=0.0, off=1.0, delay=0.0),
            ScheduledWindow(window_id="X2", a="A2", b="B2", start=0.0, end=1.0, on=0.0, off=1.0, delay=0.0),
        ]
        tasks = [task for task in scenario.tasks if task.task_type == "reg"]
        segments = build_segments(scenario, local_plan, tasks)
        task_segments = _build_task_segments(tasks, segments)
        limits = _select_rolling_path_limits(
            scenario,
            tasks,
            task_segments,
            {task.task_id: float(task.data) for task in tasks},
        )
        self.assertEqual(limits[("R_hi", 0)], 2)
        self.assertEqual(limits[("R_mid_early", 0)], 2)
        self.assertNotIn(("R_mid_late", 0), limits)

    def test_joint_milp_baseline_prefers_more_completions_over_large_single_task(self) -> None:
        payload = copy.deepcopy(BASE_PAYLOAD)
        payload["planning_end"] = 1.0
        payload["capacities"] = {"A": 10.0, "B": 10.0, "X": 10.0}
        payload["stage1"]["rho"] = 0.0
        payload["candidate_windows"] = [
            {"id": "X1", "a": "A1", "b": "B1", "start": 0.0, "end": 1.0, "delay": 0.0}
        ]
        payload["tasks"] = [
            {
                "id": "R_big",
                "src": "A1",
                "dst": "B1",
                "arrival": 0.0,
                "deadline": 1.0,
                "data": 8.0,
                "weight": 1.0,
                "max_rate": 10.0,
                "type": "reg",
            },
            {
                "id": "R_s1",
                "src": "A1",
                "dst": "B1",
                "arrival": 0.0,
                "deadline": 1.0,
                "data": 5.0,
                "weight": 1.0,
                "max_rate": 10.0,
                "type": "reg",
            },
            {
                "id": "R_s2",
                "src": "A1",
                "dst": "B1",
                "arrival": 0.0,
                "deadline": 1.0,
                "data": 5.0,
                "weight": 1.0,
                "max_rate": 10.0,
                "type": "reg",
            },
        ]
        scenario = _load_payload(payload)
        local_plan = [
            ScheduledWindow(window_id="X1", a="A1", b="B1", start=0.0, end=1.0, on=0.0, off=1.0, delay=0.0)
        ]
        result = run_stage2(scenario, local_plan)
        delivered = {task_id: sum(alloc.delivered for alloc in _task_allocation(result, task_id)) for task_id in ("R_big", "R_s1", "R_s2")}
        self.assertAlmostEqual(delivered["R_s1"], 5.0, delta=1e-5)
        self.assertAlmostEqual(delivered["R_s2"], 5.0, delta=1e-5)
        self.assertLess(delivered["R_big"], 1e-6)
        self.assertAlmostEqual(result.cr_reg, 2.0 / 3.0, delta=1e-6)

    def test_direct_insert_uses_reserved_capacity_without_preemption(self) -> None:
        payload = copy.deepcopy(BASE_PAYLOAD)
        payload["tasks"] = [
            {
                "id": "R1",
                "src": "A1",
                "dst": "B1",
                "arrival": 0.0,
                "deadline": 3.0,
                "data": 14.0,
                "weight": 1.0,
                "max_rate": 5.0,
                "type": "reg",
            },
            {
                "id": "E1",
                "src": "A2",
                "dst": "B2",
                "arrival": 0.0,
                "deadline": 3.0,
                "data": 12.0,
                "weight": 5.0,
                "max_rate": 5.0,
                "type": "emg",
            },
        ]
        scenario = _load_payload(payload)
        result = run_stage2(scenario, PLAN)
        self.assertEqual(result.n_preemptions, 0)
        self.assertEqual(result.cr_emg, 1.0)
        self.assertTrue(_task_allocation(result, "R1"))
        self.assertTrue(_task_allocation(result, "E1"))

    def test_emergency_arrival_uses_split_segment_immediately(self) -> None:
        payload = copy.deepcopy(BASE_PAYLOAD)
        payload["planning_end"] = 4.0
        payload["tasks"] = [
            {
                "id": "E1",
                "src": "A1",
                "dst": "B1",
                "arrival": 1.5,
                "deadline": 3.5,
                "data": 2.0,
                "weight": 5.0,
                "max_rate": 2.0,
                "type": "emg",
            }
        ]
        scenario = _load_payload(payload)
        result = run_stage2(scenario, PLAN)
        allocations = _task_allocation(result, "E1")
        self.assertTrue(allocations)
        self.assertEqual(allocations[0].segment_index, 1)

    def test_controlled_preemption_releases_low_weight_regular_task(self) -> None:
        payload = copy.deepcopy(BASE_PAYLOAD)
        payload["planning_end"] = 4.0
        payload["capacities"] = {"A": 8.0, "B": 8.0, "X": 8.0}
        payload["candidate_windows"] = [
            {"id": "X1", "a": "A1", "b": "B1", "start": 0.0, "end": 4.0, "delay": 0.0}
        ]
        payload["tasks"] = [
            {
                "id": "R_hi",
                "src": "A1",
                "dst": "B1",
                "arrival": 0.0,
                "deadline": 2.0,
                "data": 8.0,
                "weight": 5.0,
                "max_rate": 4.0,
                "type": "reg",
            },
            {
                "id": "R_lo",
                "src": "A1",
                "dst": "B1",
                "arrival": 0.0,
                "deadline": 4.0,
                "data": 8.0,
                "weight": 1.0,
                "max_rate": 4.0,
                "type": "reg",
            },
            {
                "id": "E1",
                "src": "A1",
                "dst": "B1",
                "arrival": 1.0,
                "deadline": 3.0,
                "data": 12.0,
                "weight": 10.0,
                "max_rate": 8.0,
                "type": "emg",
            },
        ]
        scenario = _load_payload(payload)
        local_plan = [
            ScheduledWindow(window_id="X1", a="A1", b="B1", start=0.0, end=4.0, on=0.0, off=4.0, delay=0.0)
        ]
        result = run_stage2(scenario, local_plan)
        delivered = {task_id: sum(alloc.delivered for alloc in _task_allocation(result, task_id)) for task_id in ("R_hi", "R_lo", "E1")}
        self.assertGreaterEqual(result.n_preemptions, 1)
        self.assertAlmostEqual(delivered["E1"], 12.0, delta=1e-6)
        self.assertGreater(delivered["R_hi"], delivered["R_lo"])

    def test_regular_rolling_route_switch_does_not_surface_as_preemption(self) -> None:
        payload = copy.deepcopy(BASE_PAYLOAD)
        payload["planning_end"] = 8.0
        payload["capacities"] = {"A": 4.0, "B": 4.0, "X": 4.0}
        payload["stage1"]["rho"] = 0.0
        payload["stage2"].update(
            {
                "milp_mode": "rolling",
                "milp_horizon_segments": 4,
                "milp_commit_segments": 2,
                "milp_rolling_path_limit": 1,
                "milp_rolling_high_path_limit": 2,
                "milp_rolling_promoted_tasks_per_segment": 1,
                "milp_time_limit_seconds": 60.0,
                "milp_relative_gap": 0.05,
            }
        )
        payload["candidate_windows"] = [
            {"id": "X1", "a": "A1", "b": "B1", "start": 0.0, "end": 4.0, "delay": 0.0},
            {"id": "X2", "a": "A2", "b": "B2", "start": 4.0, "end": 8.0, "delay": 0.0},
        ]
        payload["tasks"] = [
            {
                "id": "R_base",
                "src": "A1",
                "dst": "B2",
                "arrival": 0.0,
                "deadline": 8.0,
                "data": 20.0,
                "weight": 1.0,
                "max_rate": 4.0,
                "type": "reg",
            },
            {
                "id": "R_hot1",
                "src": "A1",
                "dst": "B2",
                "arrival": 2.0,
                "deadline": 8.0,
                "data": 8.0,
                "weight": 6.0,
                "max_rate": 4.0,
                "type": "reg",
            },
        ]
        scenario = _load_payload(payload)
        local_plan = [
            ScheduledWindow(window_id="X1", a="A1", b="B1", start=0.0, end=4.0, on=0.0, off=4.0, delay=0.0),
            ScheduledWindow(window_id="X2", a="A2", b="B2", start=4.0, end=8.0, on=4.0, off=8.0, delay=0.0),
        ]
        result = run_stage2(scenario, local_plan)
        base_allocations = _task_allocation(result, "R_base")
        self.assertEqual(result.n_preemptions, 0)
        self.assertTrue(base_allocations)
        self.assertTrue(all(not alloc.is_preempted for alloc in base_allocations))
        self.assertGreater(len({alloc.path_id for alloc in base_allocations}), 1)

    def test_legacy_insertion_horizon_is_ignored_and_deadline_horizon_is_used(self) -> None:
        payload = copy.deepcopy(BASE_PAYLOAD)
        payload["planning_end"] = 5.0
        payload["stage2"]["insertion_horizon_seconds"] = 0.5
        payload["tasks"] = [
            {
                "id": "E1",
                "src": "A1",
                "dst": "B1",
                "arrival": 1.0,
                "deadline": 4.0,
                "data": 6.0,
                "weight": 5.0,
                "max_rate": 2.0,
                "type": "emg",
            }
        ]
        scenario = _load_payload(payload)
        result = run_stage2(scenario, PLAN)
        delivered = sum(alloc.delivered for alloc in _task_allocation(result, "E1"))
        self.assertAlmostEqual(delivered, 6.0, delta=1e-6)
        self.assertEqual(result.cr_emg, 1.0)

    def test_best_effort_commits_partial_emergency_flow_on_failure(self) -> None:
        payload = copy.deepcopy(BASE_PAYLOAD)
        payload["planning_end"] = 3.0
        payload["tasks"] = [
            {
                "id": "E1",
                "src": "A1",
                "dst": "B1",
                "arrival": 0.0,
                "deadline": 3.0,
                "data": 40.0,
                "weight": 5.0,
                "max_rate": 10.0,
                "type": "emg",
            }
        ]
        scenario = _load_payload(payload)
        result = run_stage2(scenario, PLAN)
        delivered = sum(alloc.delivered for alloc in _task_allocation(result, "E1"))
        self.assertGreater(delivered, 0.0)
        self.assertLess(delivered, 40.0)
        self.assertEqual(result.cr_emg, 0.0)


if __name__ == "__main__":
    unittest.main()


