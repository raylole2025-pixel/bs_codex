from __future__ import annotations

import unittest

from bs3.models import (
    Allocation,
    CapacityConfig,
    CandidateWindow,
    Scenario,
    ScheduledWindow,
    Stage1BaselineTrace,
    Stage1Config,
    Stage2Config,
    Task,
    TemporalLink,
)
from bs3.stage2 import run_stage2


PATH_EDGE_IDS = ("A01", "X1", "B10")


def _segments() -> list[dict[str, float | int]]:
    return [
        {"segment_index": 0, "start": 0.0, "end": 1.0},
        {"segment_index": 1, "start": 1.0, "end": 3.0},
        {"segment_index": 2, "start": 3.0, "end": 4.0},
    ]


def _plan() -> list[ScheduledWindow]:
    return [
        ScheduledWindow(
            window_id="X1",
            a="A1",
            b="B1",
            start=0.0,
            end=4.0,
            on=0.0,
            off=4.0,
            delay=0.0,
        )
    ]


def _scenario(
    *,
    cross_capacity: float,
    domain_capacity: float,
    rho: float,
    tasks: list[Task],
) -> Scenario:
    return Scenario(
        node_domain={"A0": "A", "A1": "A", "B1": "B", "B0": "B"},
        intra_links=[
            TemporalLink("A01", "A0", "A1", "A", 0.0, 4.0, delay=0.0),
            TemporalLink("B10", "B1", "B0", "B", 0.0, 4.0, delay=0.0),
        ],
        candidate_windows=[
            CandidateWindow(window_id="X1", a="A1", b="B1", start=0.0, end=4.0, delay=0.0),
        ],
        tasks=tasks,
        capacities=CapacityConfig(domain_a=domain_capacity, domain_b=domain_capacity, cross=cross_capacity),
        stage1=Stage1Config(rho=rho, t_pre=0.0, d_min=0.0),
        stage2=Stage2Config(k_paths=2, completion_tolerance=1e-6),
        planning_end=4.0,
        metadata={"name": "stage2-priority-insert-test"},
    )


def _regular_allocation(task_id: str, rate: float, segment_index: int = 1) -> Allocation:
    duration = float(_segments()[segment_index]["end"]) - float(_segments()[segment_index]["start"])
    return Allocation(
        task_id=task_id,
        segment_index=segment_index,
        path_id=f"{task_id}:baseline:{segment_index}",
        edge_ids=PATH_EDGE_IDS,
        rate=rate,
        delivered=rate * duration,
        task_type="reg",
        cross_window_id="X1",
    )


def _baseline_trace(
    *,
    allocations: list[Allocation],
    completed: dict[str, bool],
    remaining_end: dict[str, float],
) -> Stage1BaselineTrace:
    return Stage1BaselineTrace(
        rho=0.0,
        segments=_segments(),
        allocations=allocations,
        completed=completed,
        remaining_end=remaining_end,
        summary={"allocation_count": len(allocations)},
    )


def _insertion(result) -> dict[str, object]:
    insertions = list(result.metadata.get("emergency_insertions") or [])
    assert len(insertions) == 1
    return insertions[0]


class Stage2PriorityInsertTests(unittest.TestCase):
    def test_reserve_enough_uses_reserved_only_direct_insert(self) -> None:
        scenario = _scenario(
            cross_capacity=10.0,
            domain_capacity=10.0,
            rho=0.3,
            tasks=[
                Task("R1", "A0", "B0", 0.0, 4.0, 10.0, 1.0, 5.0, "reg"),
                Task("E1", "A0", "B0", 1.0, 3.0, 4.0, 5.0, 2.0, "emg"),
            ],
        )
        baseline_trace = _baseline_trace(
            allocations=[_regular_allocation("R1", rate=5.0)],
            completed={"R1": True},
            remaining_end={"R1": 0.0},
        )

        result = run_stage2(scenario, plan=_plan(), baseline_trace=baseline_trace)
        insertion = _insertion(result)

        self.assertEqual(insertion["strategy"], "direct_insert")
        self.assertEqual(insertion["capacity_tier"], "reserved_only")
        self.assertFalse(insertion["used_preemption"])
        self.assertEqual(insertion["direct_plan_delivery"], 4.0)
        self.assertTrue(any(alloc.task_id == "E1" and tuple(alloc.edge_ids) == PATH_EDGE_IDS for alloc in result.allocations))

    def test_reserve_shortfall_can_borrow_unused_regular_share(self) -> None:
        scenario = _scenario(
            cross_capacity=10.0,
            domain_capacity=10.0,
            rho=0.2,
            tasks=[
                Task("R1", "A0", "B0", 0.0, 4.0, 10.0, 1.0, 5.0, "reg"),
                Task("E1", "A0", "B0", 1.0, 3.0, 8.0, 5.0, 4.0, "emg"),
            ],
        )
        baseline_trace = _baseline_trace(
            allocations=[_regular_allocation("R1", rate=5.0)],
            completed={"R1": True},
            remaining_end={"R1": 0.0},
        )

        result = run_stage2(scenario, plan=_plan(), baseline_trace=baseline_trace)
        insertion = _insertion(result)

        self.assertEqual(insertion["strategy"], "direct_insert")
        self.assertEqual(insertion["capacity_tier"], "borrow_unused_regular_share")
        self.assertFalse(insertion["used_preemption"])
        self.assertEqual(insertion["direct_plan_delivery"], 8.0)

    def test_direct_insert_can_release_one_low_priority_regular_task(self) -> None:
        scenario = _scenario(
            cross_capacity=4.0,
            domain_capacity=4.0,
            rho=0.25,
            tasks=[
                Task("R_LOW", "A0", "B0", 0.0, 4.0, 8.0, 1.0, 4.0, "reg"),
                Task("E1", "A0", "B0", 1.0, 3.0, 4.0, 5.0, 2.0, "emg"),
            ],
        )
        baseline_trace = _baseline_trace(
            allocations=[_regular_allocation("R_LOW", rate=4.0)],
            completed={"R_LOW": True},
            remaining_end={"R_LOW": 0.0},
        )

        result = run_stage2(scenario, plan=_plan(), baseline_trace=baseline_trace)
        insertion = _insertion(result)

        self.assertEqual(insertion["strategy"], "controlled_preemption")
        self.assertEqual(insertion["capacity_tier"], "preempted")
        self.assertTrue(insertion["used_preemption"])
        self.assertEqual(insertion["preempted_task_id"], "R_LOW")
        self.assertEqual(insertion["released_segments"], [1])
        self.assertEqual(insertion["released_cross_window_ids"], ["X1"])
        self.assertEqual(sorted(insertion["released_edge_ids"]), sorted(PATH_EDGE_IDS))
        self.assertIsNotNone(insertion["preemption_score"])
        self.assertEqual(result.n_preemptions, 1)

    def test_partial_direct_insert_falls_back_to_best_effort(self) -> None:
        scenario = _scenario(
            cross_capacity=4.0,
            domain_capacity=4.0,
            rho=0.25,
            tasks=[
                Task("R_HIGH", "A0", "B0", 0.0, 4.0, 4.0, 10.0, 2.0, "reg"),
                Task("E1", "A0", "B0", 1.0, 3.0, 6.0, 5.0, 3.0, "emg"),
            ],
        )
        baseline_trace = _baseline_trace(
            allocations=[_regular_allocation("R_HIGH", rate=2.0)],
            completed={"R_HIGH": True},
            remaining_end={"R_HIGH": 0.0},
        )

        result = run_stage2(scenario, plan=_plan(), baseline_trace=baseline_trace)
        insertion = _insertion(result)

        self.assertEqual(insertion["strategy"], "direct_insert_best_effort")
        self.assertEqual(insertion["capacity_tier"], "borrow_unused_regular_share")
        self.assertFalse(insertion["used_preemption"])
        self.assertEqual(insertion["direct_plan_delivery"], 4.0)
        self.assertEqual(insertion["planned_delivery"], 4.0)

    def test_no_direct_or_preemption_capacity_is_blocked(self) -> None:
        scenario = _scenario(
            cross_capacity=4.0,
            domain_capacity=4.0,
            rho=0.25,
            tasks=[
                Task("R_HIGH", "A0", "B0", 0.0, 4.0, 8.0, 10.0, 4.0, "reg"),
                Task("E1", "A0", "B0", 1.0, 3.0, 4.0, 5.0, 2.0, "emg"),
            ],
        )
        baseline_trace = _baseline_trace(
            allocations=[_regular_allocation("R_HIGH", rate=4.0)],
            completed={"R_HIGH": True},
            remaining_end={"R_HIGH": 0.0},
        )

        result = run_stage2(scenario, plan=_plan(), baseline_trace=baseline_trace)
        insertion = _insertion(result)

        self.assertEqual(insertion["strategy"], "blocked")
        self.assertEqual(insertion["capacity_tier"], "blocked")
        self.assertFalse(insertion["used_preemption"])
        self.assertEqual(insertion["direct_plan_delivery"], 0.0)
        self.assertEqual(insertion["planned_delivery"], 0.0)


if __name__ == "__main__":
    unittest.main()
