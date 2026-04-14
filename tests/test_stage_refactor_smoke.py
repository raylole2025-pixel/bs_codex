from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from bs3.models import CapacityConfig, CandidateWindow, GAConfig, Scenario, Stage1Config, Stage2Config, Task
from bs3.scenario import load_scenario
from bs3.stage1 import run_stage1
from bs3.stage2 import run_stage2


def _scenario() -> Scenario:
    return Scenario(
        node_domain={"A1": "A", "B1": "B"},
        intra_links=[],
        candidate_windows=[
            CandidateWindow(window_id="X1", a="A1", b="B1", start=0.0, end=4.0, delay=0.0),
        ],
        tasks=[
            Task(
                task_id="R1",
                src="A1",
                dst="B1",
                arrival=0.0,
                deadline=4.0,
                data=4.0,
                weight=1.0,
                max_rate=2.0,
                task_type="reg",
            ),
            Task(
                task_id="E1",
                src="A1",
                dst="B1",
                arrival=1.0,
                deadline=3.0,
                data=2.0,
                weight=5.0,
                max_rate=2.0,
                task_type="emg",
            ),
        ],
        capacities=CapacityConfig(domain_a=4.0, domain_b=4.0, cross=4.0),
        stage1=Stage1Config(
            rho=0.25,
            t_pre=0.0,
            d_min=0.0,
            theta_cap=0.0,
            theta_hot=0.0,
            q_eval=1,
            elite_prune_count=0,
            ga=GAConfig(population_size=4, max_generations=3, stall_generations=1, top_m=1),
        ),
        stage2=Stage2Config(k_paths=2, completion_tolerance=1e-6),
        planning_end=4.0,
        metadata={},
    )


class StageRefactorSmokeTests(unittest.TestCase):
    def test_stage1_exports_selected_plan_and_baseline_trace(self) -> None:
        result = run_stage1(_scenario(), seed=1)

        self.assertTrue(result.selected_plan)
        self.assertIsNotNone(result.baseline_trace)
        self.assertEqual(result.selected_candidate_source, "best_feasible")
        self.assertGreaterEqual(result.baseline_summary.get("allocation_count", 0), 1)
        self.assertTrue(result.baseline_trace.allocations)
        self.assertIn("R1", result.baseline_trace.completed)

    def test_stage2_uses_stage1_baseline_trace_for_emergency_insert(self) -> None:
        scenario = _scenario()
        stage1_result = run_stage1(scenario, seed=1)

        result = run_stage2(scenario, stage1_result=stage1_result)

        self.assertEqual(result.solver_mode, "stage2_emergency_insert")
        self.assertEqual(result.metadata.get("baseline_source"), "stage1_result")
        self.assertIn("baseline_summary", result.metadata)
        self.assertGreater(sum(alloc.delivered for alloc in result.allocations if alloc.task_id == "E1"), 0.0)
        self.assertGreaterEqual(result.cr_emg, 1.0)

    def test_loader_rejects_removed_stage21_fields(self) -> None:
        payload = {
            "planning_end": 2.0,
            "nodes": {"A": ["A1"], "B": ["B1"]},
            "capacities": {"A": 1.0, "B": 1.0, "X": 1.0},
            "stage1": {"rho": 0.0, "t_pre": 0.0, "d_min": 0.0},
            "stage2": {
                "k_paths": 2,
                "completion_tolerance": 1e-6,
                "hotspot_relief_enabled": True,
            },
            "candidate_windows": [
                {"id": "X1", "a": "A1", "b": "B1", "start": 0.0, "end": 2.0},
            ],
            "tasks": [],
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "scenario.json"
            path.write_text(json.dumps(payload), encoding="utf-8")
            with self.assertRaises(ValueError):
                load_scenario(path)


if __name__ == "__main__":
    unittest.main()
