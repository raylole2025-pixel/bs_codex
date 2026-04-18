from __future__ import annotations

import unittest
from collections import defaultdict

from bs3.models import CapacityConfig, CandidateWindow, GAConfig, Scenario, ScheduledWindow, Stage1Config, Stage2Config, Task, TemporalLink
from bs3.stage1 import RegularEvaluator, run_stage1
from bs3.stage2 import run_stage2


def _normalize_allocations(allocations, task_type: str | None = None):
    rows = []
    for alloc in allocations:
        if task_type is not None and alloc.task_type != task_type:
            continue
        rows.append(
            (
                alloc.task_id,
                int(alloc.segment_index),
                str(alloc.path_id),
                tuple(alloc.edge_ids),
                round(float(alloc.rate), 9),
                round(float(alloc.delivered), 9),
                alloc.cross_window_id,
                bool(alloc.is_preempted),
            )
        )
    return sorted(rows)


def _cross_usage_from_allocations(allocations):
    usage = defaultdict(lambda: defaultdict(float))
    for alloc in allocations:
        if alloc.cross_window_id is None:
            continue
        usage[int(alloc.segment_index)][str(alloc.cross_window_id)] += float(alloc.rate)
    return {
        int(segment_index): {window_id: round(float(value), 9) for window_id, value in per_window.items()}
        for segment_index, per_window in usage.items()
    }


class StageSemanticValidationTests(unittest.TestCase):
    def _stage1_consistency_scenario(self) -> Scenario:
        return Scenario(
            node_domain={"A1": "A", "A2": "A", "B1": "B", "B2": "B"},
            intra_links=[
                TemporalLink("A12", "A1", "A2", "A", 0.0, 4.0, delay=0.1),
                TemporalLink("B12", "B1", "B2", "B", 0.0, 4.0, delay=0.1),
            ],
            candidate_windows=[
                CandidateWindow(window_id="X1", a="A1", b="B1", start=0.0, end=4.0, delay=0.0),
                CandidateWindow(window_id="X2", a="A2", b="B2", start=0.0, end=4.0, delay=0.0),
            ],
            tasks=[
                Task("RC1", "A1", "B1", 0.0, 4.0, 4.0, 2.0, 2.0, "reg"),
                Task("RC2", "A2", "B2", 0.0, 4.0, 4.0, 1.0, 2.0, "reg"),
                Task("RS1", "A1", "A2", 0.0, 4.0, 2.0, 3.0, 1.0, "reg"),
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

    def _no_emergency_scenario(self) -> Scenario:
        return Scenario(
            node_domain={"A1": "A", "B1": "B"},
            intra_links=[],
            candidate_windows=[
                CandidateWindow(window_id="X1", a="A1", b="B1", start=0.0, end=4.0, delay=0.0),
            ],
            tasks=[
                Task("R1", "A1", "B1", 0.0, 4.0, 4.0, 1.0, 2.0, "reg"),
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

    def _emergency_insert_scenario(self) -> Scenario:
        return Scenario(
            node_domain={"A1": "A", "B1": "B"},
            intra_links=[],
            candidate_windows=[
                CandidateWindow(window_id="X1", a="A1", b="B1", start=0.0, end=4.0, delay=0.0),
            ],
            tasks=[
                Task("R1", "A1", "B1", 0.0, 4.0, 12.0, 1.0, 3.0, "reg"),
                Task("E_direct", "A1", "B1", 1.0, 3.0, 2.0, 5.0, 1.0, "emg"),
                Task("E_preempt", "A1", "B1", 1.0, 3.0, 4.0, 4.0, 2.0, "emg"),
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

    def test_stage1_baseline_export_matches_internal_evaluation(self) -> None:
        scenario = self._stage1_consistency_scenario()
        stage1_result = run_stage1(scenario, seed=1)
        baseline_trace = stage1_result.baseline_trace
        self.assertIsNotNone(baseline_trace)
        assert baseline_trace is not None

        evaluator = RegularEvaluator(scenario)
        metrics = evaluator.evaluate(stage1_result.selected_plan)
        trace = evaluator.trace(stage1_result.selected_plan)

        self.assertAlmostEqual(stage1_result.baseline_summary["mean_completion_ratio"], metrics.mean_completion_ratio, delta=1e-9)
        self.assertAlmostEqual(stage1_result.baseline_summary["fr"], metrics.fr, delta=1e-9)
        self.assertAlmostEqual(stage1_result.baseline_summary["eta_cap"], metrics.eta_cap, delta=1e-9)
        self.assertAlmostEqual(stage1_result.baseline_summary["eta_0"], metrics.eta_0, delta=1e-9)
        self.assertEqual(stage1_result.baseline_summary["stage1_metric_regular_task_count"], 2)
        self.assertEqual(stage1_result.baseline_summary["baseline_regular_task_count"], 3)

        delivered_by_task = defaultdict(float)
        for alloc in baseline_trace.allocations:
            delivered_by_task[alloc.task_id] += float(alloc.delivered)
        for row in baseline_trace.task_states:
            self.assertIn("remaining_before", row)
            self.assertIn("remaining_after", row)
        for task_row in trace["tasks"]:
            task_id = task_row["task_id"]
            expected_remaining = round(float(task_row["data"]) - delivered_by_task.get(task_id, 0.0), 9)
            self.assertAlmostEqual(baseline_trace.remaining_end[task_id], expected_remaining, delta=1e-9)
            self.assertEqual(bool(baseline_trace.completed[task_id]), bool(task_row["completed"]))

        self.assertEqual(
            _cross_usage_from_allocations(baseline_trace.allocations),
            {
                int(segment_index): {window_id: round(float(value), 9) for window_id, value in usage.items()}
                for segment_index, usage in baseline_trace.cross_window_usage_by_segment.items()
            },
        )

    def test_stage2_without_emergency_strictly_degenerates_to_baseline(self) -> None:
        scenario = self._no_emergency_scenario()
        stage1_result = run_stage1(scenario, seed=1)
        baseline_trace = stage1_result.baseline_trace
        self.assertIsNotNone(baseline_trace)
        assert baseline_trace is not None

        result = run_stage2(scenario, stage1_result=stage1_result)

        self.assertAlmostEqual(result.cr_reg, stage1_result.baseline_summary["fr"], delta=1e-9)
        self.assertEqual(result.n_preemptions, 0)
        self.assertEqual(result.solver_mode, "stage2_emergency_insert")
        self.assertTrue(result.metadata["empty_emergency_insert"])
        self.assertEqual(result.metadata["emergency_task_count"], 0)
        self.assertEqual(result.metadata["emergency_insertions_count"], 0)
        self.assertEqual(result.metadata["emergency_insertions"], [])
        self.assertEqual(result.metadata["cross_window_usage_delta_by_segment"], {})
        self.assertEqual(
            _normalize_allocations(result.allocations, task_type="reg"),
            _normalize_allocations(baseline_trace.allocations, task_type="reg"),
        )

    def test_stage2_emergency_insert_uses_baseline_state_without_legacy_stage21(self) -> None:
        scenario = self._emergency_insert_scenario()
        stage1_result = run_stage1(scenario, seed=1)
        result = run_stage2(scenario, stage1_result=stage1_result)

        insertions = result.metadata["emergency_insertions"]
        self.assertEqual(result.metadata["baseline_source"], "stage1_result")
        self.assertEqual(result.metadata["stage2_role"], "emergency_event_insert_only")
        self.assertEqual(result.metadata["emergency_insertions_count"], 2)
        self.assertEqual(result.metadata["emergency_insertions_direct_count"], 1)
        self.assertEqual(result.metadata["emergency_insertions_used_preemption_count"], 1)
        self.assertEqual(result.n_preemptions, 1)
        self.assertEqual(insertions[0]["strategy"], "direct_insert")
        self.assertEqual(insertions[0]["capacity_tier"], "reserved_only")
        self.assertFalse(insertions[0]["used_preemption"])
        self.assertEqual(insertions[1]["strategy"], "controlled_preemption")
        self.assertEqual(insertions[1]["capacity_tier"], "preempted")
        self.assertTrue(insertions[1]["used_preemption"])
        self.assertEqual(insertions[1]["released_cross_window_ids"], ["X1"])
        self.assertIsNotNone(insertions[1]["preemption_score"])
        self.assertIn("R1", result.metadata["regular_tasks_degraded_by_emergency"])
        self.assertNotEqual(
            result.metadata["baseline_cross_window_usage_by_segment"],
            result.metadata["final_cross_window_usage_by_segment"],
        )
        self.assertGreater(sum(alloc.delivered for alloc in result.allocations if alloc.task_id == "E_direct"), 0.0)
        self.assertGreater(sum(alloc.delivered for alloc in result.allocations if alloc.task_id == "E_preempt"), 0.0)


if __name__ == "__main__":
    unittest.main()
