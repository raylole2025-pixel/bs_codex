from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from bs3.models import CapacityConfig, CandidateWindow, GAConfig, HotspotInterval, HotspotRegion, Scenario, Stage1Config, Stage2Config, Task
from bs3.scenario import scenario_to_dict
from bs3.stage1 import run_stage1
from tools.stage2_emergency_validation_lib import (
    baseline_trace_to_dict,
    run_stage2_emergency_validation,
    scheduled_window_to_dict,
    write_json,
)


def _scenario() -> Scenario:
    return Scenario(
        node_domain={"A1": "A", "A2": "A", "B1": "B", "B2": "B"},
        intra_links=[],
        candidate_windows=[
            CandidateWindow(window_id="X1", a="A1", b="B1", start=0.0, end=6.0, delay=0.0),
            CandidateWindow(window_id="X2", a="A2", b="B2", start=0.0, end=6.0, delay=0.0),
        ],
        tasks=[
            Task("R1", "A1", "B1", 0.0, 6.0, 8.0, 2.0, 2.0, "reg"),
            Task("R2", "A2", "B2", 0.0, 6.0, 6.0, 1.0, 2.0, "reg"),
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
        planning_end=6.0,
        hotspots_a=[
            HotspotRegion(
                region_id="hotA",
                weight=1.0,
                nodes=("A1",),
                intervals=(HotspotInterval(start=1.0, end=4.0, nodes=("A1",)),),
            )
        ],
        metadata={"name": "validation-test-scenario"},
    )


def _write_stage1_result(tmpdir: Path, scenario: Scenario) -> tuple[Path, Path]:
    scenario_path = tmpdir / "scenario.json"
    write_json(scenario_path, scenario_to_dict(scenario))

    stage1_result = run_stage1(scenario, seed=1)
    baseline_trace_path = tmpdir / "baseline_trace.json"
    write_json(baseline_trace_path, baseline_trace_to_dict(stage1_result.baseline_trace))

    plan_rows = [scheduled_window_to_dict(window) for window in stage1_result.selected_plan]
    candidate_stub = {
        "plan": plan_rows,
        "fr": stage1_result.baseline_summary["fr"],
        "mean_completion_ratio": stage1_result.baseline_summary["mean_completion_ratio"],
        "window_count": len(plan_rows),
        "gateway_count": len({node for row in plan_rows for node in (row["a"], row["b"])}),
        "activation_count": 2 * len(plan_rows),
    }
    payload = {
        "selected_candidate_index": 0,
        "selected_candidate_source": stage1_result.selected_candidate_source,
        "selected_plan": plan_rows,
        "baseline_summary": stage1_result.baseline_summary,
        "best_feasible": [candidate_stub, dict(candidate_stub)],
        "baseline_trace_file": str(baseline_trace_path.resolve()),
    }
    stage1_result_path = tmpdir / "stage1_result.json"
    write_json(stage1_result_path, payload)
    return scenario_path, stage1_result_path


class Stage2EmergencyValidationTests(unittest.TestCase):
    def test_generated_cases_cover_empty_baseline_and_light_insertion(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            scenario_path, stage1_result_path = _write_stage1_result(tmpdir, _scenario())
            summary = run_stage2_emergency_validation(
                scenario_path=scenario_path,
                stage1_result_path=stage1_result_path,
                output_root=tmpdir / "out",
                suite_name="test_generated",
                run_name="generated_suite",
                candidate_indices=[0],
                rho_values=[{"label": "default", "value": 0.25, "is_default": True}],
                cases=[
                    {
                        "name": "empty_control",
                        "case_type": "empty",
                        "source": {"mode": "generate", "params": {"num_emergencies": 0}},
                    },
                    {
                        "name": "light_generated",
                        "case_type": "light",
                        "source": {
                            "mode": "generate",
                            "params": {
                                "num_emergencies": 2,
                                "arrival_pattern": "uniform",
                                "deadline_tightness": "loose",
                                "data_scale": "low",
                                "weight_scale": "medium",
                                "hotspot_bias": False,
                            },
                        },
                    },
                ],
                seed=3,
            )

            run_dir = Path(summary["run_dir"])
            self.assertTrue((run_dir / "summary.json").exists())
            self.assertTrue((run_dir / "summary.md").exists())
            self.assertEqual(summary["case_count"], 2)

            empty_case = next(case for case in summary["cases"] if case["case_type"] == "empty")
            light_case = next(case for case in summary["cases"] if case["case_type"] == "light")

            self.assertEqual(empty_case["baseline"]["baseline_source"], "stage1_result")
            self.assertTrue(empty_case["baseline_impact"]["degenerates_to_baseline"])
            self.assertEqual(empty_case["stage2"]["solver_mode"], "stage2_emergency_insert")
            self.assertEqual(empty_case["diagnostics"]["emergency_total"], 0)

            self.assertEqual(light_case["baseline"]["baseline_source"], "stage1_result")
            self.assertGreater(light_case["diagnostics"]["emergency_total"], 0)
            self.assertGreaterEqual(light_case["diagnostics"]["emergency_success_count"], 1)
            self.assertIn("by_case_type", summary["aggregates"])
            self.assertTrue(Path(light_case["paths"]["summary_json"]).exists())
            self.assertTrue(Path(light_case["paths"]["summary_md"]).exists())

    def test_file_input_and_candidate_rho_comparison_are_supported(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            scenario_path, stage1_result_path = _write_stage1_result(tmpdir, _scenario())
            emergency_json = tmpdir / "emergencies.json"
            write_json(
                emergency_json,
                {
                    "tasks": [
                        {
                            "id": "E1",
                            "src": "A1",
                            "dst": "B1",
                            "arrival": 1.0,
                            "deadline": 4.0,
                            "data": 2.0,
                            "weight": 5.0,
                            "max_rate": 2.0,
                            "type": "emg",
                        }
                    ]
                },
            )

            summary = run_stage2_emergency_validation(
                scenario_path=scenario_path,
                stage1_result_path=stage1_result_path,
                output_root=tmpdir / "out",
                suite_name="file_suite",
                run_name="file_suite",
                candidate_indices=[0, 1],
                rho_values=[
                    {"label": "default", "value": 0.25, "is_default": True},
                    {"label": "rho_0_2", "value": 0.2, "is_default": False},
                ],
                cases=[
                    {
                        "name": "json_case",
                        "case_type": "json_file",
                        "source": {"mode": "json", "path": str(emergency_json.resolve())},
                    }
                ],
                seed=5,
            )

            self.assertEqual(summary["case_count"], 4)
            self.assertIn("candidate_0", summary["aggregates"]["by_candidate"])
            self.assertIn("candidate_1", summary["aggregates"]["by_candidate"])
            self.assertIn("default", summary["aggregates"]["by_rho"])
            self.assertIn("rho_0_2", summary["aggregates"]["by_rho"])

            baseline_sources = {case["baseline"]["baseline_source"] for case in summary["cases"]}
            self.assertIn("stage1_result", baseline_sources)
            self.assertIn("reconstructed_from_stage1", baseline_sources)
            self.assertTrue(all(case["emergency_task_set"]["source_mode"] == "json" for case in summary["cases"]))
            self.assertTrue(all(case["diagnostics"]["emergency_total"] == 1 for case in summary["cases"]))


if __name__ == "__main__":
    unittest.main()
