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
from bs3.regular_routing_common import build_regular_schedule_diagnostics
from bs3.scenario import build_segments, validate_scenario
from bs3.stage2 import run_stage2
from bs3.stage2_regular_block_repair import repair_regular_baseline_blocks
from bs3.stage2_regular_greedy_baseline import build_regular_baseline_stage1_greedy
from bs3.stage2_regular_joint_milp import _build_task_segments, _select_rolling_path_limits
from bs3.stage2_two_phase_scheduler import TwoPhaseEventDrivenScheduler


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
        "prefer_milp": False,
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
            regular_baseline_mode=payload["stage2"].get("regular_baseline_mode"),
            regular_repair_enabled=payload["stage2"].get("regular_repair_enabled"),
            prefer_milp=bool(payload["stage2"].get("prefer_milp", False)),
            milp_mode=str(payload["stage2"].get("milp_mode", "full")),
            milp_horizon_segments=int(payload["stage2"].get("milp_horizon_segments", 16)),
            milp_commit_segments=int(payload["stage2"].get("milp_commit_segments", 8)),
            milp_rolling_path_limit=int(payload["stage2"].get("milp_rolling_path_limit", 1)),
            milp_rolling_high_path_limit=int(payload["stage2"].get("milp_rolling_high_path_limit", 2)),
            milp_rolling_high_weight_threshold=(
                None if payload["stage2"].get("milp_rolling_high_weight_threshold") is None else float(payload["stage2"]["milp_rolling_high_weight_threshold"])
            ),
            milp_rolling_high_competition_task_threshold=int(payload["stage2"].get("milp_rolling_high_competition_task_threshold", 8)),
            milp_rolling_promoted_tasks_per_segment=int(payload["stage2"].get("milp_rolling_promoted_tasks_per_segment", 2)),
            milp_time_limit_seconds=payload["stage2"].get("milp_time_limit_seconds"),
            milp_relative_gap=payload["stage2"].get("milp_relative_gap"),
            repair_block_max_count=int(payload["stage2"].get("repair_block_max_count", 3)),
            repair_expand_segments=int(payload["stage2"].get("repair_expand_segments", 1)),
            repair_max_block_segments=int(payload["stage2"].get("repair_max_block_segments", 8)),
            repair_min_active_tasks=int(payload["stage2"].get("repair_min_active_tasks", 2)),
            repair_util_threshold=float(payload["stage2"].get("repair_util_threshold", 0.75)),
            repair_candidate_path_limit=int(payload["stage2"].get("repair_candidate_path_limit", 2)),
            repair_time_limit_seconds=payload["stage2"].get("repair_time_limit_seconds"),
            repair_accept_epsilon=float(payload["stage2"].get("repair_accept_epsilon", 1e-6)),
            hotspot_relief_enabled=bool(payload["stage2"].get("hotspot_relief_enabled", True)),
            closed_loop_relief_enabled=bool(
                payload["stage2"].get(
                    "closed_loop_relief_enabled",
                    payload["stage2"].get("hotspot_relief_enabled", True),
                )
            ),
            hotspot_util_threshold=float(payload["stage2"].get("hotspot_util_threshold", 0.95)),
            hotspot_topk_ranges=int(payload["stage2"].get("hotspot_topk_ranges", 5)),
            hotspot_expand_segments=int(payload["stage2"].get("hotspot_expand_segments", 2)),
            hotspot_single_link_fraction_threshold=float(payload["stage2"].get("hotspot_single_link_fraction_threshold", 0.6)),
            hotspot_top_tasks_per_range=int(payload["stage2"].get("hotspot_top_tasks_per_range", 12)),
            augment_window_budget=int(payload["stage2"].get("augment_window_budget", 2)),
            augment_top_windows_per_range=int(payload["stage2"].get("augment_top_windows_per_range", 3)),
            augment_selection_policy=str(payload["stage2"].get("augment_selection_policy", "global_score_only")),
            closed_loop_max_rounds=int(payload["stage2"].get("closed_loop_max_rounds", 6)),
            closed_loop_max_new_windows=int(payload["stage2"].get("closed_loop_max_new_windows", 2)),
            closed_loop_min_delta_q_peak=float(payload["stage2"].get("closed_loop_min_delta_q_peak", 1e-4)),
            closed_loop_min_delta_q_integral=float(payload["stage2"].get("closed_loop_min_delta_q_integral", 1e-6)),
            closed_loop_min_delta_high_segments=int(payload["stage2"].get("closed_loop_min_delta_high_segments", 1)),
            closed_loop_topk_ranges_per_round=int(payload["stage2"].get("closed_loop_topk_ranges_per_round", payload["stage2"].get("hotspot_topk_ranges", 5))),
            closed_loop_topk_candidates_per_range=int(payload["stage2"].get("closed_loop_topk_candidates_per_range", payload["stage2"].get("augment_top_windows_per_range", 3))),
            closed_loop_action_mode=str(payload["stage2"].get("closed_loop_action_mode", "best_global_action")),
            hot_path_limit=int(payload["stage2"].get("hot_path_limit", 4)),
            hot_promoted_tasks_per_segment=int(payload["stage2"].get("hot_promoted_tasks_per_segment", 8)),
            local_peak_horizon_cap_segments=payload["stage2"].get("local_peak_horizon_cap_segments", 48),
            local_peak_accept_epsilon=float(payload["stage2"].get("local_peak_accept_epsilon", 1e-6)),
            fail_if_milp_disabled=bool(payload["stage2"].get("fail_if_milp_disabled", True)),
        ),
        planning_end=float(payload["planning_end"]),
        metadata=copy.deepcopy(payload.get("metadata", {})),
    )
    validate_scenario(scenario)
    return scenario


def _task_allocation(result, task_id: str):
    return [alloc for alloc in result.allocations if alloc.task_id == task_id]


class Stage2BaselineInsertTests(unittest.TestCase):
    def test_stage1_greedy_baseline_builds_complete_schedule_and_not_worse_than_sequential(self) -> None:
        payload = copy.deepcopy(BASE_PAYLOAD)
        payload["planning_end"] = 3.0
        payload["stage2"].update({"prefer_milp": False, "regular_baseline_mode": "stage1_greedy", "hotspot_relief_enabled": False})
        payload["tasks"] = [
            {
                "id": "R1",
                "src": "A1",
                "dst": "B1",
                "arrival": 0.0,
                "deadline": 3.0,
                "data": 4.0,
                "weight": 2.0,
                "max_rate": 2.0,
                "type": "reg",
            },
            {
                "id": "R2",
                "src": "A2",
                "dst": "B2",
                "arrival": 0.0,
                "deadline": 3.0,
                "data": 4.0,
                "weight": 1.0,
                "max_rate": 2.0,
                "type": "reg",
            },
        ]
        scenario = _load_payload(payload)
        segments = build_segments(scenario, PLAN, [task for task in scenario.tasks if task.task_type == "reg"])
        greedy_schedule, greedy_completed, _ = build_regular_baseline_stage1_greedy(scenario, PLAN, segments)
        sequential_schedule, sequential_completed = TwoPhaseEventDrivenScheduler(scenario)._build_regular_baseline_sequential(PLAN, segments)

        self.assertTrue(greedy_schedule)
        self.assertEqual(sum(1 for done in greedy_completed.values() if done), 2)
        self.assertGreaterEqual(
            sum(1 for done in greedy_completed.values() if done),
            sum(1 for done in sequential_completed.values() if done),
        )
        self.assertGreaterEqual(len(greedy_schedule), len(sequential_schedule))

    def test_stage2_defaults_to_stage1_greedy_repair_baseline(self) -> None:
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
        self.assertEqual(result.solver_mode, "two_phase_event_insert+stage1_greedy_repair")
        self.assertFalse(result.metadata["prefer_milp"])
        self.assertEqual(result.metadata["regular_baseline_mode"], "stage1_greedy_repair")
        self.assertEqual(result.metadata["regular_baseline_source"], "stage1_greedy_repair")
        self.assertEqual(result.metadata["milp_mode"], "full")
        self.assertEqual(result.metadata["regular_task_count"], 1)

    def test_prefer_milp_no_longer_overrides_default_regular_baseline(self) -> None:
        payload = copy.deepcopy(BASE_PAYLOAD)
        payload["stage2"].update({"prefer_milp": True})
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

        self.assertEqual(result.metadata["regular_baseline_mode"], "stage1_greedy_repair")
        self.assertEqual(result.metadata["regular_baseline_source"], "stage1_greedy_repair")

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
        payload["stage2"].update({"regular_baseline_mode": "full_milp", "prefer_milp": True, "hotspot_relief_enabled": False})
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

    def test_repair_does_not_degrade_completed_regular_tasks(self) -> None:
        payload = copy.deepcopy(BASE_PAYLOAD)
        payload["planning_end"] = 2.0
        payload["stage1"]["rho"] = 0.0
        payload["stage2"].update(
            {
                "prefer_milp": False,
                "regular_baseline_mode": "stage1_greedy_repair",
                "regular_repair_enabled": True,
                "hotspot_relief_enabled": False,
                "repair_block_max_count": 2,
                "repair_util_threshold": 0.5,
            }
        )
        payload["tasks"] = [
            {
                "id": "R1",
                "src": "A1",
                "dst": "B1",
                "arrival": 0.0,
                "deadline": 2.0,
                "data": 4.0,
                "weight": 3.0,
                "max_rate": 2.0,
                "type": "reg",
            },
            {
                "id": "R2",
                "src": "A2",
                "dst": "B2",
                "arrival": 0.0,
                "deadline": 2.0,
                "data": 4.0,
                "weight": 2.0,
                "max_rate": 2.0,
                "type": "reg",
            },
            {
                "id": "R3",
                "src": "A1",
                "dst": "B2",
                "arrival": 0.0,
                "deadline": 2.0,
                "data": 4.0,
                "weight": 1.0,
                "max_rate": 2.0,
                "type": "reg",
            },
        ]
        scenario = _load_payload(payload)
        regular_tasks = [task for task in scenario.tasks if task.task_type == "reg"]
        segments = build_segments(scenario, PLAN, regular_tasks)
        baseline_schedule, _, baseline_diag = build_regular_baseline_stage1_greedy(scenario, PLAN, segments)
        repaired_schedule, repair_meta = repair_regular_baseline_blocks(scenario, PLAN, segments, baseline_schedule, baseline_diag)
        repaired_diag = repair_meta["diagnostics_after"]

        for task_id, completed_before in baseline_diag["completed"].items():
            if completed_before:
                self.assertTrue(repaired_diag["completed"][task_id])

        peak_before = max(float(row["q_peak"]) for row in baseline_diag["segment_metrics"].values())
        peak_after = max(float(row["q_peak"]) for row in repaired_diag["segment_metrics"].values())
        self.assertLessEqual(peak_after, peak_before + 1e-9)
        if abs(peak_after - peak_before) <= 1e-9:
            self.assertEqual(repaired_schedule, baseline_schedule)

    def test_regular_baseline_mode_and_metadata_fields_are_reported(self) -> None:
        payload = copy.deepcopy(BASE_PAYLOAD)
        payload["planning_end"] = 3.0
        payload["stage2"].update(
            {
                "prefer_milp": False,
                "regular_baseline_mode": "stage1_greedy_repair",
                "regular_repair_enabled": True,
                "hotspot_relief_enabled": False,
            }
        )
        payload["tasks"] = [
            {
                "id": "R1",
                "src": "A1",
                "dst": "B1",
                "arrival": 0.0,
                "deadline": 3.0,
                "data": 6.0,
                "weight": 1.0,
                "max_rate": 2.0,
                "type": "reg",
            }
        ]
        scenario = _load_payload(payload)
        result = run_stage2(scenario, PLAN)

        self.assertEqual(result.metadata["regular_baseline_mode"], "stage1_greedy_repair")
        self.assertEqual(result.metadata["regular_baseline_source"], "stage1_greedy_repair")
        for key in (
            "regular_repair_enabled",
            "repair_block_count_considered",
            "repair_block_count_accepted",
            "repair_total_improvement_peak",
            "repair_total_improvement_integral",
            "baseline_completed_count_before_repair",
            "baseline_completed_count_after_repair",
        ):
            self.assertIn(key, result.metadata)
        self.assertLessEqual(result.metadata["repair_block_count_accepted"], result.metadata["repair_block_count_considered"])
        self.assertGreaterEqual(
            result.metadata["baseline_completed_count_after_repair"],
            result.metadata["baseline_completed_count_before_repair"],
        )

    def test_regular_baseline_mode_full_milp_and_emergency_flow_remain_compatible(self) -> None:
        payload = copy.deepcopy(BASE_PAYLOAD)
        payload["planning_end"] = 4.0
        payload["stage2"].update(
            {
                "prefer_milp": False,
                "regular_baseline_mode": "full_milp",
                "hotspot_relief_enabled": False,
            }
        )
        payload["candidate_windows"] = [
            {"id": "X1", "a": "A1", "b": "B1", "start": 0.0, "end": 4.0, "delay": 0.0}
        ]
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
            },
            {
                "id": "E1",
                "src": "A1",
                "dst": "B1",
                "arrival": 1.0,
                "deadline": 3.0,
                "data": 2.0,
                "weight": 5.0,
                "max_rate": 2.0,
                "type": "emg",
            },
        ]
        scenario = _load_payload(payload)
        local_plan = [
            ScheduledWindow(window_id="X1", a="A1", b="B1", start=0.0, end=4.0, on=0.0, off=4.0, delay=0.0)
        ]
        result = run_stage2(scenario, local_plan)

        self.assertEqual(result.solver_mode, "two_phase_event_insert+joint_milp_full")
        self.assertEqual(result.metadata["regular_baseline_mode"], "full_milp")
        self.assertEqual(result.metadata["regular_baseline_source"], "full_milp")
        self.assertAlmostEqual(sum(alloc.delivered for alloc in _task_allocation(result, "E1")), 2.0, delta=1e-6)
        self.assertGreaterEqual(result.cr_emg, 1.0)

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

    def test_regular_route_switch_does_not_surface_as_preemption(self) -> None:
        payload = copy.deepcopy(BASE_PAYLOAD)
        payload["planning_end"] = 8.0
        payload["capacities"] = {"A": 4.0, "B": 4.0, "X": 4.0}
        payload["stage1"]["rho"] = 0.0
        payload["stage2"].update(
            {
                "milp_mode": "full",
                "hotspot_relief_enabled": False,
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

    def test_deadline_horizon_is_used_for_emergency_planning(self) -> None:
        payload = copy.deepcopy(BASE_PAYLOAD)
        payload["planning_end"] = 5.0
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


