from __future__ import annotations

import copy
import json
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

from bs3.models import Allocation, ScheduledWindow
from bs3.scenario import build_segments, load_scenario, scenario_to_dict
from bs3.stage2 import run_stage2
from bs3.stage2_hotspot_relief import (
    AugmentCandidate,
    CrossSegmentProfileRow,
    HotRange,
    HotRangeClassification,
    _load_summary_from_profile,
    _select_augment_windows,
    build_cross_segment_profile,
    classify_hot_range,
    detect_hot_ranges,
)


def _base_payload() -> dict:
    return {
        "metadata": {"name": "stage2-hotspot-relief-test"},
        "planning_end": 2.0,
        "nodes": {
            "A": ["A1", "A2"],
            "B": ["B1", "B2"],
        },
        "capacities": {"A": 10.0, "B": 10.0, "X": 2.0},
        "stage1": {
            "rho": 0.0,
            "t_pre": 1.0,
            "d_min": 1.0,
        },
        "stage2": {
            "k_paths": 2,
            "completion_tolerance": 1e-6,
            "prefer_milp": True,
            "regular_baseline_mode": "full_milp",
            "milp_mode": "full",
            "hotspot_relief_enabled": True,
            "hotspot_util_threshold": 0.95,
            "hotspot_topk_ranges": 3,
            "hotspot_expand_segments": 0,
            "hotspot_single_link_fraction_threshold": 0.6,
            "hotspot_top_tasks_per_range": 8,
            "augment_window_budget": 1,
            "augment_top_windows_per_range": 1,
            "hot_path_limit": 4,
            "hot_promoted_tasks_per_segment": 4,
            "local_peak_horizon_cap_segments": 8,
            "local_peak_accept_epsilon": 1e-6,
            "fail_if_milp_disabled": True,
            "regular_repair_enabled": False,
            "repair_block_max_count": 4,
            "repair_expand_segments": 2,
            "repair_max_block_segments": 9,
            "repair_min_active_tasks": 3,
            "repair_util_threshold": 0.8,
            "repair_candidate_path_limit": 3,
            "repair_time_limit_seconds": 11.0,
            "repair_accept_epsilon": 1e-5,
        },
        "intra_domain_links": [
            {"id": "A12", "u": "A1", "v": "A2", "domain": "A", "start": 0.0, "end": 2.0, "delay": 0.0},
            {"id": "B12", "u": "B1", "v": "B2", "domain": "B", "start": 0.0, "end": 2.0, "delay": 0.0},
        ],
        "candidate_windows": [
            {"id": "X1", "a": "A1", "b": "B1", "start": 0.0, "end": 2.0, "delay": 0.0},
            {"id": "X2", "a": "A2", "b": "B2", "start": 0.0, "end": 2.0, "delay": 0.0},
        ],
        "tasks": [],
    }


def _load_payload(payload: dict):
    with tempfile.TemporaryDirectory() as tmpdir:
        path = Path(tmpdir) / "scenario.json"
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        return load_scenario(path)


class Stage2HotspotReliefTests(unittest.TestCase):
    def test_stage2_config_roundtrip_preserves_repair_and_hotspot_fields(self) -> None:
        payload = _base_payload()
        scenario = _load_payload(payload)
        with tempfile.TemporaryDirectory() as tmpdir:
            roundtrip_path = Path(tmpdir) / "roundtrip.json"
            roundtrip_path.write_text(json.dumps(scenario_to_dict(scenario), indent=2), encoding="utf-8")
            loaded = load_scenario(roundtrip_path)

        for field_name, expected in (
            ("regular_baseline_mode", "full_milp"),
            ("regular_repair_enabled", False),
            ("repair_block_max_count", 4),
            ("repair_expand_segments", 2),
            ("repair_max_block_segments", 9),
            ("repair_min_active_tasks", 3),
            ("repair_util_threshold", 0.8),
            ("repair_candidate_path_limit", 3),
            ("repair_time_limit_seconds", 11.0),
            ("repair_accept_epsilon", 1e-5),
            ("hotspot_relief_enabled", True),
            ("hotspot_util_threshold", 0.95),
            ("hotspot_topk_ranges", 3),
            ("hotspot_expand_segments", 0),
            ("hotspot_single_link_fraction_threshold", 0.6),
            ("hotspot_top_tasks_per_range", 8),
            ("augment_window_budget", 1),
            ("augment_top_windows_per_range", 1),
            ("augment_selection_policy", "global_score_only"),
            ("hot_path_limit", 4),
            ("hot_promoted_tasks_per_segment", 4),
            ("local_peak_horizon_cap_segments", 8),
            ("local_peak_accept_epsilon", 1e-6),
            ("fail_if_milp_disabled", True),
        ):
            self.assertEqual(getattr(loaded.stage2, field_name), expected)

    def test_single_hotspot_single_window_is_structural(self) -> None:
        payload = _base_payload()
        payload["capacities"]["X"] = 1.0
        payload["candidate_windows"] = [
            {"id": "X1", "a": "A1", "b": "B1", "start": 0.0, "end": 2.0, "delay": 0.0}
        ]
        payload["tasks"] = [
            {
                "id": "R1",
                "src": "A1",
                "dst": "B1",
                "arrival": 0.0,
                "deadline": 2.0,
                "data": 2.0,
                "weight": 1.0,
                "max_rate": 1.0,
                "type": "reg",
            }
        ]
        scenario = _load_payload(payload)
        plan = [ScheduledWindow(window_id="X1", a="A1", b="B1", start=0.0, end=2.0, on=0.0, off=2.0, delay=0.0)]
        segments = build_segments(scenario, plan, scenario.tasks)
        schedule = {
            ("R1", segments[0].index): Allocation(
                task_id="R1",
                segment_index=segments[0].index,
                path_id="R1:seg0:X1",
                edge_ids=("X1",),
                rate=1.0,
                delivered=2.0,
                task_type="reg",
            )
        }

        profile = build_cross_segment_profile(scenario, plan, segments, schedule)
        hot_ranges = detect_hot_ranges(profile, threshold=0.95, topk=2)
        classification = classify_hot_range(profile, hot_ranges[0], single_link_fraction_threshold=0.6)

        self.assertEqual(len(hot_ranges), 1)
        self.assertTrue(classification.structural)
        self.assertEqual(classification.primary_class, "structural")
        self.assertAlmostEqual(classification.single_link_fraction, 1.0, delta=1e-9)
        self.assertAlmostEqual(classification.median_active_selected, 1.0, delta=1e-9)
        self.assertEqual(classification.active_selected_count_distribution, ((1, 1),))

    def test_high_single_link_fraction_forces_structural(self) -> None:
        profile = [
            CrossSegmentProfileRow(
                segment_index=index,
                start=float(index),
                end=float(index + 1),
                duration=1.0,
                q_r=1.0,
                active_selected_cross_count=(1 if index < 9 else 2),
                per_window_util={"X1": 1.0},
                active_window_ids=("X1",),
                selected_task_ids=("R1",),
            )
            for index in range(10)
        ]
        hot_range = HotRange(
            range_id="hot_range_1",
            start_segment_index=0,
            end_segment_index=9,
            start=0.0,
            end=10.0,
            max_q_r=1.0,
            q_integral=10.0,
            segment_indices=tuple(range(10)),
        )
        classification = classify_hot_range(profile, hot_range, single_link_fraction_threshold=0.6)

        self.assertTrue(classification.structural)
        self.assertEqual(classification.primary_class, "structural")
        self.assertAlmostEqual(classification.single_link_fraction, 0.9, delta=1e-9)

    def test_peak_segment_count_uses_peak_like_threshold(self) -> None:
        profile = [
            CrossSegmentProfileRow(
                segment_index=0,
                start=0.0,
                end=1.0,
                duration=1.0,
                q_r=1.0,
                active_selected_cross_count=1,
                per_window_util={"X1": 1.0},
                active_window_ids=("X1",),
                selected_task_ids=("R1",),
            ),
            CrossSegmentProfileRow(
                segment_index=1,
                start=1.0,
                end=2.0,
                duration=1.0,
                q_r=0.99995,
                active_selected_cross_count=1,
                per_window_util={"X1": 0.99995},
                active_window_ids=("X1",),
                selected_task_ids=("R1",),
            ),
            CrossSegmentProfileRow(
                segment_index=2,
                start=2.0,
                end=3.0,
                duration=1.0,
                q_r=0.994,
                active_selected_cross_count=1,
                per_window_util={"X1": 0.994},
                active_window_ids=("X1",),
                selected_task_ids=("R1",),
            ),
        ]

        summary = _load_summary_from_profile(profile)

        self.assertAlmostEqual(float(summary["peak_like_threshold"]), 0.9999, delta=1e-9)
        self.assertEqual(int(summary["peak_segment_count"]), 2)

    def test_structural_coverage_first_reserves_slot_for_each_structural_hotspot(self) -> None:
        payload = _base_payload()
        payload["nodes"] = {
            "A": ["A1", "A2", "A3"],
            "B": ["B1", "B2", "B3"],
        }
        payload["candidate_windows"] = [
            {"id": "X1", "a": "A1", "b": "B1", "start": 0.0, "end": 2.0, "delay": 0.0},
            {"id": "X2", "a": "A2", "b": "B2", "start": 0.0, "end": 2.0, "delay": 0.0},
            {"id": "X3", "a": "A3", "b": "B3", "start": 0.0, "end": 2.0, "delay": 0.0},
        ]
        payload["stage1"]["t_pre"] = 0.0
        payload["stage1"]["d_min"] = 1.0
        payload["stage2"]["augment_window_budget"] = 2
        scenario = _load_payload(payload)
        candidates = [
            AugmentCandidate(
                range_id="hot_range_1",
                window_id="X1",
                structural_priority=1,
                scheduled_on=0.0,
                scheduled_off=2.0,
                scheduled_duration=2.0,
                overlap_duration=2.0,
                estimated_divertable_rate=10.0,
                feasible_path_count=1,
                delay_penalty=0.0,
                switch_penalty=0.0,
                relief_score=10.0,
                contributing_task_ids=("R1",),
            ),
            AugmentCandidate(
                range_id="hot_range_2",
                window_id="X2",
                structural_priority=1,
                scheduled_on=0.0,
                scheduled_off=2.0,
                scheduled_duration=2.0,
                overlap_duration=2.0,
                estimated_divertable_rate=20.0,
                feasible_path_count=1,
                delay_penalty=0.0,
                switch_penalty=0.0,
                relief_score=100.0,
                contributing_task_ids=("R2",),
            ),
            AugmentCandidate(
                range_id="hot_range_2",
                window_id="X3",
                structural_priority=1,
                scheduled_on=0.0,
                scheduled_off=2.0,
                scheduled_duration=2.0,
                overlap_duration=2.0,
                estimated_divertable_rate=18.0,
                feasible_path_count=1,
                delay_penalty=0.0,
                switch_penalty=0.0,
                relief_score=90.0,
                contributing_task_ids=("R2",),
            ),
        ]
        hot_ranges = [
            HotRange(
                range_id="hot_range_1",
                start_segment_index=0,
                end_segment_index=0,
                start=0.0,
                end=2.0,
                max_q_r=1.0,
                q_integral=10.0,
                segment_indices=(0,),
            ),
            HotRange(
                range_id="hot_range_2",
                start_segment_index=1,
                end_segment_index=1,
                start=0.0,
                end=2.0,
                max_q_r=1.0,
                q_integral=5.0,
                segment_indices=(1,),
            ),
        ]
        classifications = {
            hot_range.range_id: HotRangeClassification(
                hot_duration=2.0,
                single_link_fraction=1.0,
                median_active_selected=1.0,
                active_selected_count_distribution=((1, 1),),
                feasible_alternative_cross_window_count=1,
                feasible_alternative_path_count=1,
                top_contributing_windows=("X1",),
                structural=True,
                reroutable=True,
                primary_class="structural",
                reason="test",
                warnings=(),
            )
            for hot_range in hot_ranges
        }

        selected_global = _select_augment_windows(
            replace(scenario, stage2=replace(scenario.stage2, augment_selection_policy="global_score_only")),
            [],
            candidates,
            hot_ranges=hot_ranges,
            classifications=classifications,
        )
        selected_structural = _select_augment_windows(
            replace(scenario, stage2=replace(scenario.stage2, augment_selection_policy="structural_coverage_first")),
            [],
            candidates,
            hot_ranges=hot_ranges,
            classifications=classifications,
        )

        self.assertEqual([candidate.window_id for candidate in selected_global], ["X2", "X3"])
        self.assertEqual([candidate.window_id for candidate in selected_structural], ["X1", "X2"])

    def test_hotspot_relief_improves_peak_without_degrading_completion(self) -> None:
        payload = _base_payload()
        payload["planning_end"] = 600.0
        payload["capacities"]["X"] = 1.0
        payload["stage1"]["d_min"] = 300.0
        for item in payload["intra_domain_links"]:
            item["end"] = 600.0
        for item in payload["candidate_windows"]:
            item["end"] = 600.0
        payload["tasks"] = [
            {
                "id": "R1",
                "src": "A1",
                "dst": "B1",
                "arrival": 0.0,
                "deadline": 600.0,
                "data": 300.0,
                "weight": 1.0,
                "max_rate": 0.5,
                "type": "reg",
            },
            {
                "id": "R2",
                "src": "A2",
                "dst": "B2",
                "arrival": 0.0,
                "deadline": 600.0,
                "data": 300.0,
                "weight": 1.0,
                "max_rate": 0.5,
                "type": "reg",
            },
        ]
        scenario = _load_payload(payload)
        fixed_plan = [
            ScheduledWindow(window_id="X1", a="A1", b="B1", start=0.0, end=600.0, on=0.0, off=600.0, delay=0.0)
        ]

        result = run_stage2(scenario, fixed_plan)

        self.assertAlmostEqual(result.cr_reg, 1.0, delta=1e-9)
        self.assertIn("X2", {window.window_id for window in result.plan})
        self.assertGreaterEqual(result.metadata["hot_ranges_considered"], 1)
        self.assertGreaterEqual(result.metadata["augment_windows_added"], 1)
        self.assertLess(result.metadata["q_peak_after"], result.metadata["q_peak_before"])
        self.assertIn("selected_augment_windows", result.metadata["hotspot_report"])
        self.assertIn("applied_augment_windows", result.metadata["hotspot_report"])
        self.assertEqual(result.metadata["hotspot_report"]["augment_selection_policy"], "global_score_only")
        for item in result.metadata["hotspot_report"]["hot_ranges"]:
            counts = item["augment_funnel_counts"]
            sequence = [
                int(counts.get("raw_overlap_candidate_count", 0)),
                int(counts.get("schedulable_after_t_pre_d_min_count", 0)),
                int(counts.get("conflict_free_count", 0)),
                int(counts.get("relief_path_ready_count", 0)),
                int(counts.get("shortlisted_count", 0)),
                int(counts.get("selected_count", 0)),
                int(counts.get("applied_count", 0)),
            ]
            self.assertEqual(sequence, sorted(sequence, reverse=True))


if __name__ == "__main__":
    unittest.main()
