from __future__ import annotations

from collections import defaultdict

from .models import CandidateWindow, Scenario
from .stage1_static_value import annotate_scenario_candidate_values

EPS = 1e-9


def _rank_by_channel(
    windows: list[CandidateWindow],
    primary_scores: dict[str, float],
    secondary_scores: dict[str, float],
) -> list[CandidateWindow]:
    return sorted(
        windows,
        key=lambda item: (
            -float(primary_scores.get(item.window_id, 0.0)),
            -float(secondary_scores.get(item.window_id, 0.0)),
            -float(item.value or 0.0),
            -float(item.duration),
            float(item.start),
            item.window_id,
        ),
    )


def _segment_local_rank(
    windows: list[CandidateWindow],
    coarse_scores: dict[str, dict[int, float]],
    reg_values: dict[str, float],
    hot_values: dict[str, float],
    coarse_idx: int,
) -> list[CandidateWindow]:
    return sorted(
        [
            window
            for window in windows
            if float(coarse_scores.get(window.window_id, {}).get(coarse_idx, 0.0)) > EPS
        ],
        key=lambda item: (
            -float(coarse_scores.get(item.window_id, {}).get(coarse_idx, 0.0)),
            -float(reg_values.get(item.window_id, 0.0)),
            -float(hot_values.get(item.window_id, 0.0)),
            -float(item.value or 0.0),
            -float(item.duration),
            float(item.start),
            item.window_id,
        ),
    )


def screen_candidate_windows(scenario: Scenario) -> list[CandidateWindow]:
    raw_windows = list(scenario.candidate_windows)
    raw_count = len(raw_windows)
    if raw_count == 0:
        scenario.metadata["stage1_screening"] = {
            "candidate_window_count_raw": 0,
            "candidate_window_count_screened": 0,
            "candidate_pool_size_limit": 0,
            "screening_mode": "candidate_pool_v49",
        }
        return []

    runtime_cache = scenario.metadata.setdefault("_runtime_cache", {}).get("stage1_static_value")
    if not runtime_cache or "reg_values" not in runtime_cache:
        annotate_scenario_candidate_values(scenario, force=True)
        runtime_cache = scenario.metadata.setdefault("_runtime_cache", {}).get("stage1_static_value", {})

    reg_values = {
        str(window_id): float(value)
        for window_id, value in dict(runtime_cache.get("reg_values", {})).items()
    }
    hot_values = {
        str(window_id): float(value)
        for window_id, value in dict(runtime_cache.get("hot_values", {})).items()
    }
    fine_segments = list(runtime_cache.get("fine_segments", []))
    coarse_segments = list(runtime_cache.get("coarse_segments", []))
    window_reg_segment_scores = dict(runtime_cache.get("window_reg_segment_scores", {}))

    base_size = min(max(int(scenario.stage1.candidate_pool_base_size), 1), raw_count)
    hot_fraction = min(max(float(scenario.stage1.candidate_pool_hot_fraction), 0.0), 1.0)
    min_per_segment = max(int(scenario.stage1.candidate_pool_min_per_coarse_segment), 0)
    max_additional = max(int(scenario.stage1.candidate_pool_max_additional), 0)
    screen_cap = base_size + max_additional

    if raw_count <= base_size:
        scenario.metadata["stage1_screening"] = {
            "candidate_window_count_raw": raw_count,
            "candidate_window_count_screened": raw_count,
            "candidate_pool_size_limit": screen_cap,
            "screening_mode": "candidate_pool_v49_noop",
            "candidate_pool_base_size": base_size,
            "candidate_pool_additional_limit": max_additional,
            "candidate_pool_min_per_coarse_segment": min_per_segment,
        }
        return raw_windows

    windows_by_id = {window.window_id: window for window in raw_windows}
    reg_ranked = _rank_by_channel(raw_windows, reg_values, hot_values)
    hot_ranked = _rank_by_channel(raw_windows, hot_values, reg_values)

    hot_channel_enabled = max(hot_values.values(), default=0.0) > EPS
    hot_quota = min(base_size, int(round(base_size * hot_fraction))) if hot_channel_enabled else 0
    reg_quota = max(base_size - hot_quota, 0)

    base_reg_ids = [window.window_id for window in reg_ranked[:reg_quota]]
    base_hot_ids = [window.window_id for window in hot_ranked[:hot_quota]]
    base_ids: set[str] = set(base_reg_ids).union(base_hot_ids)
    if len(base_ids) < base_size:
        for window in reg_ranked:
            if len(base_ids) >= base_size:
                break
            base_ids.add(window.window_id)

    fine_to_coarse: dict[int, int] = {}
    coarse_cursor = 0
    for fine in fine_segments:
        fine_idx = int(fine["index"])
        fine_start = float(fine["start"])
        fine_end = float(fine["end"])
        while coarse_cursor + 1 < len(coarse_segments) and fine_start >= float(coarse_segments[coarse_cursor]["end"]) - EPS:
            coarse_cursor += 1
        if coarse_cursor >= len(coarse_segments):
            continue
        coarse = coarse_segments[coarse_cursor]
        coarse_start = float(coarse["start"])
        coarse_end = float(coarse["end"])
        if fine_start + EPS < coarse_start or fine_end > coarse_end + EPS:
            continue
        fine_to_coarse[fine_idx] = int(coarse["index"])

    coarse_scores: dict[str, dict[int, float]] = defaultdict(lambda: defaultdict(float))
    coarse_rows = {
        int(segment["index"]): {
            "index": int(segment["index"]),
            "start": float(segment["start"]),
            "end": float(segment["end"]),
            "duration": float(segment["duration"]),
            "demand_mass": 0.0,
        }
        for segment in coarse_segments
    }
    for fine in fine_segments:
        fine_idx = int(fine["index"])
        coarse_idx = fine_to_coarse.get(fine_idx)
        if coarse_idx is None:
            continue
        coarse_rows[coarse_idx]["demand_mass"] += float(fine["duration"]) * float(fine.get("total_static_demand", 0.0))

    for window_id, entries in window_reg_segment_scores.items():
        for fine_idx, score in entries:
            coarse_idx = fine_to_coarse.get(int(fine_idx))
            if coarse_idx is None:
                continue
            coarse_scores[window_id][coarse_idx] += float(score)

    window_cover_segments = {
        window_id: {
            coarse_idx
            for coarse_idx, score in coarse_map.items()
            if float(score) > EPS
        }
        for window_id, coarse_map in coarse_scores.items()
    }

    positive_coarse = [
        coarse_idx
        for coarse_idx, row in coarse_rows.items()
        if float(row["demand_mass"]) > EPS
    ]

    base_counts: dict[int, int] = defaultdict(int)
    for window_id in base_ids:
        for coarse_idx in window_cover_segments.get(window_id, set()):
            base_counts[coarse_idx] += 1

    selected_ids = set(base_ids)
    additional_ids: list[str] = []
    current_counts: dict[int, int] = defaultdict(int, base_counts)
    segment_additions: dict[int, list[str]] = defaultdict(list)

    ranked_additions_by_segment = {
        coarse_idx: [
            window.window_id
            for window in _segment_local_rank(raw_windows, coarse_scores, reg_values, hot_values, coarse_idx)
            if window.window_id not in base_ids
        ]
        for coarse_idx in positive_coarse
    }
    priority_segments = sorted(
        positive_coarse,
        key=lambda coarse_idx: (
            0 if base_counts.get(coarse_idx, 0) == 0 else 1,
            -float(coarse_rows[coarse_idx]["demand_mass"]),
            float(coarse_rows[coarse_idx]["start"]),
            coarse_idx,
        ),
    )

    for coarse_idx in priority_segments:
        while current_counts.get(coarse_idx, 0) < min_per_segment:
            next_window_id = None
            for window_id in ranked_additions_by_segment.get(coarse_idx, []):
                if window_id in selected_ids:
                    continue
                next_window_id = window_id
                break
            if next_window_id is None or len(additional_ids) >= max_additional:
                break

            selected_ids.add(next_window_id)
            additional_ids.append(next_window_id)
            segment_additions[coarse_idx].append(next_window_id)
            for covered_idx in window_cover_segments.get(next_window_id, set()):
                current_counts[covered_idx] += 1

    selected = [windows_by_id[window_id] for window_id in selected_ids]
    selected.sort(key=lambda item: (item.start, item.end, item.window_id))

    coarse_segment_rows = []
    for coarse_idx in sorted(positive_coarse, key=lambda idx: (float(coarse_rows[idx]["start"]), idx)):
        coarse_segment_rows.append(
            {
                "index": coarse_idx,
                "start": float(coarse_rows[coarse_idx]["start"]),
                "end": float(coarse_rows[coarse_idx]["end"]),
                "demand_mass": float(coarse_rows[coarse_idx]["demand_mass"]),
                "base_coverage": int(base_counts.get(coarse_idx, 0)),
                "final_coverage": int(current_counts.get(coarse_idx, 0)),
                "target_coverage": int(min_per_segment),
                "added_window_count": len(segment_additions.get(coarse_idx, [])),
                "base_shortfall": max(0, int(min_per_segment) - int(base_counts.get(coarse_idx, 0))),
                "final_shortfall": max(0, int(min_per_segment) - int(current_counts.get(coarse_idx, 0))),
            }
        )

    scenario.metadata["stage1_screening"] = {
        "candidate_window_count_raw": raw_count,
        "candidate_window_count_screened": len(selected),
        "candidate_pool_size_limit": screen_cap,
        "screening_mode": "candidate_pool_v49",
        "candidate_pool_base_size": base_size,
        "candidate_pool_base_selected": len(base_ids),
        "candidate_pool_base_reg_quota": reg_quota,
        "candidate_pool_base_hot_quota": hot_quota,
        "candidate_pool_hot_channel_enabled": hot_channel_enabled,
        "candidate_pool_additional_limit": max_additional,
        "candidate_pool_additional_selected": len(additional_ids),
        "candidate_pool_min_per_coarse_segment": min_per_segment,
        "candidate_pool_coarse_segment_positive_demand_count": len(positive_coarse),
        "candidate_pool_coarse_segment_unmet_count": sum(
            1 for row in coarse_segment_rows if int(row["final_shortfall"]) > 0
        ),
        "candidate_pool_coarse_segments": coarse_segment_rows,
    }
    scenario.candidate_windows = selected
    return selected
