from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass

import networkx as nx

from .models import CandidateWindow, HotspotRegion, Scenario
from .scenario import build_domain_graph

EPS = 1e-9


@dataclass(frozen=True)
class StaticValueSegment:
    index: int
    start: float
    end: float

    @property
    def duration(self) -> float:
        return self.end - self.start


def _cross_regular_tasks(scenario: Scenario):
    return [
        task
        for task in scenario.tasks
        if task.task_type == "reg" and scenario.node_domain[task.src] != scenario.node_domain[task.dst]
    ]


def _static_value_segments(
    scenario: Scenario,
    regular_tasks,
    snapshot_seconds: int,
) -> list[StaticValueSegment]:
    step = max(int(snapshot_seconds), 1)
    times = {0.0, float(scenario.planning_end)}

    for task in regular_tasks:
        times.add(float(task.arrival))
        times.add(float(task.deadline))

    current = 0.0
    while current < scenario.planning_end - EPS:
        times.add(current)
        current = min(scenario.planning_end, current + step)
    times.add(float(scenario.planning_end))

    ordered = sorted(value for value in times if 0.0 <= value <= scenario.planning_end)
    return [
        StaticValueSegment(index=index, start=start, end=end)
        for index, (start, end) in enumerate(zip(ordered, ordered[1:]))
        if end > start + EPS
    ]


def _coarse_segments(scenario: Scenario, regular_tasks) -> list[StaticValueSegment]:
    times = {0.0, float(scenario.planning_end)}
    for task in regular_tasks:
        times.add(float(task.arrival))
        times.add(float(task.deadline))

    ordered = sorted(value for value in times if 0.0 <= value <= scenario.planning_end)
    return [
        StaticValueSegment(index=index, start=start, end=end)
        for index, (start, end) in enumerate(zip(ordered, ordered[1:]))
        if end > start + EPS
    ]


def _hop_lengths(graph: nx.Graph, sources: set[str]) -> dict[str, dict[str, int]]:
    results: dict[str, dict[str, int]] = {}
    for source in sources:
        if source not in graph:
            results[source] = {}
            continue
        results[source] = dict(nx.single_source_shortest_path_length(graph, source))
    return results


def _static_demand(task) -> float:
    return min(float(task.max_rate), float(task.data) / max(float(task.deadline) - float(task.arrival), EPS))


def _window_overlap(window: CandidateWindow, segment: StaticValueSegment) -> float:
    return max(0.0, min(float(window.end), float(segment.end)) - max(float(window.start), float(segment.start)))


def _candidate_runtime_cache(scenario: Scenario) -> dict:
    return scenario.metadata.setdefault("_runtime_cache", {}).setdefault("stage1_static_value", {})


def compute_candidate_static_details(
    scenario: Scenario,
    snapshot_seconds: int | None = None,
) -> dict[str, object]:
    windows = list(scenario.candidate_windows)
    reg_values = {window.window_id: 0.0 for window in windows}
    hot_values = {window.window_id: 0.0 for window in windows}
    if not windows:
        scenario.metadata.setdefault("stage1_static_value", {})
        scenario.metadata["stage1_static_value"].update(
            {
                "mode": "not_used",
                "window_value_field": "V_reg",
                "hotspot_channel_enabled": bool(scenario.hotspots_a),
                "segment_count": 0,
            }
        )
        return {
            "reg_values": reg_values,
            "hot_values": hot_values,
            "fine_segments": [],
            "coarse_segments": [],
            "window_reg_segment_scores": {},
        }

    regular_tasks = _cross_regular_tasks(scenario)
    snapshot_seconds = (
        scenario.stage1.static_value_snapshot_seconds if snapshot_seconds is None else snapshot_seconds
    )
    fine_segments = _static_value_segments(scenario, regular_tasks, snapshot_seconds)
    coarse_segments = _coarse_segments(scenario, regular_tasks)

    fine_segment_rows: list[dict[str, float | int]] = []
    reg_segment_scores: dict[str, list[tuple[int, float]]] = defaultdict(list)
    hotspot_reach_cache: dict[tuple[float, tuple[str, ...], int], frozenset[str]] = {}
    hop_limit = int(scenario.stage1.hot_hop_limit)
    hotspots: list[HotspotRegion] = list(scenario.hotspots_a)

    for segment in fine_segments:
        overlaps = {
            window.window_id: overlap
            for window in windows
            if (overlap := _window_overlap(window, segment)) > EPS
        }
        active_windows = [window for window in windows if window.window_id in overlaps]
        active_tasks = [
            task
            for task in regular_tasks
            if float(task.arrival) <= float(segment.start) < float(task.deadline)
        ]
        total_static_demand = sum(float(task.weight) * _static_demand(task) for task in active_tasks)
        fine_segment_rows.append(
            {
                "index": segment.index,
                "start": float(segment.start),
                "end": float(segment.end),
                "duration": float(segment.duration),
                "active_task_count": len(active_tasks),
                "total_static_demand": float(total_static_demand),
            }
        )
        if not active_windows:
            continue

        graph_a = build_domain_graph(scenario, "A", segment.start)
        graph_b = build_domain_graph(scenario, "B", segment.start)
        gateways_a = {window.a for window in active_windows}
        gateways_b = {window.b for window in active_windows}

        sources_a = set(gateways_a)
        sources_b = set(gateways_b)
        for task in active_tasks:
            src_domain = scenario.node_domain[task.src]
            dst_domain = scenario.node_domain[task.dst]
            if src_domain == "A":
                sources_a.add(task.src)
            else:
                sources_b.add(task.src)
            if dst_domain == "A":
                sources_a.add(task.dst)
            else:
                sources_b.add(task.dst)

        hop_from_a = _hop_lengths(graph_a, sources_a)
        hop_from_b = _hop_lengths(graph_b, sources_b)

        pair_pressure: dict[tuple[str, str], float] = defaultdict(float)
        for task in active_tasks:
            demand = _static_demand(task)
            if demand <= EPS:
                continue
            task_weight = float(task.weight) * demand
            src_domain = scenario.node_domain[task.src]
            dst_domain = scenario.node_domain[task.dst]

            if src_domain == "A" and dst_domain == "B":
                hops_a = hop_from_a.get(task.src, {})
                hops_b = hop_from_b.get(task.dst, {})
                for window in active_windows:
                    hop_a = hops_a.get(window.a)
                    hop_b = hops_b.get(window.b)
                    if hop_a is None or hop_b is None:
                        continue
                    accessibility = 1.0 / (1.0 + float(hop_a) + float(hop_b))
                    pair_pressure[(window.a, window.b)] += task_weight * accessibility
            elif src_domain == "B" and dst_domain == "A":
                hops_a = hop_from_a.get(task.dst, {})
                hops_b = hop_from_b.get(task.src, {})
                for window in active_windows:
                    hop_a = hops_a.get(window.a)
                    hop_b = hops_b.get(window.b)
                    if hop_a is None or hop_b is None:
                        continue
                    accessibility = 1.0 / (1.0 + float(hop_a) + float(hop_b))
                    pair_pressure[(window.a, window.b)] += task_weight * accessibility

        for window in active_windows:
            reg_score = overlaps[window.window_id] * pair_pressure.get((window.a, window.b), 0.0)
            if reg_score <= EPS:
                continue
            reg_values[window.window_id] += reg_score
            reg_segment_scores[window.window_id].append((segment.index, reg_score))

        if not hotspots:
            continue

        hot_segment_scores: dict[str, float] = defaultdict(float)
        for region in hotspots:
            nodes = tuple(sorted(region.active_nodes(segment.start)))
            if not nodes:
                continue
            cache_key = (float(segment.start), nodes, hop_limit)
            reachable_nodes = hotspot_reach_cache.get(cache_key)
            if reachable_nodes is None:
                reachable: set[str] = set()
                for node in nodes:
                    if node not in graph_a:
                        continue
                    hop_map = nx.single_source_shortest_path_length(graph_a, node, cutoff=hop_limit)
                    reachable.update(hop_map)
                reachable_nodes = frozenset(reachable)
                hotspot_reach_cache[cache_key] = reachable_nodes

            covered_windows = [
                window
                for window in active_windows
                if window.a in reachable_nodes and overlaps[window.window_id] > EPS
            ]
            if not covered_windows:
                continue
            scarcity_weight = float(region.weight) / (len(covered_windows) + EPS)
            for window in covered_windows:
                hot_segment_scores[window.window_id] += overlaps[window.window_id] * scarcity_weight

        for window_id, score in hot_segment_scores.items():
            if score > EPS:
                hot_values[window_id] += score

    runtime_details = {
        "reg_values": reg_values,
        "hot_values": hot_values,
        "fine_segments": fine_segment_rows,
        "coarse_segments": [
            {
                "index": segment.index,
                "start": float(segment.start),
                "end": float(segment.end),
                "duration": float(segment.duration),
            }
            for segment in coarse_segments
        ],
        "window_reg_segment_scores": {
            window_id: scores
            for window_id, scores in reg_segment_scores.items()
            if scores
        },
    }
    _candidate_runtime_cache(scenario).clear()
    _candidate_runtime_cache(scenario).update(runtime_details)

    scenario.metadata.setdefault("stage1_static_value", {})
    scenario.metadata["stage1_static_value"].update(
        {
            "mode": "reg_hot_candidate_pool_v49",
            "snapshot_seconds": snapshot_seconds,
            "segment_count": len(fine_segments),
            "coarse_segment_count": len(coarse_segments),
            "hotspot_channel_enabled": bool(hotspots),
            "window_value_field": "V_reg",
        }
    )
    return runtime_details


def compute_candidate_static_values(
    scenario: Scenario,
    snapshot_seconds: int | None = None,
) -> dict[str, float]:
    details = compute_candidate_static_details(scenario, snapshot_seconds=snapshot_seconds)
    return dict(details["reg_values"])


def annotate_scenario_candidate_values(
    scenario: Scenario,
    snapshot_seconds: int | None = None,
    force: bool = False,
) -> dict[str, float]:
    runtime_details = _candidate_runtime_cache(scenario)
    if (
        not force
        and scenario.candidate_windows
        and all(window.value is not None for window in scenario.candidate_windows)
        and runtime_details.get("reg_values") is not None
    ):
        return {window.window_id: float(window.value or 0.0) for window in scenario.candidate_windows}

    details = compute_candidate_static_details(
        scenario,
        snapshot_seconds=snapshot_seconds,
    )
    reg_values = dict(details["reg_values"])
    scenario.candidate_windows = [
        CandidateWindow(
            window_id=window.window_id,
            a=window.a,
            b=window.b,
            start=window.start,
            end=window.end,
            value=reg_values.get(window.window_id, 0.0),
            delay=window.delay,
            distance_km=window.distance_km,
        )
        for window in scenario.candidate_windows
    ]
    scenario.metadata.setdefault("stage1_static_value", {})
    scenario.metadata["stage1_static_value"]["snapshot_seconds"] = (
        scenario.stage1.static_value_snapshot_seconds if snapshot_seconds is None else snapshot_seconds
    )
    return reg_values
