from __future__ import annotations

import json
from dataclasses import asdict
from itertools import islice, product
from pathlib import Path
from typing import Iterable

import networkx as nx

from .models import (
    CandidateWindow,
    CapacityConfig,
    GAConfig,
    HotspotInterval,
    HotspotRegion,
    PathCandidate,
    Scenario,
    Segment,
    Stage1Config,
    Stage2Config,
    Task,
    TemporalLink,
)

WEIGHT_EPS = 1e-9
LIGHT_SPEED_KM_PER_S = 299_792.458


def _float(value: object, name: str) -> float:
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be numeric, got {value!r}") from exc


def _optional_float(value: object, name: str) -> float | None:
    if value in {None, ""}:
        return None
    return _float(value, name)


def _required(data: dict, key: str):
    if key not in data:
        raise ValueError(f"Missing required field: {key}")
    return data[key]


def _runtime_cache(scenario: Scenario) -> dict:
    return scenario.metadata.setdefault("_runtime_cache", {})


def _normalized_stage1_weights(
    omega_fr: object,
    omega_cap: object,
    omega_hot: object,
) -> tuple[float, float, float]:
    values = [
        max(_float(omega_fr, "stage1.omega_fr"), 0.0),
        max(_float(omega_cap, "stage1.omega_cap"), 0.0),
        max(_float(omega_hot, "stage1.omega_hot"), 0.0),
    ]
    total = sum(values)
    if total <= WEIGHT_EPS:
        return (4.0 / 9.0, 3.0 / 9.0, 2.0 / 9.0)
    return tuple(value / total for value in values)


def _load_hotspots_a(payload: dict, node_domain: dict[str, str]) -> list[HotspotRegion]:
    hotspots_cfg = payload.get("hotspots")
    if not isinstance(hotspots_cfg, dict):
        hotspots_cfg = (payload.get("metadata") or {}).get("hotspots", {})
    regions_cfg = hotspots_cfg.get("A", []) if isinstance(hotspots_cfg, dict) else []
    if not isinstance(regions_cfg, list):
        raise ValueError("hotspots.A must be a list when provided")

    regions: list[HotspotRegion] = []
    raw_weights: list[float] = []
    for idx, item in enumerate(regions_cfg, start=1):
        if not isinstance(item, dict):
            raise ValueError("Each hotspot region must be an object")
        region_id = str(item.get("id", item.get("region_id", f"hotA_{idx}")))
        nodes = tuple(str(node) for node in item.get("nodes", []))
        intervals_cfg = item.get("intervals", item.get("coverage", []))
        intervals: list[HotspotInterval] = []
        if not isinstance(intervals_cfg, list):
            raise ValueError(f"hotspots.A[{idx}].intervals must be a list")
        for interval_idx, interval in enumerate(intervals_cfg, start=1):
            if not isinstance(interval, dict):
                raise ValueError(f"hotspots.A[{idx}].intervals[{interval_idx}] must be an object")
            interval_nodes = tuple(str(node) for node in interval.get("nodes", []))
            intervals.append(
                HotspotInterval(
                    start=_float(_required(interval, "start"), f"hotspots.A[{idx}].intervals[{interval_idx}].start"),
                    end=_float(_required(interval, "end"), f"hotspots.A[{idx}].intervals[{interval_idx}].end"),
                    nodes=interval_nodes,
                )
            )
        weight = _float(item.get("weight", 1.0), f"hotspots.A[{idx}].weight")
        regions.append(
            HotspotRegion(
                region_id=region_id,
                weight=weight,
                nodes=nodes,
                intervals=tuple(intervals),
            )
        )
        raw_weights.append(weight)

    if not regions:
        return []

    total_weight = sum(max(weight, 0.0) for weight in raw_weights)
    if total_weight <= WEIGHT_EPS:
        normalized = [1.0 / len(regions)] * len(regions)
    else:
        normalized = [max(weight, 0.0) / total_weight for weight in raw_weights]

    normalized_regions: list[HotspotRegion] = []
    for region, weight in zip(regions, normalized):
        normalized_regions.append(
            HotspotRegion(
                region_id=region.region_id,
                weight=weight,
                nodes=region.nodes,
                intervals=region.intervals,
            )
        )

    for region in normalized_regions:
        all_nodes = set(region.nodes)
        for interval in region.intervals:
            all_nodes.update(interval.nodes)
            if interval.start >= interval.end:
                raise ValueError(f"Hotspot interval in {region.region_id} must satisfy start < end")
        for node in all_nodes:
            if node not in node_domain:
                raise ValueError(f"Hotspot {region.region_id} uses unknown node {node}")
            if node_domain[node] != "A":
                raise ValueError(f"Hotspot {region.region_id} must only reference A-domain nodes")
    return normalized_regions


def load_scenario(path: str | Path) -> Scenario:
    payload = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    nodes_cfg = _required(payload, "nodes")
    capacities_cfg = _required(payload, "capacities")
    stage1_cfg = _required(payload, "stage1")
    stage2_cfg = payload.get("stage2", {})

    node_domain: dict[str, str] = {}
    for domain in ("A", "B"):
        for node in nodes_cfg.get(domain, []):
            node_domain[str(node)] = domain

    intra_links: list[TemporalLink] = []
    for item in payload.get("intra_domain_links", []):
        distance_km = _optional_float(item.get("distance_km"), "link.distance_km")
        raw_delay = item.get("delay")
        if raw_delay is not None:
            delay = _float(raw_delay, "link.delay")
        elif distance_km is not None:
            delay = distance_km / LIGHT_SPEED_KM_PER_S
        else:
            delay = 0.0
        raw_weight = item.get("weight")
        weight = _float(raw_weight, "link.weight") if raw_weight is not None else (delay if delay > 0.0 else 1.0)
        intra_links.append(
            TemporalLink(
                link_id=str(_required(item, "id")),
                u=str(_required(item, "u")),
                v=str(_required(item, "v")),
                domain=str(_required(item, "domain")),
                start=_float(_required(item, "start"), "link.start"),
                end=_float(_required(item, "end"), "link.end"),
                delay=delay,
                weight=weight,
                distance_km=distance_km,
            )
        )

    candidate_windows: list[CandidateWindow] = []
    for item in payload.get("candidate_windows", []):
        distance_km = _optional_float(item.get("distance_km"), "window.distance_km")
        raw_delay = item.get("delay")
        if raw_delay is not None:
            delay = _float(raw_delay, "window.delay")
        elif distance_km is not None:
            delay = distance_km / LIGHT_SPEED_KM_PER_S
        else:
            delay = 0.0
        candidate_windows.append(
            CandidateWindow(
                window_id=str(_required(item, "id")),
                a=str(_required(item, "a")),
                b=str(_required(item, "b")),
                start=_float(_required(item, "start"), "window.start"),
                end=_float(_required(item, "end"), "window.end"),
                value=float(item["value"]) if item.get("value") is not None else None,
                delay=delay,
                distance_km=distance_km,
            )
        )

    tasks = [
        Task(
            task_id=str(_required(item, "id")),
            src=str(_required(item, "src")),
            dst=str(_required(item, "dst")),
            arrival=_float(_required(item, "arrival"), "task.arrival"),
            deadline=_float(_required(item, "deadline"), "task.deadline"),
            data=_float(_required(item, "data"), "task.data"),
            weight=_float(_required(item, "weight"), "task.weight"),
            max_rate=_float(_required(item, "max_rate"), "task.max_rate"),
            task_type=str(_required(item, "type")),
            preemption_priority=_float(item.get("preemption_priority", item.get("weight", 1.0)), "task.preemption_priority"),
        )
        for item in payload.get("tasks", [])
    ]

    capacities = CapacityConfig(
        domain_a=_float(_required(capacities_cfg, "A"), "capacities.A"),
        domain_b=_float(_required(capacities_cfg, "B"), "capacities.B"),
        cross=_float(_required(capacities_cfg, "X"), "capacities.X"),
    )

    ga_cfg = stage1_cfg.get("ga", {})
    ga = GAConfig(
        population_size=int(ga_cfg.get("population_size", 60)),
        crossover_probability=_float(ga_cfg.get("crossover_probability", 0.9), "ga.crossover_probability"),
        mutation_probability=_float(ga_cfg.get("mutation_probability", 0.2), "ga.mutation_probability"),
        max_generations=int(ga_cfg.get("max_generations", 100)),
        stall_generations=int(ga_cfg.get("stall_generations", 20)),
        top_m=int(ga_cfg.get("top_m", 5)),
        max_runtime_seconds=(
            None
            if ga_cfg.get("max_runtime_seconds") in {None, "", 0, 0.0}
            else _float(ga_cfg.get("max_runtime_seconds"), "ga.max_runtime_seconds")
        ),
    )
    theta_cap_default = stage1_cfg.get("theta_cap", stage1_cfg.get("theta_eta0", 0.08))
    omega_fr, omega_cap, omega_hot = _normalized_stage1_weights(
        stage1_cfg.get("omega_fr", stage1_cfg.get("omega_sr", stage1_cfg.get("viol_weight_sr", 4.0 / 9.0))),
        stage1_cfg.get("omega_cap", stage1_cfg.get("viol_weight_cap", 3.0 / 9.0)),
        stage1_cfg.get("omega_hot", stage1_cfg.get("viol_weight_hot", 2.0 / 9.0)),
    )
    stage1 = Stage1Config(
        rho=_float(_required(stage1_cfg, "rho"), "stage1.rho"),
        t_pre=_float(_required(stage1_cfg, "t_pre"), "stage1.t_pre"),
        d_min=_float(_required(stage1_cfg, "d_min"), "stage1.d_min"),
        theta_cap=_float(theta_cap_default, "stage1.theta_cap"),
        theta_hot=_float(stage1_cfg.get("theta_hot", 0.80), "stage1.theta_hot"),
        hot_hop_limit=int(stage1_cfg.get("hot_hop_limit", stage1_cfg.get("H_A", 4))),
        bottleneck_factor_alpha=_float(
            stage1_cfg.get("bottleneck_factor_alpha", stage1_cfg.get("alpha", 0.85)),
            "stage1.bottleneck_factor_alpha",
        ),
        eta_x=_float(stage1_cfg.get("eta_x", 0.90), "stage1.eta_x"),
        static_value_snapshot_seconds=int(stage1_cfg.get("static_value_snapshot_seconds", 600)),
        candidate_pool_base_size=max(int(stage1_cfg.get("candidate_pool_base_size", 400)), 1),
        candidate_pool_hot_fraction=min(
            max(_float(stage1_cfg.get("candidate_pool_hot_fraction", 0.30), "stage1.candidate_pool_hot_fraction"), 0.0),
            1.0,
        ),
        candidate_pool_min_per_coarse_segment=max(int(stage1_cfg.get("candidate_pool_min_per_coarse_segment", 3)), 0),
        candidate_pool_max_additional=max(int(stage1_cfg.get("candidate_pool_max_additional", 150)), 0),
        q_eval=int(stage1_cfg.get("q_eval", 4)),
        omega_fr=omega_fr,
        omega_cap=omega_cap,
        omega_hot=omega_hot,
        elite_prune_count=int(stage1_cfg.get("elite_prune_count", ga.elite_count)),
        ga=ga,
    )
    stage2_k_paths = int(stage2_cfg.get("k_paths", 2))
    raw_label_keep_limit = stage2_cfg.get("label_keep_limit")
    raw_milp_mode = str(stage2_cfg.get("milp_mode", "rolling")).strip().lower()
    raw_milp_time_limit = stage2_cfg.get("milp_time_limit_seconds")
    raw_milp_relative_gap = stage2_cfg.get("milp_relative_gap")
    raw_milp_high_weight_threshold = stage2_cfg.get("milp_rolling_high_weight_threshold")
    stage2 = Stage2Config(
        k_paths=stage2_k_paths,
        completion_tolerance=_float(stage2_cfg.get("completion_tolerance", 1e-6), "stage2.completion_tolerance"),
        prefer_milp=bool(stage2_cfg.get("prefer_milp", True)),
        milp_mode=raw_milp_mode if raw_milp_mode in {"full", "rolling"} else "rolling",
        milp_horizon_segments=max(int(stage2_cfg.get("milp_horizon_segments", 16)), 1),
        milp_commit_segments=max(int(stage2_cfg.get("milp_commit_segments", 8)), 1),
        milp_rolling_path_limit=max(int(stage2_cfg.get("milp_rolling_path_limit", 1)), 1),
        milp_rolling_high_path_limit=max(int(stage2_cfg.get("milp_rolling_high_path_limit", 2)), 1),
        milp_rolling_high_weight_threshold=(
            None
            if raw_milp_high_weight_threshold in {None, ""}
            else _float(raw_milp_high_weight_threshold, "stage2.milp_rolling_high_weight_threshold")
        ),
        milp_rolling_high_competition_task_threshold=max(int(stage2_cfg.get("milp_rolling_high_competition_task_threshold", 8)), 1),
        milp_rolling_promoted_tasks_per_segment=max(int(stage2_cfg.get("milp_rolling_promoted_tasks_per_segment", 2)), 0),
        milp_time_limit_seconds=(
            None
            if raw_milp_time_limit in {None, "", 0, 0.0}
            else _float(raw_milp_time_limit, "stage2.milp_time_limit_seconds")
        ),
        milp_relative_gap=(
            None
            if raw_milp_relative_gap in {None, ""}
            else _float(raw_milp_relative_gap, "stage2.milp_relative_gap")
        ),
        label_keep_limit=(int(raw_label_keep_limit) if raw_label_keep_limit not in {None, ""} else None),
    )

    planning_end = _float(
        payload.get(
            "planning_end",
            max(
                [0.0]
                + [task.deadline for task in tasks]
                + [window.end for window in candidate_windows]
                + [link.end for link in intra_links]
            ),
        ),
        "planning_end",
    )

    hotspots_a = _load_hotspots_a(payload, node_domain)
    scenario = Scenario(
        node_domain=node_domain,
        intra_links=intra_links,
        candidate_windows=candidate_windows,
        tasks=tasks,
        capacities=capacities,
        stage1=stage1,
        stage2=stage2,
        planning_end=planning_end,
        hotspots_a=hotspots_a,
        metadata=payload.get("metadata", {}),
    )
    validate_scenario(scenario)
    return scenario


def validate_scenario(scenario: Scenario) -> None:
    for link in scenario.intra_links:
        if link.u not in scenario.node_domain or link.v not in scenario.node_domain:
            raise ValueError(f"Intra-domain link {link.link_id} uses unknown nodes")
        if scenario.node_domain[link.u] != link.domain or scenario.node_domain[link.v] != link.domain:
            raise ValueError(f"Intra-domain link {link.link_id} endpoints must both lie in domain {link.domain}")
        if link.start >= link.end:
            raise ValueError(f"Intra-domain link {link.link_id} must satisfy start < end")

    for window in scenario.candidate_windows:
        if window.a not in scenario.node_domain or window.b not in scenario.node_domain:
            raise ValueError(f"Window {window.window_id} uses unknown nodes")
        if scenario.node_domain[window.a] != "A" or scenario.node_domain[window.b] != "B":
            raise ValueError(f"Window {window.window_id} must connect A-domain node to B-domain node")
        if window.start >= window.end:
            raise ValueError(f"Window {window.window_id} must satisfy start < end")

    for task in scenario.tasks:
        if task.src not in scenario.node_domain or task.dst not in scenario.node_domain:
            raise ValueError(f"Task {task.task_id} uses unknown nodes")
        if task.arrival >= task.deadline:
            raise ValueError(f"Task {task.task_id} must satisfy arrival < deadline")
        if task.data <= 0 or task.max_rate <= 0 or task.weight <= 0:
            raise ValueError(f"Task {task.task_id} data/max_rate/weight must be positive")
        if task.task_type not in {"reg", "emg"}:
            raise ValueError(f"Task {task.task_id} type must be reg or emg")

    total_hot_weight = sum(region.weight for region in scenario.hotspots_a)
    if scenario.hotspots_a and abs(total_hot_weight - 1.0) > 1e-6:
        raise ValueError("A-domain hotspot weights must sum to 1")
    total_stage1_weight = scenario.stage1.omega_fr + scenario.stage1.omega_cap + scenario.stage1.omega_hot
    if abs(total_stage1_weight - 1.0) > 1e-6:
        raise ValueError("Stage1 omega weights must sum to 1")


def build_segments(
    scenario: Scenario,
    scheduled_windows: Iterable,
    tasks: Iterable[Task] | None = None,
) -> list[Segment]:
    task_list = list(tasks) if tasks is not None else list(scenario.tasks)
    times = {0.0, float(scenario.planning_end)}
    for task in task_list:
        times.add(task.arrival)
        times.add(task.deadline)
    for window in scheduled_windows:
        times.add(window.on)
        times.add(window.off)
    for link in scenario.intra_links:
        times.add(link.start)
        times.add(link.end)
    ordered = sorted(value for value in times if 0.0 <= value <= scenario.planning_end)
    segments: list[Segment] = []
    for idx, (start, end) in enumerate(zip(ordered, ordered[1:])):
        if end > start:
            segments.append(Segment(index=idx, start=start, end=end))
    return segments


def active_intra_links(scenario: Scenario, domain: str, time_point: float) -> list[TemporalLink]:
    cache = _runtime_cache(scenario).setdefault("active_intra_links", {})
    key = (domain, float(time_point))
    if key not in cache:
        cache[key] = [
            link
            for link in scenario.intra_links
            if link.domain == domain and link.start <= time_point < link.end
        ]
    return cache[key]


def active_cross_links(scheduled_windows: Iterable, time_point: float):
    return [window for window in scheduled_windows if window.on <= time_point < window.off]


def build_domain_graph(scenario: Scenario, domain: str, time_point: float) -> nx.Graph:
    cache = _runtime_cache(scenario).setdefault("domain_graph", {})
    key = (domain, float(time_point))
    if key not in cache:
        graph = nx.Graph()
        all_unit_weight = True
        for node, node_domain in scenario.node_domain.items():
            if node_domain == domain:
                graph.add_node(node)
        for link in active_intra_links(scenario, domain, time_point):
            if abs(float(link.weight) - 1.0) > WEIGHT_EPS:
                all_unit_weight = False
            graph.add_edge(
                link.u,
                link.v,
                edge_id=link.link_id,
                delay=link.delay,
                weight=link.weight,
                distance_km=link.distance_km,
            )
        graph.graph["all_unit_weight"] = all_unit_weight
        cache[key] = graph
    return cache[key]


def _node_path_to_edge_ids(graph: nx.Graph, nodes: list[str]) -> tuple[list[str], float]:
    edge_ids: list[str] = []
    delay = 0.0
    for u, v in zip(nodes, nodes[1:]):
        data = graph[u][v]
        edge_ids.append(str(data["edge_id"]))
        delay += float(data.get("delay", 0.0))
    return edge_ids, delay


def single_source_domain_paths(
    scenario: Scenario,
    domain: str,
    src: str,
    time_point: float,
) -> dict[str, tuple[tuple[str, ...], tuple[str, ...], float]]:
    cache = _runtime_cache(scenario).setdefault("single_source_domain_paths", {})
    key = (domain, src, float(time_point))
    cached = cache.get(key)
    if cached is not None:
        return cached

    graph = build_domain_graph(scenario, domain, time_point)
    if src not in graph:
        cache[key] = {}
        return cache[key]

    if graph.graph.get("all_unit_weight", False):
        node_paths = nx.single_source_shortest_path(graph, src)
        results = {}
        for dst, path_nodes in node_paths.items():
            nodes = tuple(path_nodes)
            edge_ids, delay = _node_path_to_edge_ids(graph, list(nodes))
            results[str(dst)] = (nodes, tuple(edge_ids), delay)
        cache[key] = results
        return cache[key]

    lengths, node_paths = nx.single_source_dijkstra(graph, src, weight="weight")
    results = {}
    for dst, path_nodes in node_paths.items():
        nodes = tuple(path_nodes)
        edge_ids, delay = _node_path_to_edge_ids(graph, list(nodes))
        results[str(dst)] = (nodes, tuple(edge_ids), float(lengths[dst]) if dst in lengths else delay)
    cache[key] = results
    return cache[key]


def k_shortest_domain_paths(
    scenario: Scenario,
    domain: str,
    src: str,
    dst: str,
    time_point: float,
    k: int,
) -> list[tuple[tuple[str, ...], tuple[str, ...], float]]:
    cache = _runtime_cache(scenario).setdefault("k_shortest_domain_paths", {})
    key = (domain, src, dst, float(time_point), int(k))
    if key in cache:
        return cache[key]

    if src == dst:
        cache[key] = [((src,), tuple(), 0.0)]
        return cache[key]

    graph = build_domain_graph(scenario, domain, time_point)
    if src not in graph or dst not in graph:
        cache[key] = []
        return cache[key]

    try:
        if max(k, 1) == 1:
            if graph.graph.get("all_unit_weight", False):
                nodes = tuple(nx.shortest_path(graph, src, dst))
            else:
                nodes = tuple(nx.shortest_path(graph, src, dst, weight="weight"))
            edge_ids, delay = _node_path_to_edge_ids(graph, list(nodes))
            cache[key] = [(nodes, tuple(edge_ids), delay)]
            return cache[key]

        iterator = nx.shortest_simple_paths(graph, src, dst, weight="weight")
        results: list[tuple[tuple[str, ...], tuple[str, ...], float]] = []
        for nodes in islice(iterator, max(k, 1)):
            edge_ids, delay = _node_path_to_edge_ids(graph, list(nodes))
            results.append((tuple(nodes), tuple(edge_ids), delay))
        cache[key] = results
        return cache[key]
    except (nx.NetworkXNoPath, nx.NodeNotFound):
        cache[key] = []
        return cache[key]


RAW_SHORTEST_POOL_SIZE = 5
LOW_OVERLAP_THRESHOLD = 0.5


def _path_overlap_ratio(lhs_edge_ids: tuple[str, ...], rhs_edge_ids: tuple[str, ...]) -> float:
    if not lhs_edge_ids or not rhs_edge_ids:
        return 0.0
    overlap = len(set(lhs_edge_ids).intersection(rhs_edge_ids))
    return overlap / max(min(len(lhs_edge_ids), len(rhs_edge_ids)), 1)


def diverse_domain_paths(
    scenario: Scenario,
    domain: str,
    src: str,
    dst: str,
    time_point: float,
    k: int,
    raw_pool_size: int = RAW_SHORTEST_POOL_SIZE,
    overlap_threshold: float = LOW_OVERLAP_THRESHOLD,
) -> list[tuple[tuple[str, ...], tuple[str, ...], float]]:
    keep_count = max(int(k), 1)
    raw_count = max(keep_count, int(raw_pool_size), 1)
    raw_paths = k_shortest_domain_paths(scenario, domain, src, dst, time_point, raw_count)
    if keep_count <= 1 or len(raw_paths) <= 1:
        return raw_paths[:keep_count]

    selected = [raw_paths[0]]
    remaining = raw_paths[1:]
    while remaining and len(selected) < keep_count:
        eligible = [
            candidate
            for candidate in remaining
            if max(_path_overlap_ratio(candidate[1], chosen[1]) for chosen in selected) <= overlap_threshold
        ]
        if eligible:
            chosen = eligible[0]
        else:
            chosen = min(
                remaining,
                key=lambda candidate: (
                    max(_path_overlap_ratio(candidate[1], chosen[1]) for chosen in selected),
                    sum(_path_overlap_ratio(candidate[1], chosen[1]) for chosen in selected),
                    candidate[2],
                    len(candidate[1]),
                ),
            )
        selected.append(chosen)
        remaining.remove(chosen)
    return selected


def generate_candidate_paths(
    scenario: Scenario,
    scheduled_windows: list,
    task: Task,
    segment: Segment,
    k_per_side: int,
    active_windows: list | None = None,
) -> list[PathCandidate]:
    src_domain = scenario.node_domain[task.src]
    dst_domain = scenario.node_domain[task.dst]
    paths: list[PathCandidate] = []
    if max(k_per_side, 1) == 1:
        if src_domain == dst_domain:
            shortest_map = single_source_domain_paths(scenario, src_domain, task.src, segment.start)
            shortest = shortest_map.get(task.dst)
            if shortest is None:
                return paths
            nodes, edge_ids, delay = shortest
            hop = len(edge_ids)
            hop_a = hop if src_domain == "A" else 0
            hop_b = hop if src_domain == "B" else 0
            if segment.start + delay < task.deadline:
                paths.append(
                    PathCandidate(
                        path_id=f"{task.task_id}:{segment.index}:same:0",
                        nodes=nodes,
                        edge_ids=edge_ids,
                        hop_count=hop,
                        delay=delay,
                        hop_a=hop_a,
                        hop_b=hop_b,
                    )
                )
            return paths

        active_windows = active_cross_links(scheduled_windows, segment.start) if active_windows is None else active_windows
        if not active_windows:
            return paths

        if src_domain == "A" and dst_domain == "B":
            left_map = single_source_domain_paths(scenario, "A", task.src, segment.start)
            right_map = single_source_domain_paths(scenario, "B", task.dst, segment.start)
            for window in active_windows:
                left = left_map.get(window.a)
                right = right_map.get(window.b)
                if left is None or right is None:
                    continue
                left_nodes, left_edges, left_delay = left
                right_nodes_rev, right_edges_rev, right_delay = right
                nodes = left_nodes + (window.b,) + tuple(reversed(right_nodes_rev[:-1]))
                edge_ids = left_edges + (window.window_id,) + tuple(reversed(right_edges_rev))
                delay = left_delay + float(window.delay) + right_delay
                if segment.start + delay >= task.deadline:
                    continue
                paths.append(
                    PathCandidate(
                        path_id=f"{task.task_id}:{segment.index}:{window.window_id}:0:0",
                        nodes=nodes,
                        edge_ids=edge_ids,
                        hop_count=len(edge_ids),
                        delay=delay,
                        cross_window_id=window.window_id,
                        hop_a=len(left_edges),
                        hop_b=len(right_edges_rev),
                    )
                )
            return paths

        if src_domain == "B" and dst_domain == "A":
            left_map = single_source_domain_paths(scenario, "B", task.src, segment.start)
            right_map = single_source_domain_paths(scenario, "A", task.dst, segment.start)
            for window in active_windows:
                left = left_map.get(window.b)
                right = right_map.get(window.a)
                if left is None or right is None:
                    continue
                left_nodes, left_edges, left_delay = left
                right_nodes_rev, right_edges_rev, right_delay = right
                nodes = left_nodes + (window.a,) + tuple(reversed(right_nodes_rev[:-1]))
                edge_ids = left_edges + (window.window_id,) + tuple(reversed(right_edges_rev))
                delay = left_delay + float(window.delay) + right_delay
                if segment.start + delay >= task.deadline:
                    continue
                paths.append(
                    PathCandidate(
                        path_id=f"{task.task_id}:{segment.index}:{window.window_id}:0:0",
                        nodes=nodes,
                        edge_ids=edge_ids,
                        hop_count=len(edge_ids),
                        delay=delay,
                        cross_window_id=window.window_id,
                        hop_a=len(right_edges_rev),
                        hop_b=len(left_edges),
                    )
                )
            return paths

    if src_domain == dst_domain:
        for idx, (nodes, edge_ids, delay) in enumerate(
            diverse_domain_paths(scenario, src_domain, task.src, task.dst, segment.start, max(k_per_side, 1))
        ):
            hop = len(edge_ids)
            hop_a = hop if src_domain == "A" else 0
            hop_b = hop if src_domain == "B" else 0
            if segment.start + delay < task.deadline:
                paths.append(
                    PathCandidate(
                        path_id=f"{task.task_id}:{segment.index}:same:{idx}",
                        nodes=nodes,
                        edge_ids=edge_ids,
                        hop_count=hop,
                        delay=delay,
                        hop_a=hop_a,
                        hop_b=hop_b,
                    )
                )
        return paths

    active_windows = active_cross_links(scheduled_windows, segment.start) if active_windows is None else active_windows
    for window in active_windows:
        if src_domain == "A" and dst_domain == "B":
            left = diverse_domain_paths(scenario, "A", task.src, window.a, segment.start, max(k_per_side, 1))
            right = diverse_domain_paths(scenario, "B", window.b, task.dst, segment.start, max(k_per_side, 1))
            cross_nodes = (window.a, window.b)
            hop_builder = lambda left_edges, right_edges: (len(left_edges), len(right_edges))
        elif src_domain == "B" and dst_domain == "A":
            left = diverse_domain_paths(scenario, "B", task.src, window.b, segment.start, max(k_per_side, 1))
            right = diverse_domain_paths(scenario, "A", window.a, task.dst, segment.start, max(k_per_side, 1))
            cross_nodes = (window.b, window.a)
            hop_builder = lambda left_edges, right_edges: (len(right_edges), len(left_edges))
        else:
            continue

        for left_idx, right_idx in product(range(len(left)), range(len(right))):
            left_nodes, left_edges, left_delay = left[left_idx]
            right_nodes, right_edges, right_delay = right[right_idx]
            nodes = left_nodes + cross_nodes[1:] + right_nodes[1:]
            edge_ids = left_edges + (window.window_id,) + right_edges
            delay = left_delay + float(window.delay) + right_delay
            if segment.start + delay >= task.deadline:
                continue
            hop_a, hop_b = hop_builder(left_edges, right_edges)
            paths.append(
                PathCandidate(
                    path_id=f"{task.task_id}:{segment.index}:{window.window_id}:{left_idx}:{right_idx}",
                    nodes=nodes,
                    edge_ids=edge_ids,
                    hop_count=len(edge_ids),
                    delay=delay,
                    cross_window_id=window.window_id,
                    hop_a=hop_a,
                    hop_b=hop_b,
                )
            )
    return paths


def scenario_to_dict(scenario: Scenario) -> dict:
    metadata = dict(scenario.metadata)
    payload = {
        "metadata": metadata,
        "planning_end": scenario.planning_end,
        "nodes": scenario.domain_nodes,
        "capacities": {
            "A": scenario.capacities.domain_a,
            "B": scenario.capacities.domain_b,
            "X": scenario.capacities.cross,
        },
        "stage1": {
            "rho": scenario.stage1.rho,
            "t_pre": scenario.stage1.t_pre,
            "d_min": scenario.stage1.d_min,
            "theta_cap": scenario.stage1.theta_cap,
            "theta_hot": scenario.stage1.theta_hot,
            "hot_hop_limit": scenario.stage1.hot_hop_limit,
            "bottleneck_factor_alpha": scenario.stage1.bottleneck_factor_alpha,
            "eta_x": scenario.stage1.eta_x,
            "static_value_snapshot_seconds": scenario.stage1.static_value_snapshot_seconds,
            "candidate_pool_base_size": scenario.stage1.candidate_pool_base_size,
            "candidate_pool_hot_fraction": scenario.stage1.candidate_pool_hot_fraction,
            "candidate_pool_min_per_coarse_segment": scenario.stage1.candidate_pool_min_per_coarse_segment,
            "candidate_pool_max_additional": scenario.stage1.candidate_pool_max_additional,
            "q_eval": scenario.stage1.q_eval,
            "omega_fr": scenario.stage1.omega_fr,
            "omega_cap": scenario.stage1.omega_cap,
            "omega_hot": scenario.stage1.omega_hot,
            "elite_prune_count": scenario.stage1.elite_prune_count,
            "ga": asdict(scenario.stage1.ga),
        },
        "stage2": {
            "k_paths": scenario.stage2.k_paths,
            "completion_tolerance": scenario.stage2.completion_tolerance,
            "prefer_milp": scenario.stage2.prefer_milp,
            "milp_mode": scenario.stage2.milp_mode,
            "milp_horizon_segments": scenario.stage2.milp_horizon_segments,
            "milp_commit_segments": scenario.stage2.milp_commit_segments,
            "milp_rolling_path_limit": scenario.stage2.milp_rolling_path_limit,
            "milp_rolling_high_path_limit": scenario.stage2.milp_rolling_high_path_limit,
            "milp_rolling_high_weight_threshold": scenario.stage2.milp_rolling_high_weight_threshold,
            "milp_rolling_high_competition_task_threshold": scenario.stage2.milp_rolling_high_competition_task_threshold,
            "milp_rolling_promoted_tasks_per_segment": scenario.stage2.milp_rolling_promoted_tasks_per_segment,
            "milp_time_limit_seconds": scenario.stage2.milp_time_limit_seconds,
            "milp_relative_gap": scenario.stage2.milp_relative_gap,
            "label_keep_limit": scenario.stage2.label_keep_limit,
        },
        "hotspots": {
            "A": [
                {
                    "id": region.region_id,
                    "weight": region.weight,
                    "nodes": list(region.nodes),
                    "intervals": [
                        {
                            "start": interval.start,
                            "end": interval.end,
                            "nodes": list(interval.nodes),
                        }
                        for interval in region.intervals
                    ],
                }
                for region in scenario.hotspots_a
            ]
        },
        "intra_domain_links": [
            {
                "id": link.link_id,
                "u": link.u,
                "v": link.v,
                "domain": link.domain,
                "start": link.start,
                "end": link.end,
                "delay": link.delay,
                "weight": link.weight,
                "distance_km": link.distance_km,
            }
            for link in scenario.intra_links
        ],
        "candidate_windows": [
            {
                "id": window.window_id,
                "a": window.a,
                "b": window.b,
                "start": window.start,
                "end": window.end,
                "value": window.value,
                "delay": window.delay,
                "distance_km": window.distance_km,
            }
            for window in scenario.candidate_windows
        ],
        "tasks": [
            {
                "id": task.task_id,
                "src": task.src,
                "dst": task.dst,
                "arrival": task.arrival,
                "deadline": task.deadline,
                "data": task.data,
                "weight": task.weight,
                "max_rate": task.max_rate,
                "type": task.task_type,
                "preemption_priority": task.preemption_priority,
            }
            for task in scenario.tasks
        ],
    }
    return payload





