from __future__ import annotations

import argparse
import csv
import json
import math
import re
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import networkx as nx

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from bs3.stk_access_preprocess import ConstellationSpec, circular_distance, parse_satellite


DATE_FMT = "%d %b %Y %H:%M:%S.%f"
HEADER_RE = re.compile(r"^Satellite-(.*):\s+Inertial Position & Velocity\s*$")
TIME_HEADER = "Time (UTCG)"
ROW_RE = re.compile(
    r"^\s*(\d{1,2}\s+\w{3}\s+\d{4}\s+\d{2}:\d{2}:\d{2}\.\d{3})\s+"
    r"(-?\d+(?:\.\d+)?)\s+"
    r"(-?\d+(?:\.\d+)?)\s+"
    r"(-?\d+(?:\.\d+)?)\s+"
    r"(-?\d+(?:\.\d+)?)\s+"
    r"(-?\d+(?:\.\d+)?)\s+"
    r"(-?\d+(?:\.\d+)?)\s*$"
)


@dataclass(frozen=True)
class PositionRow:
    time: datetime
    x: float
    y: float
    z: float


def parse_multisat_position_file(path: Path) -> tuple[list[str], dict[str, list[PositionRow]]]:
    lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
    if len(lines) < 2:
        raise ValueError("Position file is too short")

    header_match = HEADER_RE.match(lines[1].strip())
    if not header_match:
        raise ValueError("Cannot find satellite list header")

    raw_names = [part.strip() for part in header_match.group(1).split(",")]
    satellite_names = [name for name in raw_names if name.startswith("Satellite")]
    if not satellite_names:
        raise ValueError("No satellite names found in header")

    blocks: list[list[PositionRow]] = []
    current_rows: list[PositionRow] = []
    in_block = False

    for raw_line in lines[2:]:
        stripped = raw_line.strip()
        if not stripped:
            continue
        if TIME_HEADER in raw_line:
            if in_block and current_rows:
                blocks.append(current_rows)
                current_rows = []
            in_block = True
            continue
        if not in_block or stripped.startswith("-"):
            continue
        match = ROW_RE.match(raw_line)
        if not match:
            continue
        current_rows.append(
            PositionRow(
                time=datetime.strptime(match.group(1), DATE_FMT),
                x=float(match.group(2)),
                y=float(match.group(3)),
                z=float(match.group(4)),
            )
        )

    if current_rows:
        blocks.append(current_rows)

    if len(blocks) != len(satellite_names):
        raise ValueError(
            f"Block count {len(blocks)} does not match satellite count {len(satellite_names)}"
        )

    positions = {satellite_names[idx]: rows for idx, rows in enumerate(blocks)}
    sample_count = len(next(iter(positions.values())))
    for name, rows in positions.items():
        if len(rows) != sample_count:
            raise ValueError(f"Satellite {name} has inconsistent sample count")
    return satellite_names, positions


def euclidean_km(a: PositionRow, b: PositionRow) -> float:
    return math.sqrt((a.x - b.x) ** 2 + (a.y - b.y) ** 2 + (a.z - b.z) ** 2)


def plane_pair_key(a: int, b: int) -> tuple[int, int]:
    return (a, b) if a < b else (b, a)


def satellite_name(spec: ConstellationSpec, plane: int, sat_index: int) -> str:
    plane_digits = len(str(spec.planes))
    sat_digits = len(str(spec.sats_per_plane))
    return f"Satellite{spec.constellation_id}{plane:0{plane_digits}d}{sat_index:0{sat_digits}d}"


def same_plane_pairs(spec: ConstellationSpec) -> set[tuple[str, str]]:
    pairs: set[tuple[str, str]] = set()
    for plane in range(1, spec.planes + 1):
        for idx in range(1, spec.sats_per_plane + 1):
            nxt = idx + 1 if idx < spec.sats_per_plane else 1
            a = satellite_name(spec, plane, idx)
            b = satellite_name(spec, plane, nxt)
            pairs.add(tuple(sorted((a, b))))
    return pairs


def compute_snapshot_edges(
    positions_at_t: dict[str, PositionRow],
    satellite_refs: dict[str, object],
    spec: ConstellationSpec,
    max_distance_km: float,
) -> list[dict[str, object]]:
    edges: list[dict[str, object]] = []
    seen_pairs: set[tuple[str, str]] = set()

    for pair in same_plane_pairs(spec):
        src, dst = pair
        distance = euclidean_km(positions_at_t[src], positions_at_t[dst])
        if distance > max_distance_km:
            continue
        src_ref = satellite_refs[src]
        dst_ref = satellite_refs[dst]
        seen_pairs.add(pair)
        edges.append(
            {
                "src": src,
                "dst": dst,
                "src_plane": src_ref.plane,
                "src_index": src_ref.index,
                "dst_plane": dst_ref.plane,
                "dst_index": dst_ref.index,
                "link_type": "same_plane",
                "distance_km": distance,
            }
        )

    plane_to_sats: dict[int, list[str]] = defaultdict(list)
    for name, ref in satellite_refs.items():
        plane_to_sats[ref.plane].append(name)
    for sats in plane_to_sats.values():
        sats.sort(key=lambda item: satellite_refs[item].index)

    adjacent_plane_pairs = [(plane, plane + 1) for plane in range(1, spec.planes)] + [(spec.planes, 1)]
    for plane_a, plane_b in adjacent_plane_pairs:
        graph = nx.Graph()
        candidates_found = False
        for sat_a in plane_to_sats[plane_a]:
            ref_a = satellite_refs[sat_a]
            left = f"L::{sat_a}"
            graph.add_node(left, bipartite=0, sat=sat_a)
            for sat_b in plane_to_sats[plane_b]:
                ref_b = satellite_refs[sat_b]
                if circular_distance(ref_a.index, ref_b.index, spec.sats_per_plane) > 1:
                    continue
                distance = euclidean_km(positions_at_t[sat_a], positions_at_t[sat_b])
                if distance > max_distance_km:
                    continue
                right = f"R::{sat_b}"
                graph.add_node(right, bipartite=1, sat=sat_b)
                graph.add_edge(left, right, weight=100000.0 - distance, distance_km=distance)
                candidates_found = True
        if not candidates_found:
            continue

        matching = nx.max_weight_matching(graph, maxcardinality=True, weight="weight")
        for u, v in matching:
            left, right = (u, v) if u.startswith("L::") else (v, u)
            sat_a = graph.nodes[left]["sat"]
            sat_b = graph.nodes[right]["sat"]
            pair = tuple(sorted((sat_a, sat_b)))
            if pair in seen_pairs:
                continue
            seen_pairs.add(pair)
            src_ref = satellite_refs[pair[0]]
            dst_ref = satellite_refs[pair[1]]
            edges.append(
                {
                    "src": pair[0],
                    "dst": pair[1],
                    "src_plane": src_ref.plane,
                    "src_index": src_ref.index,
                    "dst_plane": dst_ref.plane,
                    "dst_index": dst_ref.index,
                    "link_type": "adjacent_plane",
                    "distance_km": graph.edges[left, right]["distance_km"],
                }
            )

    edges.sort(key=lambda row: (row["link_type"], row["src"], row["dst"]))
    return edges


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Compute domain-1 ISL distances from STK inertial position export.")
    parser.add_argument("--input", required=True, help="STK inertial position file")
    parser.add_argument("--output-dir", required=True, help="Output directory")
    parser.add_argument("--max-distance-km", type=float, default=5000.0)
    parser.add_argument("--constellation-id", type=int, default=1)
    parser.add_argument("--planes", type=int, default=5)
    parser.add_argument("--sats-per-plane", type=int, default=9)
    parser.add_argument("--label", default="domain1")
    args = parser.parse_args()

    spec = ConstellationSpec(
        constellation_id=args.constellation_id,
        domain="A",
        planes=args.planes,
        sats_per_plane=args.sats_per_plane,
        name=args.label,
    )
    spec_map = {args.constellation_id: spec}

    input_path = Path(args.input)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    satellite_names, positions = parse_multisat_position_file(input_path)
    satellite_refs = {name: parse_satellite(name, spec_map) for name in satellite_names}
    timestamps = [row.time for row in positions[satellite_names[0]]]

    distance_rows: list[dict[str, object]] = []
    summary_acc: dict[tuple[str, str, str], list[float]] = defaultdict(list)
    time_summary_rows: list[dict[str, object]] = []
    max_degree_observed = 0

    for idx, timestamp in enumerate(timestamps):
        positions_at_t = {name: positions[name][idx] for name in satellite_names}
        edges = compute_snapshot_edges(positions_at_t, satellite_refs, spec, args.max_distance_km)
        degree: dict[str, int] = defaultdict(int)
        same_count = 0
        adj_count = 0
        total_distance = 0.0

        for edge in edges:
            degree[edge["src"]] += 1
            degree[edge["dst"]] += 1
            same_count += 1 if edge["link_type"] == "same_plane" else 0
            adj_count += 1 if edge["link_type"] == "adjacent_plane" else 0
            total_distance += float(edge["distance_km"])
            summary_acc[(edge["src"], edge["dst"], edge["link_type"])].append(float(edge["distance_km"]))
            distance_rows.append(
                {
                    "time_utc": timestamp.strftime(DATE_FMT)[:-3],
                    "time_sec": idx * 60,
                    **edge,
                }
            )

        max_degree_observed = max(max_degree_observed, max(degree.values(), default=0))
        time_summary_rows.append(
            {
                "time_utc": timestamp.strftime(DATE_FMT)[:-3],
                "time_sec": idx * 60,
                "edge_count_total": len(edges),
                "edge_count_same_plane": same_count,
                "edge_count_adjacent_plane": adj_count,
                "max_node_degree": max(degree.values(), default=0),
                "avg_edge_distance_km": (total_distance / len(edges)) if edges else 0.0,
            }
        )

    pair_summary_rows: list[dict[str, object]] = []
    for (src, dst, link_type), distances in sorted(summary_acc.items()):
        src_ref = satellite_refs[src]
        dst_ref = satellite_refs[dst]
        pair_summary_rows.append(
            {
                "src": src,
                "dst": dst,
                "src_plane": src_ref.plane,
                "src_index": src_ref.index,
                "dst_plane": dst_ref.plane,
                "dst_index": dst_ref.index,
                "link_type": link_type,
                "sample_count": len(distances),
                "active_fraction": len(distances) / len(timestamps),
                "min_distance_km": min(distances),
                "max_distance_km": max(distances),
                "avg_distance_km": sum(distances) / len(distances),
            }
        )

    summary = {
        "input_file": str(input_path),
        "satellite_count": len(satellite_names),
        "time_sample_count": len(timestamps),
        "sample_step_seconds": 60,
        "max_distance_km": args.max_distance_km,
        "max_degree_observed": max_degree_observed,
        "pair_count": len(pair_summary_rows),
        "same_plane_pair_count": sum(1 for row in pair_summary_rows if row["link_type"] == "same_plane"),
        "adjacent_plane_pair_count": sum(1 for row in pair_summary_rows if row["link_type"] == "adjacent_plane"),
        "distance_stats": {
            "min_km": min((row["min_distance_km"] for row in pair_summary_rows), default=0.0),
            "max_km": max((row["max_distance_km"] for row in pair_summary_rows), default=0.0),
            "avg_km": (
                sum(row["avg_distance_km"] for row in pair_summary_rows) / len(pair_summary_rows)
                if pair_summary_rows
                else 0.0
            ),
        },
    }

    write_csv(output_dir / f"{args.label}_isl_distance_timeseries.csv", distance_rows)
    write_csv(output_dir / f"{args.label}_isl_pair_summary.csv", pair_summary_rows)
    write_csv(output_dir / f"{args.label}_isl_time_summary.csv", time_summary_rows)
    (output_dir / f"{args.label}_isl_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
