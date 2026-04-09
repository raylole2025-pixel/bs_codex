from __future__ import annotations

import csv
import json
import math
import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from statistics import mean, median
from typing import Iterable

import networkx as nx

DATE_FMT = "%d %b %Y %H:%M:%S.%f"
PAIR_RE = re.compile(r"^(Satellite\d+)\s+to\s+(Satellite\d+)\s*$")
ROW_RE = re.compile(
    r"^\s*(\d{1,2}\s+\w{3}\s+\d{4}\s+\d{2}:\d{2}:\d{2}\.\d{3})\s+"
    r"(\d{1,2}\s+\w{3}\s+\d{4}\s+\d{2}:\d{2}:\d{2}\.\d{3})\s+"
    r"([0-9.]+)\s*$"
)
SAT_RE = re.compile(r"^Satellite(\d+)$")
EPS = 1e-9


@dataclass(frozen=True)
class ConstellationSpec:
    constellation_id: int
    domain: str
    planes: int
    sats_per_plane: int
    name: str


@dataclass(frozen=True)
class SatelliteRef:
    name: str
    constellation_id: int
    domain: str
    plane: int
    index: int


@dataclass(frozen=True)
class ContactWindow:
    src: SatelliteRef
    dst: SatelliteRef
    start: datetime
    stop: datetime
    duration: float

    @property
    def pair_key(self) -> tuple[str, str]:
        if self.src.domain != self.dst.domain:
            ordered = sorted((self.src.name, self.dst.name), key=lambda item: (item.startswith("Satellite2"), item))
            return ordered[0], ordered[1]
        return tuple(sorted((self.src.name, self.dst.name)))  # type: ignore[return-value]



def circular_distance(a: int, b: int, modulo: int) -> int:
    delta = abs(a - b)
    return min(delta, modulo - delta)



def parse_satellite(name: str, specs: dict[int, ConstellationSpec]) -> SatelliteRef:
    match = SAT_RE.match(name)
    if not match:
        raise ValueError(f"Unsupported satellite name: {name}")
    digits = match.group(1)
    for constellation_id in sorted(specs, key=lambda item: len(str(item)), reverse=True):
        prefix = str(constellation_id)
        if not digits.startswith(prefix):
            continue
        spec = specs[constellation_id]
        rest = digits[len(prefix) :]
        plane_digits = len(str(spec.planes))
        sat_digits = len(str(spec.sats_per_plane))
        if len(rest) != plane_digits + sat_digits:
            continue
        plane = int(rest[:plane_digits])
        sat_index = int(rest[plane_digits:])
        if 1 <= plane <= spec.planes and 1 <= sat_index <= spec.sats_per_plane:
            return SatelliteRef(
                name=name,
                constellation_id=spec.constellation_id,
                domain=spec.domain,
                plane=plane,
                index=sat_index,
            )
    raise ValueError(f"Cannot parse satellite name with current specs: {name}")



def parse_access_file(path: str | Path, specs: dict[int, ConstellationSpec]) -> list[ContactWindow]:
    contacts: list[ContactWindow] = []
    current_pair: tuple[SatelliteRef, SatelliteRef] | None = None
    for raw_line in Path(path).read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw_line.strip()
        pair_match = PAIR_RE.match(line)
        if pair_match:
            src = parse_satellite(pair_match.group(1), specs)
            dst = parse_satellite(pair_match.group(2), specs)
            current_pair = (src, dst)
            continue
        if current_pair is None:
            continue
        row_match = ROW_RE.match(raw_line)
        if not row_match:
            continue
        start = datetime.strptime(row_match.group(1), DATE_FMT)
        stop = datetime.strptime(row_match.group(2), DATE_FMT)
        duration = float(row_match.group(3))
        contacts.append(
            ContactWindow(
                src=current_pair[0],
                dst=current_pair[1],
                start=start,
                stop=stop,
                duration=duration,
            )
        )
    return contacts



def merge_contacts(contacts: Iterable[ContactWindow]) -> list[ContactWindow]:
    grouped: dict[tuple[str, str], list[ContactWindow]] = defaultdict(list)
    for contact in contacts:
        grouped[contact.pair_key].append(contact)

    merged: list[ContactWindow] = []
    for pair_key, items in grouped.items():
        items.sort(key=lambda item: item.start)
        ordered_refs = {items[0].src.name: items[0].src, items[0].dst.name: items[0].dst}
        src = ordered_refs[pair_key[0]]
        dst = ordered_refs[pair_key[1]]
        cur_start = items[0].start
        cur_stop = items[0].stop
        for item in items[1:]:
            if item.start <= cur_stop + timedelta(seconds=1e-6):
                cur_stop = max(cur_stop, item.stop)
            else:
                duration = (cur_stop - cur_start).total_seconds()
                merged.append(ContactWindow(src=src, dst=dst, start=cur_start, stop=cur_stop, duration=duration))
                cur_start = item.start
                cur_stop = item.stop
        duration = (cur_stop - cur_start).total_seconds()
        merged.append(ContactWindow(src=src, dst=dst, start=cur_start, stop=cur_stop, duration=duration))
    return merged



def filter_intra_candidates(contacts: Iterable[ContactWindow], spec: ConstellationSpec, min_duration: float) -> tuple[list[ContactWindow], list[ContactWindow]]:
    same_plane: list[ContactWindow] = []
    adjacent_plane: list[ContactWindow] = []
    for contact in contacts:
        if contact.src.name == contact.dst.name or contact.duration < min_duration:
            continue
        if contact.src.constellation_id != spec.constellation_id or contact.dst.constellation_id != spec.constellation_id:
            continue
        if contact.src.plane == contact.dst.plane:
            if circular_distance(contact.src.index, contact.dst.index, spec.sats_per_plane) == 1:
                same_plane.append(contact)
            continue
        if circular_distance(contact.src.plane, contact.dst.plane, spec.planes) == 1:
            if circular_distance(contact.src.index, contact.dst.index, spec.sats_per_plane) <= 1:
                adjacent_plane.append(contact)
    return same_plane, adjacent_plane



def assign_windows_to_snapshots(
    contacts: Iterable[ContactWindow],
    analysis_start: datetime,
    snapshot_seconds: int,
    snapshot_count: int,
) -> list[list[ContactWindow]]:
    buckets: list[list[ContactWindow]] = [[] for _ in range(snapshot_count)]
    for contact in contacts:
        start_offset = (contact.start - analysis_start).total_seconds()
        stop_offset = (contact.stop - analysis_start).total_seconds()
        first_idx = max(0, math.ceil(start_offset / snapshot_seconds - EPS))
        last_exclusive = min(snapshot_count, math.ceil(stop_offset / snapshot_seconds - EPS))
        for idx in range(first_idx, last_exclusive):
            buckets[idx].append(contact)
    return buckets



def build_logical_snapshots(
    spec: ConstellationSpec,
    raw_contacts: list[ContactWindow],
    analysis_start: datetime,
    snapshot_seconds: int,
    snapshot_count: int,
    min_duration: float,
) -> list[dict[tuple[str, str], str]]:
    same_plane, adjacent_plane = filter_intra_candidates(raw_contacts, spec, min_duration=min_duration)
    same_buckets = assign_windows_to_snapshots(same_plane, analysis_start, snapshot_seconds, snapshot_count)
    adj_buckets = assign_windows_to_snapshots(adjacent_plane, analysis_start, snapshot_seconds, snapshot_count)

    snapshots: list[dict[tuple[str, str], str]] = []
    for idx in range(snapshot_count):
        selected: dict[tuple[str, str], str] = {}
        for contact in same_buckets[idx]:
            selected[contact.pair_key] = "same_plane"

        unique_candidates: dict[tuple[str, str], ContactWindow] = {}
        for contact in adj_buckets[idx]:
            existing = unique_candidates.get(contact.pair_key)
            if existing is None or _adjacent_priority(contact, spec) < _adjacent_priority(existing, spec):
                unique_candidates[contact.pair_key] = contact

        degrees = defaultdict(int)
        for contact in sorted(unique_candidates.values(), key=lambda item: _adjacent_priority(item, spec)):
            u, v = contact.pair_key
            if degrees[u] >= 2 or degrees[v] >= 2:
                continue
            degrees[u] += 1
            degrees[v] += 1
            selected[(u, v)] = "adjacent_plane"
        snapshots.append(selected)
    return snapshots



def _adjacent_priority(contact: ContactWindow, spec: ConstellationSpec) -> tuple[float, ...]:
    index_distance = circular_distance(contact.src.index, contact.dst.index, spec.sats_per_plane)
    return (float(index_distance), -contact.duration, float(contact.src.plane), float(contact.dst.plane), float(contact.src.index), float(contact.dst.index))



def stitch_snapshot_contacts(
    snapshots: list[dict[tuple[str, str], str]],
    analysis_start: datetime,
    snapshot_seconds: int,
    satellite_lookup: dict[str, SatelliteRef],
) -> list[dict[str, object]]:
    active: dict[tuple[str, str], tuple[int, str]] = {}
    rows: list[dict[str, object]] = []

    for idx, snapshot in enumerate(snapshots):
        current_edges = set(snapshot)
        for edge_key, (start_idx, link_type) in list(active.items()):
            if edge_key in current_edges:
                continue
            rows.append(_build_clean_row(edge_key, link_type, start_idx, idx, analysis_start, snapshot_seconds, satellite_lookup))
            del active[edge_key]
        for edge_key, link_type in snapshot.items():
            if edge_key not in active:
                active[edge_key] = (idx, link_type)

    for edge_key, (start_idx, link_type) in active.items():
        rows.append(
            _build_clean_row(
                edge_key,
                link_type,
                start_idx,
                len(snapshots),
                analysis_start,
                snapshot_seconds,
                satellite_lookup,
            )
        )
    rows.sort(key=lambda item: (item["start_sec"], item["src"], item["dst"]))
    return rows



def _build_clean_row(
    edge_key: tuple[str, str],
    link_type: str,
    start_idx: int,
    stop_idx: int,
    analysis_start: datetime,
    snapshot_seconds: int,
    satellite_lookup: dict[str, SatelliteRef],
) -> dict[str, object]:
    start_dt = analysis_start + timedelta(seconds=start_idx * snapshot_seconds)
    stop_dt = analysis_start + timedelta(seconds=stop_idx * snapshot_seconds)
    src_ref = satellite_lookup[edge_key[0]]
    dst_ref = satellite_lookup[edge_key[1]]
    return {
        "src": edge_key[0],
        "dst": edge_key[1],
        "src_plane": src_ref.plane,
        "src_index": src_ref.index,
        "dst_plane": dst_ref.plane,
        "dst_index": dst_ref.index,
        "link_type": link_type,
        "start_utc": start_dt.strftime(DATE_FMT),
        "stop_utc": stop_dt.strftime(DATE_FMT),
        "start_sec": (start_dt - analysis_start).total_seconds(),
        "stop_sec": (stop_dt - analysis_start).total_seconds(),
        "duration_sec": (stop_dt - start_dt).total_seconds(),
    }



def build_cross_clean_contacts(
    raw_contacts: list[ContactWindow],
    min_duration: float,
    analysis_start: datetime,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    counter = 1
    for contact in merge_contacts(raw_contacts):
        if contact.src.name == contact.dst.name or contact.duration < min_duration:
            continue
        if contact.src.domain == contact.dst.domain:
            continue
        gw_a = contact.src if contact.src.domain == "A" else contact.dst
        gw_b = contact.dst if contact.dst.domain == "B" else contact.src
        rows.append(
            {
                "window_id": f"X{counter:06d}",
                "gw_A": gw_a.name,
                "gw_B": gw_b.name,
                "gw_A_plane": gw_a.plane,
                "gw_A_index": gw_a.index,
                "gw_B_plane": gw_b.plane,
                "gw_B_index": gw_b.index,
                "start_utc": contact.start.strftime(DATE_FMT),
                "stop_utc": contact.stop.strftime(DATE_FMT),
                "start_sec": (contact.start - analysis_start).total_seconds(),
                "stop_sec": (contact.stop - analysis_start).total_seconds(),
                "duration_sec": contact.duration,
            }
        )
        counter += 1
    rows.sort(key=lambda item: (item["start_sec"], item["gw_A"], item["gw_B"]))
    return rows



def summarize_cross_contacts(rows: list[dict[str, object]]) -> dict[str, object]:
    durations = [float(row["duration_sec"]) for row in rows]
    pair_totals = defaultdict(float)
    gateways_a = set()
    gateways_b = set()
    for row in rows:
        pair_totals[(str(row["gw_A"]), str(row["gw_B"]))] += float(row["duration_sec"])
        gateways_a.add(str(row["gw_A"]))
        gateways_b.add(str(row["gw_B"]))
    if not durations:
        return {
            "window_count": 0,
            "gateway_pair_count": 0,
            "gateway_a_count": 0,
            "gateway_b_count": 0,
            "duration_stats": {},
            "pair_totals": [],
        }
    sorted_durations = sorted(durations)
    p90_idx = min(len(sorted_durations) - 1, max(0, math.ceil(len(sorted_durations) * 0.9) - 1))
    pair_rows = [
        {"gw_A": key[0], "gw_B": key[1], "total_duration_sec": value}
        for key, value in sorted(pair_totals.items())
    ]
    return {
        "window_count": len(rows),
        "gateway_pair_count": len(pair_totals),
        "gateway_a_count": len(gateways_a),
        "gateway_b_count": len(gateways_b),
        "duration_stats": {
            "min_sec": min(sorted_durations),
            "max_sec": max(sorted_durations),
            "mean_sec": mean(sorted_durations),
            "median_sec": median(sorted_durations),
            "p90_sec": sorted_durations[p90_idx],
            "total_sec": sum(sorted_durations),
        },
        "pair_totals": pair_rows,
    }



def build_snapshot_summary(
    nodes: list[str],
    snapshots: list[dict[tuple[str, str], str]],
    analysis_start: datetime,
    snapshot_seconds: int,
) -> tuple[list[dict[str, object]], list[dict[str, object]], list[nx.Graph]]:
    summary_rows: list[dict[str, object]] = []
    degree_rows: list[dict[str, object]] = []
    graphs: list[nx.Graph] = []

    for idx, snapshot in enumerate(snapshots):
        graph = nx.Graph()
        graph.add_nodes_from(nodes)
        graph.add_edges_from(snapshot)
        graphs.append(graph)
        components = list(nx.connected_components(graph))
        reachable_pairs = 0
        hop_sum = 0.0
        for component in components:
            if len(component) <= 1:
                continue
            subgraph = graph.subgraph(component)
            for _, lengths in nx.all_pairs_shortest_path_length(subgraph):
                for dst, hop in lengths.items():
                    if hop > 0:
                        hop_sum += hop
                        reachable_pairs += 1
        avg_hop = hop_sum / reachable_pairs if reachable_pairs else None
        snapshot_time = analysis_start + timedelta(seconds=idx * snapshot_seconds)
        summary_rows.append(
            {
                "snapshot_index": idx,
                "snapshot_utc": snapshot_time.strftime(DATE_FMT),
                "edge_count": graph.number_of_edges(),
                "component_count": len(components),
                "largest_component_size": max((len(component) for component in components), default=0),
                "average_shortest_hop": avg_hop,
            }
        )
        for node in nodes:
            degree_rows.append(
                {
                    "snapshot_index": idx,
                    "snapshot_utc": snapshot_time.strftime(DATE_FMT),
                    "node": node,
                    "degree": graph.degree(node),
                }
            )
    return summary_rows, degree_rows, graphs



def write_hop_matrix(
    path: str | Path,
    nodes: list[str],
    gateways: list[str],
    graphs: list[nx.Graph],
    analysis_start: datetime,
    snapshot_seconds: int,
) -> None:
    with Path(path).open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["snapshot_index", "snapshot_utc", "node", "gateway", "hop"])
        writer.writeheader()
        for idx, graph in enumerate(graphs):
            snapshot_time = analysis_start + timedelta(seconds=idx * snapshot_seconds)
            snapshot_text = snapshot_time.strftime(DATE_FMT)
            for gateway in gateways:
                if gateway not in graph:
                    lengths = {}
                else:
                    lengths = nx.single_source_shortest_path_length(graph, gateway)
                for node in nodes:
                    writer.writerow(
                        {
                            "snapshot_index": idx,
                            "snapshot_utc": snapshot_text,
                            "node": node,
                            "gateway": gateway,
                            "hop": lengths.get(node, -1),
                        }
                    )



def write_csv(path: str | Path, rows: list[dict[str, object]]) -> None:
    if not rows:
        Path(path).write_text("", encoding="utf-8")
        return
    with Path(path).open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)



def build_stage1_outputs(
    chain1_path: str | Path,
    chain2_path: str | Path,
    chain3_path: str | Path,
    output_dir: str | Path,
    snapshot_seconds: int = 60,
    min_intra_duration: float = 60.0,
    min_cross_duration: float = 300.0,
    constellation_specs: Iterable[ConstellationSpec] | None = None,
) -> dict[str, object]:
    specs = list(
        constellation_specs
        or [
            ConstellationSpec(constellation_id=1, domain="A", planes=5, sats_per_plane=9, name="RemoteSensingConstellation1"),
            ConstellationSpec(constellation_id=2, domain="B", planes=8, sats_per_plane=10, name="CommunicationConstellation"),
        ]
    )
    spec_map = {spec.constellation_id: spec for spec in specs}

    chain1_contacts = merge_contacts(
        contact for contact in parse_access_file(chain1_path, spec_map) if contact.src.name != contact.dst.name
    )
    chain2_contacts = merge_contacts(
        contact for contact in parse_access_file(chain2_path, spec_map) if contact.src.name != contact.dst.name
    )
    chain3_contacts = merge_contacts(
        contact for contact in parse_access_file(chain3_path, spec_map) if contact.src.name != contact.dst.name
    )

    all_contacts = chain1_contacts + chain2_contacts + chain3_contacts
    analysis_start = min(contact.start for contact in all_contacts)
    analysis_stop = max(contact.stop for contact in all_contacts)
    total_seconds = (analysis_stop - analysis_start).total_seconds()
    snapshot_count = math.ceil(total_seconds / snapshot_seconds)

    satellite_lookup = {}
    for contact in all_contacts:
        satellite_lookup[contact.src.name] = contact.src
        satellite_lookup[contact.dst.name] = contact.dst

    output_root = Path(output_dir)
    output_root.mkdir(parents=True, exist_ok=True)

    intra_results = {}
    for spec, contacts, prefix in (
        (spec_map[1], chain1_contacts, "A"),
        (spec_map[2], chain2_contacts, "B"),
    ):
        snapshots = build_logical_snapshots(
            spec=spec,
            raw_contacts=contacts,
            analysis_start=analysis_start,
            snapshot_seconds=snapshot_seconds,
            snapshot_count=snapshot_count,
            min_duration=min_intra_duration,
        )
        clean_rows = stitch_snapshot_contacts(snapshots, analysis_start, snapshot_seconds, satellite_lookup)
        nodes = sorted({contact.src.name for contact in contacts} | {contact.dst.name for contact in contacts})
        summary_rows, degree_rows, graphs = build_snapshot_summary(nodes, snapshots, analysis_start, snapshot_seconds)
        intra_results[prefix] = {
            "clean_rows": clean_rows,
            "summary_rows": summary_rows,
            "degree_rows": degree_rows,
            "graphs": graphs,
            "nodes": nodes,
        }
        write_csv(output_root / f"{prefix}_clean_contacts.csv", clean_rows)
        write_csv(output_root / f"{prefix}_snapshot_summary.csv", summary_rows)
        write_csv(output_root / f"{prefix}_snapshot_degrees.csv", degree_rows)

    cross_rows = build_cross_clean_contacts(chain3_contacts, min_cross_duration, analysis_start)
    cross_summary = summarize_cross_contacts(cross_rows)
    write_csv(output_root / "X_clean_contacts.csv", cross_rows)
    write_csv(output_root / "X_gateway_pair_summary.csv", list(cross_summary["pair_totals"]))

    gateways_a = sorted({str(row["gw_A"]) for row in cross_rows})
    gateways_b = sorted({str(row["gw_B"]) for row in cross_rows})
    write_hop_matrix(
        output_root / "A_hop_matrix.csv",
        intra_results["A"]["nodes"],
        gateways_a,
        intra_results["A"]["graphs"],
        analysis_start,
        snapshot_seconds,
    )
    write_hop_matrix(
        output_root / "B_hop_matrix.csv",
        intra_results["B"]["nodes"],
        gateways_b,
        intra_results["B"]["graphs"],
        analysis_start,
        snapshot_seconds,
    )

    summary = {
        "analysis_start_utc": analysis_start.strftime(DATE_FMT),
        "analysis_stop_utc": analysis_stop.strftime(DATE_FMT),
        "snapshot_seconds": snapshot_seconds,
        "snapshot_count": snapshot_count,
        "min_intra_duration_sec": min_intra_duration,
        "min_cross_duration_sec": min_cross_duration,
        "raw_contact_counts": {
            "chain1": len(chain1_contacts),
            "chain2": len(chain2_contacts),
            "chain3": len(chain3_contacts),
        },
        "clean_contact_counts": {
            "A": len(intra_results["A"]["clean_rows"]),
            "B": len(intra_results["B"]["clean_rows"]),
            "X": len(cross_rows),
        },
        "gateway_counts": {
            "A": len(gateways_a),
            "B": len(gateways_b),
        },
        "cross_summary": cross_summary,
        "files": {
            "A_clean_contacts": str((output_root / "A_clean_contacts.csv").resolve()),
            "B_clean_contacts": str((output_root / "B_clean_contacts.csv").resolve()),
            "X_clean_contacts": str((output_root / "X_clean_contacts.csv").resolve()),
            "A_snapshot_summary": str((output_root / "A_snapshot_summary.csv").resolve()),
            "B_snapshot_summary": str((output_root / "B_snapshot_summary.csv").resolve()),
            "A_snapshot_degrees": str((output_root / "A_snapshot_degrees.csv").resolve()),
            "B_snapshot_degrees": str((output_root / "B_snapshot_degrees.csv").resolve()),
            "A_hop_matrix": str((output_root / "A_hop_matrix.csv").resolve()),
            "B_hop_matrix": str((output_root / "B_hop_matrix.csv").resolve()),
            "X_gateway_pair_summary": str((output_root / "X_gateway_pair_summary.csv").resolve()),
        },
    }
    (output_root / "stage1_preprocess_summary.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    return summary


