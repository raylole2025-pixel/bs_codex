from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from statistics import median
from typing import Any

DATE_FMT = "%d %b %Y %H:%M:%S.%f"
HEADER_RE = re.compile(r"^Satellite-(.*):\s+LLA Position\s*$")
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
EARTH_RADIUS_KM = 6371.0
EPS = 1e-9


@dataclass(frozen=True)
class LLAPositionRow:
    time: datetime
    lat_deg: float
    lon_deg: float
    alt_km: float


@dataclass(frozen=True)
class HotspotCircleSpec:
    region_id: str
    label: str
    center_lat_deg: float
    center_lon_deg: float
    radius_km: float
    weight: float


DEFAULT_A_HOTSPOTS: tuple[HotspotCircleSpec, ...] = (
    HotspotCircleSpec(
        region_id="z1_A",
        label="东海-西太前沿",
        center_lat_deg=35.0,
        center_lon_deg=123.0,
        radius_km=900.0,
        weight=0.35,
    ),
    HotspotCircleSpec(
        region_id="z2_A",
        label="南海-中南半岛",
        center_lat_deg=16.0,
        center_lon_deg=112.0,
        radius_km=900.0,
        weight=0.25,
    ),
    HotspotCircleSpec(
        region_id="z3_A",
        label="北印度洋",
        center_lat_deg=11.0,
        center_lon_deg=78.0,
        radius_km=1000.0,
        weight=0.20,
    ),
    HotspotCircleSpec(
        region_id="z4_A",
        label="波斯湾-中东走廊",
        center_lat_deg=25.0,
        center_lon_deg=52.0,
        radius_km=900.0,
        weight=0.20,
    ),
)


def parse_multisat_lla_file(path: str | Path) -> tuple[list[str], dict[str, list[LLAPositionRow]]]:
    lines = Path(path).read_text(encoding="utf-8", errors="ignore").splitlines()
    if len(lines) < 2:
        raise ValueError(f"LLA file is too short: {path}")

    header_match = HEADER_RE.match(lines[1].strip())
    if not header_match:
        raise ValueError(f"Cannot find multi-satellite LLA header in {path}")

    raw_names = [part.strip() for part in header_match.group(1).split(",")]
    satellite_names = [name for name in raw_names if name.startswith("Satellite")]
    if not satellite_names:
        raise ValueError(f"No satellite names found in {path}")

    blocks: list[list[LLAPositionRow]] = []
    current_rows: list[LLAPositionRow] = []
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
            LLAPositionRow(
                time=datetime.strptime(match.group(1), DATE_FMT),
                lat_deg=float(match.group(2)),
                lon_deg=float(match.group(3)),
                alt_km=float(match.group(4)),
            )
        )

    if current_rows:
        blocks.append(current_rows)

    if len(blocks) != len(satellite_names):
        raise ValueError(
            f"LLA block count {len(blocks)} does not match satellite count {len(satellite_names)} in {path}"
        )
    if any(not rows for rows in blocks):
        raise ValueError(f"At least one satellite block is empty in {path}")

    positions = {satellite_names[idx]: rows for idx, rows in enumerate(blocks)}
    return satellite_names, positions


def haversine_km(lat1_deg: float, lon1_deg: float, lat2_deg: float, lon2_deg: float) -> float:
    lat1 = math.radians(lat1_deg)
    lon1 = math.radians(lon1_deg)
    lat2 = math.radians(lat2_deg)
    lon2 = math.radians(lon2_deg)
    d_lat = lat2 - lat1
    d_lon = lon2 - lon1
    a = math.sin(d_lat / 2.0) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(d_lon / 2.0) ** 2
    c = 2.0 * math.atan2(math.sqrt(a), math.sqrt(max(0.0, 1.0 - a)))
    return EARTH_RADIUS_KM * c


def _sample_step_seconds(reference_rows: list[LLAPositionRow]) -> float:
    if len(reference_rows) <= 1:
        return 60.0
    deltas = []
    for left, right in zip(reference_rows, reference_rows[1:]):
        delta = (right.time - left.time).total_seconds()
        if delta > EPS:
            deltas.append(delta)
    if not deltas:
        return 60.0
    return float(median(deltas))


def _time_bucket_bounds(
    reference_rows: list[LLAPositionRow],
    planning_end_seconds: float | None,
) -> tuple[list[float], list[tuple[int, float, float]], float]:
    offsets = [(row.time - reference_rows[0].time).total_seconds() for row in reference_rows]
    if not offsets:
        return [], [], 0.0

    step_seconds = _sample_step_seconds(reference_rows)
    if planning_end_seconds is None:
        planning_end_seconds = offsets[-1]

    buckets: list[tuple[int, float, float]] = []
    for idx, start in enumerate(offsets):
        if start >= planning_end_seconds - EPS:
            break
        if idx + 1 < len(offsets):
            end = min(planning_end_seconds, offsets[idx + 1])
        else:
            end = min(planning_end_seconds, start + step_seconds)
        if end > start + EPS:
            buckets.append((idx, start, end))
    return offsets, buckets, step_seconds


def build_hotspots_from_multisat_lla(
    lla_path: str | Path,
    *,
    hotspot_specs: tuple[HotspotCircleSpec, ...] = DEFAULT_A_HOTSPOTS,
    planning_end_seconds: float | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    satellite_names, positions = parse_multisat_lla_file(lla_path)
    reference_rows = positions[satellite_names[0]]
    _, buckets, step_seconds = _time_bucket_bounds(reference_rows, planning_end_seconds)
    if not buckets:
        raise ValueError(f"No valid time buckets parsed from {lla_path}")

    hotspot_payloads: list[dict[str, Any]] = []
    region_summaries: list[dict[str, Any]] = []
    analysis_start = reference_rows[0].time

    for spec in hotspot_specs:
        intervals: list[dict[str, Any]] = []
        unique_nodes: set[str] = set()
        active_duration = 0.0
        active_snapshot_count = 0
        max_active_nodes = 0

        prev_nodes: tuple[str, ...] | None = None
        prev_start = 0.0
        prev_end = 0.0

        for row_idx, start_sec, end_sec in buckets:
            nodes_now = tuple(
                sat
                for sat in satellite_names
                if haversine_km(
                    positions[sat][row_idx].lat_deg,
                    positions[sat][row_idx].lon_deg,
                    spec.center_lat_deg,
                    spec.center_lon_deg,
                )
                <= spec.radius_km + EPS
            )

            if nodes_now:
                active_snapshot_count += 1
                active_duration += end_sec - start_sec
                max_active_nodes = max(max_active_nodes, len(nodes_now))
                unique_nodes.update(nodes_now)

            if prev_nodes is None:
                prev_nodes = nodes_now
                prev_start = start_sec
                prev_end = end_sec
                continue

            if nodes_now == prev_nodes and abs(start_sec - prev_end) <= EPS:
                prev_end = end_sec
                continue

            if prev_nodes:
                intervals.append(
                    {
                        "start": prev_start,
                        "end": prev_end,
                        "nodes": list(prev_nodes),
                    }
                )
            prev_nodes = nodes_now
            prev_start = start_sec
            prev_end = end_sec

        if prev_nodes:
            intervals.append(
                {
                    "start": prev_start,
                    "end": prev_end,
                    "nodes": list(prev_nodes),
                }
            )

        hotspot_payloads.append(
            {
                "id": spec.region_id,
                "label": spec.label,
                "weight": spec.weight,
                "center_lat_deg": spec.center_lat_deg,
                "center_lon_deg": spec.center_lon_deg,
                "radius_km": spec.radius_km,
                "intervals": intervals,
            }
        )
        region_summaries.append(
            {
                "id": spec.region_id,
                "label": spec.label,
                "weight": spec.weight,
                "center_lat_deg": spec.center_lat_deg,
                "center_lon_deg": spec.center_lon_deg,
                "radius_km": spec.radius_km,
                "interval_count": len(intervals),
                "active_snapshot_count": active_snapshot_count,
                "active_duration_sec": active_duration,
                "unique_satellite_count": len(unique_nodes),
                "max_active_nodes": max_active_nodes,
            }
        )

    summary = {
        "source_file": str(Path(lla_path).resolve()),
        "analysis_start_utc": analysis_start.strftime(DATE_FMT),
        "sample_step_seconds": step_seconds,
        "bucket_count": len(buckets),
        "region_count": len(hotspot_payloads),
        "regions": region_summaries,
    }
    return hotspot_payloads, summary


def write_hotspot_summary(path: str | Path, summary: dict[str, Any]) -> None:
    Path(path).write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
