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

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from bs3.stk_access_preprocess import ConstellationSpec, parse_access_file


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
    header_match = HEADER_RE.match(lines[1].strip())
    if not header_match:
        raise ValueError("Cannot find satellite list header")

    raw_names = [part.strip() for part in header_match.group(1).split(",")]
    satellite_names = [name for name in raw_names if name.startswith("Satellite")]
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
    return satellite_names, positions


def euclidean_km(a: PositionRow, b: PositionRow) -> float:
    return math.sqrt((a.x - b.x) ** 2 + (a.y - b.y) ** 2 + (a.z - b.z) ** 2)


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


@dataclass(frozen=True)
class WindowRecord:
    pair_id: str
    src: str
    dst: str
    start: datetime
    stop: datetime
    duration: float


def main() -> None:
    parser = argparse.ArgumentParser(description="Compute cross-domain link distances from STK access and position exports.")
    parser.add_argument("--access", required=True, help="Cross-domain access file")
    parser.add_argument("--domain1-pos", required=True, help="Domain-1 inertial position file")
    parser.add_argument("--domain2-pos", required=True, help="Domain-2 inertial position file")
    parser.add_argument("--output-dir", required=True, help="Output directory")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    spec_a = ConstellationSpec(constellation_id=1, domain="A", planes=5, sats_per_plane=9, name="Domain1")
    spec_b = ConstellationSpec(constellation_id=2, domain="B", planes=8, sats_per_plane=10, name="Domain2")
    specs = {1: spec_a, 2: spec_b}

    _, positions_a = parse_multisat_position_file(Path(args.domain1_pos))
    _, positions_b = parse_multisat_position_file(Path(args.domain2_pos))
    timestamps = [row.time for row in next(iter(positions_a.values()))]
    time_index = {ts: idx for idx, ts in enumerate(timestamps)}

    contacts = parse_access_file(Path(args.access), specs)
    window_records: list[WindowRecord] = []
    for idx, contact in enumerate(contacts, start=1):
        window_records.append(
            WindowRecord(
                pair_id=f"W{idx:06d}",
                src=contact.src.name,
                dst=contact.dst.name,
                start=contact.start,
                stop=contact.stop,
                duration=contact.duration,
            )
        )

    timeseries_rows: list[dict[str, object]] = []
    pair_distances: dict[tuple[str, str], list[float]] = defaultdict(list)
    window_distances: dict[str, list[float]] = defaultdict(list)
    time_active_rows: list[dict[str, object]] = []

    windows_by_time_index: dict[int, list[WindowRecord]] = defaultdict(list)
    for window in window_records:
        for ts, idx in time_index.items():
            if window.start <= ts < window.stop:
                windows_by_time_index[idx].append(window)

    for idx, ts in enumerate(timestamps):
        active_windows = windows_by_time_index.get(idx, [])
        if not active_windows:
            time_active_rows.append(
                {
                    "time_utc": ts.strftime(DATE_FMT)[:-3],
                    "time_sec": idx * 60,
                    "active_crosslink_count": 0,
                    "avg_distance_km": 0.0,
                    "min_distance_km": 0.0,
                    "max_distance_km": 0.0,
                }
            )
            continue

        distances_now: list[float] = []
        for window in active_windows:
            pos_a = positions_a[window.src][idx]
            pos_b = positions_b[window.dst][idx]
            distance = euclidean_km(pos_a, pos_b)
            distances_now.append(distance)
            pair_distances[(window.src, window.dst)].append(distance)
            window_distances[window.pair_id].append(distance)
            timeseries_rows.append(
                {
                    "time_utc": ts.strftime(DATE_FMT)[:-3],
                    "time_sec": idx * 60,
                    "pair_id": window.pair_id,
                    "src": window.src,
                    "dst": window.dst,
                    "window_start_utc": window.start.strftime(DATE_FMT)[:-3],
                    "window_stop_utc": window.stop.strftime(DATE_FMT)[:-3],
                    "window_duration_sec": window.duration,
                    "distance_km": distance,
                }
            )

        time_active_rows.append(
            {
                "time_utc": ts.strftime(DATE_FMT)[:-3],
                "time_sec": idx * 60,
                "active_crosslink_count": len(active_windows),
                "avg_distance_km": sum(distances_now) / len(distances_now),
                "min_distance_km": min(distances_now),
                "max_distance_km": max(distances_now),
            }
        )

    pair_summary_rows: list[dict[str, object]] = []
    for (src, dst), distances in sorted(pair_distances.items()):
        pair_summary_rows.append(
            {
                "src": src,
                "dst": dst,
                "sample_count": len(distances),
                "active_fraction": len(distances) / len(timestamps),
                "min_distance_km": min(distances),
                "max_distance_km": max(distances),
                "avg_distance_km": sum(distances) / len(distances),
            }
        )

    window_summary_rows: list[dict[str, object]] = []
    for window in window_records:
        distances = window_distances.get(window.pair_id, [])
        window_summary_rows.append(
            {
                "pair_id": window.pair_id,
                "src": window.src,
                "dst": window.dst,
                "start_utc": window.start.strftime(DATE_FMT)[:-3],
                "stop_utc": window.stop.strftime(DATE_FMT)[:-3],
                "duration_sec": window.duration,
                "sample_count": len(distances),
                "min_distance_km": min(distances) if distances else None,
                "max_distance_km": max(distances) if distances else None,
                "avg_distance_km": (sum(distances) / len(distances)) if distances else None,
            }
        )

    all_distances = [row["distance_km"] for row in timeseries_rows]
    summary = {
        "access_file": str(Path(args.access)),
        "domain1_position_file": str(Path(args.domain1_pos)),
        "domain2_position_file": str(Path(args.domain2_pos)),
        "window_count": len(window_records),
        "sample_time_count": len(timestamps),
        "sample_step_seconds": 60,
        "pair_count": len(pair_summary_rows),
        "distance_stats": {
            "min_km": min(all_distances) if all_distances else 0.0,
            "max_km": max(all_distances) if all_distances else 0.0,
            "avg_km": (sum(all_distances) / len(all_distances)) if all_distances else 0.0,
        },
        "active_crosslink_count": {
            "min": min((row["active_crosslink_count"] for row in time_active_rows), default=0),
            "max": max((row["active_crosslink_count"] for row in time_active_rows), default=0),
            "avg": (
                sum(row["active_crosslink_count"] for row in time_active_rows) / len(time_active_rows)
                if time_active_rows
                else 0.0
            ),
        },
    }

    write_csv(output_dir / "crosslink_distance_timeseries.csv", timeseries_rows)
    write_csv(output_dir / "crosslink_pair_summary.csv", pair_summary_rows)
    write_csv(output_dir / "crosslink_window_summary.csv", window_summary_rows)
    write_csv(output_dir / "crosslink_time_summary.csv", time_active_rows)
    (output_dir / "crosslink_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
