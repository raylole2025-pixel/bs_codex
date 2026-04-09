from __future__ import annotations

import csv
import math
import re
from bisect import bisect_left
from dataclasses import dataclass
from dataclasses import replace
from datetime import datetime
from pathlib import Path

from .models import Scenario

LIGHT_SPEED_KM_PER_S = 299_792.458
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


def _pair_key(src: str, dst: str, ordered: bool) -> tuple[str, str]:
    if ordered:
        return (src, dst)
    return tuple(sorted((src, dst)))


class DistanceSeries:
    def __init__(self, points: list[tuple[float, float]]) -> None:
        points.sort(key=lambda item: item[0])
        self.times = [item[0] for item in points]
        self.prefix = [0.0]
        for _, distance in points:
            self.prefix.append(self.prefix[-1] + distance)

    def average_over(self, start: float, end: float) -> float | None:
        if end <= start or not self.times:
            return None
        left = bisect_left(self.times, start)
        right = bisect_left(self.times, end)
        if right <= left:
            return None
        total = self.prefix[right] - self.prefix[left]
        return total / (right - left)


@dataclass(frozen=True)
class PositionRow:
    time: datetime
    x: float
    y: float
    z: float


class PositionEphemeris:
    def __init__(self, positions: dict[str, list[PositionRow]]) -> None:
        if not positions:
            raise ValueError("PositionEphemeris requires at least one satellite track")
        first_rows = next(iter(positions.values()))
        if not first_rows:
            raise ValueError("PositionEphemeris requires non-empty satellite tracks")
        t0 = first_rows[0].time
        self.sample_seconds = [(row.time - t0).total_seconds() for row in first_rows]
        self.positions = positions

    def average_pair_distance(self, src: str, dst: str, start: float, end: float) -> float | None:
        src_rows = self.positions.get(src)
        dst_rows = self.positions.get(dst)
        if src_rows is None or dst_rows is None:
            return None
        left = bisect_left(self.sample_seconds, start)
        right = bisect_left(self.sample_seconds, end)
        if right <= left:
            midpoint = 0.5 * (start + end)
            idx = bisect_left(self.sample_seconds, midpoint)
            if idx >= len(self.sample_seconds):
                idx = len(self.sample_seconds) - 1
            elif idx > 0 and abs(self.sample_seconds[idx - 1] - midpoint) < abs(self.sample_seconds[idx] - midpoint):
                idx -= 1
            return _euclidean_km(src_rows[idx], dst_rows[idx])
        total = 0.0
        for idx in range(left, right):
            total += _euclidean_km(src_rows[idx], dst_rows[idx])
        return total / (right - left)


def _load_timeseries(path: str | Path, ordered: bool) -> dict[tuple[str, str], DistanceSeries]:
    grouped: dict[tuple[str, str], list[tuple[float, float]]] = {}
    with Path(path).open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            key = _pair_key(str(row["src"]), str(row["dst"]), ordered=ordered)
            grouped.setdefault(key, []).append((float(row["time_sec"]), float(row["distance_km"])))
    return {key: DistanceSeries(points) for key, points in grouped.items()}


def _load_pair_average(path: str | Path, ordered: bool) -> dict[tuple[str, str], float]:
    averages: dict[tuple[str, str], float] = {}
    with Path(path).open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            value = row.get("avg_distance_km")
            if value in {None, ""}:
                continue
            key = _pair_key(str(row["src"]), str(row["dst"]), ordered=ordered)
            averages[key] = float(value)
    return averages


def _parse_multisat_position_file(path: str | Path) -> PositionEphemeris:
    lines = Path(path).read_text(encoding="utf-8", errors="ignore").splitlines()
    if len(lines) < 2:
        raise ValueError(f"Position file is too short: {path}")
    header_match = HEADER_RE.match(lines[1].strip())
    if not header_match:
        raise ValueError(f"Cannot find satellite list header in {path}")

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
            f"Block count {len(blocks)} does not match satellite count {len(satellite_names)} in {path}"
        )
    return PositionEphemeris({satellite_names[idx]: rows for idx, rows in enumerate(blocks)})


def _euclidean_km(a: PositionRow, b: PositionRow) -> float:
    return math.sqrt((a.x - b.x) ** 2 + (a.y - b.y) ** 2 + (a.z - b.z) ** 2)


def enrich_scenario_distances(
    scenario: Scenario,
    *,
    domain_a_timeseries_csv: str | Path | None = None,
    domain_b_timeseries_csv: str | Path | None = None,
    cross_timeseries_csv: str | Path | None = None,
    domain_a_pair_summary_csv: str | Path | None = None,
    domain_b_pair_summary_csv: str | Path | None = None,
    cross_pair_summary_csv: str | Path | None = None,
    domain_a_position_file: str | Path | None = None,
    domain_b_position_file: str | Path | None = None,
    light_speed_km_per_s: float = LIGHT_SPEED_KM_PER_S,
    intra_proc_delay_s: float = 0.0,
    cross_proc_delay_s: float = 0.0,
) -> tuple[Scenario, dict[str, float | int | str]]:
    a_series = _load_timeseries(domain_a_timeseries_csv, ordered=False) if domain_a_timeseries_csv else {}
    b_series = _load_timeseries(domain_b_timeseries_csv, ordered=False) if domain_b_timeseries_csv else {}
    x_series = _load_timeseries(cross_timeseries_csv, ordered=True) if cross_timeseries_csv else {}

    a_avg = _load_pair_average(domain_a_pair_summary_csv, ordered=False) if domain_a_pair_summary_csv else {}
    b_avg = _load_pair_average(domain_b_pair_summary_csv, ordered=False) if domain_b_pair_summary_csv else {}
    x_avg = _load_pair_average(cross_pair_summary_csv, ordered=True) if cross_pair_summary_csv else {}
    a_ephemeris = _parse_multisat_position_file(domain_a_position_file) if domain_a_position_file else None
    b_ephemeris = _parse_multisat_position_file(domain_b_position_file) if domain_b_position_file else None

    enriched_intra = 0
    fallback_intra = 0
    enriched_cross = 0
    fallback_cross = 0

    new_intra = []
    for link in scenario.intra_links:
        key = _pair_key(link.u, link.v, ordered=False)
        series = a_series if link.domain == "A" else b_series
        avg_map = a_avg if link.domain == "A" else b_avg
        ephemeris = a_ephemeris if link.domain == "A" else b_ephemeris
        distance = ephemeris.average_pair_distance(link.u, link.v, link.start, link.end) if ephemeris is not None else None
        if distance is None:
            distance = series.get(key).average_over(link.start, link.end) if key in series else None
        used_fallback = False
        if distance is None:
            distance = avg_map.get(key)
            used_fallback = distance is not None
        if distance is not None:
            delay = distance / light_speed_km_per_s + intra_proc_delay_s
            new_intra.append(replace(link, distance_km=distance, delay=delay, weight=delay))
            enriched_intra += 1
            if used_fallback:
                fallback_intra += 1
        else:
            new_intra.append(link)

    new_windows = []
    for window in scenario.candidate_windows:
        key = _pair_key(window.a, window.b, ordered=True)
        distance = None
        if a_ephemeris is not None and b_ephemeris is not None:
            a_rows = a_ephemeris.positions.get(window.a)
            b_rows = b_ephemeris.positions.get(window.b)
            if a_rows is not None and b_rows is not None:
                left = bisect_left(a_ephemeris.sample_seconds, window.start)
                right = bisect_left(a_ephemeris.sample_seconds, window.end)
                if right <= left:
                    midpoint = 0.5 * (window.start + window.end)
                    idx = bisect_left(a_ephemeris.sample_seconds, midpoint)
                    if idx >= len(a_ephemeris.sample_seconds):
                        idx = len(a_ephemeris.sample_seconds) - 1
                    elif idx > 0 and abs(a_ephemeris.sample_seconds[idx - 1] - midpoint) < abs(a_ephemeris.sample_seconds[idx] - midpoint):
                        idx -= 1
                    distance = _euclidean_km(a_rows[idx], b_rows[idx])
                else:
                    total = 0.0
                    for idx in range(left, right):
                        total += _euclidean_km(a_rows[idx], b_rows[idx])
                    distance = total / (right - left)
        if distance is None:
            distance = x_series.get(key).average_over(window.start, window.end) if key in x_series else None
        used_fallback = False
        if distance is None:
            distance = x_avg.get(key)
            used_fallback = distance is not None
        if distance is not None:
            delay = distance / light_speed_km_per_s + cross_proc_delay_s
            new_windows.append(replace(window, distance_km=distance, delay=delay))
            enriched_cross += 1
            if used_fallback:
                fallback_cross += 1
        else:
            new_windows.append(window)

    scenario.intra_links = new_intra
    scenario.candidate_windows = new_windows
    scenario.metadata.setdefault("distance_enrichment", {})
    scenario.metadata["distance_enrichment"] = {
        "light_speed_km_per_s": light_speed_km_per_s,
        "intra_proc_delay_s": intra_proc_delay_s,
        "cross_proc_delay_s": cross_proc_delay_s,
        "domain_a_timeseries_csv": (str(Path(domain_a_timeseries_csv)) if domain_a_timeseries_csv else None),
        "domain_b_timeseries_csv": (str(Path(domain_b_timeseries_csv)) if domain_b_timeseries_csv else None),
        "cross_timeseries_csv": (str(Path(cross_timeseries_csv)) if cross_timeseries_csv else None),
        "domain_a_position_file": (str(Path(domain_a_position_file)) if domain_a_position_file else None),
        "domain_b_position_file": (str(Path(domain_b_position_file)) if domain_b_position_file else None),
        "enriched_intra_link_count": enriched_intra,
        "fallback_intra_link_count": fallback_intra,
        "total_intra_link_count": len(new_intra),
        "enriched_candidate_window_count": enriched_cross,
        "fallback_candidate_window_count": fallback_cross,
        "total_candidate_window_count": len(new_windows),
    }
    return scenario, dict(scenario.metadata["distance_enrichment"])
