from __future__ import annotations

import csv
import math
from pathlib import Path

import matplotlib
import networkx as nx

matplotlib.use("Agg")
import matplotlib.pyplot as plt
matplotlib.rcParams["font.sans-serif"] = ["Microsoft YaHei", "SimHei", "Noto Sans CJK SC", "DejaVu Sans"]
matplotlib.rcParams["axes.unicode_minus"] = False

from .models import Scenario, Stage1Candidate
from .scenario import active_cross_links, build_domain_graph, build_segments
from .stage1 import RegularEvaluator


SECONDS_PER_HOUR = 3600.0


def _write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames = list(rows[0].keys())
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _plot_cross_timeline(candidate: Stage1Candidate, output_path: Path) -> None:
    plan = sorted(candidate.plan, key=lambda item: (item.on, item.off, item.window_id))
    if not plan:
        return

    fig_height = max(4.2, 0.72 * len(plan) + 1.8)
    fig, ax = plt.subplots(figsize=(15, fig_height))
    y_positions = list(range(len(plan)))
    labels: list[str] = []

    for idx, window in enumerate(plan):
        start_h = window.on / SECONDS_PER_HOUR
        duration_h = (window.off - window.on) / SECONDS_PER_HOUR
        ax.broken_barh([(start_h, duration_h)], (idx - 0.35, 0.7), facecolors="#1877b7", edgecolors="black", linewidth=0.8)
        labels.append(f"{window.a} -> {window.b} | {window.window_id}")

    ax.set_yticks(y_positions)
    ax.set_yticklabels(labels)
    ax.set_xlabel("时间（小时）")
    ax.set_ylabel("已配置跨域链路")
    ax.set_title("阶段1最优方案跨域链路配置时序图")
    ax.grid(True, axis="x", linestyle="--", alpha=0.35)
    ax.set_xlim(left=0.0)
    fig.tight_layout()
    fig.savefig(output_path, dpi=180, bbox_inches="tight")
    plt.close(fig)


def _plot_capacity_profile(segment_rows: list[dict], output_path: Path) -> None:
    if not segment_rows:
        return

    fig, ax = plt.subplots(figsize=(13, 5.2))
    for row in segment_rows:
        start_h = row["start"] / SECONDS_PER_HOUR
        end_h = row["end"] / SECONDS_PER_HOUR
        demand_req = row.get("demand_req", row.get("demand_agg", 0.0))
        ax.hlines(demand_req, start_h, end_h, colors="#d62728", linewidth=2.2)
        ax.hlines(row["cross_capacity"], start_h, end_h, colors="#2ca02c", linewidth=2.2)
        ax.hlines(row["cross_rate_used"], start_h, end_h, colors="#1f77b4", linewidth=2.2)

    ax.plot([], [], color="#d62728", linewidth=2.2, label="Required Demand Rate")
    ax.plot([], [], color="#2ca02c", linewidth=2.2, label="Available Cross Capacity")
    ax.plot([], [], color="#1f77b4", linewidth=2.2, label="Allocated Cross Rate")
    ax.set_xlabel("Time (hours)")
    ax.set_ylabel("Rate (Mbps)")
    ax.set_title("Stage1 Best Plan: Required Demand vs Cross Capacity")
    ax.grid(True, linestyle="--", alpha=0.35)
    ax.legend(loc="upper right")
    fig.tight_layout()
    fig.savefig(output_path, dpi=180, bbox_inches="tight")
    plt.close(fig)


def _plot_candidate_tradeoff(candidates: list[Stage1Candidate], output_path: Path) -> None:
    if not candidates:
        return

    fig, ax = plt.subplots(figsize=(8.8, 5.6))

    gateway_counts = [item.gateway_count for item in candidates]
    fr_values = [item.fr for item in candidates]
    cross_active = [item.cross_active_fraction for item in candidates]

    scatter = ax.scatter(
        gateway_counts,
        fr_values,
        c=cross_active,
        s=180,
        cmap="viridis",
        edgecolors="black",
        alpha=0.92,
    )
    for idx, item in enumerate(candidates, start=1):
        ax.annotate(
            f"R{idx}",
            (item.gateway_count, item.fr),
            textcoords="offset points",
            xytext=(5, 4),
        )

    ax.set_xlabel("Gateway Count G(S)")
    ax.set_ylabel("FR")
    ax.set_ylim(-0.02, 1.05)
    ax.set_title("Candidate Tradeoff: Gateway Count vs FR")
    ax.grid(True, linestyle="--", alpha=0.35)
    fig.colorbar(scatter, ax=ax, label="cross_active_fraction")

    fig.tight_layout()
    fig.savefig(output_path, dpi=180, bbox_inches="tight")
    plt.close(fig)


def _plot_task_completion(task_rows: list[dict], output_path: Path) -> None:
    if not task_rows:
        return

    ordered = sorted(task_rows, key=lambda row: (row.get("completion_ratio", 0.0), row.get("task_id", "")), reverse=True)
    labels = [row["task_id"] for row in ordered]
    values = [row.get("completion_ratio", 0.0) for row in ordered]
    colors = [
        "#2ca02c"
        if bool(row.get("completed", 0))
        else "#ff7f0e"
        if value + 1e-9 >= 0.8
        else "#d62728"
        for row, value in zip(ordered, values)
    ]

    fig_height = max(4.5, 0.26 * len(labels) + 1.8)
    fig, ax = plt.subplots(figsize=(12.5, fig_height))
    y = list(range(len(labels)))
    ax.barh(y, values, color=colors, edgecolor="black", alpha=0.9)
    ax.axvline(1.0, color="#2ca02c", linestyle="--", linewidth=1.5, label="Full-Completion Threshold")
    ax.axvline(0.80, color="#ff7f0e", linestyle=":", linewidth=1.5, label="0.80 Reference")
    ax.set_yticks(y)
    ax.set_yticklabels(labels)
    ax.invert_yaxis()
    ax.set_xlim(0.0, 1.05)
    ax.set_xlabel("Completion Ratio")
    ax.set_ylabel("Task")
    ax.set_title("Stage1 Best Plan: Task Completion Ratios")
    ax.grid(True, axis="x", linestyle="--", alpha=0.35)
    ax.legend(loc="lower right")
    fig.tight_layout()
    fig.savefig(output_path, dpi=180, bbox_inches="tight")
    plt.close(fig)


def _plot_cross_link_requirement(segment_rows: list[dict], output_path: Path) -> None:
    if not segment_rows:
        return

    fig, ax = plt.subplots(figsize=(13, 5.2))
    for row in segment_rows:
        start_h = row["start"] / SECONDS_PER_HOUR
        end_h = row["end"] / SECONDS_PER_HOUR
        ax.hlines(row.get("cross_link_count", 0), start_h, end_h, colors="#1f77b4", linewidth=2.2)
        ax.hlines(row.get("required_cross_link_count", 0), start_h, end_h, colors="#d62728", linewidth=2.2)

    ax.plot([], [], color="#1f77b4", linewidth=2.2, label="Actual Active Cross-Link Count")
    ax.plot([], [], color="#d62728", linewidth=2.2, label="Required Cross-Link Count")
    ax.set_xlabel("Time (hours)")
    ax.set_ylabel("Cross-Link Count")
    ax.set_title("Stage1 Best Plan: Actual vs Required Cross-Link Count")
    ax.grid(True, linestyle="--", alpha=0.35)
    ax.legend(loc="upper right")
    fig.tight_layout()
    fig.savefig(output_path, dpi=180, bbox_inches="tight")
    plt.close(fig)


def _collect_hotspot_diagnostics(scenario: Scenario, candidate: Stage1Candidate) -> tuple[list[dict], list[dict]]:
    if not scenario.hotspots_a:
        return [], []

    hop_limit = scenario.stage1.hot_hop_limit
    reach_cache = scenario.metadata.setdefault("_runtime_cache", {}).setdefault("hotspot_reachability_visualization", {})
    region_stats = {
        region.region_id: {
            "region_id": region.region_id,
            "weight": region.weight,
            "active_duration_sec": 0.0,
            "covered_duration_sec": 0.0,
        }
        for region in scenario.hotspots_a
    }
    segment_rows: list[dict] = []

    for segment in build_segments(scenario, candidate.plan, []):
        if segment.duration <= 0.0:
            continue

        active_windows = active_cross_links(candidate.plan, segment.start)
        gateways = {window.a for window in active_windows}
        graph_a = build_domain_graph(scenario, "A", segment.start) if active_windows else None
        active_weight = 0.0
        covered_weight = 0.0
        active_region_count = 0
        covered_region_count = 0

        for region in scenario.hotspots_a:
            nodes = region.active_nodes(segment.start)
            if not nodes:
                continue

            active_region_count += 1
            active_weight += region.weight
            region_stats[region.region_id]["active_duration_sec"] += segment.duration

            covered = 0
            if gateways and graph_a is not None:
                cache_key = (float(segment.start), tuple(nodes), int(hop_limit))
                reachable_gateways = reach_cache.get(cache_key)
                if reachable_gateways is None:
                    reachable: set[str] = set()
                    for node in nodes:
                        if node not in graph_a:
                            continue
                        hop_map = nx.single_source_shortest_path_length(graph_a, node, cutoff=hop_limit)
                        reachable.update(hop_map)
                    reachable_gateways = frozenset(reachable)
                    reach_cache[cache_key] = reachable_gateways
                if gateways.intersection(reachable_gateways):
                    covered = 1

            if covered:
                covered_region_count += 1
                covered_weight += region.weight
                region_stats[region.region_id]["covered_duration_sec"] += segment.duration

        segment_rows.append(
            {
                "segment_index": segment.index,
                "start": segment.start,
                "end": segment.end,
                "duration": segment.duration,
                "active_gateway_count": len(gateways),
                "active_region_count": active_region_count,
                "covered_region_count": covered_region_count,
                "active_weight": active_weight,
                "covered_weight": covered_weight,
                "normalized_coverage": (covered_weight / active_weight) if active_weight > 0.0 else None,
            }
        )

    region_rows: list[dict] = []
    for region in scenario.hotspots_a:
        stats = region_stats[region.region_id]
        active_duration = stats["active_duration_sec"]
        covered_duration = stats["covered_duration_sec"]
        region_rows.append(
            {
                "region_id": region.region_id,
                "weight": region.weight,
                "active_duration_sec": active_duration,
                "covered_duration_sec": covered_duration,
                "active_duration_h": active_duration / SECONDS_PER_HOUR,
                "covered_duration_h": covered_duration / SECONDS_PER_HOUR,
                "coverage_given_active": (covered_duration / active_duration) if active_duration > 0.0 else None,
            }
        )

    return segment_rows, region_rows


def _plot_hotspot_profile(segment_rows: list[dict], output_path: Path) -> None:
    if not segment_rows:
        return

    fig, (ax_top, ax_bottom) = plt.subplots(
        2,
        1,
        figsize=(13, 7.2),
        sharex=True,
        gridspec_kw={"height_ratios": [2.0, 1.0]},
    )

    for row in segment_rows:
        start_h = row["start"] / SECONDS_PER_HOUR
        end_h = row["end"] / SECONDS_PER_HOUR
        ax_top.hlines(100.0 * row["active_weight"], start_h, end_h, colors="#d62728", linewidth=2.2)
        ax_top.hlines(100.0 * row["covered_weight"], start_h, end_h, colors="#1f77b4", linewidth=2.2)
        normalized = row.get("normalized_coverage")
        if normalized is not None:
            ax_bottom.hlines(100.0 * normalized, start_h, end_h, colors="#2ca02c", linewidth=2.2)
        else:
            ax_bottom.hlines(math.nan, start_h, end_h, colors="#2ca02c", linewidth=2.2)

    ax_top.plot([], [], color="#d62728", linewidth=2.2, label="Active Hotspot Weight")
    ax_top.plot([], [], color="#1f77b4", linewidth=2.2, label="Covered Hotspot Weight")
    ax_top.set_ylabel("Weight (%)")
    ax_top.set_title("Hotspot Activity vs Covered Weight")
    ax_top.grid(True, linestyle="--", alpha=0.35)
    ax_top.legend(loc="upper right")

    ax_bottom.axhline(80.0, color="#ff7f0e", linestyle="--", linewidth=1.5, label="Theta_hot = 80%")
    ax_bottom.set_xlabel("Time (hours)")
    ax_bottom.set_ylabel("Coverage (%)")
    ax_bottom.set_ylim(-2.0, 105.0)
    ax_bottom.set_title("Hotspot Coverage Normalized Over Active Periods")
    ax_bottom.grid(True, linestyle="--", alpha=0.35)
    ax_bottom.legend(loc="lower right")

    fig.tight_layout()
    fig.savefig(output_path, dpi=180, bbox_inches="tight")
    plt.close(fig)


def _plot_hotspot_region_breakdown(region_rows: list[dict], output_path: Path) -> None:
    if not region_rows:
        return

    ordered = sorted(region_rows, key=lambda row: row["region_id"])
    labels = [row["region_id"] for row in ordered]
    active_h = [row["active_duration_h"] for row in ordered]
    covered_h = [row["covered_duration_h"] for row in ordered]
    coverage_labels = [
        "n/a" if row["coverage_given_active"] is None else f"{100.0 * row['coverage_given_active']:.1f}%"
        for row in ordered
    ]

    fig, ax = plt.subplots(figsize=(10.8, 5.6))
    y = list(range(len(labels)))
    ax.barh(y, active_h, color="#f4a261", edgecolor="black", alpha=0.85, label="Active Duration")
    ax.barh(y, covered_h, color="#2a9d8f", edgecolor="black", alpha=0.9, label="Covered Duration")
    for idx, (active, covered, text) in enumerate(zip(active_h, covered_h, coverage_labels)):
        ax.text(max(active, covered) + 0.05, idx, f"cov={text}", va="center", fontsize=9)

    ax.set_yticks(y)
    ax.set_yticklabels(labels)
    ax.invert_yaxis()
    ax.set_xlabel("Hours")
    ax.set_ylabel("Hotspot Region")
    ax.set_title("Hotspot Breakdown: Active vs Covered Duration")
    ax.grid(True, axis="x", linestyle="--", alpha=0.35)
    ax.legend(loc="lower right")
    fig.tight_layout()
    fig.savefig(output_path, dpi=180, bbox_inches="tight")
    plt.close(fig)


def _plot_solution_scorecard(candidate: Stage1Candidate, output_path: Path) -> None:
    metric_labels = ["FR", "MR", "Hotspot Cov.", "1 - Eta_cap"]
    metric_values = [
        100.0 * candidate.fr,
        100.0 * candidate.mean_completion_ratio,
        100.0 * candidate.avg_hot_coverage,
        100.0 * (1.0 - candidate.eta_cap),
    ]
    metric_colors = ["#2a9d8f", "#5e60ce", "#1f77b4", "#e9c46a"]

    fig, (ax_bar, ax_text) = plt.subplots(
        1,
        2,
        figsize=(11.5, 4.8),
        gridspec_kw={"width_ratios": [1.6, 1.0]},
    )

    y = list(range(len(metric_labels)))
    ax_bar.barh(y, metric_values, color=metric_colors, edgecolor="black", alpha=0.9)
    for idx, value in enumerate(metric_values):
        ax_bar.text(value + 1.0, idx, f"{value:.1f}%", va="center", fontsize=10)
    ax_bar.set_yticks(y)
    ax_bar.set_yticklabels(metric_labels)
    ax_bar.set_xlim(0.0, 105.0)
    ax_bar.invert_yaxis()
    ax_bar.set_xlabel("Percent")
    ax_bar.set_title("Stage1 Quantitative Scorecard")
    ax_bar.grid(True, axis="x", linestyle="--", alpha=0.35)

    ax_text.axis("off")
    lines = [
        f"Activation Count : {candidate.activation_count}",
        f"Window Count     : {candidate.window_count}",
        f"Gateway Count    : {candidate.gateway_count}",
        f"Max Hot Gap      : {candidate.max_hot_gap / 60.0:.1f} min",
        f"Cross Active     : {100.0 * candidate.cross_active_fraction:.1f}%",
        f"Violation        : {candidate.violation:.4f}",
    ]
    ax_text.text(
        0.02,
        0.95,
        "\n".join(lines),
        va="top",
        ha="left",
        fontsize=11,
        family="monospace",
    )

    fig.tight_layout()
    fig.savefig(output_path, dpi=180, bbox_inches="tight")
    plt.close(fig)


def _plot_ga_convergence(history_rows: list[dict], output_path: Path) -> None:
    if not history_rows:
        return

    generations = [int(row["generation"]) for row in history_rows]
    violation = [
        float(row["population_best_violation"]) if row.get("population_best_violation") not in {"", None} else math.nan
        for row in history_rows
    ]
    hotspot_pct = [
        100.0 * float(row["population_best_avg_hot_coverage"]) if row.get("population_best_avg_hot_coverage") not in {"", None} else math.nan
        for row in history_rows
    ]
    eta_cap_pct = [
        100.0 * float(row["population_best_eta_cap"]) if row.get("population_best_eta_cap") not in {"", None} else math.nan
        for row in history_rows
    ]
    feasible_archive = [int(row.get("feasible_archive_size", 0) or 0) for row in history_rows]

    first_feasible = next((idx for idx, value in enumerate(feasible_archive) if value > 0), None)

    fig, (ax_top, ax_bottom) = plt.subplots(
        2,
        1,
        figsize=(11.8, 7.0),
        sharex=True,
        gridspec_kw={"height_ratios": [1.0, 1.0]},
    )

    ax_top.plot(generations, violation, color="#d62728", marker="o", linewidth=2.0, label="Best Violation")
    ax_top.set_ylabel("Violation")
    ax_top.grid(True, linestyle="--", alpha=0.35)
    ax_top.legend(loc="upper right")
    ax_top_right = ax_top.twinx()
    ax_top_right.step(generations, feasible_archive, where="mid", color="#1f77b4", linewidth=2.0, label="Feasible Archive Size")
    ax_top_right.set_ylabel("Feasible Count")
    if first_feasible is not None:
        gen = generations[first_feasible]
        ax_top.axvline(gen, color="#2ca02c", linestyle="--", linewidth=1.5)
        ax_top.text(gen + 0.2, max(v for v in violation if not math.isnan(v)) * 0.92, f"first feasible: g={gen}", color="#2ca02c")

    ax_bottom.plot(generations, hotspot_pct, color="#2a9d8f", marker="o", linewidth=2.0, label="Best Hotspot Coverage")
    ax_bottom.plot(generations, eta_cap_pct, color="#ff7f0e", marker="s", linewidth=2.0, label="Best Eta_cap")
    ax_bottom.axhline(80.0, color="#2a9d8f", linestyle="--", linewidth=1.2, alpha=0.7)
    ax_bottom.axhline(8.0, color="#ff7f0e", linestyle="--", linewidth=1.2, alpha=0.7)
    ax_bottom.set_xlabel("Generation")
    ax_bottom.set_ylabel("Percent")
    ax_bottom.set_ylim(-2.0, 105.0)
    ax_bottom.grid(True, linestyle="--", alpha=0.35)
    ax_bottom.legend(loc="center right")
    ax_bottom.set_title("GA Convergence: Coverage and Capacity Gap")

    fig.tight_layout()
    fig.savefig(output_path, dpi=180, bbox_inches="tight")
    plt.close(fig)


def export_stage1_run_artifacts(
    scenario: Scenario,
    candidates: list[Stage1Candidate],
    output_dir: str | Path,
    prefix: str,
    history_rows: list[dict] | None = None,
) -> dict[str, str]:
    output_root = Path(output_dir)
    output_root.mkdir(parents=True, exist_ok=True)
    artifacts: dict[str, str] = {}
    if not candidates:
        return artifacts

    best = candidates[0]
    evaluator = RegularEvaluator(scenario)
    trace = evaluator.trace(best.plan)

    timeline_rows = [
        {
            "rank": idx,
            "window_id": window.window_id,
            "a": window.a,
            "b": window.b,
            "on": window.on,
            "off": window.off,
            "duration": window.off - window.on,
        }
        for idx, window in enumerate(sorted(best.plan, key=lambda item: (item.on, item.off, item.window_id)), start=1)
    ]
    candidate_rows = [
        {
            "rank": idx,
            "gateway_count": candidate.gateway_count,
            "window_count": candidate.window_count,
            "activation_count": candidate.activation_count,
            "activation_time": candidate.activation_time,
            "mean_completion_ratio": candidate.mean_completion_ratio,
            "fr": candidate.fr,
            "eta_cap": candidate.eta_cap,
            "eta_0": candidate.eta_0,
            "hotspot_coverage": candidate.hotspot_coverage,
            "hotspot_max_gap": candidate.hotspot_max_gap,
            "max_cross_gap": candidate.max_cross_gap,
            "cross_active_fraction": candidate.cross_active_fraction,
        }
        for idx, candidate in enumerate(candidates, start=1)
    ]

    timeline_csv = output_root / f"{prefix}_best_cross_timeline.csv"
    capacity_csv = output_root / f"{prefix}_best_capacity_profile.csv"
    link_count_csv = output_root / f"{prefix}_best_cross_link_requirement.csv"
    task_csv = output_root / f"{prefix}_best_task_completion.csv"
    candidate_csv = output_root / f"{prefix}_candidate_tradeoff.csv"
    hotspot_profile_csv = output_root / f"{prefix}_hotspot_profile.csv"
    hotspot_region_csv = output_root / f"{prefix}_hotspot_region_breakdown.csv"
    ga_history_csv = output_root / f"{prefix}_ga_history.csv"
    _write_csv(timeline_csv, timeline_rows)
    _write_csv(capacity_csv, trace["segments"])
    _write_csv(link_count_csv, trace["segments"])
    _write_csv(task_csv, trace["tasks"])
    _write_csv(candidate_csv, candidate_rows)
    hotspot_segment_rows, hotspot_region_rows = _collect_hotspot_diagnostics(scenario, best)
    _write_csv(hotspot_profile_csv, hotspot_segment_rows)
    _write_csv(hotspot_region_csv, hotspot_region_rows)
    _write_csv(ga_history_csv, history_rows or [])

    timeline_png = output_root / f"{prefix}_best_cross_timeline.png"
    capacity_png = output_root / f"{prefix}_best_capacity_profile.png"
    link_count_png = output_root / f"{prefix}_best_cross_link_requirement.png"
    tradeoff_png = output_root / f"{prefix}_candidate_tradeoff.png"
    task_png = output_root / f"{prefix}_best_task_completion.png"
    hotspot_profile_png = output_root / f"{prefix}_hotspot_profile.png"
    hotspot_region_png = output_root / f"{prefix}_hotspot_region_breakdown.png"
    solution_scorecard_png = output_root / f"{prefix}_solution_scorecard.png"
    ga_convergence_png = output_root / f"{prefix}_ga_convergence.png"
    _plot_cross_timeline(best, timeline_png)
    _plot_capacity_profile(trace["segments"], capacity_png)
    _plot_cross_link_requirement(trace["segments"], link_count_png)
    _plot_candidate_tradeoff(candidates, tradeoff_png)
    _plot_task_completion(trace["tasks"], task_png)
    _plot_hotspot_profile(hotspot_segment_rows, hotspot_profile_png)
    _plot_hotspot_region_breakdown(hotspot_region_rows, hotspot_region_png)
    _plot_solution_scorecard(best, solution_scorecard_png)
    _plot_ga_convergence(history_rows or [], ga_convergence_png)

    artifacts.update(
        {
            "best_cross_timeline_csv": str(timeline_csv),
            "best_cross_timeline_png": str(timeline_png),
            "best_capacity_profile_csv": str(capacity_csv),
            "best_capacity_profile_png": str(capacity_png),
            "best_cross_link_requirement_csv": str(link_count_csv),
            "best_cross_link_requirement_png": str(link_count_png),
            "best_task_completion_csv": str(task_csv),
            "best_task_completion_png": str(task_png),
            "candidate_tradeoff_csv": str(candidate_csv),
            "candidate_tradeoff_png": str(tradeoff_png),
            "hotspot_profile_csv": str(hotspot_profile_csv),
            "hotspot_profile_png": str(hotspot_profile_png),
            "hotspot_region_breakdown_csv": str(hotspot_region_csv),
            "hotspot_region_breakdown_png": str(hotspot_region_png),
            "solution_scorecard_png": str(solution_scorecard_png),
            "ga_history_csv": str(ga_history_csv),
            "ga_convergence_png": str(ga_convergence_png),
        }
    )
    return artifacts
