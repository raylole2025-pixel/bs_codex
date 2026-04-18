from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

matplotlib.rcParams["font.sans-serif"] = ["Microsoft YaHei", "SimHei", "Noto Sans CJK SC", "DejaVu Sans"]
matplotlib.rcParams["axes.unicode_minus"] = False


STRATEGY_ORDER = [
    "direct_insert",
    "controlled_preemption",
    "controlled_preemption_two_victim",
    "controlled_preemption_recovery_victim_fallback",
    "controlled_preemption_best_effort",
    "direct_insert_best_effort",
    "blocked",
]

STRATEGY_COLORS = {
    "direct_insert": "#2b8a3e",
    "controlled_preemption": "#1c7ed6",
    "controlled_preemption_two_victim": "#0b7285",
    "controlled_preemption_recovery_victim_fallback": "#f08c00",
    "controlled_preemption_best_effort": "#e67700",
    "direct_insert_best_effort": "#c92a2a",
    "blocked": "#868e96",
}


@dataclass(frozen=True)
class Stage2PlotMetrics:
    label: str
    result_path: Path
    cr_emg: float
    cr_reg: float
    n_preemptions: int
    u_cross: float
    u_all: float
    emergency_total: int
    strategy_counts: dict[str, int]
    failure_class_counts: dict[str, int]
    completion_path_counts: dict[str, int]
    delivered_ratios: list[float]
    arrival_order: list[float]
    strategy_by_task: list[str]
    recovery_event_count: int
    recovered_complete_count: int
    recovered_partial_count: int
    reclaim_count: int
    reuse_fallback_count: int
    reused_victim_count: int
    degraded_regular_count: int


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def resolve_stage2_result_path(path_like: str | Path) -> Path:
    path = Path(path_like)
    if path.is_dir():
        candidate = path / "stage2_result.json"
        if candidate.exists():
            return candidate.resolve()
        raise FileNotFoundError(f"stage2_result.json not found in {path}")
    return path.resolve()


def load_stage2_plot_metrics(path_like: str | Path, label: str | None = None) -> Stage2PlotMetrics:
    result_path = resolve_stage2_result_path(path_like)
    payload = _load_json(result_path)
    metadata = dict(payload.get("metadata") or {})
    insertions = list(metadata.get("emergency_insertions") or [])
    strategy_counts = {
        strategy: sum(1 for item in insertions if str(item.get("strategy")) == strategy)
        for strategy in STRATEGY_ORDER
    }
    delivered_ratios = [
        float(item.get("planned_delivery", 0.0)) / max(float(item.get("data", 0.0)), 1e-9)
        for item in insertions
    ]
    recovery_event_count = len(list(metadata.get("recovery_events") or []))
    recovered_complete_count = int(metadata.get("recovered_regular_completed_count", 0))
    recovered_partial_count = max(recovery_event_count - recovered_complete_count, 0)
    return Stage2PlotMetrics(
        label=label or result_path.parent.name,
        result_path=result_path,
        cr_emg=float(payload.get("cr_emg", 0.0)),
        cr_reg=float(payload.get("cr_reg", 0.0)),
        n_preemptions=int(payload.get("n_preemptions", 0)),
        u_cross=float(payload.get("u_cross", 0.0)),
        u_all=float(payload.get("u_all", 0.0)),
        emergency_total=len(insertions),
        strategy_counts=strategy_counts,
        failure_class_counts={
            str(key): int(value) for key, value in dict(metadata.get("insertion_failure_class_counts") or {}).items()
        },
        completion_path_counts={
            str(key): int(value) for key, value in dict(metadata.get("insertion_completion_path_counts") or {}).items()
        },
        delivered_ratios=delivered_ratios,
        arrival_order=[float(item.get("arrival", 0.0)) for item in insertions],
        strategy_by_task=[str(item.get("strategy", "")) for item in insertions],
        recovery_event_count=recovery_event_count,
        recovered_complete_count=recovered_complete_count,
        recovered_partial_count=recovered_partial_count,
        reclaim_count=sum(1 for item in insertions if bool(item.get("used_recovery_reclaim"))),
        reuse_fallback_count=sum(
            1 for item in insertions if str(item.get("strategy")) == "controlled_preemption_recovery_victim_fallback"
        ),
        reused_victim_count=int(metadata.get("reused_preempted_recoverable_count", 0)),
        degraded_regular_count=len(list(metadata.get("regular_tasks_degraded_by_emergency") or [])),
    )


def _autolabel_bars(ax: plt.Axes, values: list[float], *, decimals: int = 0) -> None:
    for index, value in enumerate(values):
        fmt = f"{{:.{decimals}f}}" if decimals > 0 else "{:.0f}"
        ax.text(index, value, fmt.format(value), ha="center", va="bottom", fontsize=9)


def plot_single_run(metrics: Stage2PlotMetrics, output_dir: str | Path) -> list[Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    generated: list[Path] = []

    strategy_labels = [key for key in STRATEGY_ORDER if metrics.strategy_counts.get(key, 0) > 0]
    strategy_values = [metrics.strategy_counts[key] for key in strategy_labels]
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.bar(
        strategy_labels,
        strategy_values,
        color=[STRATEGY_COLORS.get(key, "#868e96") for key in strategy_labels],
    )
    ax.set_title(f"{metrics.label} Strategy Distribution")
    ax.set_ylabel("Emergency Count")
    ax.tick_params(axis="x", rotation=30)
    _autolabel_bars(ax, strategy_values)
    fig.tight_layout()
    path = out / f"{metrics.label}_strategy_distribution.png"
    fig.savefig(path, dpi=180)
    plt.close(fig)
    generated.append(path)

    fig, ax = plt.subplots(figsize=(10, 5))
    x_values = list(range(1, len(metrics.delivered_ratios) + 1))
    for strategy in STRATEGY_ORDER:
        strategy_points = [
            (index + 1, metrics.delivered_ratios[index])
            for index, current in enumerate(metrics.strategy_by_task)
            if current == strategy
        ]
        if not strategy_points:
            continue
        ax.scatter(
            [item[0] for item in strategy_points],
            [item[1] for item in strategy_points],
            label=strategy,
            color=STRATEGY_COLORS.get(strategy, "#868e96"),
            s=45,
        )
    ax.plot(x_values, metrics.delivered_ratios, color="#495057", linewidth=1.0, alpha=0.6)
    ax.set_ylim(0.0, 1.05)
    ax.set_xlabel("Emergency Arrival Order")
    ax.set_ylabel("Delivered Ratio")
    ax.set_title(f"{metrics.label} Emergency Delivered Ratio")
    if metrics.delivered_ratios:
        ax.legend(fontsize=8, loc="lower left")
    fig.tight_layout()
    path = out / f"{metrics.label}_delivery_timeline.png"
    fig.savefig(path, dpi=180)
    plt.close(fig)
    generated.append(path)

    overview_labels = [
        "preemptions",
        "recovery_events",
        "recovered_complete",
        "recovered_partial",
        "reclaim_used",
        "reuse_fallback",
        "reg_degraded",
    ]
    overview_values = [
        metrics.n_preemptions,
        metrics.recovery_event_count,
        metrics.recovered_complete_count,
        metrics.recovered_partial_count,
        metrics.reclaim_count,
        metrics.reuse_fallback_count,
        metrics.degraded_regular_count,
    ]
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.bar(overview_labels, overview_values, color="#4263eb")
    ax.set_title(f"{metrics.label} Preemption And Recovery Overview")
    ax.set_ylabel("Count")
    ax.tick_params(axis="x", rotation=25)
    _autolabel_bars(ax, overview_values)
    fig.tight_layout()
    path = out / f"{metrics.label}_preemption_recovery_overview.png"
    fig.savefig(path, dpi=180)
    plt.close(fig)
    generated.append(path)

    return generated


def plot_comparison(metrics_list: list[Stage2PlotMetrics], output_dir: str | Path, comparison_name: str = "comparison") -> list[Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    generated: list[Path] = []
    labels = [item.label for item in metrics_list]

    fig, ax = plt.subplots(figsize=(10, 5))
    x_positions = list(range(len(labels)))
    width = 0.35
    emg_values = [item.cr_emg for item in metrics_list]
    reg_values = [item.cr_reg for item in metrics_list]
    ax.bar([x - width / 2 for x in x_positions], emg_values, width=width, label="cr_emg", color="#1c7ed6")
    ax.bar([x + width / 2 for x in x_positions], reg_values, width=width, label="cr_reg", color="#2b8a3e")
    ax.set_xticks(x_positions, labels)
    ax.set_ylim(0.0, 1.05)
    ax.set_ylabel("Completion Rate")
    ax.set_title("Stage2 Completion Rate Comparison")
    ax.legend()
    fig.tight_layout()
    path = out / f"{comparison_name}_completion_rates.png"
    fig.savefig(path, dpi=180)
    plt.close(fig)
    generated.append(path)

    fig, ax = plt.subplots(figsize=(11, 6))
    bottoms = [0] * len(labels)
    for strategy in STRATEGY_ORDER:
        values = [item.strategy_counts.get(strategy, 0) for item in metrics_list]
        if not any(values):
            continue
        ax.bar(
            labels,
            values,
            bottom=bottoms,
            label=strategy,
            color=STRATEGY_COLORS.get(strategy, "#868e96"),
        )
        bottoms = [base + value for base, value in zip(bottoms, values)]
    ax.set_ylabel("Emergency Count")
    ax.set_title("Stage2 Strategy Mix Comparison")
    ax.legend(fontsize=8, loc="upper right")
    fig.tight_layout()
    path = out / f"{comparison_name}_strategy_mix.png"
    fig.savefig(path, dpi=180)
    plt.close(fig)
    generated.append(path)

    fig, ax = plt.subplots(figsize=(11, 6))
    metric_names = [
        "n_preemptions",
        "recovery_event_count",
        "recovered_complete_count",
        "reclaim_count",
        "reuse_fallback_count",
        "degraded_regular_count",
    ]
    metric_labels = [
        "preemptions",
        "recovery_events",
        "recovered_complete",
        "reclaim_used",
        "reuse_fallback",
        "reg_degraded",
    ]
    width = 0.12
    offsets = [index - (len(metric_names) - 1) / 2 for index in range(len(metric_names))]
    for offset, metric_name, metric_label in zip(offsets, metric_names, metric_labels):
        values = [float(getattr(item, metric_name)) for item in metrics_list]
        ax.bar(
            [x + offset * width for x in x_positions],
            values,
            width=width,
            label=metric_label,
        )
    ax.set_xticks(x_positions, labels)
    ax.set_ylabel("Count")
    ax.set_title("Stage2 Preemption And Recovery Comparison")
    ax.legend(fontsize=8, loc="upper right")
    fig.tight_layout()
    path = out / f"{comparison_name}_preemption_recovery.png"
    fig.savefig(path, dpi=180)
    plt.close(fig)
    generated.append(path)

    summary_payload = {
        "comparison_name": comparison_name,
        "runs": [
            {
                "label": item.label,
                "result_path": str(item.result_path),
                "cr_emg": item.cr_emg,
                "cr_reg": item.cr_reg,
                "n_preemptions": item.n_preemptions,
                "strategy_counts": dict(item.strategy_counts),
                "failure_class_counts": dict(item.failure_class_counts),
                "completion_path_counts": dict(item.completion_path_counts),
            }
            for item in metrics_list
        ],
        "generated_files": [str(path) for path in generated],
    }
    summary_path = out / f"{comparison_name}_plot_manifest.json"
    summary_path.write_text(json.dumps(summary_payload, indent=2, ensure_ascii=False), encoding="utf-8")
    generated.append(summary_path)
    return generated
