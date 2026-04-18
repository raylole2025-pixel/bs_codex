from __future__ import annotations

import argparse
from collections import defaultdict
import json
import math
import re
import sys
import time
import zipfile
import xml.etree.ElementTree as ET
from copy import deepcopy
from dataclasses import asdict
from pathlib import Path
from typing import Any

# 这个脚本是项目里最重要的批量实验入口之一。
# 它负责把“工作簿任务集 -> 独立场景 -> 阶段 1/阶段 2 结果”整条链路串起来，
# 因此这里会保留比普通脚本更详细的中文说明。
#
# 主要流程如下：
# 1. 读取 xlsx 工作簿中的 README 和多个任务集 sheet；
# 2. 为每个 sheet 生成任务 JSON 与实验场景；
# 3. 可选地补充距离/时延，并进行静态价值注释与候选窗口筛选；
# 4. 运行阶段 1，必要时继续执行完整 pipeline；
# 5. 输出中间场景、结果 JSON、图表工件和批量摘要。

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from bs3.distance_enrichment import enrich_scenario_distances
from bs3.models import Stage2Config
from bs3.pipeline import run_pipeline
from bs3.scenario import load_scenario, scenario_to_dict
from bs3.stage1_candidate_pool import screen_candidate_windows
from bs3.stage1 import activation_count, gateway_count, run_stage1
from bs3.stage1_visualization import export_stage1_run_artifacts
from bs3.stage1_window_values import annotate_scenario_candidate_values

NS = {
    "a": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
    "r": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
}

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_BASE_SCENARIO = PROJECT_ROOT / "inputs" / "templates" / "stage1_scenario_template.json"
DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "results" / "generated" / "stage1_taskset_runs"
DEFAULT_DISTANCE_ROOT = PROJECT_ROOT / "mydata" / "distances"
DEFAULT_DOMAIN_A_TIMESERIES = DEFAULT_DISTANCE_ROOT / "domain1_isl_distance_20260323" / "domain1_isl_distance_timeseries.csv"
DEFAULT_DOMAIN_B_TIMESERIES = DEFAULT_DISTANCE_ROOT / "domain2_isl_distance_20260323" / "domain2_isl_distance_timeseries.csv"
DEFAULT_CROSS_TIMESERIES = DEFAULT_DISTANCE_ROOT / "crosslink_distance_20260323" / "crosslink_distance_timeseries.csv"
DEFAULT_DOMAIN_A_SUMMARY = DEFAULT_DISTANCE_ROOT / "domain1_isl_distance_20260323" / "domain1_isl_pair_summary.csv"
DEFAULT_DOMAIN_B_SUMMARY = DEFAULT_DISTANCE_ROOT / "domain2_isl_distance_20260323" / "domain2_isl_pair_summary.csv"
DEFAULT_CROSS_SUMMARY = DEFAULT_DISTANCE_ROOT / "crosslink_distance_20260323" / "crosslink_pair_summary.csv"


def _float(value: Any) -> float:
    """把工作簿中读出的数值统一转换成浮点数。"""

    return float(value)


def _col_to_index(label: str) -> int:
    """把 Excel 列标识（如 A、AB）转换成 0 基列号。"""

    idx = 0
    for ch in label:
        if ch.isalpha():
            idx = idx * 26 + (ord(ch.upper()) - 64)
    return idx - 1


def read_xlsx_sheets(path: Path) -> dict[str, list[list[str]]]:
    """用轻量方式读取 xlsx 工作簿的所有 sheet。

    这里没有依赖 `openpyxl`，而是直接解析 xlsx 压缩包里的 XML，
    这样能减少依赖，也足够满足项目里“只读表格内容”的需求。
    """

    sheets: dict[str, list[list[str]]] = {}
    with zipfile.ZipFile(path) as zf:
        shared_strings: list[str] = []
        if "xl/sharedStrings.xml" in zf.namelist():
            # xlsx 会把重复字符串集中存到 sharedStrings 表，
            # 后面读取单元格时需要按索引回查这里的内容。
            root = ET.fromstring(zf.read("xl/sharedStrings.xml"))
            for si in root.findall("a:si", NS):
                parts = [node.text or "" for node in si.iterfind(".//a:t", NS)]
                shared_strings.append("".join(parts))

        workbook = ET.fromstring(zf.read("xl/workbook.xml"))
        rels = ET.fromstring(zf.read("xl/_rels/workbook.xml.rels"))
        rel_map = {rel.attrib["Id"]: rel.attrib["Target"].lstrip("/") for rel in rels}

        for sheet in workbook.find("a:sheets", NS):
            name = sheet.attrib["name"]
            rel_id = sheet.attrib["{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id"]
            target = rel_map[rel_id]
            root = ET.fromstring(zf.read(target))
            rows: list[list[str]] = []
            for row in root.findall(".//a:sheetData/a:row", NS):
                values: dict[int, str] = {}
                for cell in row.findall("a:c", NS):
                    ref = cell.attrib.get("r", "")
                    match = re.match(r"([A-Z]+)(\d+)", ref)
                    if not match:
                        continue
                    idx = _col_to_index(match.group(1))
                    cell_type = cell.attrib.get("t")
                    if cell_type == "inlineStr":
                        value = "".join(node.text or "" for node in cell.iterfind(".//a:t", NS))
                    else:
                        raw = cell.find("a:v", NS)
                        if raw is None:
                            value = ""
                        elif cell_type == "s":
                            value = shared_strings[int(raw.text)]
                        else:
                            value = raw.text or ""
                    values[idx] = value
                if values:
                    # 用当前行出现过的最大列号补齐缺失单元格，
                    # 避免后面按表头位置索引时发生列错位。
                    max_idx = max(values)
                    rows.append([values.get(i, "") for i in range(max_idx + 1)])
            sheets[name] = rows
    return sheets


def read_task_sets(path: Path) -> tuple[list[tuple[str, str]], dict[str, list[dict[str, str]]]]:
    """把工作簿拆成 README 区和多个任务集 sheet。"""

    sheet_rows = read_xlsx_sheets(path)
    readme_rows = [(row[0], row[1] if len(row) > 1 else "") for row in sheet_rows.get("README", []) if row]
    task_sets: dict[str, list[dict[str, str]]] = {}
    for name, rows in sheet_rows.items():
        if name == "README" or not rows:
            continue
        header = rows[0]
        items: list[dict[str, str]] = []
        for row in rows[1:]:
            if not any(str(cell).strip() for cell in row):
                continue
            values = row + [""] * (len(header) - len(row))
            items.append({header[idx]: values[idx] for idx in range(len(header))})
        task_sets[name] = items
    return readme_rows, task_sets


def workbook_task_to_payload(row: dict[str, str]) -> dict[str, Any]:
    """把工作簿中的一行任务记录转换成场景任务结构。"""

    task_type = str(row.get("task_type", "reg")).strip().lower() or "reg"
    if task_type not in {"reg", "emg"}:
        task_type = "reg"
    return {
        "id": row["task_id"],
        "src": row["src_sat"],
        "dst": row["dst_sat"],
        "arrival": _float(row["arrival_sec"]),
        "deadline": _float(row["deadline_sec"]),
        "data": _float(row["data_volume_Mb"]),
        "weight": _float(row["priority_weight"]),
        "max_rate": _float(row["b_max_Mbps"]),
        "type": task_type,
        "preemption_priority": _float(row.get("preemption_priority", row["priority_weight"])),
        "task_class": row.get("task_class", ""),
        "arrival_utcg": row.get("arrival_utcg", ""),
        "deadline_utcg": row.get("deadline_utcg", ""),
        "avg_required_Mbps": _float(row["avg_required_Mbps"]) if row.get("avg_required_Mbps") else None,
        "notes": row.get("notes", ""),
    }


def task_stats(tasks: list[dict[str, Any]]) -> dict[str, Any]:
    """统计任务集规模、时间窗长度和并发压力。"""

    if not tasks:
        return {
            "count": 0,
            "total_data_Mb": 0.0,
            "weighted_average_priority": 0.0,
            "peak_concurrency": 0,
            "peak_avg_required_Mbps": 0.0,
            "min_window_s": 0.0,
            "max_window_s": 0.0,
            "mean_window_s": 0.0,
        }

    events: list[tuple[float, int, float]] = []
    durations: list[float] = []
    total_weight = 0.0
    weighted_priority = 0.0
    total_data = 0.0
    for task in tasks:
        arrival = float(task["arrival"])
        deadline = float(task["deadline"])
        avg_required = float(task.get("avg_required_Mbps") or (task["data"] / max(deadline - arrival, 1e-9)))
        duration = deadline - arrival
        durations.append(duration)
        total_data += float(task["data"])
        total_weight += 1.0
        weighted_priority += float(task["weight"])
        # 用扫描线方法估计高峰并发和高峰需求带宽。
        events.append((arrival, 1, avg_required))
        events.append((deadline, -1, avg_required))

    events.sort(key=lambda item: (item[0], -item[1]))
    active = 0
    active_avg = 0.0
    peak_concurrency = 0
    peak_avg = 0.0
    for _, delta, avg_required in events:
        if delta > 0:
            active += 1
            active_avg += avg_required
        else:
            active -= 1
            active_avg -= avg_required
        peak_concurrency = max(peak_concurrency, active)
        peak_avg = max(peak_avg, active_avg)

    return {
        "count": len(tasks),
        "total_data_Mb": total_data,
        "weighted_average_priority": weighted_priority / max(total_weight, 1.0),
        "peak_concurrency": peak_concurrency,
        "peak_avg_required_Mbps": peak_avg,
        "min_window_s": min(durations),
        "max_window_s": max(durations),
        "mean_window_s": sum(durations) / len(durations),
    }


def candidate_to_dict(candidate) -> dict[str, Any]:
    """把阶段 1 候选解展开成可直接写入 JSON 的字典。"""

    data = asdict(candidate)
    data["fr"] = candidate.fr
    data["mean_completion_ratio"] = candidate.mean_completion_ratio
    data["hotspot_coverage"] = data.get("avg_hot_coverage")
    data["hotspot_max_gap"] = data.get("max_hot_gap")
    data["plan"] = [asdict(window) for window in candidate.plan]
    return data


def baseline_trace_to_dict(trace) -> dict[str, Any] | None:
    if trace is None:
        return None
    data = asdict(trace)
    data["allocations"] = [asdict(item) for item in trace.allocations]
    return data


def stage2_result_to_dict(result, t_pre: float) -> dict[str, Any]:
    """把阶段 2 结果补齐派生指标后展开为字典。"""

    data = asdict(result)
    data["gateway_count"] = gateway_count(result.plan)
    data["activation_count"] = activation_count(result.plan, t_pre)
    data["plan"] = [asdict(window) for window in result.plan]
    data["allocations"] = [asdict(item) for item in result.allocations]
    return data


def summarize_stage2_task_outcomes(result, scenario) -> dict[str, Any]:
    """统计阶段 2 对 overall / reg / emg 三个层面的完成效果。"""

    delivered_by_task: dict[str, float] = defaultdict(float)
    for allocation in result.allocations:
        delivered_by_task[allocation.task_id] += float(allocation.delivered)

    def summarize(task_type: str | None) -> dict[str, Any]:
        selected = [
            task
            for task in scenario.tasks
            if task_type is None or task.task_type == task_type
        ]
        if not selected:
            return {
                "count": 0,
                "success_count": 0,
                "failure_count": 0,
                "success_rate": 1.0,
                "mean_completion": 1.0,
                "weighted_completion": 1.0,
                "delivered_Mb": 0.0,
                "total_data_Mb": 0.0,
                "failed_task_ids": [],
            }

        completed = 0
        mean_completion_sum = 0.0
        weighted_completion_sum = 0.0
        total_weight = 0.0
        delivered_total = 0.0
        total_data = 0.0
        failed_task_ids: list[str] = []

        for task in selected:
            delivered = min(float(task.data), max(0.0, delivered_by_task.get(task.task_id, 0.0)))
            completion = delivered / max(float(task.data), 1e-9)
            total_data += float(task.data)
            delivered_total += delivered
            mean_completion_sum += completion
            total_weight += float(task.weight)
            weighted_completion_sum += float(task.weight) * completion
            if float(task.data) - delivered <= max(float(scenario.stage2.completion_tolerance) * float(task.data), 1e-9):
                completed += 1
            else:
                failed_task_ids.append(task.task_id)

        return {
            "count": len(selected),
            "success_count": completed,
            "failure_count": len(selected) - completed,
            "success_rate": completed / len(selected),
            "mean_completion": mean_completion_sum / len(selected),
            "weighted_completion": weighted_completion_sum / max(total_weight, 1e-9),
            "delivered_Mb": delivered_total,
            "total_data_Mb": total_data,
            "failed_task_ids": failed_task_ids,
        }

    return {
        "overall": summarize(None),
        "reg": summarize("reg"),
        "emg": summarize("emg"),
    }


def json_ready_scenario_payload(scenario) -> dict[str, Any]:
    """导出可写盘的场景结构，并去掉运行期缓存。"""

    payload = scenario_to_dict(scenario)
    metadata = dict(payload.get("metadata", {}))
    # `_runtime_cache` 只在当前进程里加速计算，不应该写进持久文件。
    metadata.pop("_runtime_cache", None)
    payload["metadata"] = metadata
    return payload


def build_scenario_payload(
    base_payload: dict[str, Any],
    workbook_path: Path,
    sheet_name: str,
    tasks: list[dict[str, Any]],
    args,
) -> dict[str, Any]:
    """基于基础模板和任务集构造一次独立实验场景。"""

    payload = deepcopy(base_payload)
    payload.setdefault("metadata", {})
    payload["metadata"].update(
        {
            "name": f"stage1-taskset-{sheet_name}",
            "source": workbook_path.name,
            "taskset_workbook": str(workbook_path),
            "taskset_sheet": sheet_name,
            "task_units": {"data": "Mb", "rate": "Mbps"},
            "runner": "apps/run_stage1_workbook_batch.py",
        }
    )
    payload["capacities"] = {"A": args.cap_a, "B": args.cap_b, "X": args.cap_x}
    # 把这次实验使用的参数完整写回场景文件，
    # 这样未来回看结果时，不需要再去猜当时依赖了哪些默认值。
    payload["stage1"] = {
        "theta_cap": args.theta_cap,
        "theta_hot": args.theta_hot,
        "rho": args.rho,
        "t_pre": args.t_pre,
        "d_min": args.d_min,
        "hot_hop_limit": args.hot_hop_limit,
        "bottleneck_factor_alpha": args.alpha,
        "eta_x": args.eta_x,
        "static_value_snapshot_seconds": args.snapshot_seconds,
        "candidate_pool_base_size": args.candidate_pool_base_size,
        "candidate_pool_hot_fraction": args.candidate_pool_hot_fraction,
        "candidate_pool_min_per_coarse_segment": args.candidate_pool_min_per_coarse_segment,
        "candidate_pool_max_additional": args.candidate_pool_max_additional,
        "q_eval": args.q_eval,
        "omega_fr": args.omega_fr,
        "omega_cap": args.omega_cap,
        "omega_hot": args.omega_hot,
        "elite_prune_count": args.elite_prune_count,
        "ga": {
            "population_size": args.population_size,
            "crossover_probability": args.crossover_probability,
            "mutation_probability": args.mutation_probability,
            "max_generations": args.max_generations,
            "stall_generations": args.stall_generations,
            "top_m": args.top_m,
            "max_runtime_seconds": args.max_runtime_seconds,
        },
    }
    stage2_defaults = asdict(Stage2Config(k_paths=args.stage2_k_paths, completion_tolerance=1e-6))
    stage2_overrides = dict(payload.get("stage2", {}))
    for key, value in stage2_overrides.items():
        if key in stage2_defaults:
            stage2_defaults[key] = value
    stage2_defaults["k_paths"] = args.stage2_k_paths
    stage2_defaults["completion_tolerance"] = 1e-6
    payload["stage2"] = stage2_defaults
    payload["tasks"] = tasks
    # 静态价值会在后续重新计算，因此先清空模板中已有的旧值。
    for window in payload.get("candidate_windows", []):
        if "value" in window:
            window["value"] = None
    return payload


def write_json(path: Path, payload: Any) -> None:
    """用 UTF-8 编码写出 JSON 文件。"""

    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def maybe_enrich_with_distances(scenario, args) -> tuple[Any, dict[str, Any] | None]:
    """按命令行配置决定是否执行距离/时延增强。"""

    if args.disable_distance_enrichment:
        return scenario, None

    required = [args.domain_a_timeseries, args.domain_b_timeseries, args.cross_timeseries]
    if any(not item for item in required):
        return scenario, None

    scenario, stats = enrich_scenario_distances(
        scenario,
        domain_a_timeseries_csv=args.domain_a_timeseries,
        domain_b_timeseries_csv=args.domain_b_timeseries,
        cross_timeseries_csv=args.cross_timeseries,
        domain_a_pair_summary_csv=args.domain_a_summary,
        domain_b_pair_summary_csv=args.domain_b_summary,
        cross_pair_summary_csv=args.cross_summary,
        domain_a_position_file=args.domain_a_position,
        domain_b_position_file=args.domain_b_position,
        light_speed_km_per_s=args.light_speed_kmps,
        intra_proc_delay_s=args.intra_proc_delay_sec,
        cross_proc_delay_s=args.cross_proc_delay_sec,
    )
    return scenario, stats


def enforce_cross_distance_limit(scenario, max_cross_distance_km: float | None) -> None:
    """删除超过最大允许距离的跨域候选窗口。"""

    if max_cross_distance_km in {None, 0, 0.0}:
        return

    kept = []
    dropped = 0
    for window in scenario.candidate_windows:
        if window.distance_km is not None and window.distance_km > max_cross_distance_km:
            dropped += 1
            continue
        kept.append(window)
    scenario.candidate_windows = kept
    scenario.metadata.setdefault("stage1_constraints", {})
    scenario.metadata["stage1_constraints"]["max_cross_distance_km"] = max_cross_distance_km
    scenario.metadata["stage1_constraints"]["dropped_candidate_window_count"] = dropped


def main() -> None:
    """批量运行阶段 1 或完整 pipeline。"""

    parser = argparse.ArgumentParser(description="Run stage1 or the full stage1+stage2 pipeline on workbook task sets.")
    parser.add_argument("--workbook", required=True, help="Path to hybrid_regular_task_sets.xlsx")
    parser.add_argument(
        "--base-scenario",
        default=str(DEFAULT_BASE_SCENARIO),
        help="Base scenario template JSON",
    )
    parser.add_argument(
        "--output-root",
        default=str(DEFAULT_OUTPUT_ROOT),
        help="Root folder for this workbook test batch",
    )
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--sheets", nargs="*", default=["medium48", "stress96"])
    parser.add_argument("--cap-a", type=float, default=600.0)
    parser.add_argument("--cap-b", type=float, default=2000.0)
    parser.add_argument("--cap-x", type=float, default=1000.0)
    parser.add_argument("--rho", type=float, default=0.20)
    parser.add_argument("--t-pre", type=float, default=1800.0)
    parser.add_argument("--d-min", type=float, default=600.0)
    parser.add_argument("--theta-cap", dest="theta_cap", type=float, default=0.08)
    parser.add_argument("--theta-hot", type=float, default=0.80)
    parser.add_argument("--hot-hop-limit", type=int, default=4)
    parser.add_argument("--alpha", type=float, default=0.85)
    parser.add_argument("--eta-x", type=float, default=0.90)
    parser.add_argument("--snapshot-seconds", type=int, default=600)
    parser.add_argument("--candidate-pool-base-size", type=int, default=400)
    parser.add_argument("--candidate-pool-hot-fraction", type=float, default=0.30)
    parser.add_argument("--candidate-pool-min-per-coarse-segment", type=int, default=3)
    parser.add_argument("--candidate-pool-max-additional", type=int, default=150)
    parser.add_argument("--q-eval", type=int, default=4)
    parser.add_argument("--omega-fr", dest="omega_fr", type=float, default=4.0 / 9.0)
    parser.add_argument("--omega-cap", type=float, default=3.0 / 9.0)
    parser.add_argument("--omega-hot", type=float, default=2.0 / 9.0)
    parser.add_argument("--elite-prune-count", type=int, default=6)
    parser.add_argument("--population-size", type=int, default=60)
    parser.add_argument("--crossover-probability", type=float, default=0.90)
    parser.add_argument("--mutation-probability", type=float, default=0.20)
    parser.add_argument("--max-generations", type=int, default=100)
    parser.add_argument("--stall-generations", type=int, default=20)
    parser.add_argument("--top-m", type=int, default=5)
    parser.add_argument("--max-runtime-seconds", type=float, default=None)
    parser.add_argument("--run-stage2", action="store_true", help="Run the full pipeline and export stage2 metrics.")
    parser.add_argument("--stage2-k-paths", type=int, default=2)
    parser.add_argument("--skip-artifacts", action="store_true")
    parser.add_argument("--disable-distance-enrichment", action="store_true")
    parser.add_argument(
        "--domain-a-timeseries",
        default=str(DEFAULT_DOMAIN_A_TIMESERIES),
    )
    parser.add_argument(
        "--domain-b-timeseries",
        default=str(DEFAULT_DOMAIN_B_TIMESERIES),
    )
    parser.add_argument(
        "--cross-timeseries",
        default=str(DEFAULT_CROSS_TIMESERIES),
    )
    parser.add_argument(
        "--domain-a-summary",
        default=str(DEFAULT_DOMAIN_A_SUMMARY),
    )
    parser.add_argument(
        "--domain-b-summary",
        default=str(DEFAULT_DOMAIN_B_SUMMARY),
    )
    parser.add_argument(
        "--cross-summary",
        default=str(DEFAULT_CROSS_SUMMARY),
    )
    parser.add_argument("--domain-a-position", help="Optional Domain-A inertial position export")
    parser.add_argument("--domain-b-position", help="Optional Domain-B inertial position export")
    parser.add_argument("--light-speed-kmps", type=float, default=299792.458)
    parser.add_argument("--intra-proc-delay-sec", type=float, default=0.0002)
    parser.add_argument("--cross-proc-delay-sec", type=float, default=0.0010)
    parser.add_argument("--max-cross-distance-km", type=float, default=5000.0)
    args = parser.parse_args()

    workbook_path = Path(args.workbook)
    base_scenario_path = Path(args.base_scenario)
    output_root = Path(args.output_root)
    output_root.mkdir(parents=True, exist_ok=True)

    base_payload = json.loads(base_scenario_path.read_text(encoding="utf-8-sig"))
    readme_rows, task_sets = read_task_sets(workbook_path)

    # README sheet 通常保存任务集说明或建模假设，单独提取便于存档。
    readme_path = output_root / "README_extracted.tsv"
    readme_text = "\n".join(f"{key}\t{value}" for key, value in readme_rows)
    readme_path.write_text(readme_text, encoding="utf-8")

    requested_sheets = [name for name in args.sheets if name in task_sets]
    summary: dict[str, Any] = {
        "workbook": str(workbook_path),
        "base_scenario": str(base_scenario_path),
        "output_root": str(output_root),
        "seed": args.seed,
        "capacities_Mbps": {"A": args.cap_a, "B": args.cap_b, "X": args.cap_x},
        "stage1_params": {
            "fr_required": 1.0,
            "theta_cap": args.theta_cap,
            "theta_hot": args.theta_hot,
            "rho": args.rho,
            "t_pre": args.t_pre,
            "d_min": args.d_min,
            "hot_hop_limit": args.hot_hop_limit,
            "bottleneck_factor_alpha": args.alpha,
            "eta_x": args.eta_x,
            "q_eval": args.q_eval,
            "snapshot_seconds": args.snapshot_seconds,
            "candidate_pool_base_size": args.candidate_pool_base_size,
            "candidate_pool_hot_fraction": args.candidate_pool_hot_fraction,
            "candidate_pool_min_per_coarse_segment": args.candidate_pool_min_per_coarse_segment,
            "candidate_pool_max_additional": args.candidate_pool_max_additional,
            "omega_fr": args.omega_fr,
            "omega_cap": args.omega_cap,
            "omega_hot": args.omega_hot,
            "elite_prune_count": args.elite_prune_count,
            "population_size": args.population_size,
            "crossover_probability": args.crossover_probability,
            "mutation_probability": args.mutation_probability,
            "max_generations": args.max_generations,
            "stall_generations": args.stall_generations,
            "top_m": args.top_m,
            "max_runtime_seconds": args.max_runtime_seconds,
            "skip_artifacts": args.skip_artifacts,
            "distance_enrichment_enabled": not args.disable_distance_enrichment,
            "light_speed_kmps": args.light_speed_kmps,
            "intra_proc_delay_sec": args.intra_proc_delay_sec,
            "cross_proc_delay_sec": args.cross_proc_delay_sec,
            "max_cross_distance_km": args.max_cross_distance_km,
        },
        "run_stage2": args.run_stage2,
        "stage2_params": {
            "k_paths": args.stage2_k_paths,
            "completion_tolerance": 1e-6,
        },
        "readme_file": str(readme_path),
        "runs": [],
    }

    shared_runtime_cache: dict[str, Any] = {}
    can_try_stress = True

    for sheet_name in requested_sheets:
        # 如果较轻的任务集已经失败，默认跳过更重的 stress，
        # 避免一次批量运行消耗过长时间。
        if sheet_name.lower().startswith("stress") and not can_try_stress:
            summary["runs"].append({"sheet": sheet_name, "status": "skipped_due_to_previous_failure"})
            continue

        set_dir = output_root / sheet_name
        set_dir.mkdir(parents=True, exist_ok=True)

        workbook_rows = task_sets[sheet_name]
        tasks = [workbook_task_to_payload(row) for row in workbook_rows]
        stats = task_stats(tasks)

        tasks_path = set_dir / f"{sheet_name}_tasks.json"
        write_json(tasks_path, tasks)

        # 先落盘“原始任务注入后的场景”，再逐步得到 weighted / annotated 版本，
        # 方便逐阶段排查问题。
        raw_payload = build_scenario_payload(base_payload, workbook_path, sheet_name, tasks, args)
        raw_scenario_path = set_dir / f"{sheet_name}_scenario_input.json"
        write_json(raw_scenario_path, raw_payload)

        scenario = load_scenario(raw_scenario_path)
        scenario, enrich_stats = maybe_enrich_with_distances(scenario, args)
        enforce_cross_distance_limit(scenario, args.max_cross_distance_km)
        weighted_scenario_path = None
        if enrich_stats is not None:
            weighted_scenario_path = set_dir / f"{sheet_name}_scenario_weighted.json"
            write_json(weighted_scenario_path, json_ready_scenario_payload(scenario))
        # 共享运行时缓存可以减少重复的图构建和热点可达性计算。
        scenario.metadata["_runtime_cache"] = shared_runtime_cache
        annotate_scenario_candidate_values(scenario, force=True)
        screen_candidate_windows(scenario)

        annotated_scenario_path = set_dir / f"{sheet_name}_scenario_annotated.json"
        write_json(annotated_scenario_path, json_ready_scenario_payload(scenario))

        started = time.perf_counter()
        error_text = None
        pipeline_result = None
        try:
            # `--run-stage2` 为真时运行完整两阶段流程，否则只做阶段 1。
            if args.run_stage2:
                pipeline_result = run_pipeline(scenario, seed=args.seed)
                result = pipeline_result.stage1
            else:
                result = run_stage1(scenario, seed=args.seed)
        except Exception as exc:
            result = None
            error_text = f"{type(exc).__name__}: {exc}"
        elapsed = time.perf_counter() - started

        run_record: dict[str, Any] = {
            "sheet": sheet_name,
            "task_stats": stats,
            "tasks_file": str(tasks_path),
            "scenario_input_file": str(raw_scenario_path),
            "scenario_annotated_file": str(annotated_scenario_path),
            "scenario_weighted_file": (str(weighted_scenario_path) if weighted_scenario_path is not None else None),
            "runtime_seconds": elapsed,
            "status": "failed" if error_text else "completed",
            "distance_enrichment": enrich_stats,
        }

        if result is None:
            run_record["error"] = error_text
            can_try_stress = False
            summary["runs"].append(run_record)
            continue

        # 图表工件导出相对耗时，因此提供跳过选项。
        artifacts = {} if args.skip_artifacts else export_stage1_run_artifacts(scenario, result.best_feasible, set_dir, sheet_name)

        result_payload = {
            "sheet": sheet_name,
            "seed": args.seed,
            "runtime_seconds": elapsed,
            "generations": result.generations,
            "used_feedback": result.used_feedback,
            "timed_out": result.timed_out,
            "elapsed_seconds": result.elapsed_seconds,
            "selected_candidate_index": result.selected_candidate_index,
            "selected_candidate_source": result.selected_candidate_source,
            "selected_plan": [asdict(window) for window in result.selected_plan],
            "baseline_summary": result.baseline_summary,
            "best_feasible": [candidate_to_dict(item) for item in result.best_feasible],
            "population_best": candidate_to_dict(result.population_best) if result.population_best else None,
            "task_stats": stats,
            "stage1_screening": scenario.metadata.get("stage1_screening", {}),
            "artifacts": artifacts,
            "distance_enrichment": enrich_stats,
        }
        baseline_trace_path = set_dir / f"{sheet_name}_baseline_trace.json"
        if result.baseline_trace is not None:
            write_json(baseline_trace_path, baseline_trace_to_dict(result.baseline_trace))
            result_payload["baseline_trace_file"] = str(baseline_trace_path.resolve())
        else:
            result_payload["baseline_trace_file"] = None

        if args.run_stage2 and pipeline_result is not None:
            stage2_results = []
            for idx, stage2_result in enumerate(pipeline_result.stage2_results):
                stage2_payload = stage2_result_to_dict(stage2_result, scenario.stage1.t_pre)
                stage2_payload["candidate_index"] = idx
                stage2_payload["is_recommended"] = stage2_result is pipeline_result.recommended
                stage2_payload["task_outcomes"] = summarize_stage2_task_outcomes(stage2_result, scenario)
                stage2_results.append(stage2_payload)
            result_payload["stage2_results"] = stage2_results
            result_payload["recommended"] = next(
                (item for item in stage2_results if item["is_recommended"]),
                None,
            )

        result_suffix = "pipeline_result" if args.run_stage2 else "stage1_result"
        result_path = set_dir / f"{sheet_name}_{result_suffix}.json"
        write_json(result_path, result_payload)

        run_record["result_file"] = str(result_path)
        run_record["generations"] = result.generations
        run_record["timed_out"] = result.timed_out
        run_record["elapsed_seconds"] = result.elapsed_seconds
        run_record["feasible_count"] = len(result.best_feasible)
        run_record["stage1_screening"] = scenario.metadata.get("stage1_screening", {})
        run_record["artifacts"] = artifacts
        if result.best_feasible:
            best = result.best_feasible[0]
            run_record["best_summary"] = {
                "mean_completion_ratio": best.mean_completion_ratio,
                "fr": best.fr,
                "eta_cap": best.eta_cap,
                "eta_0": best.eta_0,
                "hotspot_coverage": best.hotspot_coverage,
                "hotspot_max_gap": best.hotspot_max_gap,
                "gateway_count": best.gateway_count,
                "window_count": best.window_count,
                "activation_count": best.activation_count,
                "max_cross_gap": best.max_cross_gap,
                "cross_active_fraction": best.cross_active_fraction,
            }
        else:
            can_try_stress = False

        # 推荐阶段 2 解会再整理成扁平摘要，方便批量结果横向比较。
        if args.run_stage2 and result_payload.get("recommended") is not None:
            recommended = result_payload["recommended"]
            recommended_outcomes = recommended["task_outcomes"]
            run_record["recommended_stage2"] = {
                "solver_mode": recommended["solver_mode"],
                "cr_reg": recommended["cr_reg"],
                "cr_emg": recommended["cr_emg"],
                "success_rate_overall": recommended_outcomes["overall"]["success_rate"],
                "success_count_overall": recommended_outcomes["overall"]["success_count"],
                "failure_count_overall": recommended_outcomes["overall"]["failure_count"],
                "success_rate_reg": recommended_outcomes["reg"]["success_rate"],
                "success_rate_emg": recommended_outcomes["emg"]["success_rate"],
                "n_preemptions": recommended["n_preemptions"],
                "u_cross": recommended["u_cross"],
                "u_all": recommended["u_all"],
                "gateway_count": recommended["gateway_count"],
                "activation_count": recommended["activation_count"],
                **dict(recommended.get("metadata", {}) or {}),
            }

        summary["runs"].append(run_record)

    summary_path = output_root / "batch_summary.json"
    write_json(summary_path, summary)
    print(json.dumps(summary, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()

