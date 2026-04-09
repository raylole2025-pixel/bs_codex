"""从预处理拓扑产物构建阶段 1 场景模板。

这个脚本承上启下：

- 上游输入是 `apps/preprocess_stk_access.py` 生成的拓扑资产；
- 下游输出是可直接被阶段 1 / 批量实验脚本加载的基础场景 JSON。

它还支持可选地根据 STK 的多星 LLA 位置文件自动构建 A 域热点区域，
从而把“热点覆盖”纳入阶段 1 的评价指标。
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

# 兼容直接执行脚本文件的场景。
if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from bs3.hotspot_builder import build_hotspots_from_multisat_lla, write_hotspot_summary
from bs3.stage1_template_builder import build_stage1_scenario_template


def main() -> None:
    """读取预处理结果并导出基础场景模板。"""

    parser = argparse.ArgumentParser(description="Build a stage1 scenario template from STK preprocess outputs.")
    parser.add_argument("--preprocess-dir", required=True, help="Directory created by apps/preprocess_stk_access.py")
    parser.add_argument("--output", default="inputs/templates/stage1_scenario_template.json", help="Output scenario json path")
    parser.add_argument("--tasks-json", help="Optional task json file (list or {'tasks': [...]})")
    parser.add_argument("--cap-a", type=float, default=600.0, help="Default A-domain link capacity in Mbps")
    parser.add_argument("--cap-b", type=float, default=2000.0, help="Default B-domain link capacity in Mbps")
    parser.add_argument("--cap-x", type=float, default=1000.0, help="Default cross-domain link capacity in Mbps")
    parser.add_argument("--theta", type=float, default=None, help="Deprecated and ignored in Stage1 4.8; feasibility now uses FR=1")
    parser.add_argument("--theta-sr", type=float, default=None, help="Deprecated and ignored in Stage1 4.8; feasibility now uses FR=1")
    parser.add_argument("--theta-cap", type=float, default=0.08, help="Stage1 cross-capacity shortfall threshold")
    parser.add_argument("--theta-hot", type=float, default=0.80, help="Stage1 hotspot coverage threshold")
    parser.add_argument("--theta-c", type=float, default=None, help="Deprecated and ignored in Stage1 4.8; feasibility now uses FR=1")
    parser.add_argument("--rho", type=float, default=0.20, help="Reserved cross-domain capacity ratio")
    parser.add_argument("--t-pre", type=float, default=1800.0, help="Cross-link preheat time in seconds")
    parser.add_argument("--d-min", type=float, default=600.0, help="Minimum effective cross-link duration in seconds")
    parser.add_argument("--hot-hop-limit", type=int, default=4, help="Maximum A-domain hop count from hotspot to active gateway")
    parser.add_argument("--alpha", type=float, default=0.85, help="Stage1 bottleneck trigger factor for enabling backup path")
    parser.add_argument("--eta-x", type=float, default=0.90, help="Near-best transmitted-data threshold for final path selection")
    parser.add_argument("--snapshot-seconds", type=int, default=600, help="Static-value coarse preprocessing step in seconds")
    parser.add_argument("--q-eval", type=int, default=4, help="Evaluate regular satisfaction every q accepted windows")
    parser.add_argument("--omega-fr", "--omega-sr", dest="omega_fr", type=float, default=4.0 / 9.0, help="Violation aggregation weight for FR shortfall")
    parser.add_argument("--omega-cap", type=float, default=3.0 / 9.0, help="Violation aggregation weight for capacity shortfall")
    parser.add_argument("--omega-hot", type=float, default=2.0 / 9.0, help="Violation aggregation weight for hotspot shortfall")
    parser.add_argument("--elite-prune-count", type=int, default=6, help="Number of elite feasible candidates to prune each round")
    parser.add_argument("--domain-a-lla-position", help="STK multi-satellite LLA Position export for building default A-domain hotspots")
    parser.add_argument("--hotspot-summary-output", help="Optional JSON path for hotspot construction summary")
    parser.add_argument("--scenario-name", default="stage1-from-stk-preprocess", help="Scenario name stored in metadata")
    args = parser.parse_args()

    hotspots_a = None
    metadata_updates = None
    # 如果给了 LLA 位置文件，就顺手计算默认热点区域，
    # 并把热点构建摘要附加到场景 metadata 中。
    if args.domain_a_lla_position:
        hotspots_a, hotspot_summary = build_hotspots_from_multisat_lla(args.domain_a_lla_position)
        metadata_updates = {"hotspot_generation": hotspot_summary}
        if args.hotspot_summary_output:
            write_hotspot_summary(args.hotspot_summary_output, hotspot_summary)

    # 真正的模板拼装逻辑在 `bs3.stage1_template_builder` 中实现；
    # 这个入口主要负责收集参数和输出摘要。
    payload = build_stage1_scenario_template(
        preprocess_dir=args.preprocess_dir,
        output_path=args.output,
        tasks_path=args.tasks_json,
        capacities={"A": args.cap_a, "B": args.cap_b, "X": args.cap_x},
        stage1_config={
            "theta_cap": args.theta_cap,
            "theta_hot": args.theta_hot,
            "rho": args.rho,
            "t_pre": args.t_pre,
            "d_min": args.d_min,
            "hot_hop_limit": args.hot_hop_limit,
            "bottleneck_factor_alpha": args.alpha,
            "eta_x": args.eta_x,
            "static_value_snapshot_seconds": args.snapshot_seconds,
            "q_eval": args.q_eval,
            "omega_fr": args.omega_fr,
            "omega_cap": args.omega_cap,
            "omega_hot": args.omega_hot,
            "elite_prune_count": args.elite_prune_count,
        },
        hotspots_a=hotspots_a,
        metadata_updates=metadata_updates,
        scenario_name=args.scenario_name,
    )

    # 命令行只输出最关键的规模信息，避免把大型 JSON 直接打印到终端。
    compact = {
        "output": str(Path(args.output).resolve()),
        "node_count_A": len(payload["nodes"]["A"]),
        "node_count_B": len(payload["nodes"]["B"]),
        "intra_link_count": len(payload["intra_domain_links"]),
        "candidate_window_count": len(payload["candidate_windows"]),
        "task_count": len(payload["tasks"]),
        "hotspot_count_A": len(((payload.get("hotspots") or {}).get("A") or [])),
        "precomputed_hops": payload["metadata"]["precomputed_hops"],
        "stage1": payload["stage1"],
    }
    print(json.dumps(compact, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
