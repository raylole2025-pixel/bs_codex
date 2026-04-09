"""阶段 1 预处理入口。

这个脚本把 STK 导出的 access 文本转换成项目内部可复用的拓扑资产，
包括清洗后的接触窗口、快照级统计、hop 矩阵和预处理摘要。
这些产物会作为后续模板构建和批量实验的上游输入。
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

# 兼容直接执行脚本文件的场景。
if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from bs3.stk_access_preprocess import ConstellationSpec, build_stage1_outputs



def main() -> None:
    """读取 STK access 文本并生成阶段 1 预处理产物。"""

    parser = argparse.ArgumentParser(description="Preprocess STK access exports for stage1 data products.")
    parser.add_argument("--chain1", required=True, help="Path to constellation-1 intra-domain access export")
    parser.add_argument("--chain2", required=True, help="Path to constellation-2 intra-domain access export")
    parser.add_argument("--chain3", required=True, help="Path to cross-domain access export")
    parser.add_argument("--output-dir", default="mydata/topology/stage1_preprocess_user", help="Directory for generated outputs")
    parser.add_argument("--snapshot", type=int, default=60, help="Snapshot size in seconds")
    parser.add_argument("--min-intra", type=float, default=60.0, help="Minimum intra-domain duration in seconds")
    parser.add_argument("--min-cross", type=float, default=300.0, help="Minimum cross-domain duration in seconds")
    parser.add_argument("--a-planes", type=int, default=5, help="Plane count of constellation 1")
    parser.add_argument("--a-sats", type=int, default=9, help="Satellites per plane of constellation 1")
    parser.add_argument("--b-planes", type=int, default=8, help="Plane count of constellation 2")
    parser.add_argument("--b-sats", type=int, default=10, help="Satellites per plane of constellation 2")
    args = parser.parse_args()

    # 把命令行给出的星座规模整理成统一的数据结构，
    # 让底层预处理逻辑只依赖 `ConstellationSpec`。
    specs = [
        ConstellationSpec(constellation_id=1, domain="A", planes=args.a_planes, sats_per_plane=args.a_sats, name="RemoteSensingConstellation1"),
        ConstellationSpec(constellation_id=2, domain="B", planes=args.b_planes, sats_per_plane=args.b_sats, name="CommunicationConstellation"),
    ]

    # 真正的接触窗口清洗、快照构建、hop 矩阵导出都在这个函数里完成。
    summary = build_stage1_outputs(
        chain1_path=args.chain1,
        chain2_path=args.chain2,
        chain3_path=args.chain3,
        output_dir=args.output_dir,
        snapshot_seconds=args.snapshot,
        min_intra_duration=args.min_intra,
        min_cross_duration=args.min_cross,
        constellation_specs=specs,
    )

    # 终端里只打印精简摘要，避免一次输出过多细节。
    compact = {
        "analysis_start_utc": summary["analysis_start_utc"],
        "analysis_stop_utc": summary["analysis_stop_utc"],
        "snapshot_seconds": summary["snapshot_seconds"],
        "snapshot_count": summary["snapshot_count"],
        "raw_contact_counts": summary["raw_contact_counts"],
        "clean_contact_counts": summary["clean_contact_counts"],
        "gateway_counts": summary["gateway_counts"],
        "cross_duration_stats": summary["cross_summary"]["duration_stats"],
        "files": summary["files"],
    }
    print(json.dumps(compact, indent=2, ensure_ascii=False))
    # 详细统计已经写进 summary 文件，便于后续脚本继续读取。
    print(f"\nFull summary written to: {Path(args.output_dir, 'stage1_preprocess_summary.json').resolve()}")


if __name__ == "__main__":
    main()
