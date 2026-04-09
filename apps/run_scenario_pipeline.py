"""BS3 的单场景命令行入口。

这个脚本面向“已经有一个完整场景 JSON”的使用场景：

1. 读取场景文件；
2. 调用 `bs3.pipeline.run_pipeline` 依次执行阶段 1 和阶段 2；
3. 把 dataclass 形式的结果整理成纯 JSON 可序列化结构；
4. 按需输出到标准输出或指定文件。

它本身不负责构造拓扑、预处理 STK 数据或批量实验，
而是项目里最直接、最适合单次复现实验的运行入口。
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict
from pathlib import Path

# 允许直接以 `python apps/run_scenario_pipeline.py ...` 的方式运行。这种方式运行会导致__package__为None
# 当脚本不是以模块方式启动时，手动把项目根目录加入 `sys.path`，
# 这样下面的 `from bs3...` 才能被找到，否则程序会找不到bs3模块。
if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from bs3.pipeline import run_pipeline
from bs3.scenario import load_scenario
from bs3.stage1 import activation_count, activation_time, gateway_count


def _candidate_to_dict(candidate):
    """把阶段 1 候选解展开成适合写入 JSON 的普通字典。"""

    data = asdict(candidate)
    data["fr"] = candidate.fr
    data["mean_completion_ratio"] = candidate.mean_completion_ratio
    data["plan"] = [asdict(window) for window in candidate.plan]
    return data
"""
# 转换前Python 对象
candidate = Stage1Candidate(
    chromosome=("cw1", "cw3"),
    plan=[ScheduledWindow(window_id="cw1", a="A1", b="B1", start=0, end=10, on=5, off=10, ...)],
    fitness=(0.44, 0.33, 0.22),
    ...
)

# 转换后（普通字典）
data = {
    "chromosome": ("cw1", "cw3"),
    "plan": [                          ← 嵌套对象也变成了字典
        {"window_id": "cw1", "a": "A1", "b": "B1", "start": 0, "end": 10, "on": 5, "off": 10, ...}
    ],
    "fitness": (0.44, 0.33, 0.22),
    "fr": 0.92,                        ← 新增的别名
    "mean_completion_ratio": 0.88,     ← 新增的别名
    ...
}"""

def _stage2_to_dict(result, t_pre: float):
    """把阶段 2 结果补充派生指标后展开为字典。"""

    data = asdict(result)
    data["gateway_count"] = gateway_count(result.plan)
    data["activation_count"] = activation_count(result.plan, t_pre)
    data["activation_time"] = activation_time(result.plan, t_pre)
    data["plan"] = [asdict(window) for window in result.plan]
    data["allocations"] = [asdict(item) for item in result.allocations]
    return data


def main() -> None:
    """解析命令行并执行单场景两阶段求解。"""
    # parser是一个对象名
    parser = argparse.ArgumentParser(description="Run the BS3 two-stage scheduler.")
    parser.add_argument("scenario", help="Path to the scenario JSON file")
    parser.add_argument("--seed", type=int, default=None, help="Random seed for stage1 GA")
    parser.add_argument("--output", type=str, default=None, help="Optional JSON output path")
    args = parser.parse_args()

    # 场景加载负责把 JSON 还原成带类型信息的内部数据结构；
    # pipeline 则串联“阶段 1 链路构型 + 阶段 2 滚动调度”。
    scenario = load_scenario(args.scenario)
    result = run_pipeline(scenario, seed=args.seed)

    # 输出结构尽量保持稳定、显式，便于后处理脚本直接消费。
    payload = {
        "stage1": {
            "generations": result.stage1.generations,
            "used_feedback": result.stage1.used_feedback,
            "best_feasible": [_candidate_to_dict(item) for item in result.stage1.best_feasible],
            "population_best": _candidate_to_dict(result.stage1.population_best) if result.stage1.population_best else None,
        },
        "stage2_results": [_stage2_to_dict(item, scenario.stage1.t_pre) for item in result.stage2_results],
        "recommended": _stage2_to_dict(result.recommended, scenario.stage1.t_pre) if result.recommended else None,
    }

    # `ensure_ascii=False` 可以确保中文以 UTF-8 直接写出，
    # 不会退化成难读的转义序列。
    text = json.dumps(payload, indent=2, ensure_ascii=False)
    if args.output:
        Path(args.output).write_text(text, encoding="utf-8")
    else:
        print(text)


if __name__ == "__main__":
    main()
