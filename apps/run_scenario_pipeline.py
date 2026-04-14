from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from bs3.pipeline import run_pipeline
from bs3.scenario import load_scenario
from bs3.stage1 import activation_count, gateway_count


def _candidate_to_dict(candidate):
    data = asdict(candidate)
    data["fr"] = candidate.fr
    data["mean_completion_ratio"] = candidate.mean_completion_ratio
    data["plan"] = [asdict(window) for window in candidate.plan]
    return data


def _stage2_to_dict(result, t_pre: float):
    data = asdict(result)
    data["gateway_count"] = gateway_count(result.plan)
    data["activation_count"] = activation_count(result.plan, t_pre)
    data["plan"] = [asdict(window) for window in result.plan]
    data["allocations"] = [asdict(item) for item in result.allocations]
    return data


def _baseline_trace_to_dict(trace):
    if trace is None:
        return None
    data = asdict(trace)
    data["allocations"] = [asdict(item) for item in trace.allocations]
    return data


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run the BS3 main pipeline: Stage 1 baseline export plus Stage 2 emergency insertion."
    )
    parser.add_argument("scenario", help="Path to the scenario JSON file")
    parser.add_argument("--seed", type=int, default=None, help="Random seed for Stage 1 GA")
    parser.add_argument("--output", type=str, default=None, help="Optional JSON output path")
    args = parser.parse_args()

    scenario = load_scenario(args.scenario)
    result = run_pipeline(scenario, seed=args.seed)

    payload = {
        "stage1": {
            "generations": result.stage1.generations,
            "used_feedback": result.stage1.used_feedback,
            "selected_candidate_index": result.stage1.selected_candidate_index,
            "selected_candidate_source": result.stage1.selected_candidate_source,
            "selected_plan": [asdict(window) for window in result.stage1.selected_plan],
            "baseline_summary": result.stage1.baseline_summary,
            "baseline_trace": _baseline_trace_to_dict(result.stage1.baseline_trace),
            "best_feasible": [_candidate_to_dict(item) for item in result.stage1.best_feasible],
            "population_best": _candidate_to_dict(result.stage1.population_best) if result.stage1.population_best else None,
        },
        "stage2_results": [_stage2_to_dict(item, scenario.stage1.t_pre) for item in result.stage2_results],
        "recommended": _stage2_to_dict(result.recommended, scenario.stage1.t_pre) if result.recommended else None,
    }

    text = json.dumps(payload, indent=2, ensure_ascii=False)
    if args.output:
        Path(args.output).write_text(text, encoding="utf-8")
    else:
        print(text)


if __name__ == "__main__":
    main()
