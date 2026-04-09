from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from bs3.scenario import load_scenario
from bs3.stage1 import run_stage1


def _candidate_to_dict(candidate):
    data = asdict(candidate)
    data["plan"] = [asdict(window) for window in candidate.plan]
    return data


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a single Stage1 validation with detailed diagnostics.")
    parser.add_argument("scenario", help="Path to the weighted or annotated scenario JSON file")
    parser.add_argument("--seed", type=int, default=7, help="Fixed random seed for Stage1 GA")
    parser.add_argument("--output", type=str, default=None, help="Optional Stage1 result JSON output path")
    args = parser.parse_args()

    scenario = load_scenario(args.scenario)
    result = run_stage1(scenario, seed=args.seed, diagnostics=True)

    if args.output:
        payload = {
            "scenario": str(Path(args.scenario).resolve()),
            "seed": args.seed,
            "generations": result.generations,
            "used_feedback": result.used_feedback,
            "timed_out": result.timed_out,
            "elapsed_seconds": result.elapsed_seconds,
            "stage1_screening": scenario.metadata.get("stage1_screening", {}),
            "best_feasible": [_candidate_to_dict(item) for item in result.best_feasible],
            "population_best": _candidate_to_dict(result.population_best) if result.population_best else None,
            "history": result.history,
        }
        output_path = Path(args.output)
        output_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"stage1_result_file={output_path.resolve()}", flush=True)


if __name__ == "__main__":
    main()
