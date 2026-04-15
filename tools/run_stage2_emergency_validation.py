from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tools.stage2_emergency_validation_lib import (
    DEFAULT_OUTPUT_ROOT,
    build_suite_cases,
    load_experiment_spec,
    parse_candidate_indices,
    parse_rho_values,
    run_stage2_emergency_validation,
)


def _build_cases_from_args(args: argparse.Namespace) -> tuple[str, list[dict], list[int], list[dict]]:
    default_rho_payload = json.loads(Path(args.scenario).read_text(encoding="utf-8-sig"))
    default_rho = float(default_rho_payload.get("stage1", {}).get("rho", 0.0))

    if args.experiment_spec:
        spec = load_experiment_spec(args.experiment_spec, default_rho=default_rho)
        return (
            str(spec["suite_name"]),
            list(spec["cases"]),
            list(spec["candidate_indices"]),
            list(spec["rho_values"]),
        )

    suite_name = args.suite or "smoke"
    cases = build_suite_cases(suite_name) if not args.skip_builtin_suite else []
    if args.emergency_json:
        cases.append(
            {
                "name": args.custom_case_name or Path(args.emergency_json).stem,
                "case_type": "file_json",
                "description": "Emergency tasks loaded from JSON.",
                "source": {"mode": "json", "path": str(Path(args.emergency_json).resolve())},
            }
        )
    if args.emergency_csv:
        cases.append(
            {
                "name": args.custom_case_name or Path(args.emergency_csv).stem,
                "case_type": "file_csv",
                "description": "Emergency tasks loaded from CSV.",
                "source": {"mode": "csv", "path": str(Path(args.emergency_csv).resolve())},
            }
        )
    if args.emergency_workbook:
        if not args.emergency_sheet:
            raise ValueError("--emergency-sheet is required when --emergency-workbook is provided")
        cases.append(
            {
                "name": args.custom_case_name or f"{Path(args.emergency_workbook).stem}_{args.emergency_sheet}",
                "case_type": "file_workbook",
                "description": "Emergency tasks loaded from workbook sheet.",
                "source": {
                    "mode": "workbook",
                    "path": str(Path(args.emergency_workbook).resolve()),
                    "sheet": args.emergency_sheet,
                },
            }
        )
    if args.num_emergencies is not None:
        cases.append(
            {
                "name": args.custom_case_name or "custom_generated",
                "case_type": "custom_generated",
                "description": "Emergency tasks generated directly from CLI parameters.",
                "source": {
                    "mode": "generate",
                    "params": {
                        "num_emergencies": int(args.num_emergencies),
                        "arrival_pattern": args.arrival_pattern,
                        "deadline_tightness": args.deadline_tightness,
                        "data_scale": args.data_scale,
                        "weight_scale": args.weight_scale,
                        "hotspot_bias": bool(args.hotspot_bias),
                    },
                },
            }
        )
    if not cases:
        raise ValueError("No experiment cases were constructed. Provide a suite, spec, input file, or generation parameters.")
    candidate_indices = parse_candidate_indices(args.candidate_indices)
    rho_values = parse_rho_values(args.rho_values, default_rho=default_rho)
    return suite_name, cases, candidate_indices, rho_values


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run Stage2 emergency insertion validation on top of Stage1 selected_plan + baseline_trace."
    )
    parser.add_argument("--scenario", required=True, help="Scenario JSON used as the topology/task template")
    parser.add_argument("--stage1-result", required=True, help="Stage1 result JSON")
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT), help="Output root directory")
    parser.add_argument("--run-name", default=None, help="Optional run directory name")
    parser.add_argument("--seed", type=int, default=7, help="Base seed for generated emergency cases")
    parser.add_argument("--suite", default="smoke", help="Built-in suite: smoke or small-validation")
    parser.add_argument("--skip-builtin-suite", action="store_true", help="Do not add built-in suite cases")
    parser.add_argument("--experiment-spec", default=None, help="Optional JSON spec that defines cases/candidates/rho")
    parser.add_argument("--candidate-indices", default="0", help="Comma-separated candidate indices, e.g. 0,1,2")
    parser.add_argument("--rho-values", default="default", help="Comma-separated rho values, e.g. default,0.2,0.35")

    parser.add_argument("--emergency-json", default=None, help="Path to emergency task JSON")
    parser.add_argument("--emergency-csv", default=None, help="Path to emergency task CSV")
    parser.add_argument("--emergency-workbook", default=None, help="Path to emergency task workbook")
    parser.add_argument("--emergency-sheet", default=None, help="Workbook sheet name")

    parser.add_argument("--custom-case-name", default=None, help="Optional case name for file/custom-generated input")
    parser.add_argument("--num-emergencies", type=int, default=None, help="Generate a custom emergency case with N tasks")
    parser.add_argument("--arrival-pattern", default="uniform", help="uniform or clustered")
    parser.add_argument("--deadline-tightness", default="medium", help="loose, medium, tight, or numeric multiplier")
    parser.add_argument("--data-scale", default="medium", help="low, medium, high, or numeric multiplier")
    parser.add_argument("--weight-scale", default="medium", help="low, medium, medium_high, high, or numeric multiplier")
    parser.add_argument("--hotspot-bias", action="store_true", help="Bias generated emergency tasks toward hotspot regions")
    args = parser.parse_args()

    suite_name, cases, candidate_indices, rho_values = _build_cases_from_args(args)
    summary = run_stage2_emergency_validation(
        scenario_path=args.scenario,
        stage1_result_path=args.stage1_result,
        output_root=args.output_root,
        suite_name=suite_name,
        run_name=args.run_name,
        candidate_indices=candidate_indices,
        rho_values=rho_values,
        cases=cases,
        seed=args.seed,
    )
    print(json.dumps({"run_dir": summary["run_dir"], "summary_json": str(Path(summary["run_dir"]) / "summary.json")}, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
