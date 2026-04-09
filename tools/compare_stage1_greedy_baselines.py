from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from bs3.scenario import load_scenario
from bs3.stage1 import Stage1GA
from bs3.stage1_visualization import export_stage1_run_artifacts


def _candidate_summary(candidate) -> dict:
    return {
        "gateway_count": candidate.gateway_count,
        "window_count": candidate.window_count,
        "activation_count": candidate.activation_count,
        "activation_time": candidate.activation_time,
        "sr_theta_c": candidate.sr_theta_c,
        "eta_cap": candidate.eta_cap,
        "eta_0": candidate.eta_0,
        "hotspot_coverage": candidate.hotspot_coverage,
        "hotspot_max_gap": candidate.hotspot_max_gap,
        "cross_active_fraction": candidate.cross_active_fraction,
        "max_cross_gap": candidate.max_cross_gap,
        "fitness": list(candidate.fitness),
        "accepted_order": list(candidate.accepted_order),
        "chromosome_prefix": list(candidate.chromosome[: min(40, len(candidate.chromosome))]),
    }


def _payload_summary(candidate: dict) -> dict:
    return {
        "gateway_count": candidate["gateway_count"],
        "window_count": candidate["window_count"],
        "activation_count": candidate.get("activation_count"),
        "activation_time": candidate.get("activation_time", candidate.get("occupation_time")),
        "sr_theta_c": candidate.get("sr_theta_c", candidate.get("sr_near")),
        "eta_cap": candidate.get("eta_cap", candidate.get("cross_capacity_gap", candidate.get("link_shortfall"))),
        "eta_0": candidate.get("eta_0", candidate.get("zero_cross_demand_ratio")),
        "hotspot_coverage": candidate.get("hotspot_coverage", candidate.get("avg_hot_coverage")),
        "hotspot_max_gap": candidate.get("hotspot_max_gap", candidate.get("max_hot_gap")),
        "cross_active_fraction": candidate["cross_active_fraction"],
        "max_cross_gap": candidate["max_cross_gap"],
        "fitness": candidate["fitness"],
        "accepted_order": candidate.get("accepted_order", []),
        "chromosome_prefix": list(candidate.get("chromosome", [])[:40]),
    }


def _load_ga_best(result_path: Path) -> dict:
    payload = json.loads(result_path.read_text(encoding="utf-8"))
    best = payload.get("best_feasible", [])
    if not best:
        raise ValueError(f"No best_feasible candidates in {result_path}")
    return best[0]


def _compare(ga_best: dict, baseline: dict) -> dict:
    return {
        "gateway_delta_vs_ga": baseline["gateway_count"] - ga_best["gateway_count"],
        "window_delta_vs_ga": baseline["window_count"] - ga_best["window_count"],
        "activation_count_delta_vs_ga": baseline["activation_count"] - ga_best["activation_count"],
        "sr_theta_c_delta_vs_ga": baseline["sr_theta_c"] - ga_best["sr_theta_c"],
        "eta_cap_delta_vs_ga": baseline["eta_cap"] - ga_best["eta_cap"],
        "eta0_delta_vs_ga": baseline["eta_0"] - ga_best["eta_0"],
        "hotspot_coverage_delta_vs_ga": baseline["hotspot_coverage"] - ga_best["hotspot_coverage"],
        "hotspot_max_gap_delta_vs_ga": baseline["hotspot_max_gap"] - ga_best["hotspot_max_gap"],
        "across_delta_vs_ga": baseline["cross_active_fraction"] - ga_best["cross_active_fraction"],
    }


def _rank_methods(entries: list[dict]) -> list[dict]:
    return sorted(entries, key=lambda item: tuple(item["fitness"]))


def run_case(scenario_path: Path, ga_result_path: Path, output_dir: Path, seed: int) -> dict:
    scenario = load_scenario(scenario_path)
    ga = Stage1GA(scenario, seed=seed)

    greedy_value = ga._evaluate_chromosome(ga._sorted_windows_by_value())
    greedy_density = ga._evaluate_chromosome(ga._sorted_windows_by_density())
    ga_best = _load_ga_best(ga_result_path)

    case_dir = output_dir / scenario_path.stem.replace("_scenario_annotated", "")
    case_dir.mkdir(parents=True, exist_ok=True)

    baselines = {
        "greedy_value": greedy_value,
        "greedy_density": greedy_density,
    }

    artifacts = {}
    methods: list[dict] = [
        {
            "method": "ga_best",
            **_payload_summary(ga_best),
        }
    ]

    for name, candidate in baselines.items():
        artifacts[name] = export_stage1_run_artifacts(scenario, [candidate], case_dir / name, f"{scenario_path.stem}_{name}")
        methods.append({"method": name, **_candidate_summary(candidate)})

    ranked = _rank_methods(methods)
    baseline_rows = []
    for method in methods[1:]:
        baseline_rows.append(
            {
                "method": method["method"],
                **method,
                "comparison_vs_ga": _compare(ga_best, method),
            }
        )

    summary = {
        "scenario": str(scenario_path),
        "ga_result": str(ga_result_path),
        "seed": seed,
        "screening": scenario.metadata.get("stage1_screening", {}),
        "ga_best": methods[0],
        "baselines": baseline_rows,
        "ranked_by_fitness": [
            {
                "method": item["method"],
                "fitness": item["fitness"],
                "gateway_count": item["gateway_count"],
                "window_count": item["window_count"],
                "activation_count": item["activation_count"],
                "sr_theta_c": item["sr_theta_c"],
                "eta_cap": item["eta_cap"],
                "eta_0": item["eta_0"],
                "hotspot_coverage": item["hotspot_coverage"],
                "hotspot_max_gap": item["hotspot_max_gap"],
                "cross_active_fraction": item["cross_active_fraction"],
            }
            for item in ranked
        ],
        "artifacts": artifacts,
    }

    summary_path = case_dir / "greedy_baseline_summary.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    md_lines = [
        f"# Greedy Baseline Comparison: {scenario_path.stem}",
        "",
        f"- Scenario: `{scenario_path}`",
        f"- GA result: `{ga_result_path}`",
        f"- Seed: `{seed}`",
        "",
        "## Ranked by fitness",
        "",
    ]
    for idx, item in enumerate(ranked, start=1):
        md_lines.append(
            f"{idx}. `{item['method']}` | G={item['gateway_count']} | M={item['window_count']} | "
            f"N_act={item['activation_count']} | SR={item['sr_theta_c']:.6f} | "
            f"eta_cap={item['eta_cap']:.3f} | Hot={item['hotspot_coverage']:.6f} | "
            f"HotGap={item['hotspot_max_gap']:.1f} | eta0={item['eta_0']:.6f} | "
            f"cross_active_fraction={item['cross_active_fraction']:.6f}"
        )
    md_lines.extend(["", "## Baseline deltas vs GA", ""])
    for item in baseline_rows:
        delta = item["comparison_vs_ga"]
        md_lines.append(
            f"- `{item['method']}`: dG={delta['gateway_delta_vs_ga']}, dM={delta['window_delta_vs_ga']}, "
            f"dN_act={delta['activation_count_delta_vs_ga']}, dSR={delta['sr_theta_c_delta_vs_ga']:.6f}, "
            f"deta_cap={delta['eta_cap_delta_vs_ga']:.3f}, dHot={delta['hotspot_coverage_delta_vs_ga']:.6f}, "
            f"dHotGap={delta['hotspot_max_gap_delta_vs_ga']:.1f}, deta0={delta['eta0_delta_vs_ga']:.6f}, "
            f"dcross_active_fraction={delta['across_delta_vs_ga']:.6f}"
        )
    (case_dir / "greedy_baseline_summary.md").write_text("\n".join(md_lines), encoding="utf-8")
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Run no-GA greedy baselines against an existing Stage1 GA result.")
    parser.add_argument("--scenario", action="append", required=True, help="Annotated scenario JSON path.")
    parser.add_argument("--ga-result", action="append", required=True, help="Existing GA stage1_result JSON path.")
    parser.add_argument("--output-dir", required=True, help="Output directory for comparison results.")
    parser.add_argument("--seed", type=int, default=7)
    args = parser.parse_args()

    if len(args.scenario) != len(args.ga_result):
        raise SystemExit("--scenario and --ga-result must have the same count")

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    combined = []
    for scenario_path_str, result_path_str in zip(args.scenario, args.ga_result, strict=True):
        combined.append(
            run_case(Path(scenario_path_str), Path(result_path_str), output_dir, seed=args.seed)
        )

    (output_dir / "combined_summary.json").write_text(
        json.dumps({"cases": combined}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
