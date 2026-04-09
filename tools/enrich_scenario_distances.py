from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from bs3.distance_enrichment import enrich_scenario_distances
from bs3.scenario import load_scenario, scenario_to_dict


def main() -> None:
    parser = argparse.ArgumentParser(description="Enrich a scenario JSON with distance/delay-based link weights.")
    parser.add_argument("--scenario", required=True, help="Input scenario JSON")
    parser.add_argument("--output", required=True, help="Output scenario JSON")
    parser.add_argument("--domain-a-timeseries", required=True, help="Domain-A ISL distance timeseries CSV")
    parser.add_argument("--domain-b-timeseries", required=True, help="Domain-B ISL distance timeseries CSV")
    parser.add_argument("--cross-timeseries", required=True, help="Cross-link distance timeseries CSV")
    parser.add_argument("--domain-a-position", help="Optional Domain-A inertial position export; preferred for intra-domain averaging")
    parser.add_argument("--domain-b-position", help="Optional Domain-B inertial position export; preferred for intra-domain averaging")
    parser.add_argument("--domain-a-summary", help="Optional Domain-A pair summary CSV")
    parser.add_argument("--domain-b-summary", help="Optional Domain-B pair summary CSV")
    parser.add_argument("--cross-summary", help="Optional cross-link pair summary CSV")
    parser.add_argument("--light-speed-kmps", type=float, default=299792.458)
    parser.add_argument("--intra-proc-delay-sec", type=float, default=0.0)
    parser.add_argument("--cross-proc-delay-sec", type=float, default=0.0)
    args = parser.parse_args()

    scenario = load_scenario(args.scenario)
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

    output_path = Path(args.output)
    output_path.write_text(json.dumps(scenario_to_dict(scenario), indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps({"output": str(output_path.resolve()), "stats": stats}, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
