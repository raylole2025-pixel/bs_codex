from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from bs3.stage2_result_plotting import load_stage2_plot_metrics, plot_comparison, plot_single_run


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate diagnostic plots from one or more Stage2 result files.")
    parser.add_argument(
        "inputs",
        nargs="+",
        help="Stage2 result JSON files or directories containing stage2_result.json",
    )
    parser.add_argument(
        "--labels",
        nargs="*",
        default=None,
        help="Optional labels aligned with inputs.",
    )
    parser.add_argument(
        "--output-dir",
        required=True,
        help="Directory for generated plots.",
    )
    parser.add_argument(
        "--comparison-name",
        default="stage2_comparison",
        help="Base filename prefix for comparison plots.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.labels is not None and len(args.labels) not in {0, len(args.inputs)}:
        raise ValueError("--labels must be omitted or match the number of inputs")

    labels = list(args.labels or [])
    metrics = [
        load_stage2_plot_metrics(path_like=input_path, label=(labels[index] if labels else None))
        for index, input_path in enumerate(args.inputs)
    ]
    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    for item in metrics:
        single_dir = output_dir / item.label
        generated = plot_single_run(item, single_dir)
        print(f"[single] {item.label}")
        for path in generated:
            print(f"  {path}")

    if len(metrics) > 1:
        generated = plot_comparison(metrics, output_dir, comparison_name=args.comparison_name)
        print(f"[comparison] {args.comparison_name}")
        for path in generated:
            print(f"  {path}")


if __name__ == "__main__":
    main()
