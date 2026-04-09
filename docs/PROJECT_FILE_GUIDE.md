# BS3 File Guide

This document records the repository layout after the April 1, 2026 cleanup.

## Directory Layout

```text
bs3/
|- apps/                  main entry scripts
|- tools/                 helper and analysis scripts
|- bs3/                   core Python package
|- examples/              minimal runnable examples
|- inputs/                current task inputs
|- mydata/
|  |- constellation/      constellation notes
|  |- stk_access/         raw STK access exports
|  |- distances/          reusable distance time series and summaries
|  `- topology/           reusable topology assets
|- outputs/
|  |- active/
|  |  |- stage1/
|  |  |- stage2/
|  |  `- analysis/
|  `- archive_unused/
|- docs/
`- tests/
```

## Why These Buckets Exist

- `inputs/` stores files that are used directly as workload input.
- `mydata/` stores reusable prerequisites, not one-off run results.
- `outputs/active/` stores the current results worth keeping close at hand.
- `outputs/archive_unused/` stores historical experiments, probes, and superseded outputs that are no longer part of the default workflow.

## Reusable Prerequisites

The following are now treated as reusable assets:

- `mydata/constellation/星座构型.txt`
- `mydata/stk_access/bs2_Chain1_Individual_Strand_Access.txt`
- `mydata/stk_access/bs2_Chain2_Individual_Strand_Access.txt`
- `mydata/stk_access/bs2_Chain3_Individual_Strand_Access.txt`
- `mydata/distances/`
- `mydata/topology/stage1_preprocess_user/`

`mydata/topology/stage1_preprocess_user/` now keeps only the reusable topology files:

- cleaned `A/B/X` contact CSVs
- `A/B` hop matrices
- snapshot summaries
- `stage1_preprocess_summary.json`
- `stage1_hotspot_summary.json`

The runnable base template now lives at:

- `inputs/templates/stage1_scenario_template.json`

## Retained Active Outputs

Stage 1:

- `outputs/active/stage1/stage1_hybrid_medium_linkshort_opt3_20260322`
- `outputs/active/stage1/stage1_hybrid_stress96_linkshort_opt2_45min_20260322`
- `outputs/active/stage1/stage1_weighted_runner_smoke_20260323`
- `outputs/active/stage1/taskset_runs`

Stage 2:

- `outputs/active/stage2/stage2_medium48_mixed_hard_20260328`
- `outputs/active/stage2/stage2_medium48_mixed_showcase_20260328`
- `outputs/active/stage2/stage2_stress96_mixed_hard_20260328`
- `outputs/active/stage2/stage2_stress96_mixed_showcase_20260328`
- `outputs/active/stage2/taskset_runs`

Analysis:

- `outputs/active/analysis/stage1_greedy_baseline_compare_20260323`
- `outputs/active/analysis/stage1_hybrid_linkshort_latest_plot_summary_20260322.json`

## Archived Outputs

The following groups were moved to `outputs/archive_unused/20260401/`:

- GA pool and ablation probes:
  - `ga_pool_medium_20260324`
  - `medium_ga_pool_ablation_20260324.json`
  - `medium_ga_usefulness_ablation_20260324.json`
- single-run GA value experiment:
  - `medium47_ga_value_20260324`
- hotspot trial runs:
  - `stage1_medium48_hotspot_*`
  - `stage1_stress96_hotspot_active_normalized_quant_20260324`
- temporary probe files:
  - `tmp_medium_pool*.json`
  - `tmp_stage1_template_baseline.json`
- old screen check:
  - `stage1_medium48_screen_check.json`

Files that used to be mixed into the preprocess output but are really historical run products were also split out to `outputs/archive_unused/20260401/stage1_preprocess_user_legacy/`:

- `A_hop_matrix.csv.pkl`
- `B_hop_matrix.csv.pkl`
- `stage1_formal_run_result_optimized.json`
- `stage1_formal_run_result_v2.json`
- `stage1_profile_evaluate_plan.txt`
- `stage1_scenario_with_tasks.json`
- `stage1_scenario_with_tasks_v2.json`
- `stage1_small_run_result.json`
- `stage1_small_run_result_optimized.json`

## Main Scripts

Apps:

- `apps/run_scenario_pipeline.py`
- `apps/preprocess_stk_access.py`
- `apps/build_stage1_template_from_preprocess.py`
- `apps/run_stage1_workbook_batch.py`
- `apps/run_stage2_workbook_sheet.py`

Tools:

- `tools/compute_isl_distances.py`
- `tools/compute_cross_domain_link_distances.py`
- `tools/enrich_scenario_distances.py`
- `tools/compare_stage1_greedy_baselines.py`
