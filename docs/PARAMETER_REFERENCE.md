# BS3 Parameter Reference

> This file is kept for historical context.  
> The canonical parameter document is `docs/PARAMETER_SETTINGS.md`.

This is a trimmed, updated parameter reference aligned to the cleaned repository layout on 2026-04-01.

## Parameter Sources

When values disagree, read them in this order:

1. The retained scenario/result files under `outputs/active/`
2. The defaults inside `apps/` entry scripts
3. Runtime fallback defaults in `bs3/scenario.py`

## Retained Scenario References

Current retained Stage 1 annotated scenarios:

- `outputs/active/stage1/stage1_hybrid_medium_linkshort_opt3_20260322/medium48_hybrid/medium48_hybrid_scenario_annotated.json`
- `outputs/active/stage1/stage1_hybrid_stress96_linkshort_opt2_45min_20260322/stress96_hybrid/stress96_hybrid_scenario_annotated.json`

Current retained Stage 1 comparison summary:

- `outputs/active/analysis/stage1_greedy_baseline_compare_20260323/combined_summary.json`

## Default Script Locations

- Stage 1 batch runner: `apps/run_stage1_workbook_batch.py`
- Stage 2 sheet runner: `apps/run_stage2_workbook_sheet.py`
- Preprocess entry: `apps/preprocess_stk_access.py`
- Scenario-template builder: `apps/build_stage1_template_from_preprocess.py`
- Distance enrichment tool: `tools/enrich_scenario_distances.py`

## Default Input and Output Paths

`apps/run_stage1_workbook_batch.py`:

- base scenario: `inputs/templates/stage1_scenario_template.json`
- output root: `outputs/active/stage1/taskset_runs`
- distance root: `mydata/distances/`

`apps/run_stage2_workbook_sheet.py`:

- output root: `outputs/active/stage2/taskset_runs`
- stage2 mode: fixed regular-baseline plus event-driven emergency insertion
- completion tolerance: `1e-6`
- key exposed params: `k_paths`, `completion_tolerance`

`apps/preprocess_stk_access.py`:

- output dir: `mydata/topology/stage1_preprocess_user`

`apps/build_stage1_template_from_preprocess.py`:

- default output: `inputs/templates/stage1_scenario_template.json`

## Distance Files

The current default distance products are:

- `mydata/distances/domain1_isl_distance_20260323/domain1_isl_distance_timeseries.csv`
- `mydata/distances/domain2_isl_distance_20260323/domain2_isl_distance_timeseries.csv`
- `mydata/distances/crosslink_distance_20260323/crosslink_distance_timeseries.csv`
- `mydata/distances/domain1_isl_distance_20260323/domain1_isl_pair_summary.csv`
- `mydata/distances/domain2_isl_distance_20260323/domain2_isl_pair_summary.csv`
- `mydata/distances/crosslink_distance_20260323/crosslink_pair_summary.csv`

## Active Stage 1 Baseline

For the current Stage 1 baseline, see [STAGE1_PARAMETER_BASELINE.md](/D:/Codex_Project/bs3/docs/STAGE1_PARAMETER_BASELINE.md).

For the full parameter-by-parameter table with source locations and meanings, see [CURRENT_PARAMETER_TABLE.md](/D:/Codex_Project/bs3/docs/CURRENT_PARAMETER_TABLE.md).
