# BS3 Two-Stage Scheduler

BS3 is a two-stage cross-domain scheduling project:

- Stage 1 selects cross-domain windows, fixes the activation plan `S*`, and exports the regular-task baseline state.
- Stage 2 reads `selected_plan + baseline_trace` from Stage 1 and only performs online emergency insertion with controlled preemption when needed.

The former Stage2-1 hotspot-relief / closed-loop regular-load path has been removed from the active repository code.

## Quick Start

```bash
python apps/run_scenario_pipeline.py examples/sample_scenario.json --seed 7
```

Write the result to a file:

```bash
python apps/run_scenario_pipeline.py examples/sample_scenario.json --seed 7 --output result.json
```

## Repository Layout

```text
bs3/
|- apps/                  main entry scripts
|- tools/                 analysis and helper scripts
|- bs3/                   core package
|- examples/              minimal runnable examples
|- inputs/                task inputs and runnable templates
|- results/               curated retained results and future generated outputs
|- mydata/
|  |- constellation/      constellation notes
|  |- stk_access/         raw STK access exports
|  |- distances/          reusable distance products
|  `- topology/           reusable topology assets
|- docs/                  project docs
`- tests/                 regression tests
```

## Common Entry Points

- `apps/run_scenario_pipeline.py`
- `apps/preprocess_stk_access.py`
- `apps/build_stage1_template_from_preprocess.py`
- `apps/run_stage1_workbook_batch.py`
- `apps/run_stage1_single_validation.py`
- `apps/run_stage2_workbook_sheet.py`
- `tools/run_stage2_emergency_validation.py`
- `tools/enrich_scenario_distances.py`

## Important Default Paths

- Base scenario template: `inputs/templates/stage1_scenario_template.json`
- Distance data: `mydata/distances/`
- Retained results: `results/`
- Generated experiment outputs: `results/generated/`

## Parameter Documentation

- Canonical parameter table: `docs/PARAMETER_SETTINGS.md`

## Stage2 Emergency Validation

The active repository structure is:

- Stage 1: select `selected_plan` and export `baseline_trace`
- Stage 2: read that baseline and only do online emergency insertion

The legacy Stage2-1 hotspot-relief / closed-loop regular-load path is not part of the active code path.

### 1. Prepare Stage1 Output

Minimal example:

```bash
python apps/run_stage1_single_validation.py examples/sample_scenario.json --seed 7 --output results/generated/sample_stage1_result.json
```

This produces a Stage1 result JSON and a companion `*_baseline_trace.json`.

### 2. Run Smoke Validation

```bash
python tools/run_stage2_emergency_validation.py ^
  --scenario examples/sample_scenario.json ^
  --stage1-result results/generated/sample_stage1_result.json ^
  --suite smoke ^
  --run-name smoke_validation
```

The built-in `smoke` suite includes:

- `empty_control`
- `light_load`

### 3. Run Small Formal Validation

```bash
python tools/run_stage2_emergency_validation.py ^
  --scenario examples/sample_scenario.json ^
  --stage1-result results/generated/sample_stage1_result.json ^
  --suite small-validation ^
  --candidate-indices 0,1,2 ^
  --rho-values default,0.2,0.3 ^
  --run-name small_validation
```

The built-in `small-validation` suite includes:

- `empty_control`
- `light_load`
- `medium_load`
- `heavy_load`
- `hotspot_bias`

### 4. Run With Custom Emergency Input

JSON:

```bash
python tools/run_stage2_emergency_validation.py ^
  --scenario examples/sample_scenario.json ^
  --stage1-result results/generated/sample_stage1_result.json ^
  --skip-builtin-suite ^
  --emergency-json inputs/templates/stage2_emergency_tasks_template.json ^
  --custom-case-name json_case
```

Custom experiment spec:

```bash
python tools/run_stage2_emergency_validation.py ^
  --scenario examples/sample_scenario.json ^
  --stage1-result results/generated/sample_stage1_result.json ^
  --experiment-spec inputs/templates/stage2_emergency_validation_spec_template.json
```

Workbook input is also supported with:

- `--emergency-workbook <path>`
- `--emergency-sheet <sheet_name>`

CSV input is supported with:

- `--emergency-csv <path>`

Generated custom cases are supported with:

- `--num-emergencies`
- `--arrival-pattern`
- `--deadline-tightness`
- `--data-scale`
- `--weight-scale`
- `--hotspot-bias`

### 5. Output Structure

Each run is written under:

- `results/generated/stage2_emergency_validation/<run_name>/`

Run-level files:

- `summary.json`: machine-readable aggregate summary across all cases
- `summary.md`: readable report with findings, grouped metrics, and per-case table

Each case directory contains:

- `effective_scenario.json`: scenario actually passed into Stage2
- `emergency_tasks.json`: normalized emergency set used by the case
- `stage2_result.json`: full Stage2 result payload
- `case_summary.json`: case-level diagnostic summary
- `case_summary.md`: readable case recap

### 6. Main Metrics To Watch

- Empty-emergency cases should strictly degenerate to the baseline
- `cr_emg`, `cr_reg`, `n_preemptions`, `u_cross`, `u_all`, `elapsed_seconds`
- `regular_tasks_degraded_by_emergency`
- `cross_window_usage_delta_by_segment`
- direct insert vs controlled preemption vs failed emergency tasks
- differences across `rho` values and Stage1 candidate indices
