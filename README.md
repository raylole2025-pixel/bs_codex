# BS3 Two-Stage Scheduler

BS3 is a two-stage cross-domain scheduling project:

- Stage 1 selects cross-domain windows, fixes the activation plan `S*`, and exports the regular-task baseline state.
- Stage 2 reads `selected_plan + baseline_trace` from Stage 1 and only performs online emergency insertion with controlled preemption when needed.

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
- Results root: `results/`
- Stage2 validation outputs: `results/stage2_results/`

## Parameter Documentation

- Canonical parameter table: `docs/PARAMETER_SETTINGS.md`

## Stage2 Emergency Validation

The active repository structure is:

- Stage 1: select `selected_plan` and export `baseline_trace`
- Stage 2: read that baseline and only do online emergency insertion

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

- `results/stage2_results/<run_name>/`

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

### 7. Current Stage2 Strategy Ladder

Stage 2 now handles emergency tasks through a single event-driven path:

- `direct_insert`
- `direct_insert_best_effort`
- `controlled_preemption`
- `controlled_preemption_two_victim`
- `controlled_preemption_recovery_victim_fallback`
- `controlled_preemption_best_effort`
- `blocked`

This repository no longer carries an alternate Stage2 solver family. All CLI entrypoints go through `bs3/stage2.py -> bs3/stage2_emergency_scheduler.py`.

### 8. Stage2 Plotting

Use `tools/plot_stage2_results.py` to generate Stage2 plots from one or more `stage2_result.json` files or result directories.

- Single-run plots:
  - strategy distribution
  - delivered-ratio timeline
  - preemption and recovery overview
- Multi-run comparison plots:
  - `cr_emg` vs `cr_reg`
  - stacked strategy mix
  - preemption and recovery comparison

Active Stage2 implementation now lives in `bs3/stage2_emergency_scheduler.py`. The older `bs3/stage2_two_phase_scheduler.py` file is kept as a thin compatibility shim.
