# BS3 Two-Stage Scheduler

BS3 is a two-stage cross-domain scheduling project:

- Stage 1 selects cross-domain windows, fixes the activation plan `S*`, and exports the regular-task baseline state.
- Stage 2 reads `selected_plan + baseline_trace` from Stage 1 and only performs online emergency insertion with controlled preemption when needed.

Legacy Stage2-1 hotspot-relief / closed-loop regular-load logic is retained only for reference and is no longer on the default execution path.

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
- `apps/run_stage2_workbook_sheet.py`
- `tools/enrich_scenario_distances.py`
- `tools/run_stage2_milp_experiments.py`
- `tools/run_stage2_rolling_robustness.py`

## Important Default Paths

- Base scenario template: `inputs/templates/stage1_scenario_template.json`
- Distance data: `mydata/distances/`
- Retained results: `results/`
- Generated experiment outputs: `results/generated/`

## Parameter Documentation

- Canonical parameter table: `docs/PARAMETER_SETTINGS.md`
