# BS3 Two-Stage Scheduler

BS3 is a two-stage cross-domain scheduling project:

- Stage 1 selects cross-domain windows and builds an activation plan.
- Stage 2 now uses a two-step flow on top of the fixed Stage 1 plan:
  1. offline regular-task baseline planning
  2. online event-driven emergency insertion with local repair/preemption

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
|- mydata/
|  |- constellation/      constellation notes
|  |- stk_access/         raw STK access exports
|  |- distances/          reusable distance products
|  `- topology/           reusable topology assets
|- outputs/
|  |- active/             current retained outputs
|  `- archive_unused/     archived old or temporary outputs
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
- `tools/compare_stage1_greedy_baselines.py`

Legacy script names are still kept as compatibility wrappers.

## Important Default Paths

- Base scenario template: `inputs/templates/stage1_scenario_template.json`
- Distance data: `mydata/distances/`
- Active outputs: `outputs/active/`
- Archived old outputs: `outputs/archive_unused/`

## Parameter Documentation

- Canonical parameter table: `docs/PARAMETER_SETTINGS.md`
- 4.6 alignment checklist: `docs/ALIGNMENT_CHECK_4_6.md`
