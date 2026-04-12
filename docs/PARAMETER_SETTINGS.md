# BS3 Parameter Settings

Updated: 2026-04-11

This document lists the Stage1/Stage2 parameters that are currently recognized by the codebase. The Stage2 section now includes the hotspot-relief path:

- hotspot-guided window augmentation
- hotspot-segment candidate expansion
- local peak-constrained MILP

## Scenario JSON

### `stage1`

| Field | Default |
| --- | --- |
| `rho` | `0.2` |
| `t_pre` | `1800.0` |
| `d_min` | `600.0` |
| `theta_cap` | `0.08` |
| `theta_hot` | `0.80` |
| `hot_hop_limit` | `4` |
| `bottleneck_factor_alpha` | `0.85` |
| `eta_x` | `0.90` |
| `static_value_snapshot_seconds` | `600` |
| `candidate_pool_base_size` | `400` |
| `candidate_pool_hot_fraction` | `0.30` |
| `candidate_pool_min_per_coarse_segment` | `3` |
| `candidate_pool_max_additional` | `150` |
| `q_eval` | `4` |
| `omega_fr` | `4/9` |
| `omega_cap` | `3/9` |
| `omega_hot` | `2/9` |
| `elite_prune_count` | `6` |

### `stage1.ga`

| Field | Default |
| --- | --- |
| `population_size` | `60` |
| `crossover_probability` | `0.9` |
| `mutation_probability` | `0.2` |
| `max_generations` | `100` |
| `stall_generations` | `20` |
| `top_m` | `5` |
| `max_runtime_seconds` | `null` |

### `stage2`

| Field | Default |
| --- | --- |
| `k_paths` | `2` |
| `completion_tolerance` | `1e-6` |
| `regular_baseline_mode` | `null` |
| `regular_repair_enabled` | `null` |
| `prefer_milp` | `true` |
| `milp_mode` | `full` |
| `milp_horizon_segments` | `16` |
| `milp_commit_segments` | `8` |
| `milp_rolling_path_limit` | `1` |
| `milp_rolling_high_path_limit` | `2` |
| `milp_rolling_high_weight_threshold` | `null` |
| `milp_rolling_high_competition_task_threshold` | `8` |
| `milp_rolling_promoted_tasks_per_segment` | `2` |
| `milp_time_limit_seconds` | `null` |
| `milp_relative_gap` | `null` |
| `repair_block_max_count` | `3` |
| `repair_expand_segments` | `1` |
| `repair_max_block_segments` | `8` |
| `repair_min_active_tasks` | `2` |
| `repair_util_threshold` | `0.75` |
| `repair_candidate_path_limit` | `2` |
| `repair_time_limit_seconds` | `null` |
| `repair_accept_epsilon` | `1e-6` |
| `hotspot_relief_enabled` | `true` |
| `hotspot_util_threshold` | `0.95` |
| `hotspot_topk_ranges` | `5` |
| `hotspot_expand_segments` | `2` |
| `hotspot_single_link_fraction_threshold` | `0.6` |
| `hotspot_top_tasks_per_range` | `12` |
| `augment_window_budget` | `2` |
| `augment_top_windows_per_range` | `3` |
| `hot_path_limit` | `4` |
| `hot_promoted_tasks_per_segment` | `8` |
| `local_peak_horizon_cap_segments` | `48` |
| `local_peak_accept_epsilon` | `1e-6` |
| `fail_if_milp_disabled` | `true` |
| `label_keep_limit` | `null` |

### Stage2 baseline mode rules

- `regular_baseline_mode` should be `full_milp` for the current Stage2-1 path; `rolling_milp` is only accepted as a backward-compatible alias and is normalized to `full_milp`.
- If `regular_baseline_mode` is omitted:
  - `prefer_milp=true` => `full_milp`
  - `prefer_milp=false` => `stage1_greedy_repair`

### Hotspot-relief rules

- When `hotspot_relief_enabled=true` and `fail_if_milp_disabled=true`, Stage2 will raise an error if the run is not actually configured for the full MILP baseline.
- Hotspot relief performs:
  - hotspot profile construction over cross-segment load
  - hot-range detection and structural-bottleneck classification
  - optional window augmentation from `scenario.candidate_windows`
  - local MILP re-optimization with peak and peak-integral objectives

## CLI / workbook injection

### `apps/run_stage1_workbook_batch.py`

- Writes Stage2 config using `Stage2Config` defaults plus CLI overrides.
- This preserves the full Stage2 field set in generated scenario JSON.

### `apps/run_stage2_workbook_sheet.py`

- Preserves and roundtrips all Stage2 fields from the base scenario.
- `--k-paths` still overrides `stage2.k_paths`.

### `tools/run_stage2_hotspot_relief.py`

Direct hotspot-relief experiment entry for fixed-plan validation.

Inputs:

- `--scenario-path`
- `--stage1-result-path` or `--fixed-plan-path`

Outputs:

- `result.json`
- `result_summary.json`
- `hotspot_report.json`
- `before_after_load_summary.json`

## Removed legacy fields

The loader still rejects the following removed fields:

- `stage1.k_paths`
- `stage1.near_completion_ratio`
- `stage1.omega_sr`
- `stage1.theta`
- `stage1.theta_c`
- `stage1.theta_eta0`
- `stage1.theta_sr`
- `stage1.viol_weight_cap`
- `stage1.viol_weight_hot`
- `stage1.viol_weight_sr`
- `stage2.affected_task_limit`
- `stage2.best_effort_on_failure`
- `stage2.insertion_horizon_seconds`
