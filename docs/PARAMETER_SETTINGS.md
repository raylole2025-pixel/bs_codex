# BS3 Parameter Settings

Updated: 2026-04-14

This repository now uses a two-stage structure:

- Stage 1: cross-domain link configuration plus regular-task baseline export
- Stage 2: emergency-task insertion on top of the exported baseline

The old Stage2-1 hotspot-relief / closed-loop / regular repair path has been removed. Its old configuration fields are no longer part of the active schema.

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
| `label_keep_limit` | `null` |

`label_keep_limit` controls how many nondominated labels Stage 2 keeps per bucket during emergency insertion. When omitted, the code derives the effective value from `k_paths`.

## Active Outputs

### Stage 1

- `selected_plan`
- `baseline_summary`
- `baseline_trace`

### Stage 2

- emergency insertion result
- regular/emergency completion metrics
- insertion events
- baseline/final cross-window usage comparison

## Removed Stage2 Fields

The loader now rejects old Stage2-1 fields, including:

- `regular_baseline_mode`
- `regular_repair_enabled`
- `prefer_milp`
- `milp_*`
- `repair_*`
- `hotspot_relief_enabled`
- `closed_loop_relief_enabled`
- `hotspot_*`
- `augment_*`
- `closed_loop_*`
- `hot_path_limit`
- `hot_promoted_tasks_per_segment`
- `local_peak_*`
- `fail_if_milp_disabled`

The loader also continues to reject older removed fields:

- `stage2.affected_task_limit`
- `stage2.best_effort_on_failure`
- `stage2.insertion_horizon_seconds`
