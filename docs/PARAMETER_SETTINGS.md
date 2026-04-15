# BS3 Parameter Settings

Updated: 2026-04-15

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

## Stage 2 Emergency Insertion Policy

Stage 2 now keeps the existing external strategy labels:

- `direct_insert`
- `controlled_preemption`
- `direct_insert_best_effort`
- `blocked`

Emergency insertion still runs on top of a fixed Stage 1 `selected_plan` plus `baseline_trace`, and it does not add any new required JSON fields.

### Capacity tiers on active cross-domain windows

For every active cross-domain window `x` and segment `r`, Stage 2 computes:

- `total_free = Cx - used_reg - used_emg`
- `reserve_free = max(rho * Cx - used_emg, 0)`

where:

- `used_reg` is committed regular usage on that active cross-domain window in the segment
- `used_emg` is committed emergency usage from previously inserted emergency tasks

Emergency direct insert treats these tiers as:

- `reserved_only`: requested rate fits inside `reserve_free`
- `borrow_unused_regular_share`: `reserve_free` is not enough, but `total_free` is still enough
- `preempted`: direct insert only becomes feasible after releasing one lower-priority regular task
- `blocked`: neither direct insert nor the single-task preemption pass can provide useful service

Reserve is a protected priority region, not a hard ceiling. Emergency traffic may exceed `rho * Cx` whenever total residual capacity is available. All intra-domain edges and cross-domain windows are still checked against residual capacity along the full end-to-end candidate path.

### Label planner ordering

For emergency insertion, nondominated labels now track `tier_cost` and use these orderings:

- partial key: `(remaining_data, tier_cost, switches, load_cost, idle_steps)`
- terminal key: `(tier_cost, finish_time, switches, load_cost, idle_steps)`

`load_cost` is now the sum of each chosen segment-path's post-allocation maximum edge utilization over the full path, not only the cross-domain window.

### Controlled preemption defaults

Controlled preemption is limited to one regular task per emergency insertion attempt. Candidate regular tasks must:

- overlap the emergency corridor on real future edges
- be lower priority than the emergency task
- fall into the lowest regular weight tier when no explicit A/B/C class exists

The implementation ranks candidates by a loss-to-release score using these module defaults:

- `PREEMPTION_WEIGHT_COEFF = 0.45`
- `PREEMPTION_SENT_RATIO_COEFF = 0.35`
- `PREEMPTION_SLACK_COEFF = 0.20`
- `PREEMPTION_SCORE_EPS = 1e-6`

No extra scenario schema fields are required for these constants. Each emergency insertion event records the chosen capacity tier, direct-plan delivery, whether preemption was used, the released task/window/edge details, and the computed preemption score.

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
