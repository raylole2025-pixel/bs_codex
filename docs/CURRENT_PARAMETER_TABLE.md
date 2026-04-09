# Current Parameter Table

> This file is kept for quick reference.  
> The canonical parameter document is `docs/PARAMETER_SETTINGS.md`.

This document summarizes the parameters used by the current codebase after the Stage1 4.9 candidate-pool alignment on 2026-04-09.

The current Stage1 metric set is aligned to the "阶段一4.8" note:

- formal Stage1 feasibility uses only `FR`, `eta_cap`, and weighted hotspot coverage `bar C^{A-hot}`
- feasible-solution ranking uses only `N_act`, `eta_cap`, and hotspot coverage
- `MR` remains an evaluation and recovery metric for infeasible solutions, but is not a hard feasibility constraint
- candidate screening now uses a 4.9-style candidate pool: `V_reg`/`V_hot` global base pool plus coarse-segment guarantee additions

## 1. Scenario Input Fields

| Parameter | Source | Meaning |
| --- | --- | --- |
| `planning_end` | scenario JSON root | Planning horizon end time in seconds. |
| `nodes.A` / `nodes.B` | scenario JSON root | Node sets for domain A and domain B. |
| `capacities.A` / `capacities.B` / `capacities.X` | scenario JSON root | Uniform capacity of A-domain links, B-domain links, and cross-domain links. |
| `intra_domain_links[].id` | scenario JSON root | Unique intra-domain link id. |
| `intra_domain_links[].u` / `v` | scenario JSON root | Two endpoints of the intra-domain link. |
| `intra_domain_links[].domain` | scenario JSON root | Domain label of the link, `A` or `B`. |
| `intra_domain_links[].start` / `end` | scenario JSON root | Active time interval of the intra-domain link. |
| `intra_domain_links[].delay` | scenario JSON root | Propagation plus processing delay used in path feasibility. |
| `intra_domain_links[].weight` | scenario JSON root | Generic edge weight; Stage1 falls back to this only when `distance_km` is absent. |
| `intra_domain_links[].distance_km` | scenario JSON root | Distance used by Stage1 shortest-path and backup-path construction. |
| `candidate_windows[].id` | scenario JSON root | Candidate cross-domain window id. |
| `candidate_windows[].a` / `b` | scenario JSON root | A-side gateway satellite and B-side gateway satellite. |
| `candidate_windows[].start` / `end` | scenario JSON root | Physical visibility interval of the candidate window. |
| `candidate_windows[].delay` | scenario JSON root | Cross-link propagation plus processing delay. |
| `candidate_windows[].distance_km` | scenario JSON root | Cross-link distance, used when available. |
| `candidate_windows[].value` | scenario JSON root | Regular-task static value `V_k^{reg}` written back after preprocessing; hotspot-channel values are kept in runtime metadata. |
| `tasks[].id` | scenario JSON root | Task id. |
| `tasks[].src` / `dst` | scenario JSON root | Source node and destination node. |
| `tasks[].arrival` / `deadline` | scenario JSON root | Arrival time and deadline. |
| `tasks[].data` | scenario JSON root | Total data volume. |
| `tasks[].weight` | scenario JSON root | Task priority weight. |
| `tasks[].max_rate` | scenario JSON root | Maximum service rate of the task. |
| `tasks[].type` | scenario JSON root | Task type, `reg` or `emg`. |
| `hotspots.A[].id` | scenario JSON root | A-domain hotspot region id. |
| `hotspots.A[].weight` | scenario JSON root | Hotspot weight in weighted hotspot coverage. |
| `hotspots.A[].nodes` | scenario JSON root | Static node set of the hotspot if no interval list is used. |
| `hotspots.A[].intervals[].start` / `end` | scenario JSON root | Active interval of a hotspot coverage slice. |
| `hotspots.A[].intervals[].nodes` | scenario JSON root | Covered A-domain nodes in that hotspot slice. |

## 2. Stage1 Scenario Parameters

Stored under `stage1` in the scenario JSON and parsed by [scenario.py](/D:/Codex_Project/bs3/bs3/scenario.py).

| Parameter | Current Default | Meaning |
| --- | --- | --- |
| `rho` | `0.20` | Reserved fraction of cross-domain capacity for emergency tasks. Stage1 regular tasks use `(1-rho)C_X`. |
| `t_pre` | `1800` | Preheat / preparation time before a cross-domain window becomes active. |
| `d_min` | `600` | Minimum effective build duration required to accept a candidate window. |
| `theta_cap` | `0.08` | Maximum acceptable cross-capacity shortfall ratio. |
| `theta_hot` | `0.80` | Minimum acceptable weighted hotspot coverage ratio. |
| `hot_hop_limit` | `4` | Maximum A-domain hop count from hotspot to active A-side gateway for coverage. |
| `bottleneck_factor_alpha` | `0.85` | Threshold for deciding whether the primary path is bottlenecked on one domain side and needs a backup path. |
| `eta_x` | `0.90` | Near-best transmitted-volume threshold for final path selection within one segment. |
| `static_value_snapshot_seconds` | `600` | Coarse time slice length used when computing static potential value `V_k^(0)`. |
| `candidate_pool_base_size` | `400` | Base pool size selected by the global `V_reg` / `V_hot` channels before coarse-segment补充. |
| `candidate_pool_hot_fraction` | `0.30` | Share of the base pool allocated to the hotspot-value channel; the remainder uses the regular-task value channel. |
| `candidate_pool_min_per_coarse_segment` | `3` | Minimum number of representative windows each positive-demand coarse segment should keep after screening. |
| `candidate_pool_max_additional` | `150` | Upper bound on the coarse-segment guarantee additions. |
| `q_eval` | `4` | Decoder evaluates feasibility every `q_eval` accepted windows. |
| `omega_fr` | `4/9` | Violation aggregation weight for `1 - FR`. |
| `omega_cap` | `3/9` | Violation aggregation weight for capacity shortfall. |
| `omega_hot` | `2/9` | Violation aggregation weight for hotspot shortfall. |
| `elite_prune_count` | `6` | Number of feasible elites pruned each generation. |

Notes:

- `FR` is a hard feasibility condition fixed at `1.0`; it is no longer configured by `theta_sr` or `theta_c`.
- Old fields `theta`, `theta_sr`, `theta_c`, and `near_completion_ratio` are ignored by the current Stage1 implementation.

## 3. Stage1 GA Parameters

Stored under `stage1.ga` in the scenario JSON.

| Parameter | Current Default | Meaning |
| --- | --- | --- |
| `population_size` | `60` | GA population size `P`. |
| `crossover_probability` | `0.90` | Probability of using prefix-preserving crossover. |
| `mutation_probability` | `0.20` | Total mutation probability. Mutation type is `70% insert / 30% swap`. |
| `max_generations` | `100` | Maximum number of generations. |
| `stall_generations` | `20` | Early-stop patience when best progress does not improve. |
| `top_m` | `5` | Number of top feasible plans retained in the final archive. |
| `max_runtime_seconds` | `None` | Optional runtime cap. |

## 4. Stage1 Result Metrics

These are the formal Stage1 result metrics written into retained candidate outputs.

| Metric | Meaning |
| --- | --- |
| `fr` | Weighted true completion rate `FR^{(1)}` for regular cross-domain tasks. |
| `mean_completion_ratio` | Weighted mean completion ratio `MR^{(1)}`. |
| `eta_cap` | Cross-domain capacity shortfall ratio `eta_cap`. |
| `eta_0` | Zero-cross-demand ratio retained as a diagnostic. |
| `avg_hot_coverage` / `hotspot_coverage` | Weighted hotspot coverage `bar C^{A-hot}`. |
| `max_hot_gap` / `hotspot_max_gap` | Longest continuous hotspot uncovered interval. |
| `activation_count` | Activation count `N_act = 2|S|`. |
| `activation_time` | Total gateway activation time induced by the plan. |

## 5. Stage2 Scenario Parameters

Stored under `stage2` in the scenario JSON.

| Parameter | Current Default | Meaning |
| --- | --- | --- |
| `k_paths` | `2` | Number of candidate intra-domain paths used by Stage2 scheduling. |
| `completion_tolerance` | `1e-6` | Completion tolerance ratio for judging task completion. |

## 6. Batch Runner Parameters

These come from [run_stage1_workbook_batch.py](/D:/Codex_Project/bs3/apps/run_stage1_workbook_batch.py) and are not part of the core scenario model unless written back into the generated scenario/result files.

| Parameter | Current Default | Meaning |
| --- | --- | --- |
| `cap_a` / `cap_b` / `cap_x` | `600 / 2000 / 1000` | Capacity defaults injected into generated scenarios. |
| `seed` | `7` | Random seed for Stage1 GA. |
| `candidate_pool_base_size` | `400` | CLI override for the global candidate-pool base size. |
| `candidate_pool_hot_fraction` | `0.30` | CLI override for the hotspot-value channel ratio in the base pool. |
| `candidate_pool_min_per_coarse_segment` | `3` | CLI override for the minimum coarse-segment representative count. |
| `candidate_pool_max_additional` | `150` | CLI override for the guarantee-addition cap. |
| `run_stage2` | `False` | Whether to continue into Stage2 after Stage1. |
| `stage2_k_paths` | `2` | Stage2 path count written into generated scenario payload. |
| `disable_distance_enrichment` | `False` | Whether to skip distance and delay enrichment. |
| `light_speed_kmps` | `299792.458` | Speed of light used when converting distance to propagation delay. |
| `intra_proc_delay_sec` | `0.0002` | Domain-internal processing delay added during distance enrichment. |
| `cross_proc_delay_sec` | `0.0010` | Cross-domain processing delay added during distance enrichment. |
| `max_cross_distance_km` | `5000` | Hard filter on candidate cross-domain distance before Stage1. |

Deprecated but still accepted by some CLI wrappers:

- `--theta`
- `--theta-sr`
- `--theta-c`
- `--omega-sr` as an alias of `--omega-fr`
- `--screen-pool-size`
- `--screen-block-seconds`

## 7. Fixed Internal Constants

| Constant | Location | Meaning |
| --- | --- | --- |
| `elite_count = ceil(0.1P)` | [models.py](/D:/Codex_Project/bs3/bs3/models.py) | Number of elites copied each generation. |
| `immigrant_count = ceil(0.1P)` | [models.py](/D:/Codex_Project/bs3/bs3/models.py) | Number of immigrants injected each generation. |
| `TOURNAMENT_SIZE = 3` | [stage1.py](/D:/Codex_Project/bs3/bs3/stage1.py) | Tournament selection sample size. |
| `PRUNE_EXACT_LIMIT = 8` | [stage1.py](/D:/Codex_Project/bs3/bs3/stage1.py) | Max exact pruning validations attempted per pruning round. |
