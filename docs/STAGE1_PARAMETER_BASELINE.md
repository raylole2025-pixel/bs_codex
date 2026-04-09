# Stage1 Parameter Baseline

> This file is kept for historical context.  
> The canonical parameter document is `docs/PARAMETER_SETTINGS.md`.

This file records the current Stage1 baseline after the 2026-04-08 Stage1 4.8 alignment.

The formal Stage1 metric set follows the "阶段一4.8" definition:

- feasibility constraints: `FR`, `eta_cap`, `hotspot_coverage`
- feasible-plan ranking: `N_act`, `eta_cap`, `hotspot_coverage`
- infeasible-plan recovery still uses `MR`

## Core Thresholds

- `FR = 1.0` (hard-coded feasibility condition)
- `theta_cap = 0.08`
- `theta_hot = 0.80`

## Core Capacity and Window Parameters

- `rho = 0.20`
- `t_pre = 1800 s`
- `d_min = 600 s`
- `hot_hop_limit = 4`
- `bottleneck_factor_alpha = 0.85`
- `eta_x = 0.90`
- `static_value_snapshot_seconds = 600`
- `q_eval = 4`

## Violation Aggregation Weights

- `omega_fr = 4 / 9`
- `omega_cap = 3 / 9`
- `omega_hot = 2 / 9`

## GA Defaults

- `population_size = 60`
- `crossover_probability = 0.90`
- `mutation_probability = 0.20`
- `max_generations = 100`
- `stall_generations = 20`
- `top_m = 5`
- `max_runtime_seconds = None`

## Removed Old Stage1 Parameters

The following parameters belonged to the previous `SR_{theta_c}` version and are no longer part of the current Stage1 baseline:

- `theta`
- `theta_sr`
- `theta_c`
- `near_completion_ratio`
- `omega_sr`
