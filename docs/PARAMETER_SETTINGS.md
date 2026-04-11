# BS3 参数设置总表

更新时间：2026-04-11

本文档只保留当前实现仍然有效的参数。旧参数和旧别名已经从模板、CLI 和场景加载入口中移除。

## 1. 场景 JSON

### 1.1 `stage1`

| 参数 | 默认值 |
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

阶段一正式约束与排序：

- 可行性硬约束：`FR=1`、`eta_cap<=theta_cap`、`hotspot_coverage>=theta_hot`
- 可行解排序：`activation_count`、`eta_cap`、`hotspot_coverage`

### 1.2 `stage1.ga`

| 参数 | 默认值 |
| --- | --- |
| `population_size` | `60` |
| `crossover_probability` | `0.9` |
| `mutation_probability` | `0.2` |
| `max_generations` | `100` |
| `stall_generations` | `20` |
| `top_m` | `5` |
| `max_runtime_seconds` | `null` |

### 1.3 `stage2`

| 参数 | 默认值 |
| --- | --- |
| `k_paths` | `2` |
| `completion_tolerance` | `1e-6` |
| `regular_baseline_mode` | `null` |
| `regular_repair_enabled` | `null` |
| `prefer_milp` | `true` |
| `milp_mode` | `rolling` |
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
| `label_keep_limit` | `null` |

说明：

- `regular_baseline_mode` 可选：`stage1_greedy`、`stage1_greedy_repair`、`rolling_milp`、`full_milp`
- 当未显式设置 `regular_baseline_mode` 时，按 legacy `prefer_milp + milp_mode` 兼容解析：
  - `prefer_milp=true && milp_mode=rolling -> rolling_milp`
  - `prefer_milp=true && milp_mode=full -> full_milp`
  - `prefer_milp=false -> stage1_greedy_repair`
- `stage1_greedy` 使用固定 Stage1 方案上的 segment-major greedy 常态基线构造
- `stage1_greedy_repair` 先生成 `stage1_greedy` baseline，再对少量高负载 block 做局部 MILP repair
- repair 的完成保护约束是：`block end remaining <= baseline remaining + epsilon`
- `rolling_milp` 和 `full_milp` 旧模式继续保留，用于对照或 ablation
- `Stage2-2` 仍然是事件驱动 emergency insertion / local repair / controlled preemption 主流程，只是消费新的 `baseline_schedule`

## 2. CLI 默认值

### 2.1 `apps/build_stage1_template_from_preprocess.py`

- `--cap-a=600.0 --cap-b=2000.0 --cap-x=1000.0`
- `--theta-cap=0.08 --theta-hot=0.80`
- `--rho=0.20 --t-pre=1800.0 --d-min=600.0`
- `--hot-hop-limit=4 --alpha=0.85 --eta-x=0.90`
- `--snapshot-seconds=600 --q-eval=4`
- `--omega-fr=4/9 --omega-cap=3/9 --omega-hot=2/9`
- `--elite-prune-count=6`

### 2.2 `apps/run_stage1_workbook_batch.py`

- `--base-scenario=inputs/templates/stage1_scenario_template.json`
- `--output-root=results/generated/stage1_taskset_runs`
- `--seed=7`

### 2.3 `apps/run_stage2_workbook_sheet.py`

- `--output-root=results/generated/stage2_taskset_runs`

## 3. 已删除的旧字段

以下字段现在会被视为旧字段，不再保留：

- `stage1.theta`
- `stage1.theta_sr`
- `stage1.theta_c`
- `stage1.theta_eta0`
- `stage1.near_completion_ratio`
- `stage1.omega_sr`
- `stage1.viol_weight_sr`
- `stage1.viol_weight_cap`
- `stage1.viol_weight_hot`
- `stage1.k_paths`
- `stage2.insertion_horizon_seconds`
- `stage2.affected_task_limit`
- `stage2.best_effort_on_failure`
