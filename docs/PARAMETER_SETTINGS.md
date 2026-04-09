# BS3 参数设置总表（唯一入口）

本文件是当前项目参数的统一入口，覆盖代码默认值、脚本参数默认值和兼容字段。  
更新时间：2026-04-08（对应“阶段一4.8”对齐后）。

## 1. 场景 JSON（核心运行参数）

### 1.1 `stage1` 默认值（来源：`bs3/models.py`）

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
| `q_eval` | `4` |
| `omega_fr` | `4/9` |
| `omega_cap` | `3/9` |
| `omega_hot` | `2/9` |
| `elite_prune_count` | `6` |

说明：

- `FR=1` 是阶段一的硬约束，不再通过 `theta_sr` 或 `theta_c` 配置。
- 旧字段 `theta`、`theta_sr`、`theta_c`、`near_completion_ratio` 在当前实现中均不再生效。

### 1.2 `stage1.ga` 默认值（来源：`bs3/models.py`）

| 参数 | 默认值 |
| --- | --- |
| `population_size` | `60` |
| `crossover_probability` | `0.9` |
| `mutation_probability` | `0.2` |
| `max_generations` | `100` |
| `stall_generations` | `20` |
| `top_m` | `5` |
| `max_runtime_seconds` | `null` |

派生值（不可直接配置）：

- `elite_count = ceil(0.1 * population_size)`
- `immigrant_count = ceil(0.1 * population_size)`

### 1.3 `stage2` 默认值（来源：`bs3/models.py`）

| 参数 | 默认值 |
| --- | --- |
| `k_paths` | `2` |
| `completion_tolerance` | `1e-6` |
| `label_keep_limit` | `null`（内部可选） |

派生值：

- `effective_label_keep_limit = label_keep_limit or max(8 * k_paths, 1)`

## 2. 阶段一 4.8 的正式指标

### 2.1 可行性硬约束

- `FR(S) = 1`
- `eta_cap(S) <= theta_cap`
- `hotspot_coverage(S) >= theta_hot`

### 2.2 可行解排序

- `N_act`
- `eta_cap`
- `hotspot_coverage`

### 2.3 不可行解恢复指标

- `MR`
- `Viol`

其中违约聚合为：

- `omega_fr * [1 - FR]_+`
- `omega_cap * [eta_cap - theta_cap]_+ / theta_cap`
- `omega_hot * [theta_hot - hotspot_coverage]_+ / theta_hot`

## 3. Apps 脚本默认值

### 3.1 `apps/build_stage1_template_from_preprocess.py`

- `--cap-a=600.0 --cap-b=2000.0 --cap-x=1000.0`
- `--theta-cap=0.08 --theta-hot=0.80`
- `--rho=0.20 --t-pre=1800.0 --d-min=600.0`
- `--hot-hop-limit=4 --alpha=0.85 --eta-x=0.90`
- `--snapshot-seconds=600 --q-eval=4`
- `--omega-fr=4/9 --omega-cap=3/9 --omega-hot=2/9`
- `--elite-prune-count=6`

兼容但已弃用：

- `--theta`
- `--theta-sr`
- `--theta-c`
- `--omega-sr`（作为 `--omega-fr` 别名）

### 3.2 `apps/run_stage1_workbook_batch.py`

- `--base-scenario=inputs/templates/stage1_scenario_template.json`
- `--output-root=outputs/active/stage1/taskset_runs`
- `--seed=7 --sheets=[medium48,stress96]`
- `--cap-a=600.0 --cap-b=2000.0 --cap-x=1000.0`
- `--theta-cap=0.08 --theta-hot=0.80`
- `--rho=0.20 --t-pre=1800.0 --d-min=600.0`
- `--hot-hop-limit=4 --alpha=0.85 --eta-x=0.90`
- `--snapshot-seconds=600 --q-eval=4`
- `--omega-fr=4/9 --omega-cap=3/9 --omega-hot=2/9`
- `--elite-prune-count=6`
- `--population-size=60 --crossover-probability=0.90 --mutation-probability=0.20`
- `--max-generations=100 --stall-generations=20 --top_m=5 --max-runtime-seconds=None`

兼容但已弃用：

- `--theta`
- `--theta-sr`
- `--theta-c`
- `--omega-sr`（作为 `--omega-fr` 别名）

## 4. 兼容字段与旧版本残留处理

仍支持的兼容映射：

- `theta_eta0 -> theta_cap`
- `alpha -> bottleneck_factor_alpha`
- `omega_sr -> omega_fr`
- `viol_weight_sr/cap/hot -> omega_fr/cap/hot`

仅忽略、不再生效的旧字段：

- `theta`
- `theta_sr`
- `theta_c`
- `near_completion_ratio`

## 5. 与“阶段一4.8”对齐结论（代码层）

- Stage1 可行性判据：`FR`、`eta_cap`、`hotspot_coverage`。
- Stage1 可行解排序优先项：`N_act`、`eta_cap`、`hotspot_coverage`。
- Stage1 不再使用 `theta_sr=0.9`、`theta_c=0.95` 这套旧参数。
- Stage1 结果导出使用 `fr`，不再写 `sr_theta_c`、`sr_near`。
