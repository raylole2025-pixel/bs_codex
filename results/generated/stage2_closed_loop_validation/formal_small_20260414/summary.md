# Stage2 Closed-Loop Validation Summary

- scenario_path: `D:\Codex_Project\bs3 - 副本\results\normal72x_v2_regular_tasks_adjusted\stage1_20260411_seed7\normal72x_v2_regular_tasks_adjusted_scenario_weighted.json`
- stage1_result_path: `D:\Codex_Project\bs3 - 副本\results\normal72x_v2_regular_tasks_adjusted\stage1_20260411_seed7\normal72x_v2_regular_tasks_adjusted_stage1_result.json`
- experiment_count: `7`

## Analysis

- baseline_experiment: `baseline_control`
- all_closed_loop_runs_single_action_per_round: `True`
- all_closed_loop_runs_recompute_consistent: `True`
- q_peak_improved_experiments: `[]`
- integral_only_improved_experiments: `['r3_w1', 'r3_w2', 'r3_w3', 'r5_w1', 'r5_w2', 'r5_w3']`
- stop_reason_counts: `{'no_acceptable_action': 6}`
- stable_improvement_vs_baseline_count: `6`

## Experiments

### baseline_control

- baseline_source: `stage1_greedy_repair`
- solver_mode: `two_phase_event_insert+stage1_greedy_repair`
- closed_loop_rounds_completed: `0`
- closed_loop_actions_accepted: `0`
- closed_loop_new_windows_added: `0`
- closed_loop_stop_reason: `closed_loop_disabled`
- closed_loop_new_window_hard_cap: `3`
- q_peak_before / after: `1.0000000000000002` -> `1.0000000000000002`
- q_integral_before / after: `32159.026793375` -> `32159.026793375`
- high_segment_count_before / after: `290` -> `290`
- elapsed_seconds: `10.178422600030899`

Accepted actions:
- none

### r3_w1

- baseline_source: `stage1_greedy_repair`
- solver_mode: `two_phase_event_insert+stage1_greedy_repair+hotspot_relief_local_peak_milp`
- closed_loop_rounds_completed: `2`
- closed_loop_actions_accepted: `1`
- closed_loop_new_windows_added: `1`
- closed_loop_stop_reason: `no_acceptable_action`
- closed_loop_new_window_hard_cap: `1`
- q_peak_before / after: `1.0000000000000002` -> `1.0000000000000002`
- q_integral_before / after: `32159.026793375` -> `31350.984064456876`
- high_segment_count_before / after: `290` -> `261`
- elapsed_seconds: `180.6002200000221`

Accepted actions:
- round 1: augment range=hot_range_16 window=X002479

Round summaries:
- round 1: action=augment range=hot_range_16 window=X002479
  hot_range_ids=['hot_range_22', 'hot_range_7', 'hot_range_24', 'hot_range_16', 'hot_range_20']
  q_peak=1.0000000000000002 -> 1.0000000000000002
  q_integral=32159.026793375 -> 31350.984064456876
  high_segment_count=290 -> 261
- round 2: action=None range=None window=None
  hot_range_ids=['hot_range_21', 'hot_range_7', 'hot_range_23', 'hot_range_19', 'hot_range_15']
  q_peak=1.0000000000000002 -> 1.0000000000000002
  q_integral=31350.984064456876 -> 31350.984064456876
  high_segment_count=261 -> 261

### r3_w2

- baseline_source: `stage1_greedy_repair`
- solver_mode: `two_phase_event_insert+stage1_greedy_repair+hotspot_relief_local_peak_milp`
- closed_loop_rounds_completed: `2`
- closed_loop_actions_accepted: `1`
- closed_loop_new_windows_added: `1`
- closed_loop_stop_reason: `no_acceptable_action`
- closed_loop_new_window_hard_cap: `2`
- q_peak_before / after: `1.0000000000000002` -> `1.0000000000000002`
- q_integral_before / after: `32159.026793375` -> `31350.984064456876`
- high_segment_count_before / after: `290` -> `261`
- elapsed_seconds: `181.76854949991684`

Accepted actions:
- round 1: augment range=hot_range_16 window=X002479

Round summaries:
- round 1: action=augment range=hot_range_16 window=X002479
  hot_range_ids=['hot_range_22', 'hot_range_7', 'hot_range_24', 'hot_range_16', 'hot_range_20']
  q_peak=1.0000000000000002 -> 1.0000000000000002
  q_integral=32159.026793375 -> 31350.984064456876
  high_segment_count=290 -> 261
- round 2: action=None range=None window=None
  hot_range_ids=['hot_range_21', 'hot_range_7', 'hot_range_23', 'hot_range_19', 'hot_range_15']
  q_peak=1.0000000000000002 -> 1.0000000000000002
  q_integral=31350.984064456876 -> 31350.984064456876
  high_segment_count=261 -> 261

### r3_w3

- baseline_source: `stage1_greedy_repair`
- solver_mode: `two_phase_event_insert+stage1_greedy_repair+hotspot_relief_local_peak_milp`
- closed_loop_rounds_completed: `2`
- closed_loop_actions_accepted: `1`
- closed_loop_new_windows_added: `1`
- closed_loop_stop_reason: `no_acceptable_action`
- closed_loop_new_window_hard_cap: `3`
- q_peak_before / after: `1.0000000000000002` -> `1.0000000000000002`
- q_integral_before / after: `32159.026793375` -> `31350.984064456876`
- high_segment_count_before / after: `290` -> `261`
- elapsed_seconds: `182.0756754000904`

Accepted actions:
- round 1: augment range=hot_range_16 window=X002479

Round summaries:
- round 1: action=augment range=hot_range_16 window=X002479
  hot_range_ids=['hot_range_22', 'hot_range_7', 'hot_range_24', 'hot_range_16', 'hot_range_20']
  q_peak=1.0000000000000002 -> 1.0000000000000002
  q_integral=32159.026793375 -> 31350.984064456876
  high_segment_count=290 -> 261
- round 2: action=None range=None window=None
  hot_range_ids=['hot_range_21', 'hot_range_7', 'hot_range_23', 'hot_range_19', 'hot_range_15']
  q_peak=1.0000000000000002 -> 1.0000000000000002
  q_integral=31350.984064456876 -> 31350.984064456876
  high_segment_count=261 -> 261

### r5_w1

- baseline_source: `stage1_greedy_repair`
- solver_mode: `two_phase_event_insert+stage1_greedy_repair+hotspot_relief_local_peak_milp`
- closed_loop_rounds_completed: `2`
- closed_loop_actions_accepted: `1`
- closed_loop_new_windows_added: `1`
- closed_loop_stop_reason: `no_acceptable_action`
- closed_loop_new_window_hard_cap: `1`
- q_peak_before / after: `1.0000000000000002` -> `1.0000000000000002`
- q_integral_before / after: `32159.026793375` -> `31350.984064456876`
- high_segment_count_before / after: `290` -> `261`
- elapsed_seconds: `180.73058710002806`

Accepted actions:
- round 1: augment range=hot_range_16 window=X002479

Round summaries:
- round 1: action=augment range=hot_range_16 window=X002479
  hot_range_ids=['hot_range_22', 'hot_range_7', 'hot_range_24', 'hot_range_16', 'hot_range_20']
  q_peak=1.0000000000000002 -> 1.0000000000000002
  q_integral=32159.026793375 -> 31350.984064456876
  high_segment_count=290 -> 261
- round 2: action=None range=None window=None
  hot_range_ids=['hot_range_21', 'hot_range_7', 'hot_range_23', 'hot_range_19', 'hot_range_15']
  q_peak=1.0000000000000002 -> 1.0000000000000002
  q_integral=31350.984064456876 -> 31350.984064456876
  high_segment_count=261 -> 261

### r5_w2

- baseline_source: `stage1_greedy_repair`
- solver_mode: `two_phase_event_insert+stage1_greedy_repair+hotspot_relief_local_peak_milp`
- closed_loop_rounds_completed: `2`
- closed_loop_actions_accepted: `1`
- closed_loop_new_windows_added: `1`
- closed_loop_stop_reason: `no_acceptable_action`
- closed_loop_new_window_hard_cap: `2`
- q_peak_before / after: `1.0000000000000002` -> `1.0000000000000002`
- q_integral_before / after: `32159.026793375` -> `31350.984064456876`
- high_segment_count_before / after: `290` -> `261`
- elapsed_seconds: `182.09265990008134`

Accepted actions:
- round 1: augment range=hot_range_16 window=X002479

Round summaries:
- round 1: action=augment range=hot_range_16 window=X002479
  hot_range_ids=['hot_range_22', 'hot_range_7', 'hot_range_24', 'hot_range_16', 'hot_range_20']
  q_peak=1.0000000000000002 -> 1.0000000000000002
  q_integral=32159.026793375 -> 31350.984064456876
  high_segment_count=290 -> 261
- round 2: action=None range=None window=None
  hot_range_ids=['hot_range_21', 'hot_range_7', 'hot_range_23', 'hot_range_19', 'hot_range_15']
  q_peak=1.0000000000000002 -> 1.0000000000000002
  q_integral=31350.984064456876 -> 31350.984064456876
  high_segment_count=261 -> 261

### r5_w3

- baseline_source: `stage1_greedy_repair`
- solver_mode: `two_phase_event_insert+stage1_greedy_repair+hotspot_relief_local_peak_milp`
- closed_loop_rounds_completed: `2`
- closed_loop_actions_accepted: `1`
- closed_loop_new_windows_added: `1`
- closed_loop_stop_reason: `no_acceptable_action`
- closed_loop_new_window_hard_cap: `3`
- q_peak_before / after: `1.0000000000000002` -> `1.0000000000000002`
- q_integral_before / after: `32159.026793375` -> `31350.984064456876`
- high_segment_count_before / after: `290` -> `261`
- elapsed_seconds: `182.23658619995695`

Accepted actions:
- round 1: augment range=hot_range_16 window=X002479

Round summaries:
- round 1: action=augment range=hot_range_16 window=X002479
  hot_range_ids=['hot_range_22', 'hot_range_7', 'hot_range_24', 'hot_range_16', 'hot_range_20']
  q_peak=1.0000000000000002 -> 1.0000000000000002
  q_integral=32159.026793375 -> 31350.984064456876
  high_segment_count=290 -> 261
- round 2: action=None range=None window=None
  hot_range_ids=['hot_range_21', 'hot_range_7', 'hot_range_23', 'hot_range_19', 'hot_range_15']
  q_peak=1.0000000000000002 -> 1.0000000000000002
  q_integral=31350.984064456876 -> 31350.984064456876
  high_segment_count=261 -> 261

