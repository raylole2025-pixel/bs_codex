# Stage2 Closed Loop Summary

- scenario_path: `D:\Codex_Project\bs3 - 副本\results\normal72x_v2_regular_tasks_adjusted\stage1_20260411_seed7\normal72x_v2_regular_tasks_adjusted_scenario_weighted.json`
- stage1_result_path: `D:\Codex_Project\bs3 - 副本\results\normal72x_v2_regular_tasks_adjusted\stage1_20260411_seed7\normal72x_v2_regular_tasks_adjusted_stage1_result.json`
- baseline_source: `stage1_greedy_repair`
- solver_mode: `two_phase_event_insert+stage1_greedy_repair+hotspot_relief_local_peak_milp`
- closed_loop_rounds_completed: `2`
- closed_loop_actions_accepted: `1`
- closed_loop_new_windows_added: `1`
- closed_loop_stop_reason: `no_acceptable_action`
- effective_hard_cap: `2`
- hard_cap_limiter: `both`
- q_peak_before / after: `1.0000000000000002` -> `1.0000000000000002`
- q_integral_before / after: `32159.026793375` -> `31350.984064456876`
- high_segment_count_before / after: `290` -> `261`

## Rounds

- round 1: action=augment range=hot_range_16 window=X002479
  q_peak: 1.0000000000000002 -> 1.0000000000000002
  q_integral: 32159.026793375 -> 31350.984064456876
  high_segment_count: 290 -> 261
- round 2: action=None range=None window=None
  q_peak: 1.0000000000000002 -> 1.0000000000000002
  q_integral: 31350.984064456876 -> 31350.984064456876
  high_segment_count: 261 -> 261
