### normal72x_v2_regular_tasks_adjusted_1_20260411_4_9__scn_d3434360__res_f98d0986__cand_2

- scenario_path: `D:\Codex_Project\bs3 - 副本\results\normal72x_v2_regular_tasks_adjusted\normal72x_v2_regular_tasks_adjusted_阶段1结果_20260411_按4.9整理\normal72x_v2_regular_tasks_adjusted_scenario_weighted.json`
- stage1_result_path: `D:\Codex_Project\bs3 - 副本\results\normal72x_v2_regular_tasks_adjusted\normal72x_v2_regular_tasks_adjusted_阶段1结果_20260411_按4.9整理\normal72x_v2_regular_tasks_adjusted_stage1_result.json`
- candidate_index: `2`
- baseline_source: `stage1_greedy_repair`
- baseline_solver_mode: `two_phase_event_insert+stage1_greedy_repair`
- closed_loop_solver_mode: `two_phase_event_insert+stage1_greedy_repair+hotspot_relief_local_peak_milp`
- closed_loop_rounds_completed: `2`
- closed_loop_actions_accepted: `2`
- closed_loop_new_windows_added: `2`
- closed_loop_stop_reason: `total_time_budget_exhausted`
- q_peak_before / after: `1.0000000000000002` -> `1.0000000000000002`
- q_integral_before / after: `27399.86224725` -> `25948.278944115085`
- high_segment_count_before / after: `167` -> `99`
- cr_reg_before / after: `0.9666666666666667` -> `0.98`
- elapsed_seconds: `180.60711900005117`
- single_action_per_round: `True`
- round_recompute_chain_is_consistent: `True`
- head_reordered_after_first_acceptance: `False`
- range_topk_truncation_possible: `True`
- structural_candidate_limit_hit: `True`

Accepted actions:
- round 1: augment range=hot_range_10 window=X017838
- round 2: augment range=hot_range_1 window=X018349

Round summaries:
- round 1: action=augment range=hot_range_10 window=X017838
  hot_range_ids=['hot_range_2', 'hot_range_16', 'hot_range_6', 'hot_range_15', 'hot_range_10']
  q_peak=1.0000000000000002 -> 1.0000000000000002
  q_integral=27399.86224725 -> 26409.558161999998
  high_segment_count=167 -> 133
- round 2: action=augment range=hot_range_1 window=X018349
  hot_range_ids=['hot_range_2', 'hot_range_15', 'hot_range_6', 'hot_range_14', 'hot_range_1']
  q_peak=1.0000000000000002 -> 1.0000000000000002
  q_integral=26409.558161999998 -> 25948.278944115085
  high_segment_count=133 -> 99
