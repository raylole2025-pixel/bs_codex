### stage1_20260411_seed7__scn_8a79172b__res_e66d4d9a__cand_1

- scenario_path: `D:\Codex_Project\bs3 - 副本\results\normal72x_v2_regular_tasks_adjusted\stage1_20260411_seed7\normal72x_v2_regular_tasks_adjusted_scenario_weighted.json`
- stage1_result_path: `D:\Codex_Project\bs3 - 副本\results\normal72x_v2_regular_tasks_adjusted\stage1_20260411_seed7\normal72x_v2_regular_tasks_adjusted_stage1_result.json`
- candidate_index: `1`
- baseline_source: `stage1_greedy_repair`
- baseline_solver_mode: `two_phase_event_insert+stage1_greedy_repair`
- closed_loop_solver_mode: `two_phase_event_insert+stage1_greedy_repair+hotspot_relief_local_peak_milp`
- closed_loop_rounds_completed: `2`
- closed_loop_actions_accepted: `2`
- closed_loop_new_windows_added: `2`
- closed_loop_stop_reason: `total_time_budget_exhausted`
- q_peak_before / after: `1.0000000000000002` -> `1.0000000000000002`
- q_integral_before / after: `32147.0507615` -> `30600.309812926876`
- high_segment_count_before / after: `281` -> `223`
- cr_reg_before / after: `1.0` -> `1.0`
- elapsed_seconds: `180.4986659999704`
- single_action_per_round: `True`
- round_recompute_chain_is_consistent: `True`
- head_reordered_after_first_acceptance: `True`
- range_topk_truncation_possible: `True`
- structural_candidate_limit_hit: `True`

Accepted actions:
- round 1: augment range=hot_range_16 window=X002479
- round 2: augment range=hot_range_18 window=X001371

Round summaries:
- round 1: action=augment range=hot_range_16 window=X002479
  hot_range_ids=['hot_range_22', 'hot_range_7', 'hot_range_24', 'hot_range_16', 'hot_range_19']
  q_peak=1.0000000000000002 -> 1.0000000000000002
  q_integral=32147.0507615 -> 31339.008032581874
  high_segment_count=281 -> 252
- round 2: action=augment range=hot_range_18 window=X001371
  hot_range_ids=['hot_range_21', 'hot_range_7', 'hot_range_23', 'hot_range_18', 'hot_range_15']
  q_peak=1.0000000000000002 -> 1.0000000000000002
  q_integral=31339.008032581874 -> 30600.309812926876
  high_segment_count=252 -> 223
