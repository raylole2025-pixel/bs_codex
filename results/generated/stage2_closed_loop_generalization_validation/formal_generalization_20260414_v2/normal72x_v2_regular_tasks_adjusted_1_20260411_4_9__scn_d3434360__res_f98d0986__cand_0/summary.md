### normal72x_v2_regular_tasks_adjusted_1_20260411_4_9__scn_d3434360__res_f98d0986__cand_0

- scenario_path: `D:\Codex_Project\bs3 - 副本\results\normal72x_v2_regular_tasks_adjusted\normal72x_v2_regular_tasks_adjusted_阶段1结果_20260411_按4.9整理\normal72x_v2_regular_tasks_adjusted_scenario_weighted.json`
- stage1_result_path: `D:\Codex_Project\bs3 - 副本\results\normal72x_v2_regular_tasks_adjusted\normal72x_v2_regular_tasks_adjusted_阶段1结果_20260411_按4.9整理\normal72x_v2_regular_tasks_adjusted_stage1_result.json`
- candidate_index: `0`
- baseline_source: `stage1_greedy_repair`
- baseline_solver_mode: `two_phase_event_insert+stage1_greedy_repair`
- closed_loop_solver_mode: `two_phase_event_insert+stage1_greedy_repair+hotspot_relief_local_peak_milp`
- closed_loop_rounds_completed: `2`
- closed_loop_actions_accepted: `1`
- closed_loop_new_windows_added: `1`
- closed_loop_stop_reason: `no_acceptable_action`
- q_peak_before / after: `1.0000000000000002` -> `1.0000000000000002`
- q_integral_before / after: `29317.318595500004` -> `27567.696668325003`
- high_segment_count_before / after: `243` -> `187`
- cr_reg_before / after: `0.9466666666666667` -> `0.96`
- elapsed_seconds: `180.98588560009375`
- single_action_per_round: `True`
- round_recompute_chain_is_consistent: `True`
- head_reordered_after_first_acceptance: `False`
- range_topk_truncation_possible: `True`
- structural_candidate_limit_hit: `True`

Accepted actions:
- round 1: augment range=hot_range_10 window=X011840

Round summaries:
- round 1: action=augment range=hot_range_10 window=X011840
  hot_range_ids=['hot_range_2', 'hot_range_13', 'hot_range_6', 'hot_range_10', 'hot_range_1']
  q_peak=1.0000000000000002 -> 1.0000000000000002
  q_integral=29317.318595500004 -> 27567.696668325003
  high_segment_count=243 -> 187
- round 2: action=None range=None window=None
  hot_range_ids=['hot_range_2', 'hot_range_13', 'hot_range_6', 'hot_range_1', 'hot_range_15']
  q_peak=1.0000000000000002 -> 1.0000000000000002
  q_integral=27567.696668325003 -> 27567.696668325003
  high_segment_count=187 -> 187
