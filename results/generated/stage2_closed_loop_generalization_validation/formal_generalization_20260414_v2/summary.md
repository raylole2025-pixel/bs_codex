# Stage2 Closed-Loop Generalization Validation

- case_count: `10`
- unique_scenario_count: `2`
- unique_stage1_result_count: `2`
- discovered_regular_tasksets: `['normal72x_v2_regular_tasks_adjusted']`
- notes: `Repository provided one formal regular-task corpus under results/, with two distinct scenario_weighted + stage1_result snapshots and five best_feasible candidate plans each. No second formal regular-task corpus with paired Stage1 outputs was found under results/.`

## Analysis

- all_cases_baseline_source_stage1_greedy_repair: `True`
- all_closed_loop_runs_single_action_per_round: `True`
- all_closed_loop_runs_recompute_consistent: `True`
- first_round_single_action_saturation_count: `5`
- q_peak_improved_case_count: `0`
- integral_or_highseg_only_case_count: `10`
- stop_reason_counts: `{'no_acceptable_action': 5, 'total_time_budget_exhausted': 5}`
- improvement_vs_baseline_case_count: `10`
- range_topk_truncation_possible_case_count: `10`
- structural_candidate_limit_hit_case_count: `10`

## Cases

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

### normal72x_v2_regular_tasks_adjusted_1_20260411_4_9__scn_d3434360__res_f98d0986__cand_1

- scenario_path: `D:\Codex_Project\bs3 - 副本\results\normal72x_v2_regular_tasks_adjusted\normal72x_v2_regular_tasks_adjusted_阶段1结果_20260411_按4.9整理\normal72x_v2_regular_tasks_adjusted_scenario_weighted.json`
- stage1_result_path: `D:\Codex_Project\bs3 - 副本\results\normal72x_v2_regular_tasks_adjusted\normal72x_v2_regular_tasks_adjusted_阶段1结果_20260411_按4.9整理\normal72x_v2_regular_tasks_adjusted_stage1_result.json`
- candidate_index: `1`
- baseline_source: `stage1_greedy_repair`
- baseline_solver_mode: `two_phase_event_insert+stage1_greedy_repair`
- closed_loop_solver_mode: `two_phase_event_insert+stage1_greedy_repair+hotspot_relief_local_peak_milp`
- closed_loop_rounds_completed: `2`
- closed_loop_actions_accepted: `2`
- closed_loop_new_windows_added: `1`
- closed_loop_stop_reason: `total_time_budget_exhausted`
- q_peak_before / after: `1.0000000000000002` -> `1.0000000000000002`
- q_integral_before / after: `27953.102758` -> `26975.999669590088`
- high_segment_count_before / after: `203` -> `154`
- cr_reg_before / after: `0.98` -> `0.98`
- elapsed_seconds: `180.3652808999177`
- single_action_per_round: `True`
- round_recompute_chain_is_consistent: `True`
- head_reordered_after_first_acceptance: `True`
- range_topk_truncation_possible: `True`
- structural_candidate_limit_hit: `True`

Accepted actions:
- round 1: augment range=hot_range_1 window=X018349
- round 2: reroute range=hot_range_1 window=None

Round summaries:
- round 1: action=augment range=hot_range_1 window=X018349
  hot_range_ids=['hot_range_11', 'hot_range_2', 'hot_range_12', 'hot_range_1', 'hot_range_6']
  q_peak=1.0000000000000002 -> 1.0000000000000002
  q_integral=27953.102758 -> 27491.82354011509
  high_segment_count=203 -> 169
- round 2: action=reroute range=hot_range_1 window=None
  hot_range_ids=['hot_range_10', 'hot_range_1', 'hot_range_11', 'hot_range_5', 'hot_range_4']
  q_peak=1.0000000000000002 -> 1.0000000000000002
  q_integral=27491.82354011509 -> 26975.999669590088
  high_segment_count=169 -> 154

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

### normal72x_v2_regular_tasks_adjusted_1_20260411_4_9__scn_d3434360__res_f98d0986__cand_3

- scenario_path: `D:\Codex_Project\bs3 - 副本\results\normal72x_v2_regular_tasks_adjusted\normal72x_v2_regular_tasks_adjusted_阶段1结果_20260411_按4.9整理\normal72x_v2_regular_tasks_adjusted_scenario_weighted.json`
- stage1_result_path: `D:\Codex_Project\bs3 - 副本\results\normal72x_v2_regular_tasks_adjusted\normal72x_v2_regular_tasks_adjusted_阶段1结果_20260411_按4.9整理\normal72x_v2_regular_tasks_adjusted_stage1_result.json`
- candidate_index: `3`
- baseline_source: `stage1_greedy_repair`
- baseline_solver_mode: `two_phase_event_insert+stage1_greedy_repair`
- closed_loop_solver_mode: `two_phase_event_insert+stage1_greedy_repair+hotspot_relief_local_peak_milp`
- closed_loop_rounds_completed: `2`
- closed_loop_actions_accepted: `2`
- closed_loop_new_windows_added: `1`
- closed_loop_stop_reason: `total_time_budget_exhausted`
- q_peak_before / after: `1.0000000000000002` -> `1.0000000000000002`
- q_integral_before / after: `27956.851090625` -> `26979.74800221509`
- high_segment_count_before / after: `172` -> `123`
- cr_reg_before / after: `0.9733333333333334` -> `0.9733333333333334`
- elapsed_seconds: `180.26760320004541`
- single_action_per_round: `True`
- round_recompute_chain_is_consistent: `True`
- head_reordered_after_first_acceptance: `True`
- range_topk_truncation_possible: `True`
- structural_candidate_limit_hit: `True`

Accepted actions:
- round 1: augment range=hot_range_1 window=X018349
- round 2: reroute range=hot_range_1 window=None

Round summaries:
- round 1: action=augment range=hot_range_1 window=X018349
  hot_range_ids=['hot_range_2', 'hot_range_21', 'hot_range_1', 'hot_range_13', 'hot_range_3']
  q_peak=1.0000000000000002 -> 1.0000000000000002
  q_integral=27956.851090625 -> 27495.57187274009
  high_segment_count=172 -> 138
- round 2: action=reroute range=hot_range_1 window=None
  hot_range_ids=['hot_range_1', 'hot_range_20', 'hot_range_12', 'hot_range_2', 'hot_range_18']
  q_peak=1.0000000000000002 -> 1.0000000000000002
  q_integral=27495.57187274009 -> 26979.74800221509
  high_segment_count=138 -> 123

### normal72x_v2_regular_tasks_adjusted_1_20260411_4_9__scn_d3434360__res_f98d0986__cand_4

- scenario_path: `D:\Codex_Project\bs3 - 副本\results\normal72x_v2_regular_tasks_adjusted\normal72x_v2_regular_tasks_adjusted_阶段1结果_20260411_按4.9整理\normal72x_v2_regular_tasks_adjusted_scenario_weighted.json`
- stage1_result_path: `D:\Codex_Project\bs3 - 副本\results\normal72x_v2_regular_tasks_adjusted\normal72x_v2_regular_tasks_adjusted_阶段1结果_20260411_按4.9整理\normal72x_v2_regular_tasks_adjusted_stage1_result.json`
- candidate_index: `4`
- baseline_source: `stage1_greedy_repair`
- baseline_solver_mode: `two_phase_event_insert+stage1_greedy_repair`
- closed_loop_solver_mode: `two_phase_event_insert+stage1_greedy_repair+hotspot_relief_local_peak_milp`
- closed_loop_rounds_completed: `1`
- closed_loop_actions_accepted: `1`
- closed_loop_new_windows_added: `1`
- closed_loop_stop_reason: `total_time_budget_exhausted`
- q_peak_before / after: `1.0000000000000002` -> `1.0000000000000002`
- q_integral_before / after: `29131.119057875` -> `28788.688616721`
- high_segment_count_before / after: `259` -> `232`
- cr_reg_before / after: `0.9866666666666667` -> `0.9866666666666667`
- elapsed_seconds: `180.20956119999755`
- single_action_per_round: `True`
- round_recompute_chain_is_consistent: `True`
- head_reordered_after_first_acceptance: `None`
- range_topk_truncation_possible: `True`
- structural_candidate_limit_hit: `True`

Accepted actions:
- round 1: augment range=hot_range_3 window=X004658

Round summaries:
- round 1: action=augment range=hot_range_3 window=X004658
  hot_range_ids=['hot_range_3', 'hot_range_4', 'hot_range_5', 'hot_range_2', 'hot_range_19']
  q_peak=1.0000000000000002 -> 1.0000000000000002
  q_integral=29131.119057875 -> 28788.688616721
  high_segment_count=259 -> 232

### stage1_20260411_seed7__scn_8a79172b__res_e66d4d9a__cand_0

- scenario_path: `D:\Codex_Project\bs3 - 副本\results\normal72x_v2_regular_tasks_adjusted\stage1_20260411_seed7\normal72x_v2_regular_tasks_adjusted_scenario_weighted.json`
- stage1_result_path: `D:\Codex_Project\bs3 - 副本\results\normal72x_v2_regular_tasks_adjusted\stage1_20260411_seed7\normal72x_v2_regular_tasks_adjusted_stage1_result.json`
- candidate_index: `0`
- baseline_source: `stage1_greedy_repair`
- baseline_solver_mode: `two_phase_event_insert+stage1_greedy_repair`
- closed_loop_solver_mode: `two_phase_event_insert+stage1_greedy_repair+hotspot_relief_local_peak_milp`
- closed_loop_rounds_completed: `2`
- closed_loop_actions_accepted: `1`
- closed_loop_new_windows_added: `1`
- closed_loop_stop_reason: `no_acceptable_action`
- q_peak_before / after: `1.0000000000000002` -> `1.0000000000000002`
- q_integral_before / after: `32159.026793375` -> `31350.984064456876`
- high_segment_count_before / after: `290` -> `261`
- cr_reg_before / after: `1.0` -> `1.0`
- elapsed_seconds: `181.70622910000384`
- single_action_per_round: `True`
- round_recompute_chain_is_consistent: `True`
- head_reordered_after_first_acceptance: `True`
- range_topk_truncation_possible: `True`
- structural_candidate_limit_hit: `True`

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

### stage1_20260411_seed7__scn_8a79172b__res_e66d4d9a__cand_2

- scenario_path: `D:\Codex_Project\bs3 - 副本\results\normal72x_v2_regular_tasks_adjusted\stage1_20260411_seed7\normal72x_v2_regular_tasks_adjusted_scenario_weighted.json`
- stage1_result_path: `D:\Codex_Project\bs3 - 副本\results\normal72x_v2_regular_tasks_adjusted\stage1_20260411_seed7\normal72x_v2_regular_tasks_adjusted_stage1_result.json`
- candidate_index: `2`
- baseline_source: `stage1_greedy_repair`
- baseline_solver_mode: `two_phase_event_insert+stage1_greedy_repair`
- closed_loop_solver_mode: `two_phase_event_insert+stage1_greedy_repair+hotspot_relief_local_peak_milp`
- closed_loop_rounds_completed: `2`
- closed_loop_actions_accepted: `1`
- closed_loop_new_windows_added: `1`
- closed_loop_stop_reason: `no_acceptable_action`
- q_peak_before / after: `1.0000000000000002` -> `1.0000000000000002`
- q_integral_before / after: `32546.566843` -> `31738.524114081876`
- high_segment_count_before / after: `303` -> `274`
- cr_reg_before / after: `1.0` -> `1.0`
- elapsed_seconds: `182.25100479996763`
- single_action_per_round: `True`
- round_recompute_chain_is_consistent: `True`
- head_reordered_after_first_acceptance: `True`
- range_topk_truncation_possible: `True`
- structural_candidate_limit_hit: `True`

Accepted actions:
- round 1: augment range=hot_range_15 window=X002479

Round summaries:
- round 1: action=augment range=hot_range_15 window=X002479
  hot_range_ids=['hot_range_22', 'hot_range_11', 'hot_range_7', 'hot_range_24', 'hot_range_15']
  q_peak=1.0000000000000002 -> 1.0000000000000002
  q_integral=32546.566843 -> 31738.524114081876
  high_segment_count=303 -> 274
- round 2: action=None range=None window=None
  hot_range_ids=['hot_range_21', 'hot_range_11', 'hot_range_7', 'hot_range_23', 'hot_range_16']
  q_peak=1.0000000000000002 -> 1.0000000000000002
  q_integral=31738.524114081876 -> 31738.524114081876
  high_segment_count=274 -> 274

### stage1_20260411_seed7__scn_8a79172b__res_e66d4d9a__cand_3

- scenario_path: `D:\Codex_Project\bs3 - 副本\results\normal72x_v2_regular_tasks_adjusted\stage1_20260411_seed7\normal72x_v2_regular_tasks_adjusted_scenario_weighted.json`
- stage1_result_path: `D:\Codex_Project\bs3 - 副本\results\normal72x_v2_regular_tasks_adjusted\stage1_20260411_seed7\normal72x_v2_regular_tasks_adjusted_stage1_result.json`
- candidate_index: `3`
- baseline_source: `stage1_greedy_repair`
- baseline_solver_mode: `two_phase_event_insert+stage1_greedy_repair`
- closed_loop_solver_mode: `two_phase_event_insert+stage1_greedy_repair+hotspot_relief_local_peak_milp`
- closed_loop_rounds_completed: `2`
- closed_loop_actions_accepted: `1`
- closed_loop_new_windows_added: `1`
- closed_loop_stop_reason: `no_acceptable_action`
- q_peak_before / after: `1.0000000000000002` -> `1.0000000000000002`
- q_integral_before / after: `32147.060694375` -> `31339.017965456875`
- high_segment_count_before / after: `291` -> `262`
- cr_reg_before / after: `1.0` -> `1.0`
- elapsed_seconds: `181.88938429998234`
- single_action_per_round: `True`
- round_recompute_chain_is_consistent: `True`
- head_reordered_after_first_acceptance: `True`
- range_topk_truncation_possible: `True`
- structural_candidate_limit_hit: `True`

Accepted actions:
- round 1: augment range=hot_range_15 window=X002479

Round summaries:
- round 1: action=augment range=hot_range_15 window=X002479
  hot_range_ids=['hot_range_21', 'hot_range_11', 'hot_range_7', 'hot_range_23', 'hot_range_15']
  q_peak=1.0000000000000002 -> 1.0000000000000002
  q_integral=32147.060694375 -> 31339.017965456875
  high_segment_count=291 -> 262
- round 2: action=None range=None window=None
  hot_range_ids=['hot_range_20', 'hot_range_11', 'hot_range_7', 'hot_range_22', 'hot_range_18']
  q_peak=1.0000000000000002 -> 1.0000000000000002
  q_integral=31339.017965456875 -> 31339.017965456875
  high_segment_count=262 -> 262

### stage1_20260411_seed7__scn_8a79172b__res_e66d4d9a__cand_4

- scenario_path: `D:\Codex_Project\bs3 - 副本\results\normal72x_v2_regular_tasks_adjusted\stage1_20260411_seed7\normal72x_v2_regular_tasks_adjusted_scenario_weighted.json`
- stage1_result_path: `D:\Codex_Project\bs3 - 副本\results\normal72x_v2_regular_tasks_adjusted\stage1_20260411_seed7\normal72x_v2_regular_tasks_adjusted_stage1_result.json`
- candidate_index: `4`
- baseline_source: `stage1_greedy_repair`
- baseline_solver_mode: `two_phase_event_insert+stage1_greedy_repair`
- closed_loop_solver_mode: `two_phase_event_insert+stage1_greedy_repair+hotspot_relief_local_peak_milp`
- closed_loop_rounds_completed: `2`
- closed_loop_actions_accepted: `1`
- closed_loop_new_windows_added: `1`
- closed_loop_stop_reason: `no_acceptable_action`
- q_peak_before / after: `1.0000000000000002` -> `1.0000000000000002`
- q_integral_before / after: `32135.084662499998` -> `31327.041933581873`
- high_segment_count_before / after: `282` -> `253`
- cr_reg_before / after: `1.0` -> `1.0`
- elapsed_seconds: `181.7200372000225`
- single_action_per_round: `True`
- round_recompute_chain_is_consistent: `True`
- head_reordered_after_first_acceptance: `True`
- range_topk_truncation_possible: `True`
- structural_candidate_limit_hit: `True`

Accepted actions:
- round 1: augment range=hot_range_15 window=X002479

Round summaries:
- round 1: action=augment range=hot_range_15 window=X002479
  hot_range_ids=['hot_range_21', 'hot_range_11', 'hot_range_7', 'hot_range_23', 'hot_range_15']
  q_peak=1.0000000000000002 -> 1.0000000000000002
  q_integral=32135.084662499998 -> 31327.041933581873
  high_segment_count=282 -> 253
- round 2: action=None range=None window=None
  hot_range_ids=['hot_range_20', 'hot_range_11', 'hot_range_7', 'hot_range_22', 'hot_range_17']
  q_peak=1.0000000000000002 -> 1.0000000000000002
  q_integral=31327.041933581873 -> 31327.041933581873
  high_segment_count=253 -> 253

