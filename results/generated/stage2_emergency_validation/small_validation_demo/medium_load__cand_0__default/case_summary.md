# medium_load__cand_0__default

- case_type: `medium`
- candidate_index: `0`
- rho: `default` -> `0.3`
- baseline_source: `stage1_result`
- emergency_count: `10`
- solver_mode: `stage2_emergency_insert`
- cr_reg_before / after: `1.0` -> `0.8333333333333334`
- cr_emg: `0.19137814837991923`
- n_preemptions: `1`
- regular_completion_rate_dropped: `True`
- strict_baseline_degenerate: `False`

## Emergency Diagnostics

- direct_success_task_ids: `medium_load_emg_01`
- controlled_preemption_task_ids: `medium_load_emg_02`
- failed_task_ids: `medium_load_emg_03, medium_load_emg_04, medium_load_emg_05, medium_load_emg_06, medium_load_emg_07, medium_load_emg_08, medium_load_emg_09, medium_load_emg_10`
- degraded_regular_tasks: `R3`
