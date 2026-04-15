# light_load__cand_0__default

- case_type: `light`
- candidate_index: `0`
- rho: `default` -> `0.3`
- baseline_source: `stage1_result`
- emergency_count: `3`
- solver_mode: `stage2_emergency_insert`
- cr_reg_before / after: `1.0` -> `0.8333333333333334`
- cr_emg: `1.0`
- n_preemptions: `1`
- regular_completion_rate_dropped: `True`
- strict_baseline_degenerate: `False`

## Emergency Diagnostics

- direct_success_task_ids: `light_load_emg_01, light_load_emg_02`
- controlled_preemption_task_ids: `light_load_emg_03`
- failed_task_ids: `none`
- degraded_regular_tasks: `R3`
