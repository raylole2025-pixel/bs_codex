# hotspot_bias__cand_0__default

- case_type: `hotspot`
- candidate_index: `0`
- rho: `default` -> `0.3`
- baseline_source: `stage1_result`
- emergency_count: `8`
- solver_mode: `stage2_emergency_insert`
- cr_reg_before / after: `1.0` -> `0.8333333333333334`
- cr_emg: `0.2386488224422744`
- n_preemptions: `1`
- regular_completion_rate_dropped: `True`
- strict_baseline_degenerate: `False`

## Emergency Diagnostics

- direct_success_task_ids: `hotspot_bias_emg_01`
- controlled_preemption_task_ids: `hotspot_bias_emg_02`
- failed_task_ids: `hotspot_bias_emg_03, hotspot_bias_emg_04, hotspot_bias_emg_05, hotspot_bias_emg_06, hotspot_bias_emg_07, hotspot_bias_emg_08`
- degraded_regular_tasks: `R3`
