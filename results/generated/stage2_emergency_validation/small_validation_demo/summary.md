# Stage2 Emergency Validation Summary

- suite_name: `small-validation`
- scenario: `D:\Codex_Project\bs3 - 副本\examples\sample_scenario.json`
- stage1_result: `D:\Codex_Project\bs3 - 副本\results\generated\sample_stage1_result.json`
- case_count: `5`
- candidate_indices: `0`
- rho_labels: `default`

## Key Findings

- Empty-emergency degeneration: `True`
- Hotspot assessment: `hotspot_biased_emergencies_do_not_show_higher_success`
- Controlled preemption used in cases: `light_load__cand_0__default, medium_load__cand_0__default, heavy_load__cand_0__default, hotspot_bias__cand_0__default`
- Preemption still insufficient in cases: `light_load__cand_0__default, medium_load__cand_0__default, heavy_load__cand_0__default, hotspot_bias__cand_0__default`
- Observed stage2 style: `balanced_tradeoff`

## By Case Type

| case_type | count | mean_cr_emg | mean_cr_reg_before | mean_cr_reg_after | mean_preemptions | mean_elapsed_s |
|---|---:|---:|---:|---:|---:|---:|
| empty | 1 | 1.0000 | 1.0000 | 1.0000 | 0.00 | 0.0002 |
| heavy | 1 | 0.0543 | 1.0000 | 0.8333 | 1.00 | 0.0198 |
| hotspot | 1 | 0.2386 | 1.0000 | 0.8333 | 1.00 | 0.0170 |
| light | 1 | 0.7540 | 1.0000 | 0.8333 | 1.00 | 0.0108 |
| medium | 1 | 0.1914 | 1.0000 | 0.8333 | 1.00 | 0.0140 |

## By Rho

| rho | count | mean_cr_emg | mean_cr_reg_after | mean_preemptions |
|---|---:|---:|---:|---:|
| default | 5 | 0.4477 | 0.8667 | 0.80 |

## By Candidate

| candidate | count | mean_cr_emg | mean_cr_reg_after | mean_preemptions |
|---|---:|---:|---:|---:|
| candidate_0 | 5 | 0.4477 | 0.8667 | 0.80 |

## Cases

| case_id | case_type | candidate | rho | emg_count | cr_emg | cr_reg_before | cr_reg_after | preemptions | degraded_reg_tasks |
|---|---|---:|---|---:|---:|---:|---:|---:|---:|
| empty_control__cand_0__default | empty | 0 | default | 0 | 1.0000 | 1.0000 | 1.0000 | 0 | 0 |
| light_load__cand_0__default | light | 0 | default | 4 | 0.7540 | 1.0000 | 0.8333 | 1 | 1 |
| medium_load__cand_0__default | medium | 0 | default | 10 | 0.1914 | 1.0000 | 0.8333 | 1 | 1 |
| heavy_load__cand_0__default | heavy | 0 | default | 16 | 0.0543 | 1.0000 | 0.8333 | 1 | 1 |
| hotspot_bias__cand_0__default | hotspot | 0 | default | 8 | 0.2386 | 1.0000 | 0.8333 | 1 | 1 |
