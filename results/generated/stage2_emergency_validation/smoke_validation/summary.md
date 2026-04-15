# Stage2 Emergency Validation Summary

- suite_name: `smoke`
- scenario: `D:\Codex_Project\bs3 - 副本\examples\sample_scenario.json`
- stage1_result: `D:\Codex_Project\bs3 - 副本\results\generated\sample_stage1_result.json`
- case_count: `2`
- candidate_indices: `0`
- rho_labels: `default`

## Key Findings

- Empty-emergency degeneration: `True`
- Hotspot assessment: `insufficient_data`
- Controlled preemption used in cases: `light_load__cand_0__default`
- Preemption still insufficient in cases: `none`
- Observed stage2 style: `aggressive_but_regular_damage_visible`

## By Case Type

| case_type | count | mean_cr_emg | mean_cr_reg_before | mean_cr_reg_after | mean_preemptions | mean_elapsed_s |
|---|---:|---:|---:|---:|---:|---:|
| empty | 1 | 1.0000 | 1.0000 | 1.0000 | 0.00 | 0.0002 |
| light | 1 | 1.0000 | 1.0000 | 0.8333 | 1.00 | 0.0093 |

## By Rho

| rho | count | mean_cr_emg | mean_cr_reg_after | mean_preemptions |
|---|---:|---:|---:|---:|
| default | 2 | 1.0000 | 0.9167 | 0.50 |

## By Candidate

| candidate | count | mean_cr_emg | mean_cr_reg_after | mean_preemptions |
|---|---:|---:|---:|---:|
| candidate_0 | 2 | 1.0000 | 0.9167 | 0.50 |

## Cases

| case_id | case_type | candidate | rho | emg_count | cr_emg | cr_reg_before | cr_reg_after | preemptions | degraded_reg_tasks |
|---|---|---:|---|---:|---:|---:|---:|---:|---:|
| empty_control__cand_0__default | empty | 0 | default | 0 | 1.0000 | 1.0000 | 1.0000 | 0 | 0 |
| light_load__cand_0__default | light | 0 | default | 3 | 1.0000 | 1.0000 | 0.8333 | 1 | 1 |
