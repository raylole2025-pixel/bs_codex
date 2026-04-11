# R4 Stability

| name | stage1_label | success_counts.reg | cr_reg | u_cross | u_all | n_preemptions | elapsed_seconds |
| --- | --- | --- | --- | --- | --- | --- | --- |
| R4_seed7_best0 | seed7#0 | 61 | 0.886667 | 0.581832 | 0.013080 | 0 | 22.384955 |
| R4_seed7_alt1 | seed7#1 | 61 | 0.886667 | 0.583061 | 0.013120 | 0 | 21.935317 |
| R4_seed13_best0 | seed13#0 | 64 | 0.920000 | 0.589933 | 0.013184 | 0 | 20.773466 |
| R4_seed13_alt1 | seed13#1 | 62 | 0.900000 | 0.580531 | 0.013048 | 0 | 23.191772 |

# Preemption Status

- status: `unavailable_in_practice`
- regular rolling probe exhibited a route switch but kept n_preemptions=0 and all Allocation.is_preempted=False
- stage2 scheduler increments n_preemptions only in the emergency insertion branch
- controlled emergency probe produced n_preemptions=1, so a narrow preemption path exists outside rolling regular MILP