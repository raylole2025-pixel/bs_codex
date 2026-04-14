### r3_w1

- baseline_source: `stage1_greedy_repair`
- solver_mode: `two_phase_event_insert+stage1_greedy_repair+hotspot_relief_local_peak_milp`
- closed_loop_rounds_completed: `2`
- closed_loop_actions_accepted: `1`
- closed_loop_new_windows_added: `1`
- closed_loop_stop_reason: `no_acceptable_action`
- closed_loop_new_window_hard_cap: `1`
- q_peak_before / after: `1.0000000000000002` -> `1.0000000000000002`
- q_integral_before / after: `32159.026793375` -> `31350.984064456876`
- high_segment_count_before / after: `290` -> `261`
- elapsed_seconds: `180.6002200000221`

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
