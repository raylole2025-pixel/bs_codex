from __future__ import annotations

from .models import ScheduledWindow, Scenario, Stage1BaselineTrace, Stage1Result, Stage2Result
from .stage2_emergency_scheduler import run_stage2_emergency_scheduler


def run_stage2(
    scenario: Scenario,
    plan: list[ScheduledWindow] | None = None,
    baseline_trace: Stage1BaselineTrace | None = None,
    stage1_result: Stage1Result | None = None,
) -> Stage2Result:
    resolved_plan = list(plan or [])
    resolved_trace = baseline_trace
    if stage1_result is not None:
        if not resolved_plan:
            resolved_plan = list(stage1_result.selected_plan)
        if resolved_trace is None:
            resolved_trace = stage1_result.baseline_trace
    if not resolved_plan:
        raise ValueError("run_stage2 requires a selected plan or stage1_result.selected_plan")
    return run_stage2_emergency_scheduler(scenario, resolved_plan, baseline_trace=resolved_trace)
