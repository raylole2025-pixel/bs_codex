from __future__ import annotations

from .models import ScheduledWindow, Scenario, Stage2Result
from .stage2_two_phase_scheduler import run_stage2_two_phase_event_insert


def run_stage2(scenario: Scenario, plan: list[ScheduledWindow]) -> Stage2Result:
    return run_stage2_two_phase_event_insert(scenario, plan)
