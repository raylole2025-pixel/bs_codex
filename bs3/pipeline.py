from __future__ import annotations

from .models import PipelineResult, Scenario
from .stage1 import activation_count, run_stage1
from .stage2 import run_stage2


def _stage2_sort_key(result, t_pre: float) -> tuple[float, ...]:
    return (
        -result.cr_emg,
        float(result.n_preemptions),
        -result.cr_reg,
        float(activation_count(result.plan, t_pre)),
    )


def run_pipeline(scenario: Scenario, seed: int | None = None) -> PipelineResult:
    stage1_result = run_stage1(scenario, seed=seed)
    stage2_results = []
    if stage1_result.selected_plan:
        stage2_results = [run_stage2(scenario, stage1_result=stage1_result)]
    recommended = stage2_results[0] if stage2_results else None
    return PipelineResult(stage1=stage1_result, stage2_results=stage2_results, recommended=recommended)
