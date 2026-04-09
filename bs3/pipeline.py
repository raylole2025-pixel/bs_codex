from __future__ import annotations

from .models import PipelineResult, Scenario
from .stage1 import activation_count, activation_time, run_stage1
from .stage2 import run_stage2


def _stage2_sort_key(result, t_pre: float) -> tuple[float, ...]:
    return (
        -result.cr_emg,
        float(result.n_preemptions),
        -result.cr_reg,
        float(activation_count(result.plan, t_pre)),
        activation_time(result.plan, t_pre),
    )


def run_pipeline(scenario: Scenario, seed: int | None = None) -> PipelineResult:
    stage1_result = run_stage1(scenario, seed=seed)
    candidate_plans = stage1_result.best_feasible[:]
    if not candidate_plans and stage1_result.population_best is not None:
        candidate_plans = [stage1_result.population_best]

    stage2_results = [run_stage2(scenario, candidate.plan) for candidate in candidate_plans]
    recommended = min(stage2_results, key=lambda item: _stage2_sort_key(item, scenario.stage1.t_pre)) if stage2_results else None
    return PipelineResult(stage1=stage1_result, stage2_results=stage2_results, recommended=recommended)
