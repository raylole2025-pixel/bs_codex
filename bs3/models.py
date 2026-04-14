from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any, Literal


Domain = Literal["A", "B"]
TaskType = Literal["reg", "emg"]


@dataclass(frozen=True)
class CapacityConfig:
    domain_a: float
    domain_b: float
    cross: float

# 候选跨域窗口
@dataclass(frozen=True)
class CandidateWindow:
    window_id: str
    a: str
    b: str
    start: float
    end: float
    value: float | None = None
    delay: float = 0.0
    distance_km: float | None = None

    @property
    # 将一个方法 伪装成属性 来访问
    def duration(self) -> float:
        return self.end - self.start

# 已调度跨域窗口
@dataclass(frozen=True)
class ScheduledWindow:
    window_id: str
    a: str
    b: str
    start: float
    end: float
    on: float
    off: float
    value: float | None = None
    delay: float = 0.0
    distance_km: float | None = None

    @property
    def active_interval(self) -> tuple[float, float]:
        return self.on, self.off

    @property
    def physical_interval(self) -> tuple[float, float]:
        return self.start, self.end

# 域内链路
@dataclass(frozen=True)
class TemporalLink:
    link_id: str
    u: str
    v: str
    domain: Domain
    start: float
    end: float
    delay: float = 0.0
    weight: float = 1.0
    distance_km: float | None = None

# 任务
@dataclass(frozen=True)
class Task:
    task_id: str
    src: str
    dst: str
    arrival: float
    deadline: float
    data: float
    weight: float
    max_rate: float
    task_type: TaskType
    preemption_priority: float = 1.0

# 时间分段
@dataclass(frozen=True)
class Segment:
    index: int
    start: float
    end: float

    @property
    def duration(self) -> float:
        return self.end - self.start

# 完整路径候选
@dataclass(frozen=True)
class PathCandidate:
    path_id: str
    nodes: tuple[str, ...]
    edge_ids: tuple[str, ...]
    hop_count: int
    delay: float
    cross_window_id: str | None = None
    hop_a: int = 0
    hop_b: int = 0

# 热点区间，在start到end时间内，节点nodes是热点节点
@dataclass(frozen=True)
class HotspotInterval:
    start: float
    end: float
    nodes: tuple[str, ...]

# 热点区域
@dataclass(frozen=True)
class HotspotRegion:
    region_id: str
    weight: float
    nodes: tuple[str, ...] = ()
    intervals: tuple[HotspotInterval, ...] = ()

    # 返回某时刻该区域内的活跃热点节点
    def active_nodes(self, time_point: float) -> tuple[str, ...]:
        if self.intervals:
            active: list[str] = []
            seen: set[str] = set()
            for interval in self.intervals:
                if interval.start <= time_point < interval.end:
                    for node in interval.nodes:
                        if node not in seen:
                            seen.add(node)
                            active.append(node)
            return tuple(active)
        return self.nodes


@dataclass(frozen=True)
class GAConfig:
    population_size: int = 60
    crossover_probability: float = 0.9
    mutation_probability: float = 0.2
    max_generations: int = 100
    stall_generations: int = 20
    top_m: int = 5
    max_runtime_seconds: float | None = None

    @property
    def elite_count(self) -> int:
        # 精英个体数，至少为1
        return max(1, math.ceil(0.1 * self.population_size))

    @property
    def immigrant_count(self) -> int:
        # 移民个体数，至少为1
        return max(1, math.ceil(0.1 * self.population_size))


@dataclass(frozen=True)
class Stage1Config:
    rho: float
    t_pre: float
    d_min: float
    theta_cap: float = 0.08
    theta_hot: float = 0.80
    hot_hop_limit: int = 4
    # 瓶颈因子：识别瓶颈链路的系数
    bottleneck_factor_alpha: float = 0.85  
    # 如果一个路径的可传输数据量达到最优解的eta_x，就认为它是一个"可用选项"
    eta_x: float = 0.90
    # 将整个规划时间轴 按 600 秒为一个快照窗口 划分成多个时间段，然后在每个时间段内评估候选窗口的价值
    static_value_snapshot_seconds: int = 600
    # candidate pool 主池规模
    candidate_pool_base_size: int = 400
    # 主池中热点价值通道的占比；剩余部分走常态任务价值通道
    candidate_pool_hot_fraction: float = 0.30
    # 每个有需求粗分段至少保留的代表窗口数量
    candidate_pool_min_per_coarse_segment: int = 3
    # 分段保底补充窗口的总上限
    candidate_pool_max_additional: int = 150
    q_eval: int = 4
    omega_fr: float = 4.0 / 9.0
    omega_cap: float = 3.0 / 9.0
    omega_hot: float = 2.0 / 9.0
    elite_prune_count: int = 6
    ga: GAConfig = field(default_factory=GAConfig)


@dataclass(frozen=True)
class Stage2Config:
    k_paths: int = 2
    completion_tolerance: float = 1e-6
    label_keep_limit: int | None = None

    @property
    def effective_label_keep_limit(self) -> int:
        if self.label_keep_limit not in {None, 0}:
            return max(int(self.label_keep_limit), 1)
        return max(8 * int(self.k_paths), 1)


@dataclass
class Scenario:
    node_domain: dict[str, Domain]
    intra_links: list[TemporalLink]
    candidate_windows: list[CandidateWindow]
    tasks: list[Task]
    capacities: CapacityConfig
    stage1: Stage1Config
    stage2: Stage2Config
    planning_end: float
    hotspots_a: list[HotspotRegion] = field(default_factory=list)   # A域热点
    metadata: dict[str, Any] = field(default_factory=dict)

    # 返回每个域内的节点
    # 返回:
    # {
    #     "A": ["A1", "A2"],  # A域的节点，按字母排序
    #     "B": ["B1", "B2"]    # B域的节点，按字母排序
    # }
    @property
    def domain_nodes(self) -> dict[Domain, list[str]]:
        grouped: dict[Domain, list[str]] = {"A": [], "B": []}
        for node, domain in self.node_domain.items():
            grouped[domain].append(node)
        grouped["A"].sort()
        grouped["B"].sort()
        return grouped


@dataclass
class Stage1Candidate:
    chromosome: tuple[str, ...]
    accepted_order: tuple[str, ...]
    plan: list[ScheduledWindow]
    feasible: bool
    violation: float
    mean_completion_ratio: float
    fr: float
    eta_cap: float
    eta_0: float
    avg_hot_coverage: float
    max_hot_gap: float
    activation_count: int
    unique_gateway_count: int
    window_count: int
    cross_active_fraction: float
    max_cross_gap: float
    fitness: tuple[float, ...]

    @property
    def hotspot_coverage(self) -> float:
        return self.avg_hot_coverage

    @property
    def hotspot_max_gap(self) -> float:
        return self.max_hot_gap

    @property
    def cross_capacity_gap(self) -> float:
        return self.eta_cap

    @property
    def link_shortfall(self) -> float:
        return self.eta_cap

    @property
    def zero_cross_demand_ratio(self) -> float:
        return self.eta_0

    @property
    def gateway_count(self) -> int:
        return self.unique_gateway_count


@dataclass
class Stage1Result:
    best_feasible: list[Stage1Candidate]
    population_best: Stage1Candidate | None
    generations: int
    selected_candidate_index: int | None = None
    selected_candidate_source: str | None = None
    selected_plan: list[ScheduledWindow] = field(default_factory=list)
    baseline_summary: dict[str, Any] = field(default_factory=dict)
    baseline_trace: "Stage1BaselineTrace | None" = None
    used_feedback: bool = True
    timed_out: bool = False
    elapsed_seconds: float | None = None
    history: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class Allocation:
    task_id: str
    segment_index: int
    path_id: str
    edge_ids: tuple[str, ...]
    rate: float
    delivered: float
    task_type: TaskType
    cross_window_id: str | None = None
    is_preempted: bool = False


@dataclass
class Stage1BaselineTrace:
    rho: float
    segments: list[dict[str, Any]] = field(default_factory=list)
    allocations: list[Allocation] = field(default_factory=list)
    task_states: list[dict[str, Any]] = field(default_factory=list)
    window_states: list[dict[str, Any]] = field(default_factory=list)
    remaining_before_by_segment: dict[str, dict[int, float]] = field(default_factory=dict)
    remaining_after_by_segment: dict[str, dict[int, float]] = field(default_factory=dict)
    remaining_end: dict[str, float] = field(default_factory=dict)
    completed: dict[str, bool] = field(default_factory=dict)
    cross_window_usage_by_segment: dict[int, dict[str, float]] = field(default_factory=dict)
    available_cross_capacity_by_segment: dict[int, dict[str, float]] = field(default_factory=dict)
    occupied_cross_windows_by_segment: dict[int, list[str]] = field(default_factory=dict)
    active_cross_windows_by_segment: dict[int, list[str]] = field(default_factory=dict)
    active_intra_edges_by_segment: dict[int, dict[str, list[str]]] = field(default_factory=dict)
    available_intra_capacity_by_segment: dict[int, dict[str, float]] = field(default_factory=dict)
    summary: dict[str, Any] = field(default_factory=dict)


@dataclass
class Stage2Result:
    plan: list[ScheduledWindow]
    cr_reg: float
    cr_emg: float
    n_preemptions: int
    u_cross: float
    u_all: float
    allocations: list[Allocation]
    solver_mode: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class PipelineResult:
    stage1: Stage1Result
    stage2_results: list[Stage2Result]
    recommended: Stage2Result | None



