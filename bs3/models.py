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
    # 当任务传输量小于这个值，就认为任务完成
    completion_tolerance: float = 1e-6
    # 阶段2-1 常态基线模式；默认固定为阶段1 greedy repair，不再由 prefer_milp 自动主导
    regular_baseline_mode: str | None = "stage1_greedy_repair"
    # 是否启用 greedy baseline 之后的局部 repair；None 表示按 baseline mode 自动决定
    regular_repair_enabled: bool | None = None
    # legacy 兼容字段；不再决定阶段2-1默认 baseline
    prefer_milp: bool = False
    # legacy 兼容字段；rolling/full MILP 仅在 regular_baseline_mode 显式指定时生效
    milp_mode: str = "full"
    # rolling 模式下的展望窗口长度（按事件分段数计）
    milp_horizon_segments: int = 16
    # rolling 模式下每轮正式提交的分段数
    milp_commit_segments: int = 8
    # rolling 模式下每个 task-segment 保留的候选路径数量
    milp_rolling_path_limit: int = 1
    # rolling 模式下高优先级 task-segment 放宽后的候选路径数量
    milp_rolling_high_path_limit: int = 2
    # rolling 模式下高权重任务阈值；None 表示按常态任务权重上四分位自动估计
    milp_rolling_high_weight_threshold: float | None = None
    # rolling 模式下判定“高竞争”的活跃任务数阈值（按分段）
    milp_rolling_high_competition_task_threshold: int = 8
    # rolling 模式下每个分段最多放宽到高路径数的任务数
    milp_rolling_promoted_tasks_per_segment: int = 2
    # CBC 求解时间上限（秒）；None 表示不设限
    milp_time_limit_seconds: float | None = None
    # CBC 相对 gap；None 表示使用求解器默认值
    milp_relative_gap: float | None = None
    # greedy baseline 后最多尝试的 repair block 数
    repair_block_max_count: int = 3
    # repair block 左右各扩的 segment 数
    repair_expand_segments: int = 1
    # 单个 repair block 的最大长度
    repair_max_block_segments: int = 8
    # 允许进入 repair 的最小活跃常态任务数
    repair_min_active_tasks: int = 2
    # repair block 选择阈值（按 q_peak + imbalance）
    repair_util_threshold: float = 0.75
    # repair MILP 每个 task-segment 保留的候选路径上限
    repair_candidate_path_limit: int = 2
    # repair MILP 求解时间上限（秒）；None 表示不设限
    repair_time_limit_seconds: float | None = None
    # repair 接受阈值
    repair_accept_epsilon: float = 1e-6
    # 兼容旧开关；默认与 closed_loop_relief_enabled 保持一致
    hotspot_relief_enabled: bool = True
    # 阶段2-1 闭环式减载主控开关
    closed_loop_relief_enabled: bool = True
    # 热点分段识别阈值（q_r）
    hotspot_util_threshold: float = 0.95
    # 最多考虑的热点区间数量
    hotspot_topk_ranges: int = 5
    # 热点区间局部 MILP 左右各扩的 segment 数
    hotspot_expand_segments: int = 2
    # 判定“单链路主导”的阈值
    hotspot_single_link_fraction_threshold: float = 0.6
    # 每个热点区间最多保留的热点贡献任务数
    hotspot_top_tasks_per_range: int = 12
    # 全局最多允许新增的补窗数量；闭环模式下仅作为硬上限保护
    augment_window_budget: int = 2
    # 每个热点区间最多保留的补窗候选数量
    augment_top_windows_per_range: int = 3
    # 旧版一次性选窗策略；闭环模式下仅作为兼容字段保留
    augment_selection_policy: str = "global_score_only"
    # 闭环模式最大轮数
    closed_loop_max_rounds: int = 6
    # 闭环模式最多允许正式接受的新窗口数量
    closed_loop_max_new_windows: int = 2
    # q_peak 的最小改善阈值
    closed_loop_min_delta_q_peak: float = 1e-4
    # q_integral 的最小改善阈值
    closed_loop_min_delta_q_integral: float = 1e-6
    # 高负载/峰值平台分段数的最小改善阈值
    closed_loop_min_delta_high_segments: int = 1
    # 每轮最多关注的热点区间数量
    closed_loop_topk_ranges_per_round: int = 5
    # 每个热点区间每轮最多保留的候选动作数量
    closed_loop_topk_candidates_per_range: int = 3
    # 闭环动作选择模式：优先重路由，或按全局边际收益统一比较
    closed_loop_action_mode: str = "best_global_action"
    # 热点 task-segment 的候选路径上限
    hot_path_limit: int = 4
    # 每个热点分段最多提升为热点扩张候选的任务数
    hot_promoted_tasks_per_segment: int = 8
    # 单次局部峰值 MILP 的最大 horizon 长度；None 表示不截断
    local_peak_horizon_cap_segments: int | None = 48
    # 局部峰值 MILP 接受阈值
    local_peak_accept_epsilon: float = 1e-6
    # 启用热点补窗时，若未实际使用 MILP，则直接报错
    fail_if_milp_disabled: bool = True
    # 控制每个时间段的 路径候选集大小（因为阶段2要选出每个时间段内的最优路径，所以要控制候选集大小，只保留支配解）
    label_keep_limit: int | None = None

    @property
    # 用于计算 有效的非支配解保留数量
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
    is_preempted: bool = False


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



