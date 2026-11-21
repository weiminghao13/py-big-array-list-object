"""配置对象：所有阈值集中可配。

向后兼容：所有字段都有默认值，等于重构前的硬编码常量。
不传 config 时，行为与重构前完全一致。

用法：
    cfg = BigArrayConfig(linear_parallel_threshold=50_000)
    arr = BigArrayList(data, config=cfg)

也可类级别覆盖默认值，影响所有后续实例：
    BigArrayConfig.set_default(external_sort_chunk_target=64)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class BigArrayConfig:
    """全部阈值配置；数量、字节数和开关的单位见各字段注释。"""

    # ===== 并行分流阈值 =====
    # 语义（P0-2）：内存 sorted() 的上限。数据量 < 该值直接 sorted()；
    # >= 该值才考虑并行/外部排序。30 万 float 实测：sorted() 0.046s vs
    # 分块并行 1.139s（IPC + spawn 开销远超并行收益），故默认提到 1M。
    linear_parallel_threshold: int = 1_000_000

    # ===== 内存并行排序分块（P0-2 Step 2）=====
    # 中数据并行排序分支的任务粒度：块大小 = max(该值, n // (workers * 4))。
    # 旧的 choose_chunk_size（100~1000 条/块）会把 30 万数据切成 3000 块，
    # IPC 次数爆炸（负优化）。该分支现在仅当用户显式 opt-in 时才走
    # （见 memory_parallel_sort_enabled）。
    parallel_chunk_min_size: int = 65_536

    # 内存并行排序分支开关：False（默认）时 [linear_parallel_threshold,
    # external_sort_threshold) 区间直接 sorted()；True 时走分块并行
    # （适合"CPU 核数多 + 元素比较贵"的场景）。
    memory_parallel_sort_enabled: bool = False

    # ===== 内存排序分块 =====
    # 内存多进程排序的单块大小映射表（数据量 -> 块大小）
    # 100k→100/块、1M→300/块、10M→800/块、100M→1000/块
    chunk_size_map: dict[int, int] | None = None  # None → 用默认 _DEFAULT_CHUNK_MAP

    # ===== 外部排序 =====
    # 目标块数：32 路归并，heapq.merge 效率最高
    external_sort_chunk_target: int = 32
    # 每块至少 10 万条（再小进程调度不划算）
    external_sort_min_chunk: int = 100_000
    # 每块最多 500 万条（sorted() 单块内存安全）
    external_sort_max_chunk: int = 5_000_000
    # 数据量 >= 该值才走外部排序（否则走内存多进程分块）
    external_sort_threshold: int = 1_000_000
    # 真正的低内存归并：逐条 load 而非整块 load。
    # True  → 内存峰值 O(块数)，速度较慢但能处理远超内存的数据
    # False → 整块 load，归并内存峰值 ≈ 数据总量，速度快 3~5 倍
    #  True（符合"外部排序"语义）；要恢复旧行为显式设 False
    external_sort_streaming_merge: bool = True

    # ===== 批量 pickle（P0-1）=====
    # 内部临时文件批量帧格式的每批元素数。批量 dump/load 比逐条快 13~37 倍。
    # streaming 归并内存 = O(块数 × 该值)，默认 32 × 4096 × 100B ≈ 13MB，仍远小于数据总量。
    pickle_batch_size: int = 4096

    # ===== 外部排序流水线深度（P0-3）=====
    # None → max(2, 2 * (max_workers or cpu_count))，让每个 worker 都有飞行任务；
    # 显式正整数则强制覆盖。内存峰值 = O(深度 × chunk_size)，chunk_size 已有 5M 上限封顶。
    external_sort_pipeline_depth: int | None = None

    # ===== 分桶去重（P1-1）=====
    # True 时大数据 distinct 走哈希分桶去重 O(n)（不保首次出现顺序）；
    # False 时走旧的排序去重 O(n log n)（保首次出现顺序）。
    distinct_hash_bucketing_enabled: bool = True

    # ===== 分桶查找 =====
    # 桶数上限（避免 OS 文件描述符耗尽，Linux 默认 1024）
    bucket_max_count: int = 1024
    # 每个桶文件的读写缓冲（字节）；独立于少量外部排序文件使用的 8MB 缓冲。
    bucket_file_buffer_size: int = 64 * 1024

    # ===== Cython 快路径多线程（第三步）=====
    # int 快路径 sum/min/max：数据量 >= 该阈值时切换 prange 多线程版本
    # （小数据并行开销大于收益，单线程 C 循环已足够快）
    fast_parallel_threshold: int = 1_000_000
    # 并行线程数：0 -> 自动（omp_get_max_threads，通常 = 逻辑核数）
    fast_parallel_threads: int = 0

    # ===== 默认配置管理 =====
    _default: BigArrayConfig | None = None

    @classmethod
    def default(cls) -> BigArrayConfig:
        """当前生效的默认配置（实例级覆盖优先于类级默认）。"""
        if cls._default is not None:
            return cls._default
        return cls()

    @classmethod
    def set_default(cls, **kwargs: Any) -> BigArrayConfig:
        """类级别覆盖默认值；影响后续所有未显式传 config 的实例。

        kwargs 不全传时，缺失字段保留原默认值。
        """
        cls._default = cls(**kwargs)
        return cls._default

    @classmethod
    def reset_default(cls) -> None:
        """恢复类级默认值为内置默认。"""
        cls._default = None


# 内存排序分块大小默认映射（不变于重构前）
_DEFAULT_CHUNK_MAP: dict[int, int] = {
    100_000: 100,  # 十万级 → 100 条/块
    1_000_000: 300,  # 百万级 → 300 条/块
    10_000_000: 800,  # 千万级 → 800 条/块
    100_000_000: 1_000,  # 亿级   → 1000 条/块
}
