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
    """全部阈值配置。

    所有字段单位均为"元素条数"。各字段的工程意义见注释。
    """

    # ===== 并行分流阈值 =====
    # 数据量 >= 该值才走多进程路径；小数据单进程更快
    linear_parallel_threshold: int = 100_000

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

    # ===== 分桶查找 =====
    # 桶数上限（避免 OS 文件描述符耗尽，Linux 默认 1024）
    bucket_max_count: int = 1024

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
