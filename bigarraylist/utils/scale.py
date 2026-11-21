"""量级判定 + 分块大小（简化版，去掉 ScaleValue 多层包装）。

向后兼容：保留 ScaleHelper / ScaleValue / chunk_wise_processing /
choose_chunk_size / choose_external_sort_chunk_size 全部公开符号，
调用方零改动。内部只是把冗余的 ScaleValue 包装换成 int 常量。
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from typing import Final, TypeVar

T = TypeVar("T")

# ==================== 量级档位常量（int，去掉 ScaleValue 包装） ====================
HUNDRED_THOUSAND: Final[int] = 100_000
MILLION: Final[int] = 1_000_000
TEN_MILLION: Final[int] = 10_000_000
HUNDRED_MILLION: Final[int] = 100_000_000

# 数据量 → 分块大小 映射表（每个 chunk 100~1000 条）
# 数据量越大，分块越大：减少块数、摊薄调度开销；但不超过 1000，保证单块内存可控
_CHUNK_SIZE_MAP: dict[int, int] = {
    HUNDRED_THOUSAND: 100,
    MILLION: 300,
    TEN_MILLION: 800,
    HUNDRED_MILLION: 1_000,
}


class ScaleValue:
    """纯数据载体：保存一个数据量级的数值。

    向后兼容保留——重构前公开 API 之一，外部代码可能直接 import 使用。
    重构后已不再被内部使用，仅作为对外符号保留。
    """

    def __init__(self, value: int) -> None:
        self._value: Final[int] = value

    @property
    def value(self) -> int:
        return self._value

    def __repr__(self) -> str:
        return f"ScaleValue({self._value})"

    def __eq__(self, other: object) -> bool:
        return isinstance(other, ScaleValue) and self._value == other._value

    def __hash__(self) -> int:
        return hash(self._value)


class ScaleHelper:
    """数据量级助手。

    向后兼容保留——重构前公开 API 之一。内部不再使用本类，
    所有阈值改走 BigArrayConfig + 直接 int 常量。本类仅薄包装。
    """

    HUNDRED_THOUSAND = ScaleValue(HUNDRED_THOUSAND)
    MILLION = ScaleValue(MILLION)
    TEN_MILLION = ScaleValue(TEN_MILLION)
    HUNDRED_MILLION = ScaleValue(HUNDRED_MILLION)

    def __init__(self, value: int) -> None:
        self.value = value

    @staticmethod
    def get_scale(total_count: int) -> ScaleValue:
        """根据总条数返回对应量级对象。"""
        if total_count >= HUNDRED_MILLION:
            return ScaleHelper.HUNDRED_MILLION
        if total_count >= TEN_MILLION:
            return ScaleHelper.TEN_MILLION
        if total_count >= MILLION:
            return ScaleHelper.MILLION
        if total_count >= HUNDRED_THOUSAND:
            return ScaleHelper.HUNDRED_THOUSAND
        raise ValueError(f"数据量 {total_count}，小于十万级阈值 {HUNDRED_THOUSAND}")

    def __str__(self) -> str:
        v = self.value
        if v >= HUNDRED_MILLION:
            tag = "HUNDRED_MILLION"
        elif v >= TEN_MILLION:
            tag = "TEN_MILLION"
        elif v >= MILLION:
            tag = "MILLION"
        elif v >= HUNDRED_THOUSAND:
            tag = "HUNDRED_THOUSAND"
        else:
            raise ValueError(f"数据量 {v}，小于十万级阈值 {HUNDRED_THOUSAND}")
        return f"{self.__class__.__name__}({tag})"


def chunk_wise_processing(iterable: Iterable[T]) -> Iterator[list[T]]:
    """按 choose_chunk_size 选择的大小分块产出。

    返回：迭代器，每一项是一个 list[T]（单块，长度 <= chunk_size）。
    要求 iterable 有 __len__；没有的请先过 list(...)。
    """
    total = len(iterable)  # type: ignore[arg-type]
    chunk_size = choose_chunk_size(total)

    it = iter(iterable)
    while True:
        chunk: list[T] = []
        for _ in range(chunk_size):
            try:
                chunk.append(next(it))
            except StopIteration:
                break
        if not chunk:
            return
        yield chunk


def choose_chunk_size(total_count: int) -> int:
    """根据总条数选择分块大小（100~1000）。

    ⚠️ 历史遗留：该粒度对多进程路径过细（1000 次 IPC 是负优化），
    新代码请用 choose_parallel_chunk_size。保留供向后兼容（测试/外部调用）。
    """
    scale = ScaleHelper.get_scale(total_count)
    return _CHUNK_SIZE_MAP[scale.value]


def choose_parallel_chunk_size(total_count: int, workers: int, min_size: int = 65_536) -> int:
    """多进程路径的任务粒度（P0-2 Step 2 / P0-3）。

    块大小 = max(min_size, n // (workers * 4))：
        - 30 万数据 8 核 → 8~32 块，每块 1~4 万元素
        - IPC 次数从旧 choose_chunk_size 的数千次降到几十次
    workers 至少按 4 兜底，避免除零/过细。
    """
    w = max(1, workers)
    return max(min_size, total_count // (w * 4))


def choose_external_sort_chunk_size(
    total_count: int,
    *,
    target: int = 32,
    min_chunk: int = 100_000,
    max_chunk: int = 5_000_000,
) -> int:
    """外部排序分块大小。

    默认参数对齐重构前行为（目标 32 块，单块 100k~5M）。
    新增 target/min_chunk/max_chunk 形参，支持通过 BigArrayConfig 覆盖。
    """
    chunk_size = total_count // target
    return max(min_chunk, min(max_chunk, chunk_size))
