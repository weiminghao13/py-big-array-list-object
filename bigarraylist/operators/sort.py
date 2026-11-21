"""排序算子 mixin：sort / is_sorted / 外部排序接入。

核心改进（对比重构前）：
    - 不再内联 _external_sort 实现，改为调用 algorithms.external_sort.external_sort
    - 通过 self._get_config() 读 BigArrayConfig 的阈值，所有阈值可配
    - 默认走 streaming 模式（内存峰值 O(块数) 而非 O(数据总量)）
"""

from __future__ import annotations

import bisect as _bisect_mod
import heapq
from collections.abc import Callable, Generator, Iterable, Iterator
from typing import Any, Generic, TypeVar

from .._fast import fast_numeric
from ..algorithms._pool import default_workers, get_pool
from ..algorithms.external_sort import external_sort
from ..algorithms.parallel import chunk_is_sorted, sort_chunk
from ..config import BigArrayConfig
from ..utils.scale import choose_parallel_chunk_size

T = TypeVar("T")

# is_sorted 小数据分支的"尚无前一个元素"哨兵：不能用 None，
# 否则源中真含 None 时会跳过与前一个元素的比较（静默漏判）。
_NO_PREV = object()


class _ReverseWrapper:
    """包装元素，比较时取反，用于 heapq.merge 降序归并。

    heapq.merge 只支持升序；把每块降序排列后用本类包装，
    __lt__ 取反 → 降序序列变成"包装升序" → merge 正常工作 →
    解包后得到全局降序结果。

    key 不为 None 时，比较的是 key(item) 而非 item 本身。
    """

    __slots__ = ("key", "value")

    def __init__(self, key: Any, value: Any) -> None:
        self.key = key
        self.value = value

    def __lt__(self, other: _ReverseWrapper) -> bool:
        return self.key > other.key  # type: ignore[no-any-return]

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, _ReverseWrapper):
            return NotImplemented
        return self.key == other.key  # type: ignore[no-any-return]


def _wrap_reverse(
    it: Iterable[T], key: Callable[[T], Any] | None
) -> Generator[_ReverseWrapper, None, None]:
    """把迭代器元素包装成 _ReverseWrapper（用于降序归并）。"""
    for item in it:
        k = key(item) if key is not None else item
        yield _ReverseWrapper(k, item)


def _unwrap_reverse(it: Iterable[_ReverseWrapper]) -> Generator[T, None, None]:
    """解包 _ReverseWrapper，产出原始元素。"""
    for wrapped in it:
        yield wrapped.value


class SortMixin(Generic[T]):
    """排序算子 mixin。不声明 __slots__（多继承冲突）。

    继承 Generic[T] 仅用于类型推导（让 T 在 mixin 内被绑定）；
    运行时通过 cooperative multiple inheritance 与 _CoreBase 组合，
    _fresh_iter / __len__ / _get_config 实际由 _CoreBase 提供。
    """

    # 协作契约：实际实现由 _CoreBase 通过 MRO 提供
    def __len__(self) -> int:  # pragma: no cover - 协作契约
        raise NotImplementedError

    def _fresh_iter(self) -> Iterator[T]:  # pragma: no cover - 协作契约
        raise NotImplementedError

    def _get_config(self) -> BigArrayConfig:  # pragma: no cover - 协作契约
        raise NotImplementedError

    def is_sorted(self, max_workers: int | None = None) -> bool:
        """判断数据是否已按升序排列。

        数值内存源用 C 单遍检查并提前退出；其他大数据仍按块并行，
        同时验证块内有序及相邻块边界。
        """
        n = len(self)
        if n <= 1:
            return True
        fast = fast_numeric(self)
        if fast is not None:
            return fast.py_is_sorted()  # type: ignore[no-any-return]

        cfg: BigArrayConfig = self._get_config()
        data = self._fresh_iter()

        # 小数据：直接流式 O(n) 判断，进程启动开销不划算
        if n < cfg.linear_parallel_threshold:
            prev: Any = _NO_PREV
            for item in data:
                if prev is not _NO_PREV and prev > item:
                    return False
                prev = item
            return True

        workers = max_workers or default_workers()
        chunk_size = choose_parallel_chunk_size(n, workers, cfg.parallel_chunk_min_size)
        chunk_bounds: list[tuple[T, T]] = []
        pool = get_pool(max_workers)
        futures = []
        chunk: list[T] = []
        try:
            for item in data:
                chunk.append(item)
                if len(chunk) >= chunk_size:
                    futures.append(pool.submit(chunk_is_sorted, (chunk,)))
                    chunk_bounds.append((chunk[0], chunk[-1]))
                    chunk = []
            if chunk:
                futures.append(pool.submit(chunk_is_sorted, (chunk,)))
                chunk_bounds.append((chunk[0], chunk[-1]))

            # 1) 所有 chunk 内部必须有序
            for fut in futures:
                if not fut.result():
                    return False

            # 2) chunk 之间必须有序：第 i 块尾 <= 第 i+1 块头
            for i in range(len(chunk_bounds) - 1):
                if chunk_bounds[i][1] > chunk_bounds[i + 1][0]:  # type: ignore[operator]
                    return False

            return True
        except BaseException:
            # 早停/异常：取消未启动的 future（共享池保持运行）
            for fut in futures:
                if not fut.done():
                    fut.cancel()
            raise

    def _big_iter_sort(
        self,
        max_workers: int | None = None,
        key: Callable[[T], Any] | None = None,
        reverse: bool = False,
    ) -> Generator[T, None, None]:
        """大数据排序：按数据量分流。

        - 小数据（< config.linear_parallel_threshold）：直接 sorted() 物化，单进程
        - 中数据（threshold ~ external_sort_threshold）：
            * memory_parallel_sort_enabled=False（默认）：直接 sorted()——
              实测 30 万 float 走分块并行 1.139s vs sorted() 0.046s（负优化），
              1M 以内纯 C Timsort ≈ 0.15s，不值得开进程（P0-2 Step 1）
            * memory_parallel_sort_enabled=True：分块并行排序 + 内存多路归并
              （适合"CPU 核数多 + 元素比较贵"的场景），任务粒度
              max(parallel_chunk_min_size, n // (workers*4))（P0-2 Step 2）
        - 大数据（>= config.external_sort_threshold）：外部排序
          * 默认 streaming 模式：阶段 2 批量帧 load（每批 pickle_batch_size 条），
            内存峰值 = O(块数 × batch_size)，仍远小于数据总量（P0-1）
          * block 模式（external_sort_streaming_merge=False）：归并阶段所有块
            整块 load 到内存，峰值 ≈ 数据总量，但速度快 3~5 倍

        参数：
            max_workers: 进程池并行度；None 用默认
            key:         排序键函数（类似 ``sorted(data, key=...)``）；None 按元素本身排序
                         ⚠️ 多进程路径要求 key 可 pickle（模块级函数或 functools.partial）
            reverse:     True 降序，False 升序（默认）。全链路支持。

        全程操作 _fresh_iter()（外部排序内部也用），不碰 self.__data——
        可以重复调用，不会污染实例状态。
        """
        n = len(self)
        cfg: BigArrayConfig = self._get_config()

        # 小数据，或（默认关掉中数据并行分支时的）中数据：直接 sorted()。
        # 纯 C Timsort 对 1M 以内数据远快于任何多进程方案（P0-2）。
        if n < cfg.external_sort_threshold and not (
            cfg.memory_parallel_sort_enabled and n >= cfg.linear_parallel_threshold
        ):
            yield from sorted(self._fresh_iter(), key=key, reverse=reverse)  # type: ignore[type-var, arg-type]
            return

        # 大数据走外部排序（统一走 algorithms.external_sort）
        if n >= cfg.external_sort_threshold:
            yield from external_sort(
                self._fresh_iter(),
                n,
                config=cfg,
                max_workers=max_workers,
                key=key,
                reverse=reverse,
            )
            return

        # 中数据（memory_parallel_sort_enabled=True 才到这里）：
        # 分块并行排序 + 内存多路归并；共享池 + 大块粒度（P0-2 Step 2）
        data = self._fresh_iter()
        workers = max_workers or default_workers()
        chunk_size = choose_parallel_chunk_size(n, workers, cfg.parallel_chunk_min_size)

        pool = get_pool(max_workers)
        futures: list[Any] = []
        chunk: list[T] = []
        try:
            for item in data:
                chunk.append(item)
                if len(chunk) >= chunk_size:
                    futures.append(pool.submit(sort_chunk, (chunk, key, reverse)))
                    chunk = []
            if chunk:
                futures.append(pool.submit(sort_chunk, (chunk, key, reverse)))
            sorted_chunks = [fut.result() for fut in futures]
        except BaseException:
            for fut in futures:
                if not fut.done():
                    fut.cancel()
            raise

        if reverse:
            # 每块已降序排列；用 _ReverseWrapper 包装后 heapq.merge 升序归并，
            # 解包后即为全局降序。
            wrapped = [_wrap_reverse(sc, key) for sc in sorted_chunks]
            yield from _unwrap_reverse(heapq.merge(*wrapped))
        else:
            yield from heapq.merge(*sorted_chunks, key=key)

    def _external_sort(
        self,
        max_workers: int | None = None,
        key: Callable[[T], Any] | None = None,
        reverse: bool = False,
    ) -> Generator[T, None, None]:
        """外部排序（向后兼容别名）。

        重构后实际实现位于 algorithms.external_sort.external_sort。
        本方法仅作为旧调用入口（如有人写 self._external_sort(...)）。
        """
        cfg: BigArrayConfig = self._get_config()
        yield from external_sort(
            self._fresh_iter(),
            len(self),
            config=cfg,
            max_workers=max_workers,
            key=key,
            reverse=reverse,
        )

    def _binary_search(
        self,
        target: T,
        max_workers: int | None = None,
        key: Callable[[T], Any] | None = None,
    ) -> Generator[tuple[int, T], None, None]:
        """二分查找（先排序，再 bisect）。

        数值内存源且无 key 时先用 C 排序副本物化；其他源沿用三级排序分流。
        随后用 bisect_left / bisect_right 夹出 [left, right) 命中区间，
        返回 (排序后下标, 命中元素)，而不是原始数据下标。
        """
        fast = fast_numeric(self) if key is None else None
        if fast is not None:
            sorted_list: list[T] = fast.py_sorted_copy(False).py_to_list()
        else:
            sorted_iter: Generator[T, None, None] = self._big_iter_sort(
                max_workers=max_workers, key=key
            )
            sorted_list = list(sorted_iter)
        if key is None:
            left = _bisect_mod.bisect_left(sorted_list, target)  # type: ignore[call-overload]
            right = _bisect_mod.bisect_right(sorted_list, target)  # type: ignore[call-overload]
        else:
            # Python 3.10+ bisect 支持 key 参数（对 list 元素和 target 都应用 key）
            left = _bisect_mod.bisect_left(sorted_list, target, key=key)  # type: ignore[call-overload]
            right = _bisect_mod.bisect_right(sorted_list, target, key=key)  # type: ignore[call-overload]
        for i in range(left, right):
            yield (i, sorted_list[i])


__all__ = ["SortMixin"]
