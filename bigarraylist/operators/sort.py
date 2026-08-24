"""排序算子 mixin：sort / is_sorted / 外部排序接入。

核心改进（对比重构前）：
    - 不再内联 _external_sort 实现，改为调用 algorithms.external_sort.external_sort
    - 通过 self._get_config() 读 BigArrayConfig 的阈值，所有阈值可配
    - 默认走 streaming 模式（内存峰值 O(块数) 而非 O(数据总量)）
"""

from __future__ import annotations

import bisect as _bisect_mod
import heapq
from collections.abc import Callable, Generator, Iterator
from concurrent.futures import ProcessPoolExecutor
from typing import Any, Generic, TypeVar

from ..algorithms.external_sort import external_sort
from ..algorithms.parallel import chunk_is_sorted, sort_chunk
from ..config import BigArrayConfig
from ..utils.scale import choose_chunk_size

T = TypeVar("T")


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

        全程操作 _fresh_iter()，不碰 self.__data——可重复调用。
        大数据场景下：
            - 每个 chunk 内部判断局部有序（并行干）
            - 相邻 chunk 之间再比较"前一块尾 ≤ 后一块头"（必要条件）
        两者同时满足才算全局有序。
        """
        n = len(self)
        if n <= 1:
            return True

        cfg: BigArrayConfig = self._get_config()
        data = self._fresh_iter()

        # 小数据：直接流式 O(n) 判断，进程启动开销不划算
        if n < cfg.linear_parallel_threshold:
            prev: T | None = None
            for item in data:
                if prev is not None and prev > item:  # type: ignore[operator]
                    return False
                prev = item
            return True

        chunk_size = choose_chunk_size(n)
        chunk_bounds: list[tuple[T, T]] = []
        pool = ProcessPoolExecutor(max_workers=max_workers)
        try:
            futures = []
            chunk: list[T] = []
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
        finally:
            pool.shutdown(wait=False)

    def _big_iter_sort(
        self,
        max_workers: int | None = None,
        key: Callable[[T], Any] | None = None,
    ) -> Generator[T, None, None]:
        """大数据排序：按数据量分流。

        - 小数据（< config.linear_parallel_threshold）：直接 sorted() 物化，单进程
        - 中数据（threshold ~ external_sort_threshold）：分块并行排序 + 内存多路归并
        - 大数据（>= config.external_sort_threshold）：外部排序
          * 默认 streaming 模式：阶段 2 逐条 load，内存峰值 = O(块数)
          * block 模式（external_sort_streaming_merge=False）：归并阶段所有块
            整块 load 到内存，峰值 ≈ 数据总量，但速度快 3~5 倍

        参数：
            max_workers: 进程池并行度；None 用默认
            key:         排序键函数（类似 ``sorted(data, key=...)``）；None 按元素本身排序
                         ⚠️ 多进程路径要求 key 可 pickle（模块级函数或 functools.partial）

        全程操作 _fresh_iter()（外部排序内部也用），不碰 self.__data——
        可以重复调用，不会污染实例状态。
        """
        n = len(self)
        cfg: BigArrayConfig = self._get_config()

        if n < cfg.linear_parallel_threshold:
            yield from sorted(self._fresh_iter(), key=key)  # type: ignore[type-var, arg-type]
            return

        # 大数据走外部排序（统一走 algorithms.external_sort）
        if n >= cfg.external_sort_threshold:
            yield from external_sort(
                self._fresh_iter(),
                n,
                config=cfg,
                max_workers=max_workers,
                key=key,
            )
            return

        # 中数据：分块并行排序 + 内存多路归并
        data = self._fresh_iter()
        chunk_size = choose_chunk_size(n)

        with ProcessPoolExecutor(max_workers=max_workers) as pool:
            futures: list[Any] = []
            chunk: list[T] = []
            for item in data:
                chunk.append(item)
                if len(chunk) >= chunk_size:
                    futures.append(pool.submit(sort_chunk, (chunk, key)))
                    chunk = []
            if chunk:
                futures.append(pool.submit(sort_chunk, (chunk, key)))
            sorted_chunks = [fut.result() for fut in futures]

        yield from heapq.merge(*sorted_chunks, key=key)

    def _external_sort(
        self,
        max_workers: int | None = None,
        key: Callable[[T], Any] | None = None,
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
        )

    def _binary_search(
        self,
        target: T,
        max_workers: int | None = None,
        key: Callable[[T], Any] | None = None,
    ) -> Generator[tuple[int, T], None, None]:
        """二分查找（先排序，再 bisect）。

        正确流程（也是唯一可行的流程）：
            1) 用 _big_iter_sort 做分块并行排序 → 拿到有序迭代器
            2) 把有序迭代器**物化**成 list（bisect 要求 arr[mid] 随机访问，绕不开）
            3) 用 bisect_left / bisect_right 夹出 [left, right) 命中区间
            4) 逐个 yield (排序后下标, 命中元素)

        参数：
            target:      要查找的元素；若 key 不为 None，则比较的是 key(target)
            max_workers: 排序阶段进程池并行度
            key:         排序键函数；查找时对 target 和元素都应用 key 后比较

        全程操作 _fresh_iter()（在 _big_iter_sort 内部），不碰 self.__data。
        ⚠️ 返回的下标是"**排序后**的下标"，不是原始数据的下标。
        """
        sorted_iter: Generator[T, None, None] = self._big_iter_sort(
            max_workers=max_workers, key=key
        )
        sorted_list: list[T] = list(sorted_iter)
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
