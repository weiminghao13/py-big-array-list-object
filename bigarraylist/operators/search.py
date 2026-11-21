"""查找算子 mixin：并行线性查找 / 二分查找 / 持久化分桶查找 / 二级索引 / 自动查找入口。

核心改进（对比重构前）：
    - _bucket_find 改为复用 algorithms.bucket.build_buckets，
      持久化 BucketIndex 让多次查询真正复用（重构前每次重建桶）。
    - 新增 _build_buckets() 公共方法，返回 BucketIndex 给用户跨多次 find 复用。
    - 阈值改走 BigArrayConfig（self._get_config()）。
"""

from __future__ import annotations

from collections import deque
from collections.abc import Callable, Generator, Iterator
from concurrent.futures import Future
from typing import Any, Generic, TypeVar

from ..algorithms._pool import default_workers, get_pool
from ..algorithms.bucket import BucketIndex, build_buckets
from ..algorithms.parallel import linear_find_in_chunk
from ..config import BigArrayConfig
from ..utils.scale import choose_parallel_chunk_size

T = TypeVar("T")


class SearchMixin(Generic[T]):
    """查找算子 mixin。不声明 __slots__。

    继承 Generic[T] 仅用于类型推导；运行时通过 cooperative multiple
    inheritance 与 _CoreBase 组合，_fresh_iter / __len__ / _get_config
    实际由 _CoreBase 提供（MRO 中 _CoreBase 在前，stub 被覆盖）。
    """

    # 协作契约：实际实现由 _CoreBase 通过 MRO 提供
    def __len__(self) -> int:  # pragma: no cover - 协作契约
        raise NotImplementedError

    def _fresh_iter(self) -> Iterator[T]:  # pragma: no cover - 协作契约
        raise NotImplementedError

    def _get_config(self) -> BigArrayConfig:  # pragma: no cover - 协作契约
        raise NotImplementedError

    def _parallel_find(
        self,
        target: T,
        max_workers: int | None = None,
    ) -> Generator[tuple[int, T], None, None]:
        """并行分块线性查找（惰性生成器）：每个进程负责一个 chunk 的线性查找。

        - 数据量 < config.linear_parallel_threshold：直接线性 yield（不开进程）
        - 数据量 >= threshold：滑动窗口并行，按块提交顺序 yield
        - 惰性早停：调用方 break / close() 时，取消所有未启动的 chunk 任务，
          已在跑的任务让其自然结束（结果丢弃），不会白跑全部数据

        滑动窗口设计：保持最多 workers*2 个 future 在飞行，yield 一块后补一块，
        既保证并行度又避免一次性提交全部 chunk（早停场景下浪费 CPU）。

        任务粒度用 choose_parallel_chunk_size（P0-3）：块大小
        max(parallel_chunk_min_size, n // (workers*4))，IPC 次数从数千降到几十。
        共享进程池（P1-2）：不再每次调用冷启动 spawn；早停时仍取消未启动的
        future（GeneratorExit 语义不变），只是池不 shutdown。
        """
        n = len(self)
        cfg: BigArrayConfig = self._get_config()
        data = self._fresh_iter()

        # 小数据：进程启动开销 > 收益，直接线性 yield
        if n < cfg.linear_parallel_threshold:
            for i, item in enumerate(data):
                if item == target:
                    yield (i, item)
            return

        workers = max_workers or default_workers()
        chunk_size = choose_parallel_chunk_size(n, workers, cfg.parallel_chunk_min_size)
        window_size = workers * 2  # 保持足够的飞行任务数

        pool = get_pool(max_workers)
        in_flight: deque[Future[list[tuple[int, T]]]] = deque()
        chunk: list[T] = []
        current_index = 0
        source_exhausted = False

        def _submit_chunk() -> None:
            """从 data 读一个 chunk 并提交到进程池。"""
            nonlocal chunk, current_index, source_exhausted
            chunk = []
            for item in data:
                chunk.append(item)
                if len(chunk) >= chunk_size:
                    break
            if not chunk:
                source_exhausted = True
                return
            fut = pool.submit(linear_find_in_chunk, (chunk, target, current_index))
            in_flight.append(fut)
            current_index += len(chunk)

        def _fill_window() -> None:
            """补充飞行任务直到窗口满或数据源耗尽。"""
            while len(in_flight) < window_size and not source_exhausted:
                _submit_chunk()

        try:
            _fill_window()
            while in_flight:
                fut = in_flight.popleft()
                yield from fut.result()
                _fill_window()  # 产出一块后补一块
        except GeneratorExit:
            # 调用方停止消费：取消所有未完成的 future，然后重新抛出
            # （共享池保持运行，只取消任务不 shutdown——P1-2）
            for fut in in_flight:
                if not fut.done():
                    fut.cancel()
            raise

    def _bucket_find(
        self,
        target: T,
        key_func: Callable[[T], Any] | None = None,
        num_buckets: int | None = None,
        max_workers: int | None = None,
    ) -> Generator[tuple[int, T], None, None]:
        """分桶查找：按 key 的哈希值把数据流式分到 N 个桶文件，
        只扫目标桶即可定位所有命中元素，避免全量线性扫描。

        重构后改进：内部走 algorithms.bucket.build_buckets 拿到 BucketIndex，
        再调 BucketIndex.find(target)。多次查询应直接用 build_buckets 复用。
        本方法每次调用都重建桶——保持零副作用、向后兼容。
        """
        n = len(self)
        cfg: BigArrayConfig = self._get_config()

        # 小数据：不分桶，直接线性扫（建桶开销不划算）
        if n < cfg.linear_parallel_threshold:
            kf = key_func if key_func is not None else (lambda x: x)
            target_key = kf(target)
            for i, item in enumerate(self._fresh_iter()):
                if kf(item) == target_key:
                    yield (i, item)
            return

        # 大数据：建桶 + 单桶扫（建完即查即弃；多次查询请直接用 build_buckets）
        idx = build_buckets(
            self._fresh_iter(),
            n,
            key_func=key_func,
            num_buckets=num_buckets,
            config=cfg,
        )
        try:
            yield from idx.find(target)
        finally:
            idx.close()

    def _build_buckets(
        self,
        key_func: Callable[[T], Any] | None = None,
        num_buckets: int | None = None,
    ) -> BucketIndex:
        """建持久化分桶索引，返回 BucketIndex。

        与 _bucket_find 的区别：
            - _bucket_find：建桶 + 查一次 + 销毁桶，仅适合单次查
            - _build_buckets：建桶一次返回 BucketIndex，可多次 find(target)，
              跨多次查询真正复用建桶成本。

        使用：
            with big._build_buckets(key_func=lambda u: u.id) as idx:
                for tgt in targets:
                    hits = list(idx.find(tgt))
        """
        n = len(self)
        cfg: BigArrayConfig = self._get_config()
        return build_buckets(
            self._fresh_iter(),
            n,
            key_func=key_func,
            num_buckets=num_buckets,
            config=cfg,
        )

    def _build_index(self, key_func: Callable[[T], Any] | None = None) -> dict:
        """建二级索引：dict[key] -> list[原始下标]。

        内存够用时用 dict，O(n) 时间 O(n) 内存。建好的索引可反复用：
            - 判断 key 在不在：O(1)
            - 拿到所有命中下标列表：O(1)

        注意：惰性流不支持随机访问（__getitem__ 抛 NotImplementedError），
        所以索引存的是下标，要拿元素仍需流式扫。索引的核心价值是
        "建目录快速定位下标"，省去线性扫判断 key 在不在的 O(n) 开销。

        约束：key_func 必须满足 a==b → key(a)==key(b)，
        且 hash(key(a)) == hash(key(b))，否则索引会漏命中。

        全程操作 _fresh_iter()，不碰 self.__data——可重复调用。
        """
        if key_func is None:
            key_func = lambda x: x  # noqa: E731
        index: dict[Any, list[int]] = {}
        for i, item in enumerate(self._fresh_iter()):
            k = key_func(item)
            if k in index:
                index[k].append(i)
            else:
                index[k] = [i]
        return index

    def _auto_find(
        self,
        target: T,
        key_func: Callable[[T], Any] | None = None,
        num_buckets: int | None = None,
        max_workers: int | None = None,
    ) -> Generator[tuple[int, T], None, None]:
        """单次等值查找：线性扫描，大数据可按阈值并行分块。

        ``num_buckets`` 仅为旧签名兼容而保留，不再触发建桶。重复查询应由
        调用方复用 ``build_buckets()`` 返回的索引，避免每次支付全量落盘成本。
        指定 ``key_func`` 时保持 ``key(item) == key(target)`` 的匹配语义。
        """
        if key_func is None:
            yield from self._parallel_find(target, max_workers=max_workers)
            return
        target_key = key_func(target)
        for i, item in enumerate(self._fresh_iter()):
            if key_func(item) == target_key:
                yield (i, item)


__all__ = ["SearchMixin"]
