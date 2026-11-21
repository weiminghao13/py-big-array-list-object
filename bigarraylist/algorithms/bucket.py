"""持久化分桶（Hash Partitioning）+ 单桶扫描查找。

对比重构前：
    - 重构前：bucket_find() 每次调用都重建桶（每次都付 O(n) 建桶成本，
      多次查询没有任何加速）。
    - 重构后：build_buckets() 返回 BucketIndex 对象，建桶一次 O(n)，
      之后任意多次 .find(target) 只扫单桶 O(n/N)（桶文件生命周期由
      BucketIndex 管理，close()/退出上下文时清理）。

MapReduce 核心思想落地：相同 key 一定在同一个桶 → 查找时只需扫一个桶。

性能（P0-1 / P1-3）：
    - 建桶按批量帧格式落盘（每批 pickle_batch_size 条），比逐条快 5~10x
    - 桶内记录 (i, key, item)，find 直接比 key，不再对每条重算 key_func
    - 桶号 → 路径的 dict 在建桶时维护，find 定位 O(1)（不再逐个正则解析文件名）
"""

from __future__ import annotations

import math
import os
import tempfile
from collections.abc import Callable, Generator, Iterable
from typing import Any, Generic, TypeVar

from ..config import BigArrayConfig
from .fileio import batched_file_iter, dump_batches

T = TypeVar("T")


class BucketIndex(Generic[T]):
    """持久化分桶索引。

    建桶一次后可多次 find，每次只扫目标桶。
    支持上下文管理器：
        with build_buckets(data, key_func) as idx:
            for tgt in targets:
                hits = list(idx.find(tgt))
    """

    __slots__ = (
        "__file_buffer_size",
        "__key_func",
        "__num_buckets",
        "__owns_files",
        "__path_by_idx",
        "__temp_paths",
    )

    def __init__(
        self,
        temp_paths: list[str],
        num_buckets: int,
        key_func: Callable[[T], Any],
        owns_files: bool = True,
        path_by_idx: dict[int, str] | None = None,
        file_buffer_size: int = 64 * 1024,
    ) -> None:
        self.__temp_paths = temp_paths
        self.__num_buckets = num_buckets
        self.__key_func = key_func
        self.__owns_files = owns_files  # False 时 close() 不删文件（外部持久化场景）
        self.__file_buffer_size = file_buffer_size
        # 桶号 → 路径；None 时按旧约定从文件名解析（向后兼容手工构造）
        if path_by_idx is not None:
            self.__path_by_idx = path_by_idx
        else:
            self.__path_by_idx = {self._extract_bucket_idx(p): p for p in temp_paths}

    def __enter__(self) -> BucketIndex:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: Any,
    ) -> None:
        self.close()

    def close(self) -> None:
        """清理所有桶临时文件。多次调用幂等。"""
        if not self.__owns_files:
            return
        paths = self.__temp_paths
        self.__temp_paths = []
        self.__path_by_idx = {}
        for path in paths:
            try:
                os.unlink(path)
            except OSError:
                pass

    @property
    def num_buckets(self) -> int:
        return self.__num_buckets

    def find(self, target: T) -> Generator[tuple[int, T], None, None]:
        """查找所有 key(item) == key(target) 的元素。

        返回生成器，每项为 (原始下标, 命中元素)。
        只扫目标桶，O(n/N) 时间；O(桶内批大小) 额外内存。
        桶内已预存 key，直接比较，不再重算 key_func（P1-3）。
        """
        target_key = self.__key_func(target)
        target_bucket = hash(target_key) % self.__num_buckets

        # O(1) 定位目标桶（不存在 → 该桶为空）
        target_path = self.__path_by_idx.get(target_bucket)
        if target_path is None:
            return  # 目标桶为空

        with open(target_path, "rb", buffering=self.__file_buffer_size) as f:
            for _i, key, item in batched_file_iter(f):
                if key == target_key:
                    yield (_i, item)

    def find_by_bucket(self, bucket_idx: int) -> Generator[tuple[int, T], None, None]:
        """遍历指定桶的全部记录，每项为 (原始下标, 元素)。

        供分桶去重等复用场景：逐桶扫描而无需 target key。
        """
        path = self.__path_by_idx.get(bucket_idx)
        if path is None:
            return  # 空桶
        with open(path, "rb", buffering=self.__file_buffer_size) as f:
            for _i, _key, item in batched_file_iter(f):
                yield (_i, item)

    @staticmethod
    def _extract_bucket_idx(path: str) -> int:
        """从临时文件名 big_bucket_{idx}_XXXX.tmp 提取桶下标。

        向后兼容保留（无 path_by_idx 的手工构造场景）；
        内部路径已改为 dict 直查。
        """
        import re

        m = re.search(r"big_bucket_(\d+)_", path)
        if m is None:
            return -1
        return int(m.group(1))


def _choose_num_buckets(total_count: int, max_buckets: int) -> int:
    """自动选桶数：sqrt(n) 附近的 2 的幂，上限 max_buckets。

    取 2 的幂让 hash % num_buckets 利用位运算高效；上限避免文件描述符爆掉。
    """
    target_n = max(8, math.isqrt(total_count))
    n = 1
    while n * 2 <= target_n and n < max_buckets:
        n *= 2
    return n


def build_buckets(
    data: Iterable[T],
    total_count: int,
    key_func: Callable[[T], Any] | None = None,
    num_buckets: int | None = None,
    *,
    config: BigArrayConfig | None = None,
) -> BucketIndex:
    """对数据流式建桶，返回可复用的 BucketIndex。

    参数：
        data:         数据源迭代器（建议来自 _fresh_iter）
        total_count:  数据总条数
        key_func:     从元素提取分桶 key；None 用元素本身
                      （必须满足 a==b → key(a)==key(b) 且 hash 兼容，否则漏命中）
        num_buckets:   桶数；None 自动选（sqrt(n) 附近 2 的幂，上限 config.bucket_max_count）
        config:        配置对象；None 用 BigArrayConfig.default()

    返回：BucketIndex，支持多次 find(target) 和上下文管理器。

    内存模型：
        - 阶段 1（建桶）：流式读，按批打包 (下标, key, 元素) pickle 落盘
        - 文件缓冲 = 桶数 × bucket_file_buffer_size（默认每桶 64KB）
        - 元素批缓冲 = O(桶数 × pickle_batch_size)
    """
    cfg = config if config is not None else BigArrayConfig.default()
    if key_func is None:
        key_func = lambda x: x  # noqa: E731 恒等默认值
    batch_size = cfg.pickle_batch_size
    file_buffer_size = cfg.bucket_file_buffer_size

    if num_buckets is None:
        num_buckets = _choose_num_buckets(total_count, cfg.bucket_max_count)
    if num_buckets < 1:
        raise ValueError(f"num_buckets 必须 >= 1，得到 {num_buckets}")

    # 每桶一个批缓冲：攒满 batch_size 条再一次性 dump（P0-1）
    bucket_bufs: list[list[tuple[int, Any, T]]] = [[] for _ in range(num_buckets)]
    bucket_files: list[Any] = [None] * num_buckets
    temp_paths: list[str] = []
    path_by_idx: dict[int, str] = {}

    def _flush(bidx: int) -> None:
        buf = bucket_bufs[bidx]
        if buf and bucket_files[bidx] is not None:
            dump_batches(buf, bucket_files[bidx], batch_size)
            buf.clear()

    try:
        # 阶段 1：流式分桶（每批 (下标, key, 元素) 一起落盘；key 预存供 find 复用）
        for i, item in enumerate(data):
            k = key_func(item)
            bidx = hash(k) % num_buckets
            if bucket_files[bidx] is None:
                tmp = tempfile.NamedTemporaryFile(
                    suffix=".tmp",
                    prefix=f"big_bucket_{bidx}_",
                    delete=False,
                    buffering=file_buffer_size,
                )
                temp_paths.append(tmp.name)
                path_by_idx[bidx] = tmp.name
                bucket_files[bidx] = tmp
            bucket_bufs[bidx].append((i, k, item))
            if len(bucket_bufs[bidx]) >= batch_size:
                _flush(bidx)

        # 冲刷所有残余缓冲并关闭文件
        for bidx in range(num_buckets):
            _flush(bidx)
            if bucket_files[bidx] is not None:
                bucket_files[bidx].close()

        return BucketIndex(
            temp_paths,
            num_buckets,
            key_func,
            owns_files=True,
            path_by_idx=path_by_idx,
            file_buffer_size=file_buffer_size,
        )
    except BaseException:
        # 出错时清理已开的文件和已建的临时文件
        for f in bucket_files:
            if f is not None:
                try:
                    f.close()
                except Exception:
                    pass
        for path in temp_paths:
            try:
                os.unlink(path)
            except OSError:
                pass
        raise


__all__ = ["BucketIndex", "build_buckets"]
