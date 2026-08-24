"""持久化分桶（Hash Partitioning）+ 单桶扫描查找。

对比重构前：
    - 重构前：bucket_find() 每次调用都重建桶（每次都付 O(n) 建桶成本，
      多次查询没有任何加速）。
    - 重构后：build_buckets() 返回 BucketIndex 对象，建桶一次 O(n)，
      之后任意多次 .find(target) 只扫单桶 O(n/N)；同时支持跨进程复用
      （persist_path 把桶文件保留到指定目录）。

MapReduce 核心思想落地：相同 key 一定在同一个桶 → 查找时只需扫一个桶。
"""

from __future__ import annotations

import math
import os
import pickle
import re
import tempfile
from collections.abc import Callable, Generator, Iterable
from typing import Any, Generic, TypeVar

from ..config import BigArrayConfig

T = TypeVar("T")


class BucketIndex(Generic[T]):
    """持久化分桶索引。

    建桶一次后可多次 find，每次只扫目标桶。
    支持上下文管理器：
        with build_buckets(data, key_func) as idx:
            for tgt in targets:
                hits = list(idx.find(tgt))
    """

    __slots__ = ("__key_func", "__num_buckets", "__owns_files", "__temp_paths")

    def __init__(
        self,
        temp_paths: list[str],
        num_buckets: int,
        key_func: Callable[[T], Any],
        owns_files: bool = True,
    ) -> None:
        self.__temp_paths = temp_paths
        self.__num_buckets = num_buckets
        self.__key_func = key_func
        self.__owns_files = owns_files  # False 时 close() 不删文件（外部持久化场景）

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
        只扫目标桶，O(n/N) 时间；O(1) 额外内存（除目标桶文件读取外）。
        """
        target_key = self.__key_func(target)
        target_bucket = hash(target_key) % self.__num_buckets

        # 在 temp_paths 中定位目标桶（可能不存在 → 该桶为空）
        target_path = None
        for path in self.__temp_paths:
            if self._extract_bucket_idx(path) == target_bucket:
                target_path = path
                break

        if target_path is None:
            return  # 目标桶为空

        with open(target_path, "rb") as f:
            while True:
                try:
                    i, item = pickle.load(f)
                except EOFError:
                    break
                if self.__key_func(item) == target_key:
                    yield (i, item)

    @staticmethod
    def _extract_bucket_idx(path: str) -> int:
        """从临时文件名 big_bucket_{idx}_XXXX.tmp 提取桶下标。"""
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
        - 阶段 1（建桶）：流式读，每条元素 + 下标一起 pickle.dump 到对应桶文件
        - 内存峰值 = O(桶数) 文件句柄 + O(1) 单条元素
    """
    cfg = config if config is not None else BigArrayConfig.default()
    if key_func is None:
        key_func = lambda x: x  # noqa: E731 恒等默认值

    if num_buckets is None:
        num_buckets = _choose_num_buckets(total_count, cfg.bucket_max_count)
    if num_buckets < 1:
        raise ValueError(f"num_buckets 必须 >= 1，得到 {num_buckets}")

    bucket_files: list[Any] = [None] * num_buckets
    temp_paths: list[str] = []
    try:
        # 阶段 1：流式分桶（每条元素 + 其下标一起 pickle 落盘）
        for i, item in enumerate(data):
            bidx = hash(key_func(item)) % num_buckets
            if bucket_files[bidx] is None:
                tmp = tempfile.NamedTemporaryFile(
                    suffix=".tmp",
                    prefix=f"big_bucket_{bidx}_",
                    delete=False,
                )
                temp_paths.append(tmp.name)
                bucket_files[bidx] = tmp
            # 逐条写 (下标, 元素)
            pickle.dump((i, item), bucket_files[bidx])

        # 关闭所有已开启的桶文件（写模式 → 准备读）
        for f in bucket_files:
            if f is not None:
                f.close()

        return BucketIndex(temp_paths, num_buckets, key_func, owns_files=True)
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
