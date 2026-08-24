"""真正的低内存外部排序。

核心改进（对比重构前的整块版本）：
    - 重构前：阶段 2 整块 pickle.load，所有 chunk_list 同时在内存，
      内存峰值 ≈ 数据总量，违背"外部排序"的初衷。
    - 重构后：阶段 1 逐条 pickle.dump 到文件（每条独立可 load），
      阶段 2 每个文件用一个 lazy 文件迭代器，每次只 load 一条。
      内存峰值 = O(块数) 文件句柄 + O(块数) 单条元素。

模式选择（由 BigArrayConfig.external_sort_streaming_merge 控制）：
    - True  → streaming（逐条 load，省内存，速度稍慢）
    - False → block（整块 load，快 3~5 倍，内存 ≈ 数据总量；与重构前一致）

通过 lazy_file_iter / lazy_block_iter 两种文件迭代器抽象两种模式，
上层调用方代码完全相同。
"""

from __future__ import annotations

import heapq
import os
import pickle
import tempfile
from collections import deque
from collections.abc import Callable, Generator, Iterable
from concurrent.futures import Future, ProcessPoolExecutor
from typing import IO, Any, TypeVar

from ..config import BigArrayConfig
from ..utils.scale import choose_external_sort_chunk_size
from .parallel import sort_chunk

T = TypeVar("T")

# 双缓冲流水线深度：阶段 1 同时最多保持 2 个 chunk 在飞行
# （1 个在 worker 中排序 + 1 个在主进程读取/落盘）。
# 排序 N 与 "读 N+1 + 落 N-1" 并行，并行度从 1 提升到 2，
# 内存峰值仍为 O(chunk_size)（与串行版一致，不退化成中数据分支）。
_BUFFER_DEPTH = 2


def _streaming_file_iter(f: IO[bytes]) -> Generator[T, None, None]:
    """逐条 pickle.load 模式的文件迭代器。

    要求文件是用"逐条 pickle.dump"写入的（每次 dump 一个元素）。
    每次 next 调用 pickle.load(f)，读到 EOF 抛 StopIteration。
    内存：O(1) per file（同时持有的元素仅 1 个）。
    """
    try:
        while True:
            try:
                yield pickle.load(f)
            except EOFError:
                return
    except GeneratorExit:
        # 调用方 close() 生成器时也要让上层 finally 关闭文件
        raise


def _block_file_iter(f: IO[bytes]) -> Generator[T, None, None]:
    """整块 pickle.load 模式的文件迭代器（向后兼容用）。

    要求文件是用"整块 pickle.dump(list)"写入的（一次 dump 整个 list）。
    一次 pickle.load 读出整块 list，然后 yield from 展开。
    内存：O(整块大小) per file；所有文件同时存在时，总内存 ≈ 数据总量。
    """
    chunk_list = pickle.load(f)
    yield from chunk_list


def _dump_streaming(chunk: list[T], f: IO[bytes]) -> None:
    """逐条 pickle.dump 一个 chunk 的元素到文件 f。"""
    for item in chunk:
        pickle.dump(item, f)


def _dump_block(chunk: list[T], f: IO[bytes]) -> None:
    """整块 pickle.dump 一个 chunk 到文件 f（向后兼容模式）。"""
    pickle.dump(chunk, f)


def external_sort(
    data: Iterable[T],
    total_count: int,
    *,
    config: BigArrayConfig | None = None,
    max_workers: int | None = None,
    key: Callable[[T], Any] | None = None,
) -> Generator[T, None, None]:
    """外部排序（External Sort）：分块排序 + 落盘 + 多路归并。

    参数：
        data:         数据源迭代器（建议来自 _fresh_iter）
        total_count:  数据总条数（用于选块大小）
        config:       配置对象；None 用 BigArrayConfig.default()
        max_workers:  阶段 1 排序的进程池并行度
        key:          排序键函数（类似 ``sorted(data, key=...)``）；
                      None 按元素本身排序。
                      ⚠️ 多进程路径要求 key 可 pickle（模块级函数或 functools.partial，
                      lambda 在 spawn 模式下不可 pickle）

    返回：生成器，按 key 的非降序逐条产出已排序元素。

    内存模型（streaming 模式，默认）：
        - 阶段 1：双缓冲流水线排序 + 逐条落盘。submit 一块后立即继续读下一块，
          飞行队列满（_BUFFER_DEPTH=2 块）时才 .result() 落盘最老的一块；
          排序 N 与 "读 N+1 + 落 N-1" 并行。内存峰值 ≈ 2*chunk_size。
        - 阶段 2：所有块各持一个文件句柄 + lazy 迭代器，每次 yield 一条。
          内存峰值 = O(块数)（≈ 32）文件句柄 + O(块数) 临时元素。
        适合"数据量远超内存"的场景。

    内存模型（block 模式，external_sort_streaming_merge=False）：
        - 阶段 1：双缓冲流水线排序 + 整块落盘。比逐条快 3~5 倍。
        - 阶段 2：所有块整块 pickle.load 到内存，heapq.merge 同时驱动所有 list。
          内存峰值 ≈ 数据总量。仅当数据总量放得下内存时使用。

    约束：
        - 元素必须可 pickle（落盘要求）
        - 文件描述符上限：块数受 OS 限制（Linux 默认 1024）
          块数由 choose_external_sort_chunk_size 控制（目标 32）
    """
    cfg = config if config is not None else BigArrayConfig.default()
    streaming = cfg.external_sort_streaming_merge
    chunk_size = choose_external_sort_chunk_size(
        total_count,
        target=cfg.external_sort_chunk_target,
        min_chunk=cfg.external_sort_min_chunk,
        max_chunk=cfg.external_sort_max_chunk,
    )
    _dump = _dump_streaming if streaming else _dump_block
    _file_iter_cls = _streaming_file_iter if streaming else _block_file_iter

    temp_paths: list[str] = []

    def _dump_chunk(sorted_chunk: list) -> None:
        tmp = tempfile.NamedTemporaryFile(suffix=".tmp", prefix="big_sort_", delete=False)
        try:
            _dump(sorted_chunk, tmp)
            tmp.close()
        except Exception:
            tmp.close()
            try:
                os.unlink(tmp.name)
            except OSError:
                pass
            raise
        temp_paths.append(tmp.name)

    try:
        # 阶段 1：双缓冲流水线排序 + 落盘
        # 改进点（对比串行版）：原版每块 submit().result() 串行，排序与 I/O 完全串行。
        # 双缓冲：submit 一块后立即继续读下一块；飞行队列满（_BUFFER_DEPTH 块）时
        # 才 .result() 落盘最老的一块。排序 N 与 "读 N+1 + 落 N-1" 并行，并行度 1→2。
        # 内存：飞行队列最多 2 个 future；每个 future 的 chunk 在 worker pickled 后即可
        # 释放，主进程峰值 ≈ 2*chunk_size（与串行版一致，不退化成中数据分支）。
        in_flight: deque[Future[list[T]]] = deque()
        with ProcessPoolExecutor(max_workers=max_workers) as pool:
            chunk: list = []
            try:
                for item in data:
                    chunk.append(item)
                    if len(chunk) >= chunk_size:
                        in_flight.append(pool.submit(sort_chunk, (chunk, key)))
                        chunk = []
                        if len(in_flight) >= _BUFFER_DEPTH:
                            _dump_chunk(in_flight.popleft().result())
                if chunk:  # 末尾不足一块的也要排
                    in_flight.append(pool.submit(sort_chunk, (chunk, key)))
                # 排空飞行队列
                while in_flight:
                    _dump_chunk(in_flight.popleft().result())
            except BaseException:
                # 异常时取消未启动的 future；已启动的让其自然结束（pool.__exit__ 会 join）
                for fut in in_flight:
                    if not fut.running():
                        fut.cancel()
                raise

        # 阶段 2：多路归并
        # heapq.merge 接受多个有序迭代器，逐条比较产出
        files: list[IO[bytes]] = []
        try:
            files.extend(open(path, "rb") for path in temp_paths)
            file_iters: list[Generator[T, None, None]] = [_file_iter_cls(f) for f in files]
            yield from heapq.merge(*file_iters, key=key)  # type: ignore[arg-type]
        finally:
            for f in files:
                try:
                    f.close()
                except Exception:
                    pass
    finally:
        for path in temp_paths:
            try:
                os.unlink(path)
            except OSError:
                pass


__all__ = ["external_sort"]
