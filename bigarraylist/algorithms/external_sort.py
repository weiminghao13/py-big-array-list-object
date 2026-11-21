"""真正的低内存外部排序。

核心改进（对比重构前的整块版本）：
    - 重构前：阶段 2 整块 pickle.load，所有 chunk_list 同时在内存，
      内存峰值 ≈ 数据总量，违背"外部排序"的初衷。
    - 重构后：阶段 1 逐条 pickle.dump 到文件（每条独立可 load），
      阶段 2 每个文件用一个 lazy 文件迭代器，每次只 load 一条。
      内存峰值 = O(块数) 文件句柄 + O(块数) 单条元素。

阶段 1 的块落盘分两条路（方案③）：
    - 纯数值块（int64/float64）：主进程内 C introsort 后直接写二进制帧，
      不经进程池、不 pickle——省掉 IPC 往返与子进程冷启动
    - 其它块（含 key、非数值）：提交进程池 sorted()，结果按自描述帧落盘
阶段 2 的同类型纯数值帧在 C 层流式多路归并；其它帧回退 heapq.merge。

模式选择（由 BigArrayConfig.external_sort_streaming_merge 控制）：
    - True  → streaming（逐批 load，省内存，速度稍慢）
    - False → block（整块 load，快 3~5 倍，内存 ≈ 数据总量；与重构前一致）

通过 lazy_file_iter / lazy_block_iter 两种文件迭代器抽象两种模式，
上层调用方代码完全相同。
"""

from __future__ import annotations

import heapq
import os
import tempfile
from collections import deque
from collections.abc import Callable, Generator, Iterable
from concurrent.futures import Future, ProcessPoolExecutor
from typing import IO, Any, TypeVar

from .._fast import NUMERIC_MERGE_AVAILABLE, merge_numeric_files
from ..config import BigArrayConfig
from ..utils.scale import choose_external_sort_chunk_size
from ._pool import default_workers, get_pool
from .fileio import (
    IO_BUFFER_SIZE,
    dump_frames,
    dump_sorted_frames_kind,
    frames_iter,
)
from .parallel import sort_chunk

T = TypeVar("T")


class _ReverseWrapper:
    """包装元素，比较时取反，用于 heapq.merge 降序归并（external_sort 内部用）。

    与 operators/sort.py 中的同名类功能一致，此处独立定义避免循环导入。
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
    for item in it:
        k = key(item) if key is not None else item
        yield _ReverseWrapper(k, item)


def _unwrap_reverse(it: Iterable[_ReverseWrapper]) -> Generator[T, None, None]:
    for wrapped in it:
        yield wrapped.value


# 旧的双缓冲流水线深度常量：保留名字向后兼容（可被 import），
# 实际深度由 _resolve_pipeline_depth 按 worker 数/config 动态决定（P0-3）。
_BUFFER_DEPTH = 2


def _resolve_pipeline_depth(max_workers: int | None, config: BigArrayConfig) -> int:
    """流水线深度：默认 max(2, 2 * workers)，让每个 worker 都有飞行任务。

    旧版硬编码 2：8 核机器 6 核空转。内存峰值 = O(深度 × chunk_size)，
    chunk_size 由 choose_external_sort_chunk_size 保证 ≤ 5M，可控。
    config.external_sort_pipeline_depth 显式设置时强制覆盖。
    """
    if config.external_sort_pipeline_depth is not None:
        return max(1, config.external_sort_pipeline_depth)
    return max(2, 2 * (max_workers or default_workers()))


def _streaming_file_iter(f: IO[bytes]) -> Generator[T, None, None]:
    """streaming 归并模式的文件迭代器（自描述帧格式，方案③）。

    文件用 dump_frames / dump_sorted_frames 写入。每帧读回一批再逐条 yield：
    - 数值帧零 pickle（int64/float64 原生字节），非数值帧回退 pickle
    - 对外仍是逐条产出，heapq.merge 零改动
    内存：O(batch_size) per file（32 块 × 4096 × 100B ≈ 13MB，
    仍远小于 block 模式，不破坏 streaming 省内存语义）。
    """
    try:
        yield from frames_iter(f)
    except GeneratorExit:
        # 调用方 close() 生成器时也要让上层 finally 关闭文件
        raise


def _block_file_iter(f: IO[bytes]) -> Generator[T, None, None]:
    """整块 pickle.load 模式的文件迭代器（向后兼容用）。

    要求文件是用"整块 pickle.dump(list)"写入的（一次 dump 整个 list）。
    一次 pickle.load 读出整块 list，然后 yield from 展开。
    内存：O(整块大小) per file；所有文件同时存在时，总内存 ≈ 数据总量。
    """
    import pickle

    chunk_list = pickle.load(f)
    yield from chunk_list


def _dump_streaming(chunk: list[T], f: IO[bytes], batch_size: int = 4096) -> None:
    """按自描述帧格式 dump 一个 chunk 到文件 f（方案③）。

    数值批自动走 int64/float64 二进制帧（零 pickle），非数值批回退 pickle 帧。
    本函数用于"已由进程池排好序"的块。
    """
    dump_frames(chunk, f, batch_size)


def _dump_block(chunk: list[T], f: IO[bytes]) -> None:
    """整块 pickle.dump 一个 chunk 到文件 f（向后兼容模式）。"""
    import pickle

    pickle.dump(chunk, f)


def external_sort(
    data: Iterable[T],
    total_count: int,
    *,
    config: BigArrayConfig | None = None,
    max_workers: int | None = None,
    key: Callable[[T], Any] | None = None,
    reverse: bool = False,
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
        reverse:      True 降序，False 升序（默认）。

    返回：生成器，按 key 的非降序（或非升序 if reverse）逐条产出已排序元素。

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
    batch_size = cfg.pickle_batch_size
    depth = _resolve_pipeline_depth(max_workers, cfg)
    chunk_size = choose_external_sort_chunk_size(
        total_count,
        target=cfg.external_sort_chunk_target,
        min_chunk=cfg.external_sort_min_chunk,
        max_chunk=cfg.external_sort_max_chunk,
    )
    if streaming:
        batch_size_arg = batch_size

        def _dump(chunk: list[T], f: IO[bytes]) -> None:
            _dump_streaming(chunk, f, batch_size_arg)

        _file_iter_cls: Callable[[IO[bytes]], Generator[T, None, None]] = _streaming_file_iter
    else:
        _dump = _dump_block
        _file_iter_cls = _block_file_iter

    temp_paths: list[str] = []
    numeric_merge_candidate = NUMERIC_MERGE_AVAILABLE and streaming and key is None
    numeric_merge_kind: int | None = None

    def _new_temp() -> IO[bytes]:
        return tempfile.NamedTemporaryFile(
            suffix=".tmp",
            prefix="big_sort_",
            delete=False,
            buffering=IO_BUFFER_SIZE,
        )

    def _discard(tmp: IO[bytes]) -> None:
        tmp.close()
        try:
            os.unlink(tmp.name)
        except OSError:
            pass

    def _dump_chunk(sorted_chunk: list) -> None:
        tmp = _new_temp()
        try:
            _dump(sorted_chunk, tmp)
            tmp.close()
        except Exception:
            _discard(tmp)
            raise
        temp_paths.append(tmp.name)

    def _try_dump_numeric(chunk: list) -> bool:
        """阶段 1 进程内化（方案③）：纯数值块直接 C 排序 + 二进制帧落盘。

        命中返回 True；非纯数值（或 key 非 None / block 模式）返回 False，
        调用方回退进程池。数值块不落进程池，等价于省掉整块的 pickle +
        IPC 往返 + worker 启动，实测 1M int 单线程 introsort ≈ 21ms。
        """
        nonlocal numeric_merge_candidate, numeric_merge_kind
        if not streaming or key is not None:
            return False
        tmp = _new_temp()
        try:
            kind = dump_sorted_frames_kind(chunk, tmp, batch_size=batch_size, reverse=reverse)
            if kind < 0:
                numeric_merge_candidate = False
                _discard(tmp)
                return False
            if numeric_merge_kind is None:
                numeric_merge_kind = kind
            elif numeric_merge_kind != kind:
                numeric_merge_candidate = False
            tmp.close()
        except Exception:
            _discard(tmp)
            raise
        temp_paths.append(tmp.name)
        return True

    try:
        # 阶段 1：流水线排序 + 落盘
        # 深度由 _resolve_pipeline_depth 决定（默认 2×workers，P0-3）：
        # submit 一块后立即继续读下一块；飞行队列满（depth 块）时
        # 才 .result() 落盘最老的一块。排序 N 与 "读 N+1 + 落 N-1" 并行。
        # 内存：飞行队列最多 depth 个 future；每个 future 的 chunk 在 worker
        # pickled 后即可释放，主进程峰值 ≈ depth*chunk_size，深度有 config 封顶。
        # 纯数值块走进程内 C 排序（_try_dump_numeric），此时 pool 懒创建——
        # 全数值流不会启动任何子进程（Windows spawn 冷启动 1~2s 也一并省掉）。
        in_flight: deque[Future[list[T]]] = deque()
        pool: ProcessPoolExecutor | None = None

        def _submit(chunk: list) -> None:
            nonlocal pool
            if pool is None:
                pool = get_pool(max_workers)
            in_flight.append(pool.submit(sort_chunk, (chunk, key, reverse)))
            if len(in_flight) >= depth:
                _dump_chunk(in_flight.popleft().result())

        chunk: list = []
        try:
            for item in data:
                chunk.append(item)
                if len(chunk) >= chunk_size:
                    if not _try_dump_numeric(chunk):
                        _submit(chunk)
                    chunk = []
            if chunk and not _try_dump_numeric(chunk):  # 末尾不足一块的也要排
                _submit(chunk)
            # 排空飞行队列
            while in_flight:
                _dump_chunk(in_flight.popleft().result())
        except BaseException:
            # 异常时取消未启动的 future（已启动的自然结束；共享池不 shutdown）
            for fut in in_flight:
                if not fut.running():
                    fut.cancel()
            raise

        # 同类型纯数值块在 C 层归并；其他数据维持通用 heapq.merge。
        files: list[IO[bytes]] = []
        try:
            files.extend(open(path, "rb", buffering=IO_BUFFER_SIZE) for path in temp_paths)
            if numeric_merge_candidate and numeric_merge_kind is not None:
                assert merge_numeric_files is not None
                yield from merge_numeric_files(files, numeric_merge_kind, reverse)
            else:
                file_iters: list[Generator[T, None, None]] = [_file_iter_cls(f) for f in files]
                if reverse:
                    wrapped = [_wrap_reverse(it, key) for it in file_iters]
                    yield from _unwrap_reverse(heapq.merge(*wrapped))
                else:
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
