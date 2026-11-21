"""内部临时文件的批量 pickle 帧格式（P0-1）。

逐条 pickle.dump/load 的开销是批量模式（pickle.dump(list)）的 13~37 倍，
且产生大量小 IO 调用。本模块统一提供批量帧读写：

    写入：每 pickle_batch_size 条元素打包成一个 list，一次 pickle.dump
    读取：每次 pickle.load 读出一批，再逐条 yield——对外仍是逐条生成器，
          上层（heapq.merge / BucketIndex.find 等）零改动。

这是内部临时文件格式，不是公开 API，可自由演化。
"""

from __future__ import annotations

import pickle
from collections.abc import Iterable, Iterator
from typing import IO, Any, TypeVar

from .._fast import (
    FRAME_IO_AVAILABLE,
    decode_float64,
    decode_int64,
    encode_float64,
    encode_int64,
    try_build_float64,
    try_build_int64,
)

# 文件读写缓冲：NamedTemporaryFile 默认缓冲偏小，8MB 显著减少 syscall
IO_BUFFER_SIZE = 8 * 1024 * 1024

T = TypeVar("T")


def dump_batches(items: Iterable[T], f: IO[bytes], batch_size: int = 4096) -> None:
    """把 items 按 batch_size 分批 pickle.dump 到 f（批量帧格式）。"""
    buf: list[T] = []
    append = buf.append
    dump = pickle.dump
    for x in items:
        append(x)
        if len(buf) >= batch_size:
            dump(buf, f, protocol=pickle.HIGHEST_PROTOCOL)
            buf = []
            append = buf.append
    if buf:
        dump(buf, f, protocol=pickle.HIGHEST_PROTOCOL)


def batched_file_iter(f: IO[bytes]) -> Iterator[Any]:
    """批量帧格式文件迭代器：每次 load 一批，逐条 yield。

    对外是逐条产出的生成器，heapq.merge 等上层接口零改动。
    内存 = O(batch_size)。帧内元素为反序列化原值（Any），
    类型由调用方按写入格式约定（如 (i, key, item) 三元组）。
    """
    load = pickle.load
    while True:
        try:
            batch = load(f)
        except EOFError:
            return
        yield from batch


def item_file_iter(f: IO[bytes]) -> Iterator[T]:
    """逐条 pickle 格式文件迭代器（兼容旧格式 / 整块模式）。"""
    load = pickle.load
    while True:
        try:
            yield load(f)
        except EOFError:
            return


# ==================== 自描述二进制帧（方案②：数值流零 pickle）====================
# 帧布局：tag(1B) + body
#   0x00 int64   : count(4B LE) + count*8B 载荷（原生字节序）
#   0x01 float64 : count(4B LE) + count*8B 载荷（原生字节序）
#   0x02 pickle  : pickle.dump(list)（非数值批的兜底帧）
# 标签自带类型，故同一文件可任意混排——数值流全程零 pickle；混入非数值元素
# 只让"那一批"退回 pickle，前后数值批不受影响。这是内部临时文件格式，可自由演化。

_FRAME_TAG_INT64 = b"\x00"
_FRAME_TAG_FLOAT64 = b"\x01"
_FRAME_TAG_PICKLE = b"\x02"


def _write_count(f: IO[bytes], n: int) -> None:
    f.write(bytes((n & 0xFF, (n >> 8) & 0xFF, (n >> 16) & 0xFF, (n >> 24) & 0xFF)))


def dump_frame(f: IO[bytes], items: list[Any]) -> None:
    """写一帧：整批全 int → int64 帧；全 float → float64 帧；否则 pickle 帧。

    编码器保证"整批可编码才写出"（否则返回 None），不会留下半截二进制帧。
    """
    n = len(items)
    if n and FRAME_IO_AVAILABLE:
        payload = encode_int64(items)
        if payload is not None:
            f.write(_FRAME_TAG_INT64)
            _write_count(f, n)
            f.write(payload)
            return
        payload = encode_float64(items)
        if payload is not None:
            f.write(_FRAME_TAG_FLOAT64)
            _write_count(f, n)
            f.write(payload)
            return
    f.write(_FRAME_TAG_PICKLE)
    pickle.dump(items, f, protocol=pickle.HIGHEST_PROTOCOL)


def dump_frames(items: Iterable[T], f: IO[bytes], batch_size: int = 4096) -> None:
    """按 batch_size 分批 dump_frame 到 f（数值批自动走二进制帧）。"""
    buf: list[T] = []
    append = buf.append
    for x in items:
        append(x)
        if len(buf) >= batch_size:
            dump_frame(f, buf)
            buf = []
            append = buf.append
    if buf:
        dump_frame(f, buf)


def _write_raw_frame(f: IO[bytes], tag: bytes, raw: bytes, batch_size: int) -> None:
    """把一段原生数值载荷按 batch_size 个元素拆成若干帧写出。

    拆分是为了让读侧（frames_iter）仍能逐批惰性加载——整块写一帧会让
    阶段 2 一次性物化整块，违背 streaming 的低内存语义。
    """
    width = 8
    step = max(1, batch_size) * width
    view = memoryview(raw)
    for off in range(0, len(raw), step):
        seg = view[off : off + step]
        f.write(tag)
        _write_count(f, len(seg) // width)
        f.write(seg)


def dump_sorted_frames_kind(
    items: list[Any],
    f: IO[bytes],
    *,
    batch_size: int = 4096,
    reverse: bool = False,
) -> int:
    """C 排序并写数值帧，返回 0=int64、1=float64、-1=非纯数值。"""
    if not items or not FRAME_IO_AVAILABLE:
        return -1
    arr = try_build_int64(items)
    tag = _FRAME_TAG_INT64
    kind = 0
    if arr is None:
        arr = try_build_float64(items)
        tag = _FRAME_TAG_FLOAT64
        kind = 1
    if arr is None:
        return -1
    arr.py_sort(reverse)
    _write_raw_frame(f, tag, arr.py_to_bytes(), batch_size)
    return kind


def dump_sorted_frames(
    items: list[Any],
    f: IO[bytes],
    *,
    batch_size: int = 4096,
    reverse: bool = False,
) -> bool:
    """整批纯数值走 C 排序和二进制帧；非纯数值返回 False。"""
    return dump_sorted_frames_kind(items, f, batch_size=batch_size, reverse=reverse) >= 0


def frames_iter(f: IO[bytes]) -> Iterator[Any]:
    """自描述帧迭代器：逐帧读回并逐条 yield，对外仍是逐条生成器。

    内存 = O(batch_size)（每帧物化成一个 list 后立即转发）。
    """
    read = f.read
    while True:
        tag = read(1)
        if not tag:
            return
        if tag == _FRAME_TAG_INT64:
            n = int.from_bytes(read(4), "little")
            raw = read(n * 8)
            if len(raw) != n * 8:
                raise EOFError("int64 帧被截断")
            yield from decode_int64(raw)
        elif tag == _FRAME_TAG_FLOAT64:
            n = int.from_bytes(read(4), "little")
            raw = read(n * 8)
            if len(raw) != n * 8:
                raise EOFError("float64 帧被截断")
            yield from decode_float64(raw)
        elif tag == _FRAME_TAG_PICKLE:
            yield from pickle.load(f)
        else:
            raise ValueError(f"未知帧标签: {tag!r}")


__all__ = [
    "IO_BUFFER_SIZE",
    "batched_file_iter",
    "dump_batches",
    "dump_frame",
    "dump_frames",
    "dump_sorted_frames",
    "dump_sorted_frames_kind",
    "frames_iter",
    "item_file_iter",
]
