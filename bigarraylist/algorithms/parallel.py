"""模块级可 pickle 函数：供 ProcessPoolExecutor 子进程调用。

⚠️ 这些函数必须放在模块级（不能是嵌套函数/lambda），否则子进程
pickle 时找不到定义。本文件按职责拆分原 core.py 顶部的四个函数。

子进程通过 `from bigarraylist.algorithms.parallel import _sort_chunk` 路径找到定义。
"""

from __future__ import annotations

import bisect as _bisect_mod
from typing import Any, TypeVar

T = TypeVar("T")


def linear_find_in_chunk(args: tuple[list[T], T, int]) -> list[tuple[int, T]]:
    """在单个 chunk 内查找所有等于 target 的元素，返回 (全局下标, 元素) 列表。"""
    chunk, target, start_index = args
    return [(i, item) for i, item in enumerate(chunk, start=start_index) if item == target]


def sort_chunk(args: tuple[Any, ...]) -> list[T]:
    """对单个 chunk 排序，返回新列表（不修改原 chunk）。

    args 形如 ``(chunk, key)``；``key=None`` 表示按元素本身排序。
    向后兼容：也接受旧式 ``(chunk,)`` 一元组（等价于 ``key=None``）。
    """
    if len(args) == 2:
        chunk, key = args
    else:
        # 旧式 (chunk,) 调用（_sort_chunk 别名的遗留调用方）
        (chunk,) = args
        key = None
    return sorted(chunk, key=key)


def chunk_is_sorted(args: tuple[list[T]]) -> bool:
    """判断单个 chunk 是否升序（模块级，供子进程 pickle 调用）。"""
    (chunk,) = args
    return all(chunk[i] <= chunk[i + 1] for i in range(len(chunk) - 1))  # type: ignore[operator]


def bisect_find_in_chunk(args: tuple[list[T], T, int]) -> list[tuple[int, T]]:
    """在**已排序**的单个 chunk 内用 bisect 找所有等于 target 的元素。

    返回 (全局下标, 元素) 列表。
    """
    chunk, target, start_index = args
    left = _bisect_mod.bisect_left(chunk, target)  # type: ignore[call-overload]
    right = _bisect_mod.bisect_right(chunk, target)  # type: ignore[call-overload]
    return [(start_index + i, chunk[i]) for i in range(left, right)]


# ==================== 向后兼容别名 ====================
# 重构前这些函数在 bigarraylist.core 模块下，名字带下划线前缀。
# 现有测试 / 外部代码若直接 import，保留别名避免破坏。
_linear_find_in_chunk: Any = linear_find_in_chunk
_sort_chunk: Any = sort_chunk
_chunk_is_sorted: Any = chunk_is_sorted
_bisect_find_in_chunk: Any = bisect_find_in_chunk

__all__ = [
    "bisect_find_in_chunk",
    "chunk_is_sorted",
    "linear_find_in_chunk",
    "sort_chunk",
]
