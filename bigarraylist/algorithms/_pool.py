"""模块级懒加载共享进程池（P1-2）。

每次 sort/find/is_sorted/external_sort 都新建 ProcessPoolExecutor 的话，
Windows spawn 模式下每个 worker 要重启解释器 + import 包，8 worker 池
冷启动 1~2s，且每次调用都付一遍。本模块提供进程级共享的懒加载池。

约束（AGENTS.md 3.4 不变）：worker 函数仍必须模块级可 pickle。
早停语义不变：调用方 GeneratorExit 时仍取消未启动的 future，
只是池不再 shutdown。
"""

from __future__ import annotations

import atexit
import os
from concurrent.futures import ProcessPoolExecutor

_pool: ProcessPoolExecutor | None = None


def get_pool(max_workers: int | None = None) -> ProcessPoolExecutor:
    """返回共享进程池；首次调用时创建，之后复用。

    max_workers 仅在首次创建时生效；后续调用传入不同值也不会重建
    （重建就失去共享意义）。传 None 用系统默认（os.cpu_count()）。
    """
    global _pool
    if _pool is None:
        _pool = ProcessPoolExecutor(max_workers=max_workers)
    return _pool


def shutdown_pool(wait: bool = False) -> None:
    """显式关闭共享池（atexit 兜底 + 测试清理用）。幂等。"""
    global _pool
    if _pool is not None:
        _pool.shutdown(wait=wait)
        _pool = None


def default_workers() -> int:
    """默认 worker 数：cpu_count，拿不到用 4。"""
    return os.cpu_count() or 4


atexit.register(shutdown_pool)

__all__ = ["default_workers", "get_pool", "shutdown_pool"]
