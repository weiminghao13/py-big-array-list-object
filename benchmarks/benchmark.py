"""BigArrayList 性能基准：完整消费、冷/热调用、Python 与进程内存。

用法：
    python benchmarks/benchmark.py             # 1k/10k/100k 常规场景
    python benchmarks/benchmark.py --large     # 加测 1M
    python benchmarks/benchmark.py --huge      # 加测 10M
    python benchmarks/benchmark.py --hotspots  # 优化热点专项（含 1M 场景）
    python benchmarks/benchmark.py --hotspots --csv

计时不启用 tracemalloc，Python 峰值来自独立 tracemalloc 复测；RSS 同时覆盖
Cython malloc。进程峰值 RSS 是操作系统提供的生命周期高水位，不能解释为
某一次调用的净增量。
"""

from __future__ import annotations

import argparse
import csv
import gc
import math
import os
import sys
import time
import tracemalloc
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from bigarraylist import BigArrayConfig, BigArrayList, build_buckets


@dataclass
class Measurement:
    result: Any
    elapsed: float
    python_peak_mb: float
    rss_before_mb: float
    rss_after_mb: float
    process_peak_mb: float

    @property
    def rss_delta_mb(self) -> float:
        return self.rss_after_mb - self.rss_before_mb


def gen_data(n: int, seed: int = 42) -> list[int]:
    import random

    rng = random.Random(seed)
    return [rng.randint(0, n) for _ in range(n)]


def _windows_memory() -> tuple[float, float]:
    import ctypes
    from ctypes import wintypes

    class ProcessMemoryCounters(ctypes.Structure):
        _fields_ = [
            ("cb", wintypes.DWORD),
            ("PageFaultCount", wintypes.DWORD),
            ("PeakWorkingSetSize", ctypes.c_size_t),
            ("WorkingSetSize", ctypes.c_size_t),
            ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
            ("QuotaPagedPoolUsage", ctypes.c_size_t),
            ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
            ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
            ("PagefileUsage", ctypes.c_size_t),
            ("PeakPagefileUsage", ctypes.c_size_t),
        ]

    counters = ProcessMemoryCounters()
    counters.cb = ctypes.sizeof(counters)
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    psapi = ctypes.WinDLL("psapi", use_last_error=True)
    kernel32.GetCurrentProcess.restype = wintypes.HANDLE
    psapi.GetProcessMemoryInfo.argtypes = [
        wintypes.HANDLE,
        ctypes.POINTER(ProcessMemoryCounters),
        wintypes.DWORD,
    ]
    psapi.GetProcessMemoryInfo.restype = wintypes.BOOL
    process = kernel32.GetCurrentProcess()
    if not psapi.GetProcessMemoryInfo(process, ctypes.byref(counters), counters.cb):
        return math.nan, math.nan
    scale = 1024 * 1024
    return counters.WorkingSetSize / scale, counters.PeakWorkingSetSize / scale


def _process_memory() -> tuple[float, float]:
    if os.name == "nt":
        return _windows_memory()
    try:
        import resource

        pages = int(Path("/proc/self/statm").read_text().split()[1])
        current = pages * os.sysconf("SC_PAGE_SIZE") / (1024 * 1024)
        peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
        return current, peak
    except (OSError, ValueError, ImportError):
        return math.nan, math.nan


def measure(func: Callable[[], Any], *, trace_python: bool = False) -> Measurement:
    """完整调用计时；Python 内存跟踪独立开关，避免拖慢计时。"""
    gc.collect()
    rss_before, _ = _process_memory()
    if trace_python:
        tracemalloc.start()
    started = time.perf_counter()
    result = func()
    elapsed = time.perf_counter() - started
    python_peak_mb = math.nan
    if trace_python:
        _, python_peak = tracemalloc.get_traced_memory()
        python_peak_mb = python_peak / (1024 * 1024)
        tracemalloc.stop()
    rss_after, process_peak = _process_memory()
    return Measurement(
        result=result,
        elapsed=elapsed,
        python_peak_mb=python_peak_mb,
        rss_before_mb=rss_before,
        rss_after_mb=rss_after,
        process_peak_mb=process_peak,
    )


def measure_profiled(func: Callable[[], Any]) -> Measurement:
    """无跟踪计时，再以独立调用采集 Python 峰值；适用于可重复操作。"""
    measured = measure(func)
    profiled = measure(func, trace_python=True)
    profiled.result = None
    measured.python_peak_mb = profiled.python_peak_mb
    measured.process_peak_mb = max(measured.process_peak_mb, profiled.process_peak_mb)
    return measured


def format_num(n: int) -> str:
    return f"{n // 10_000}万" if n >= 10_000 else str(n)


def _row(operation: str, n: int, measurement: Measurement, note: str) -> dict[str, Any]:
    return {
        "操作": operation,
        "数据量": n,
        "耗时(s)": f"{measurement.elapsed:.6f}",
        "Python峰值(MB)": f"{measurement.python_peak_mb:.2f}",
        "RSS增量(MB)": f"{measurement.rss_delta_mb:.2f}",
        "进程峰值RSS(MB)": f"{measurement.process_peak_mb:.2f}",
        "备注": note,
    }


def _print_row(row: dict[str, Any]) -> None:
    print(
        f"  {row['操作']} | {format_num(row['数据量'])} | "
        f"{float(row['耗时(s)']):.6f}s | Python {float(row['Python峰值(MB)']):.1f}MB | "
        f"RSS {float(row['RSS增量(MB)']):+.1f}MB | {row['备注']}"
    )


def _cold_warm_rows(
    operation: str,
    n: int,
    arr: BigArrayList,
    call: Callable[[], Any],
    note: str,
) -> list[dict[str, Any]]:
    cold = measure_profiled(call)
    hits = len(cold.result) if hasattr(cold.result, "__len__") else "标量"
    cold.result = None
    warm = measure_profiled(call)
    warm.result = None
    return [
        _row(f"{operation} cold", n, cold, f"{note}；含首次数值转换；结果={hits}"),
        _row(f"{operation} warm", n, warm, f"{note}；复用数值缓存；结果={hits}"),
    ]


def _bucket_rows(data: list[int], target: int, cfg: BigArrayConfig) -> list[dict[str, Any]]:
    built = measure(lambda: build_buckets(data, len(data), config=cfg))
    profiled = measure(
        lambda: build_buckets(data, len(data), config=cfg), trace_python=True
    )
    profiled.result.close()
    built.python_peak_mb = profiled.python_peak_mb
    built.process_peak_mb = max(built.process_peak_mb, profiled.process_peak_mb)
    idx = built.result
    queried = measure_profiled(lambda: list(idx.find(target)))
    hits = len(queried.result)
    queried.result = None
    idx.close()
    built.result = None
    return [
        _row("build_buckets", len(data), built, f"{idx.num_buckets} 桶；建桶阶段"),
        _row("bucket query", len(data), queried, f"单桶查询；{hits} 命中"),
    ]


def run_benchmarks(sizes: list[int]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for n in sizes:
        print(f"\n{'=' * 76}\n数据量: {n} ({format_num(n)})\n{'=' * 76}")
        data = gen_data(n)
        target = data[0]
        cfg = BigArrayConfig()

        arr = BigArrayList(data, config=cfg)
        sorted_result = measure_profiled(lambda: list(arr.sort()))
        cache = arr._get_fast_cache()
        if cache is not None and cache[2] is not None:
            branch = "数值内存 C introsort"
        elif n < cfg.external_sort_threshold and not (
            cfg.memory_parallel_sort_enabled and n >= cfg.linear_parallel_threshold
        ):
            branch = "内存 sorted()"
        elif n >= cfg.external_sort_threshold:
            branch = "外部排序"
        else:
            branch = "内存并行分块"
        sorted_result.result = None
        row = _row("sort", n, sorted_result, branch)
        rows.append(row)
        _print_row(row)

        if n <= 1_000_000:
            keyed = measure_profiled(lambda: list(BigArrayList(data, config=cfg).sort(key=abs)))
            keyed.result = None
            row = _row("sort(key=abs)", n, keyed, "通用 key 排序路径")
            rows.append(row)
            _print_row(row)

        baseline = measure_profiled(lambda: sorted(data))
        baseline.result = None
        row = _row("sorted baseline", n, baseline, "Python 内置基线")
        rows.append(row)
        _print_row(row)

        arr = BigArrayList(data, config=cfg)
        new_rows = _cold_warm_rows("find", n, arr, lambda: list(arr.find(target)), "完整 C/线性扫描")
        rows.extend(new_rows)
        for row in new_rows:
            _print_row(row)

        new_rows = _bucket_rows(data, target, cfg)
        rows.extend(new_rows)
        for row in new_rows:
            _print_row(row)
    return rows


def run_hotspots() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []

    def emit(new_rows: list[dict[str, Any]]) -> None:
        rows.extend(new_rows)
        for row in new_rows:
            _print_row(row)

    strings = [f"user-{i % 20_000}" for i in range(100_000)]
    string_cfg = BigArrayConfig(linear_parallel_threshold=50_000)
    arr = BigArrayList(strings, config=string_cfg)
    target = strings[17]
    emit(
        _cold_warm_rows(
            "auto_find strings", len(strings), arr, lambda: list(arr.auto_find(target)), "单次查询不建桶"
        )
    )

    integers = gen_data(1_000_000)
    int_target = integers[123]
    arr = BigArrayList(integers)
    emit(
        _cold_warm_rows(
            "binary_search", len(integers), arr, lambda: list(arr.binary_search(int_target)), "C 排序副本 + bisect"
        )
    )

    arr = BigArrayList(integers)
    emit(_cold_warm_rows("avg", len(integers), arr, arr.avg, "复用 sum()/len()"))

    arr = BigArrayList(integers)
    emit(
        _cold_warm_rows(
            "distinct", len(integers), arr, lambda: list(arr.distinct()), "C 哈希；首次出现顺序"
        )
    )

    arr = BigArrayList(integers)
    emit(_cold_warm_rows("is_sorted", len(integers), arr, arr.is_sorted, "C 单遍检查"))

    emit(_bucket_rows(integers, int_target, BigArrayConfig()))

    disk_cfg = BigArrayConfig()
    spilled = measure(lambda: BigArrayList.from_stream(iter(integers), config=disk_cfg))
    profiled = measure(
        lambda: BigArrayList.from_stream(iter(integers), config=disk_cfg), trace_python=True
    )
    profiled.result._get_source().close()
    spilled.python_peak_mb = profiled.python_peak_mb
    spilled.process_peak_mb = max(spilled.process_peak_mb, profiled.process_peak_mb)
    arr = spilled.result
    spilled.result = None
    row = _row("from_stream phase 1", len(integers), spilled, "初次落盘，不计入下一阶段")
    rows.append(row)
    _print_row(row)
    disk_sorted = measure_profiled(lambda: list(arr.sort()))
    disk_sorted.result = None
    row = _row("disk sort phase 2", len(integers), disk_sorted, "同类型数值帧 C 多路归并")
    rows.append(row)
    _print_row(row)
    combined = Measurement(
        result=None,
        elapsed=spilled.elapsed + disk_sorted.elapsed,
        python_peak_mb=max(spilled.python_peak_mb, disk_sorted.python_peak_mb),
        rss_before_mb=spilled.rss_before_mb,
        rss_after_mb=disk_sorted.rss_after_mb,
        process_peak_mb=max(spilled.process_peak_mb, disk_sorted.process_peak_mb),
    )
    row = _row("disk sort end-to-end", len(integers), combined, "两阶段耗时合计")
    rows.append(row)
    _print_row(row)
    return rows


def write_csv(rows: list[dict[str, Any]]) -> None:
    csv_path = Path(__file__).parent / "benchmark_report.csv"
    fieldnames = [
        "操作",
        "数据量",
        "耗时(s)",
        "Python峰值(MB)",
        "RSS增量(MB)",
        "进程峰值RSS(MB)",
        "备注",
    ]
    with open(csv_path, "w", newline="", encoding="utf-8-sig") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"\nCSV 报告已写入: {csv_path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="BigArrayList 性能基准测试")
    parser.add_argument("--large", action="store_true", help="常规场景加测 100 万")
    parser.add_argument("--huge", action="store_true", help="常规场景加测 1000 万")
    parser.add_argument("--hotspots", action="store_true", help="运行优化热点专项")
    parser.add_argument("--csv", action="store_true", help="输出 CSV 报告")
    args = parser.parse_args()

    print("BigArrayList 性能基准测试")
    print(f"Python: {sys.version.split()[0]}    OS: {os.name}")
    if args.hotspots:
        rows = run_hotspots()
    else:
        sizes = [1_000, 10_000, 100_000]
        if args.large:
            sizes.append(1_000_000)
        if args.huge:
            sizes.append(10_000_000)
        print(f"数据量: {', '.join(format_num(size) for size in sizes)}")
        rows = run_benchmarks(sizes)
    if args.csv:
        write_csv(rows)


if __name__ == "__main__":
    main()
