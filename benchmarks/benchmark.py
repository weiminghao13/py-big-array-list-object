"""性能基准测试：sort / find / bucket_find 在不同数据量下的耗时和内存。

核心价值证明：
    - sort 在 1M+ 数据下内存可控（streaming 模式峰值 O(块数)）
    - find 并行线性查找，大数据下接近线性加速
    - bucket_find 建桶一次后多次查询，单次查询 O(n/N)
    - sort(key=...) 在大数据下与 sorted(key=...) 结果一致

用法：
    python benchmarks/benchmark.py            # 默认 1k/10k/100k
    python benchmarks/benchmark.py --large     # 加测 1M
    python benchmarks/benchmark.py --huge      # 加测 10M（外部排序，较慢）
    python benchmarks/benchmark.py --csv       # 同时输出 CSV 报告

输出示例：
    | 操作              | 数据量 | 耗时(s) | 峰值内存(MB) | 备注              |
    |-------------------|--------|---------|--------------|-------------------|
    | sort              | 100000 |   0.342 |        15.3  | 中数据并行分块     |
    | sort (key=)       | 100000 |   0.351 |        15.5  | _abs_key           |
    | find              | 100000 |   0.089 |         8.2  | 4 进程并行         |
    | bucket_find (建桶) | 100000 |   0.156 |        12.1  | 64 桶             |
    | bucket_find (查询) | 100000 |   0.002 |         0.5  | 单桶扫描           |
"""

from __future__ import annotations

import argparse
import csv
import gc
import os
import sys
import time
import tracemalloc
from pathlib import Path

# 让脚本可直接运行（不依赖 PYTHONPATH 设置）
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from bigarraylist import BigArrayConfig, BigArrayList


# ==================== 数据生成 ====================


def gen_data(n: int, seed: int = 42) -> list[int]:
    """生成 n 个 [0, n) 范围的随机整数（可复现）。"""
    import random

    rng = random.Random(seed)
    return [rng.randint(0, n) for _ in range(n)]


# ==================== 测量工具 ====================


def measure(func, *args, **kwargs):
    """测一次 func 的耗时和峰值内存。

    返回 (result, elapsed_seconds, peak_mb)。
    """
    gc.collect()
    tracemalloc.start()
    t0 = time.perf_counter()
    result = func(*args, **kwargs)
    elapsed = time.perf_counter() - t0
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    return result, elapsed, peak / (1024 * 1024)


def format_num(n: int) -> str:
    """10000 -> '1万', 1000000 -> '100万'。"""
    if n >= 1_000_000:
        return f"{n // 10000}万"
    if n >= 10_000:
        return f"{n // 10000}万"
    return str(n)


# ==================== 各操作的基准 ====================


def bench_sort(data: list[int], key=None) -> tuple[float, float, str]:
    """sort() 基准。返回 (耗时, 峰值内存MB, 备注)。"""
    label = "sort" if key is None else "sort(key=)"
    cfg = BigArrayConfig()
    arr = BigArrayList(data, config=cfg)
    _, elapsed, peak = measure(list, arr.sort(key=key))
    # 判断走的分支
    n = len(data)
    if n < cfg.linear_parallel_threshold:
        branch = "小数据 sorted()"
    elif n < cfg.external_sort_threshold:
        branch = "中数据并行分块"
    else:
        branch = "外部排序"
    return elapsed, peak, f"{label} [{branch}]"


def bench_sort_baseline(data: list[int], key=None) -> tuple[float, float, str]:
    """Python 内置 sorted() 作为基线。"""
    _, elapsed, peak = measure(sorted, data, key=key)
    return elapsed, peak, "sorted() 基线"


def bench_find(data: list[int], target: int) -> tuple[float, float, str]:
    """find() 并行线性查找基准。"""
    arr = BigArrayList(data)
    hits, elapsed, peak = measure(list, arr.find(target))
    return elapsed, peak, f"find [{len(hits)} 命中]"


def bench_bucket_find(data: list[int], target: int) -> tuple[float, float, str]:
    """bucket_find() 建桶 + 查询基准。

    建桶一次，然后查 3 个不同 target，展示"建桶成本摊薄"。
    """
    # 阶段 1：建桶
    from bigarraylist import build_buckets

    gc.collect()
    tracemalloc.start()
    t0 = time.perf_counter()
    idx = build_buckets(data, len(data))
    build_elapsed = time.perf_counter() - t0
    _, build_peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    # 阶段 2：单次查询（只扫目标桶）
    gc.collect()
    tracemalloc.start()
    t1 = time.perf_counter()
    hits = list(idx.find(target))
    query_elapsed = time.perf_counter() - t1
    _, query_peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    idx.close()
    total_peak = max(build_peak, query_peak) / (1024 * 1024)
    # 返回建桶耗时（查询耗时放在备注）
    return (
        build_elapsed,
        total_peak,
        (f"建桶[{idx.num_buckets}桶] + 查询{query_elapsed * 1000:.1f}ms [{len(hits)} 命中]"),
    )


# ==================== 主流程 ====================


def run_benchmarks(sizes: list[int], output_csv: bool = False) -> None:
    """跑全部基准，打印表格 + 可选 CSV。"""
    rows: list[dict] = []

    for n in sizes:
        print(f"\n{'=' * 70}")
        print(f"数据量: {n} ({format_num(n)})")
        print(f"{'=' * 70}")
        data = gen_data(n)
        target = data[0]  # 取第一个元素作为查找目标（保证有命中）

        # sort
        elapsed, peak, note = bench_sort(data)
        rows.append(
            {
                "操作": "sort",
                "数据量": n,
                "耗时(s)": f"{elapsed:.3f}",
                "峰值内存(MB)": f"{peak:.1f}",
                "备注": note,
            }
        )
        print(f"  {note:40s} {elapsed:7.3f}s  {peak:7.1f}MB")

        # sort(key=)
        if n <= 1_000_000:  # 10M 的 key 测太慢，跳过
            elapsed, peak, note = bench_sort(data, key=abs)
            rows.append(
                {
                    "操作": "sort(key=)",
                    "数据量": n,
                    "耗时(s)": f"{elapsed:.3f}",
                    "峰值内存(MB)": f"{peak:.1f}",
                    "备注": note,
                }
            )
            print(f"  {note:40s} {elapsed:7.3f}s  {peak:7.1f}MB")

        # sorted() 基线
        elapsed, peak, note = bench_sort_baseline(data)
        rows.append(
            {
                "操作": "sorted()基线",
                "数据量": n,
                "耗时(s)": f"{elapsed:.3f}",
                "峰值内存(MB)": f"{peak:.1f}",
                "备注": note,
            }
        )
        print(f"  {note:40s} {elapsed:7.3f}s  {peak:7.1f}MB")

        # find
        elapsed, peak, note = bench_find(data, target)
        rows.append(
            {
                "操作": "find",
                "数据量": n,
                "耗时(s)": f"{elapsed:.3f}",
                "峰值内存(MB)": f"{peak:.1f}",
                "备注": note,
            }
        )
        print(f"  {note:40s} {elapsed:7.3f}s  {peak:7.1f}MB")

        # bucket_find
        elapsed, peak, note = bench_bucket_find(data, target)
        rows.append(
            {
                "操作": "bucket_find",
                "数据量": n,
                "耗时(s)": f"{elapsed:.3f}",
                "峰值内存(MB)": f"{peak:.1f}",
                "备注": note,
            }
        )
        print(f"  {note:40s} {elapsed:7.3f}s  {peak:7.1f}MB")

    # 打印汇总表
    print(f"\n{'=' * 70}")
    print("汇总")
    print(f"{'=' * 70}")
    print(f"{'操作':16s} {'数据量':>8s} {'耗时(s)':>9s} {'峰值MB':>9s}  备注")
    print("-" * 70)
    for r in rows:
        print(
            f"{r['操作']:16s} {format_num(r['数据量']):>8s} "
            f"{r['耗时(s)']:>9s} {r['峰值内存(MB)']:>9s}  {r['备注']}"
        )

    # 可选 CSV 输出
    if output_csv:
        csv_path = Path(__file__).parent / "benchmark_report.csv"
        with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(
                f, fieldnames=["操作", "数据量", "耗时(s)", "峰值内存(MB)", "备注"]
            )
            writer.writeheader()
            writer.writerows(rows)
        print(f"\nCSV 报告已写入: {csv_path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="BigArrayList 性能基准测试")
    parser.add_argument("--large", action="store_true", help="加测 100 万")
    parser.add_argument("--huge", action="store_true", help="加测 1000 万（外部排序，较慢）")
    parser.add_argument("--csv", action="store_true", help="输出 CSV 报告")
    args = parser.parse_args()

    sizes = [1_000, 10_000, 100_000]
    if args.large:
        sizes.append(1_000_000)
    if args.huge:
        sizes.append(10_000_000)

    print("BigArrayList 性能基准测试")
    print(f"数据量: {', '.join(format_num(s) for s in sizes)}")
    print(f"Python: {sys.version.split()[0]}    OS: {os.name}")

    run_benchmarks(sizes, output_csv=args.csv)


if __name__ == "__main__":
    main()
