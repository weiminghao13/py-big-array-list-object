"""蓄水池抽样（Reservoir Sampling）：从总量未知的流里等概率抽 k 个。

数学保证：每个元素被选中的概率都是 k/N（N 为实际总量，无需预先知道）。
内存 O(k)，无需预知 N。大数据分析里做随机采样的必备算法。
"""

from __future__ import annotations

import random
from collections.abc import Generator, Iterable
from typing import TypeVar

T = TypeVar("T")


def reservoir_sample(
    stream: Iterable[T],
    k: int,
    seed: int | None = None,
) -> Generator[T, None, None]:
    """蓄水池抽样算法 R。

    参数：
        stream: 任意可迭代对象（不要求预知长度）
        k:      要抽取的样本数（必须 >= 1，调用方校验）
        seed:   随机种子，None 表示系统随机；传 int 可复现

    返回：生成器，产出 k 个元素（若流总量 < k，产出全部元素）

    算法：
        - 先存前 k 个到 reservoir
        - 第 i 个（i >= k，从 0 计数即 i+1 个元素）以 k/(i+1) 概率
          随机替换池中某一个
        - 一遍流式扫描结束，池中 k 个即为等概率样本
    """
    rng = random.Random(seed)
    reservoir: list[T] = []
    for i, item in enumerate(stream):
        if i < k:
            reservoir.append(item)
        else:
            # 第 i+1 个元素（索引 i >= k），以 k/(i+1) 概率替换池中随机一个
            # randint(0, i) 返回 [0, i] 闭区间，共 i+1 个值
            # 落在 [0, k-1] 的概率 = k/(i+1)，正是我们要的
            j = rng.randint(0, i)
            if j < k:
                reservoir[j] = item
    yield from reservoir


__all__ = ["reservoir_sample"]
