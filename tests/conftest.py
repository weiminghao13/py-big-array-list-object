"""pytest 共享 fixtures 与常量。

fixtures 设计：
    - rng   ：每个测试一份全新的 Random(42)——可复现，且不依赖测试执行顺序
    - small ：小数据集（串行分支，手工验算用）
    - big_n ：大数据规模（find / sort / binary_search 的并行分支在此激活）
"""

from random import Random

import pytest

_BIG_N = 100_000
_SMALL = [3, 1, 4, 1, 5, 9, 2, 6, 5, 3, 5, 1]


@pytest.fixture
def rng() -> Random:
    """每个测试一份全新的 Random(42)：可复现、且不依赖测试执行顺序。"""
    return Random(42)


@pytest.fixture
def small() -> list:
    """小数据集（串行分支，手工验算用）。每次返回独立副本。"""
    return list(_SMALL)


@pytest.fixture
def big_n() -> int:
    """大数据规模（find / sort / binary_search 的并行分支在此激活）。"""
    return _BIG_N
