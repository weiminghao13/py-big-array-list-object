"""scale.py（数据量级助手 / 分块策略）的单元测试。"""

import pytest

from bigarraylist import (
    ScaleHelper,
    choose_chunk_size,
    choose_external_sort_chunk_size,
    chunk_wise_processing,
)

# ==================== 内存排序分块 ====================


def test_choose_chunk_size_in_100_to_1000():
    """choose_chunk_size 输出始终在 [100, 1000] 区间内。
    注意：ScaleHelper.get_scale 只接受 >= 100_000，<100k 抛 ValueError。"""
    for n in [
        100_000,
        250_000,
        1_000_000,
        5_000_000,
        10_000_000,
        50_000_000,
        100_000_000,
    ]:
        cs = choose_chunk_size(n)
        assert 100 <= cs <= 1000, f"choose_chunk_size({n})={cs} 越界"
    # 低于阈值：必须抛 ValueError（ScaleHelper 的契约）
    with pytest.raises(ValueError):
        choose_chunk_size(99_999)
    with pytest.raises(ValueError):
        choose_chunk_size(0)


def test_chunk_wise_processing_splits_correctly():
    """chunk_wise_processing 总数对、顺序保、单块 <= 1000。
    契约说明：函数首行执行 len(iterable)，所以 iterable 必须是 Sized
    （list/tuple 等）。generator 传进来会 TypeError——已知契约，不是 bug。"""
    N = 250_000
    # 1) Sized（list）输入走正常分支
    chunks = list(chunk_wise_processing(list(range(N))))
    assert sum(len(c) for c in chunks) == N
    # 2) 非 Sized（generator）传进来 → 必须 TypeError（和 docstring 一致）
    gen = (x for x in range(N))
    with pytest.raises(TypeError):
        list(chunk_wise_processing(gen))
    # 3) 元素顺序正确，且每块不超 1000（产品上限）
    flat = []
    for c in chunks:
        assert len(c) <= 1000
        flat.extend(c)
    assert flat == list(range(N))


# ==================== 外部排序分块 ====================


def test_choose_external_sort_chunk_size_targets_32_chunks():
    """外部排序分块：目标 32 路归并，每块 [100k, 5M]。

    与内存排序用的 choose_chunk_size（100~1000 条/块）完全不同：
    外部排序每块大得多，目标是把块数控制在 32 左右，减少调度与归并堆开销。
    """
    # 1M 数据 → 1M//32 ≈ 31k → 被 min 拉到 100k → 10 块
    assert choose_external_sort_chunk_size(1_000_000) == 100_000
    # 10M → 10M//32 = 312500 → 32 块
    assert choose_external_sort_chunk_size(10_000_000) == 312_500
    # 5 亿 → 5M 上限 → 100 块
    assert choose_external_sort_chunk_size(500_000_000) == 5_000_000
    # 每块始终在 [100k, 5M]
    for n in [100_000, 1_000_000, 10_000_000, 100_000_000, 500_000_000]:
        cs = choose_external_sort_chunk_size(n)
        assert 100_000 <= cs <= 5_000_000


# ==================== ScaleHelper ====================


def test_scale_helper_get_scale_and_str():
    """ScaleHelper.get_scale 返回正确档位，__str__ 正确。"""
    assert ScaleHelper.get_scale(100_000).value == 100_000
    assert ScaleHelper.get_scale(1_000_000).value == 1_000_000
    assert ScaleHelper.get_scale(10_000_000).value == 10_000_000
    assert ScaleHelper.get_scale(100_000_000).value == 100_000_000
    # < 100k 抛 ValueError
    with pytest.raises(ValueError):
        ScaleHelper.get_scale(99_999)
    # __str__ 含档位名
    assert "HUNDRED_MILLION" in str(ScaleHelper(100_000_000))
    assert "MILLION" in str(ScaleHelper(1_000_000))
