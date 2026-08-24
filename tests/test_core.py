"""core.py（BaseBigArrayList 惰性流机制）的单元测试。

聚焦底层机制：
    - 构造 & 长度（int/list/generator/无 __len__ 迭代器 四种 source）
    - 惰性迭代 + 标准 StopIteration 协议（耗尽即停，不自动重置）
    - cycle() 提供无限循环语义
    - __getitem__ 不支持随机访问（与惰性流设计冲突）
    - __contains__ / 算子零副作用（用 _fresh_iter，不污染 self.__data）
"""

import itertools

import pytest

from bigarraylist import BigArrayList

# ==================== 构造 & 长度 ====================


def test_construct_and_len():
    """三种构造方式的 len 都对。"""
    assert len(BigArrayList(5)) == 5  # int → 5 个 0
    assert len(BigArrayList([1, 2, 3])) == 3  # list
    assert len(BigArrayList(x for x in range(10))) == 10  # generator（物化拿长度）
    assert len(BigArrayList(iter(range(7)))) == 7  # 纯 iterator（物化分支）


# ==================== 惰性迭代 + StopIteration 协议 ====================


def test_lazy_iter_int_source():
    """int 构造惰性：前 N 个全是 0，耗尽后 StopIteration。"""
    n = 50
    arr = BigArrayList(n)
    assert list(itertools.islice(arr, n)) == [0] * n
    # 耗尽后再取：标准 Python 协议——抛 StopIteration，不再循环重置
    with pytest.raises(StopIteration):
        next(arr)


def test_lazy_iter_list_source():
    """list 构造惰性：按顺序产出，不会被多扫一遍。"""
    arr = BigArrayList([10, 20, 30, 40])
    assert list(itertools.islice(arr, 4)) == [10, 20, 30, 40]


def test_consumed_then_raises_stopiteration():
    """耗尽后 __next__ 抛 StopIteration（标准协议）。
    想要循环语义请用 .cycle()。"""
    ba = BigArrayList([1, 2])
    assert [next(ba), next(ba)] == [1, 2]
    with pytest.raises(StopIteration):
        next(ba)


# ==================== 随机访问不支持 ====================


def test_index_access_raises_not_implemented():
    """大数据不支持 [] 随机访问（与惰性流设计冲突）。"""
    ba = BigArrayList([1, 2, 3])
    with pytest.raises(NotImplementedError):
        _ = ba[0]


# ==================== cycle() 无限循环 ====================


def test_cycle_loops_infinitely():
    """cycle()：数据耗尽后从头再来，永不停止。
    标准协议是耗尽抛 StopIteration，循环语义请显式用 cycle()。"""
    ba = BigArrayList([1, 2, 3])
    # 取 8 个 = 3 + 3 + 2，验证循环重置
    assert list(itertools.islice(ba.cycle(), 8)) == [1, 2, 3, 1, 2, 3, 1, 2]
    # cycle() 不污染实例状态：实例本身迭代仍是标准协议（耗尽 StopIteration）
    ba2 = BigArrayList([10, 20])
    _ = list(itertools.islice(ba2.cycle(), 5))
    # 实例本身 __next__ 走标准协议，list(ba2) 正常结束
    assert list(ba2) == [10, 20]


# ==================== __contains__ 零副作用 ====================


def test_contains_zero_side_effect():
    """x in big_arr 用 _fresh_iter 查找，不消费 self.__data。
    调完 in 之后，for 循环数据仍完整。"""
    ba = BigArrayList([1, 2, 3, 4, 5])
    assert 3 in ba
    assert 999 not in ba
    # in 之后 for 循环数据完整（实例状态未被污染）
    assert list(ba) == [1, 2, 3, 4, 5]
