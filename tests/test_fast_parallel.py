"""Cython 快路径多线程（第三步）正确性测试。

.pyd 未编译（纯 Python 分发）时整文件 skip，行为与重构前一致。
"""

from __future__ import annotations

import random
import types

import pytest

from bigarraylist import BigArrayConfig, BigArrayList
from bigarraylist._fast import FAST_BACKEND_AVAILABLE, Float64Array, Int64Array

pytestmark = pytest.mark.skipif(not FAST_BACKEND_AVAILABLE, reason="Cython .pyd 未编译")


def gen(n: int, seed: int = 42) -> list[int]:
    rng = random.Random(seed)
    return [rng.randint(-(10**6), 10**6) for _ in range(n)]


# ==================== Int64Array 多线程方法 ====================


@pytest.mark.parametrize("threads", [0, 1, 2, 4])
@pytest.mark.parametrize("n", [0, 1, 7, 1_000, 100_003])
def test_sum_parallel_matches_builtin(n: int, threads: int) -> None:
    data = gen(n)
    a = Int64Array.from_list(data)
    assert a.py_sum_parallel(threads) == sum(data)


@pytest.mark.parametrize("threads", [0, 1, 2, 4])
@pytest.mark.parametrize("n", [0, 1, 7, 1_000, 100_003])
def test_min_max_parallel_matches_builtin(n: int, threads: int) -> None:
    data = gen(n)
    a = Int64Array.from_list(data)
    assert a.py_min_parallel(threads) == (min(data) if data else None)
    assert a.py_max_parallel(threads) == (max(data) if data else None)


def test_parallel_matches_serial_large() -> None:
    """大数据量下并行结果必须与单线程 C 版完全一致。"""
    data = gen(1_000_000)
    a = Int64Array.from_list(data)
    assert a.py_sum_parallel(4) == a.py_sum()
    assert a.py_min_parallel(4) == a.py_min()
    assert a.py_max_parallel(4) == a.py_max()


def test_threads_exceeding_chunks() -> None:
    """线程数 > 元素数：空分块被哨兵值吸收，结果仍正确。"""
    data = gen(3)
    a = Int64Array.from_list(data)
    assert a.py_sum_parallel(64) == sum(data)
    assert a.py_min_parallel(64) == min(data)
    assert a.py_max_parallel(64) == max(data)


# ==================== BigArrayList API 集成（阈值分流） ====================


def test_api_parallel_when_threshold_met() -> None:
    data = gen(100_000)
    cfg = BigArrayConfig(fast_parallel_threshold=1, fast_parallel_threads=2)
    arr = BigArrayList(data, config=cfg)
    assert arr.sum() == sum(data)
    assert arr.min() == min(data)
    assert arr.max() == max(data)


def test_api_serial_below_threshold() -> None:
    data = gen(10_000)
    cfg = BigArrayConfig(fast_parallel_threshold=1_000_000)
    arr = BigArrayList(data, config=cfg)
    assert arr.sum() == sum(data)
    assert arr.min() == min(data)
    assert arr.max() == max(data)


# ==================== 查找 / 计数 C 方法（方案二） ====================


class TestInt64ScanMethods:
    """Int64Array.py_contains / py_find_indices / py_count_* 单测。"""

    def test_contains(self) -> None:
        a = Int64Array.from_list([5, 1, 9, 5, 3, 5])
        assert a.py_contains(5) is True
        assert a.py_contains(9) is True
        assert a.py_contains(99) is False
        assert Int64Array.from_list([]).py_contains(1) is False

    def test_find_indices_all_hits_in_order(self) -> None:
        a = Int64Array.from_list([5, 1, 9, 5, 3, 5])
        assert a.py_find_indices(5) == [0, 3, 5]
        assert a.py_find_indices(9) == [2]
        assert a.py_find_indices(99) == []

    def test_count_equal_less_greater(self) -> None:
        a = Int64Array.from_list([5, 1, 9, 5, 3, 5])
        assert a.py_count_equal(5) == 3
        assert a.py_count_less(5) == 2  # 1, 3
        assert a.py_count_greater(5) == 1  # 9

    def test_count_on_large_random(self) -> None:
        data = gen(100_003)
        a = Int64Array.from_list(data)
        target = data[len(data) // 2]
        assert a.py_count_equal(target) == data.count(target)


class TestFloat64ScanMethods:
    def test_contains_and_find(self) -> None:
        a = Float64Array.from_list([1.5, 2.5, 1.5, 3.5])
        assert a.py_contains(1.5) is True
        assert a.py_contains(99.0) is False
        assert a.py_find_indices(1.5) == [0, 2]

    def test_count(self) -> None:
        a = Float64Array.from_list([1.5, 2.5, 1.5, 3.5])
        assert a.py_count_equal(1.5) == 2
        assert a.py_count_less(2.5) == 2  # 1.5, 1.5
        assert a.py_count_greater(2.5) == 1  # 3.5

    def test_nan_semantics_match_python(self) -> None:
        # NaN 与任何值比较都为 False：C 层 ==/< /> 与 Python float 一致
        a = Float64Array.from_list([float("nan"), 1.0, 2.0])
        assert a.py_contains(float("nan")) is False
        assert a.py_count_equal(float("nan")) == 0
        assert a.py_count_less(float("nan")) == 0


# ==================== BigArrayList 入口拦截（方案二集成） ====================


class TestContainsFastPath:
    def test_int_membership(self) -> None:
        arr = BigArrayList([5, 1, 9, 5, 3, 5])
        assert 5 in arr
        assert 9 in arr
        assert 99 not in arr

    def test_float_membership(self) -> None:
        arr = BigArrayList([1.5, 2.5, 3.5])
        assert 2.5 in arr
        assert 9.9 not in arr

    def test_bool_target_falls_back(self) -> None:
        # bool 是 int 子类但语义不同：coerce_target 拒掉，走 Python 路径。
        # Python 里 1 == True、0 == False，结果由 == 决定，不是类型门控决定。
        arr = BigArrayList([1, 0, 1, 0])
        assert (True in arr) is True
        assert (False in arr) is True

    def test_non_numeric_source_still_works(self) -> None:
        # 非数值源（字符串）：fast_numeric 返回 None，回退基类流式 any()
        arr = BigArrayList(["apple", "banana", "cherry"])
        assert "banana" in arr
        assert "durian" not in arr


class TestFindFastPath:
    """钉死 find 契约：返回生成器、产出 (全局下标, 命中元素)、顺序与源一致。"""

    def test_find_returns_all_hits(self) -> None:
        arr = BigArrayList([5, 1, 9, 5, 3, 5])
        assert list(arr.find(5)) == [(0, 5), (3, 5), (5, 5)]

    def test_find_is_generator(self) -> None:
        arr = BigArrayList([1, 2, 3])
        result = arr.find(2)
        assert isinstance(result, types.GeneratorType)
        # 用完即止
        assert list(result) == [(1, 2)]

    def test_find_no_hits_empty_generator(self) -> None:
        arr = BigArrayList([1, 2, 3])
        assert list(arr.find(42)) == []

    def test_find_empty_source(self) -> None:
        arr = BigArrayList([])
        assert list(arr.find(1)) == []

    def test_find_large_numeric_source(self) -> None:
        data = gen(100_000)
        target = data[12345]
        arr = BigArrayList(data)
        expected = [(i, v) for i, v in enumerate(data) if v == target]
        assert list(arr.find(target)) == expected

    def test_auto_find_numeric_bypasses_bucket(self) -> None:
        # 数值源 + auto_find 应走 C 扫描，结果与线性扫描一致
        data = [7, 2, 7, 9, 2, 7]
        arr = BigArrayList(data)
        assert list(arr.auto_find(7)) == [(0, 7), (2, 7), (5, 7)]

    def test_auto_find_below_threshold(self) -> None:
        # 小数据路径（n < linear_parallel_threshold）也应给出正确结果
        data = [3, 1, 4, 1, 5, 9, 2, 6]
        arr = BigArrayList(data)
        assert list(arr.auto_find(1)) == [(1, 1), (3, 1)]


class TestSortNumericBypass:
    """入口分流：数值内存源即使 external_sort_threshold 压到很低，
    也应直接 C introsort，不落盘、结果正确。"""

    def test_sort_bypasses_external_sort_when_threshold_low(self) -> None:
        cfg = BigArrayConfig(external_sort_threshold=10)  # 正常会强制走外部排序
        data = [5, 1, 9, 3, 7, 2, 8, 4, 6, 0]
        arr = BigArrayList(data, config=cfg)
        assert list(arr.sort()) == sorted(data)
        assert list(arr.sort(reverse=True)) == sorted(data, reverse=True)

    def test_sort_large_random(self) -> None:
        data = gen(200_003)
        arr = BigArrayList(data)
        assert list(arr.sort()) == sorted(data)

    def test_sort_with_key_falls_through(self) -> None:
        # key 函数存在时不走快路（C 数组无法调用 Python key），
        # 回退到 _big_iter_sort，结果仍正确
        data = [5, 1, 9, 3, 7]
        arr = BigArrayList(data)
        assert list(arr.sort(key=lambda x: -x)) == sorted(data, key=lambda x: -x)

    def test_sort_float_source(self) -> None:
        arr = BigArrayList([3.1, 1.4, 1.5, 9.2, 6.5])
        assert list(arr.sort()) == sorted([3.1, 1.4, 1.5, 9.2, 6.5])


def test_binary_search_reuses_numeric_sort_fast_path(monkeypatch) -> None:
    def fail_general_sort(self, *args, **kwargs):
        raise AssertionError("数值内存源不应绕过 C 排序快路")

    monkeypatch.setattr(BigArrayList, "_big_iter_sort", fail_general_sort)
    cfg = BigArrayConfig(external_sort_threshold=2)
    arr = BigArrayList([5, 1, 5, 3, 2], config=cfg)
    assert list(arr.binary_search(5)) == [(3, 5), (4, 5)]


def test_numeric_distinct_preserves_first_occurrence_order(monkeypatch) -> None:
    def fail_bucket_distinct(self):
        raise AssertionError("数值内存源不应分桶落盘")

    monkeypatch.setattr(BigArrayList, "_hash_bucket_distinct", fail_bucket_distinct)
    data = [3, 1, 3, 2, 1, 4, 2, 5]
    cfg = BigArrayConfig(external_sort_threshold=2)
    arr = BigArrayList(data, config=cfg)
    assert list(arr.distinct()) == [3, 1, 2, 4, 5]


def test_float_distinct_preserves_source_types_and_zero_sign() -> None:
    arr = BigArrayList([1, 1.0, -0.0, 0, 2.5, 2.5])
    result = list(arr.distinct())
    assert result == [1, -0.0, 2.5]
    assert type(result[0]) is int
    assert type(result[1]) is float


def test_float_distinct_nan_falls_back_to_python_identity_semantics() -> None:
    import math

    shared_nan = float("nan")
    result = list(BigArrayList([shared_nan, shared_nan, float("nan")]).distinct())
    assert len(result) == 2
    assert all(math.isnan(value) for value in result)


def test_numeric_distinct_matches_python_on_random_data() -> None:
    def first_indices(values) -> list[int]:
        indices = []
        seen = set()
        for i, value in enumerate(values):
            if value not in seen:
                seen.add(value)
                indices.append(i)
        return indices

    integers = gen(100_003)
    assert Int64Array.from_list(integers).py_distinct_indices() == first_indices(integers)
    floats = [float(value) for value in gen(10_003)]
    assert Float64Array.from_list(floats).py_distinct_indices() == first_indices(floats)


def test_numeric_is_sorted_uses_single_pass_c_check(monkeypatch) -> None:
    def fail_pool(*args, **kwargs):
        raise AssertionError("数值 is_sorted 不应创建进程池")

    monkeypatch.setattr("bigarraylist.operators.sort.get_pool", fail_pool)
    assert BigArrayList([1, 2, 2, 4]).is_sorted() is True
    assert BigArrayList([1, 3, 2, 4]).is_sorted() is False
    assert BigArrayList([1.0, float("nan"), 0.0]).is_sorted() is True


@pytest.mark.parametrize("values", [[5, 1, 5, 3, 2] * 20, [5.0, 1.5, 5.0, 3.5, 2.5] * 20])
@pytest.mark.parametrize("reverse", [False, True])
def test_disk_numeric_sort_uses_c_merge(monkeypatch, values, reverse) -> None:
    import importlib

    external_sort_module = importlib.import_module("bigarraylist.algorithms.external_sort")

    calls = []
    original_merge = external_sort_module.merge_numeric_files

    def recording_merge(*args, **kwargs):
        calls.append(True)
        return original_merge(*args, **kwargs)

    monkeypatch.setattr(external_sort_module, "merge_numeric_files", recording_merge)
    cfg = BigArrayConfig(
        external_sort_threshold=10,
        external_sort_min_chunk=10,
        external_sort_chunk_target=4,
        pickle_batch_size=7,
    )
    arr = BigArrayList.from_stream(iter(values), config=cfg)
    assert list(arr.sort(reverse=reverse)) == sorted(values, reverse=reverse)
    assert calls == [True]


def test_disk_non_numeric_sort_does_not_use_c_merge(monkeypatch) -> None:
    import importlib

    external_sort_module = importlib.import_module("bigarraylist.algorithms.external_sort")

    def fail_merge(*args, **kwargs):
        raise AssertionError("非数值帧不应进入 C 归并")

    monkeypatch.setattr(external_sort_module, "merge_numeric_files", fail_merge)
    values = ["pear", "apple", "orange", "apple"] * 10
    cfg = BigArrayConfig(
        external_sort_threshold=2,
        external_sort_min_chunk=4,
        external_sort_chunk_target=4,
    )
    arr = BigArrayList.from_stream(iter(values), config=cfg)
    assert list(arr.sort()) == sorted(values)
