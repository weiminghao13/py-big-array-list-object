"""api.py（BigArrayList 公共 API）的单元测试。

覆盖：CRUD / find / sort / binary_search / bucket_find / sample /
build_index / auto_find / 流式算子（map·filter·reduce·slice·distinct·
concat·chain·group_by·count_if）/ 外部排序 / 参数校验。
"""

import bisect
import glob
import itertools
import os
import tempfile
import time
import types

import pytest

from bigarraylist import BigArrayList


# 模块级类（可 pickle，供 bucket_find 多进程子进程 pickle 调用）
class _User:
    """按 id 字段分桶的测试用类。必须在模块级定义，否则 pickle 失败。"""

    def __init__(self, uid, name):
        self.id = uid
        self.name = name

    def __eq__(self, o):
        return isinstance(o, _User) and self.id == o.id

    def __hash__(self):
        return hash(self.id)


# ==================== 增删改插 ====================


def test_crud_append_remove_replace_insert():
    """增删改插四条小数据全用 list 验真。"""
    base = [1, 2, 3, 4, 5]
    assert list(BigArrayList(list(base)).append(6, 7)) == [1, 2, 3, 4, 5, 6, 7]
    assert list(BigArrayList(list(base)).remove(1, 3)) == [1, 4, 5]  # 删 idx 1,2
    assert list(BigArrayList(list(base)).replace(1, 3, 99)) == [1, 99, 4, 5]
    assert list(BigArrayList(list(base)).insert(2, 99)) == [1, 2, 99, 3, 4, 5]


# ==================== 线性查找 find ====================


def test_find_small_correctness(small):
    """find（线性并行）小数据：命中 (下标, 值) 全部对上。"""
    arr = BigArrayList(list(small))
    assert sorted(arr.find(5)) == [(4, 5), (8, 5), (10, 5)]
    # 找不到 target：返回空
    arr2 = BigArrayList(list(small))
    assert list(arr2.find(9999)) == []


def test_find_big_count_matches(rng, big_n):
    """find 大数据：命中总数等于 list.count。"""
    data = [rng.randint(0, 100) for _ in range(big_n)]
    target = 50
    expected = data.count(target)
    hits = list(BigArrayList(data).find(target))
    assert len(hits) == expected
    assert all(v == target for _, v in hits)


# ==================== 排序 sort ====================


def test_sort_small_is_sorted(small):
    """sort 小数据：结果等于 sorted(source)。"""
    assert list(BigArrayList(list(small)).sort()) == sorted(small)


def test_sort_big_strictly_monotonic(rng, big_n):
    """sort 大数据：严格非降（每个 a[i] <= a[i+1]）。"""
    data = [rng.randint(0, 1000) for _ in range(big_n)]
    out = list(BigArrayList(data).sort())
    assert len(out) == big_n
    assert all(out[i] <= out[i + 1] for i in range(len(out) - 1)), "排序结果不满足非降"
    # 和内置 sorted 完全一致
    assert out == sorted(data)


# ==================== 二分查找 binary_search ====================


def test_binary_search_small_correctness(small):
    """二分小数据：命中下标 + 值，与 sorted(list) 的 bisect 结果对齐。"""
    target = 5
    truth_sorted = sorted(small)
    left = bisect.bisect_left(truth_sorted, target)
    right = bisect.bisect_right(truth_sorted, target)
    truth = [(i, truth_sorted[i]) for i in range(left, right)]

    arr = BigArrayList(list(small))
    assert list(arr.binary_search(target)) == truth


def test_binary_search_big_count_and_range(rng, big_n):
    """二分大数据：命中数、起始、结束下标 与 sorted(data) bisect 完全一致。"""
    data = [rng.randint(0, 100) for _ in range(big_n)]
    target = 42
    sd = sorted(data)
    l_truth = bisect.bisect_left(sd, target)
    r_truth = bisect.bisect_right(sd, target)

    got = list(BigArrayList(data).binary_search(target))
    assert len(got) == r_truth - l_truth
    if got:
        assert got[0][0] == l_truth
        assert got[-1][0] == r_truth - 1
        assert all(v == target for _, v in got)


# ==================== sort(key=...) / binary_search(key=...) ====================
# 模块级 key 函数（多进程路径必须可 pickle；lambda 仅小数据分支可用）


def _neg_key(x):
    """取负键：让 sort(key=_neg_key) 等价于降序排序。模块级，可 pickle。"""
    return -x


def _abs_key(x):
    """绝对值键。模块级，可 pickle。"""
    return abs(x)


def test_sort_small_with_key(small):
    """sort(key=) 小数据：结果等于 sorted(source, key=...)。"""
    # lambda 在小数据分支（单进程）可用
    assert list(BigArrayList(list(small)).sort(key=lambda x: -x)) == sorted(small, key=lambda x: -x)
    # 内置函数也能用
    assert list(BigArrayList(list(small)).sort(key=abs)) == sorted(small, key=abs)


def test_sort_big_with_key(rng, big_n):
    """sort(key=) 大数据：走并行/外部排序路径，key 必须 pickle。
    结果与 sorted(data, key=...) 完全一致；按 key 单调非降。"""
    data = [rng.randint(-100, 100) for _ in range(big_n)]
    out = list(BigArrayList(data).sort(key=_abs_key))
    assert len(out) == big_n
    # 与内置 sorted(key=...) 完全一致
    assert out == sorted(data, key=_abs_key)
    # 按 key 单调非降
    keys = [_abs_key(x) for x in out]
    assert all(keys[i] <= keys[i + 1] for i in range(len(keys) - 1)), "按 key 非降失败"


def test_binary_search_with_key_small(small):
    """binary_search(key=) 小数据：命中与 sorted(data, key=...) + bisect(key=) 对齐。"""
    target = -5  # key = abs，abs(-5) = 5
    truth_sorted = sorted(small, key=_abs_key)
    left = bisect.bisect_left(truth_sorted, target, key=_abs_key)
    right = bisect.bisect_right(truth_sorted, target, key=_abs_key)
    truth = [(i, truth_sorted[i]) for i in range(left, right)]

    arr = BigArrayList(list(small))
    assert list(arr.binary_search(target, key=_abs_key)) == truth


def test_binary_search_with_key_big(rng, big_n):
    """binary_search(key=) 大数据：走并行排序路径，命中下标与 sorted(data, key) + bisect(key) 对齐。"""
    data = [rng.randint(-100, 100) for _ in range(big_n)]
    target = -42  # key = _neg_key，-(-42) = 42
    sd = sorted(data, key=_neg_key)
    l_truth = bisect.bisect_left(sd, target, key=_neg_key)
    r_truth = bisect.bisect_right(sd, target, key=_neg_key)
    truth = [(i, sd[i]) for i in range(l_truth, r_truth)]

    got = list(BigArrayList(data).binary_search(target, key=_neg_key))
    # 命中数、起止下标、(下标, 值) 全部与 sorted + bisect(key=) 对齐
    assert got == truth


def test_sort_with_key_backward_compat(small):
    """向后兼容：sort(key=None) 等价于旧 sort()。"""
    assert list(BigArrayList(list(small)).sort(key=None)) == sorted(small)
    # 不传 key 也等价
    assert list(BigArrayList(list(small)).sort()) == sorted(small)


# ==================== 惰性：返回 generator + 早停 ====================


def test_find_returns_generator_and_early_stop():
    """惰性保证：find 返回 generator；只要前 2 个命中，不消费后续 chunk。"""
    data = [0] * 20 + [999] * 20 + [0] * 50_000
    ba = BigArrayList(data)
    gen = ba.find(999)
    # 断言是生成器对象（说明没全物化）
    assert isinstance(gen, types.GeneratorType)
    first2 = list(itertools.islice(gen, 2))
    assert first2 == [(20, 999), (21, 999)]
    # 主动 close，早停释放剩余
    gen.close()


def test_sort_returns_generator():
    """sort 返回 generator（惰性，可 islice）。"""
    ba = BigArrayList(list(range(10)))
    gen = ba.sort()
    assert isinstance(gen, types.GeneratorType)
    first3 = list(itertools.islice(gen, 3))
    assert first3 == [0, 1, 2]
    gen.close()


# ==================== 复用排序 + 多次 bisect 很快 ====================


def test_reuse_sorted_cache_many_bisects_is_fast(rng, big_n):
    """复用排序 + 多次 bisect：三次纯二分加起来 < 20ms（O(log n) 兑现）。"""
    data = [rng.randint(0, 100) for _ in range(big_n)]
    cached_sorted = list(BigArrayList(data).sort())
    t0 = time.perf_counter()
    for target in (25, 50, 75):
        bisect.bisect_left(cached_sorted, target)
        bisect.bisect_right(cached_sorted, target)
    elapsed_ms = (time.perf_counter() - t0) * 1000
    # 100k 数据 3 次 bisect，正常 <0.5ms；给 20ms 宽限（防止 CI 抖）
    assert elapsed_ms < 20.0, f"三次纯二分耗时 {elapsed_ms:.2f} ms，O(log n) 表现异常"


# ==================== is_sorted 跨块判断 ====================


def test_is_sorted_chunk_boundary_check():
    """is_sorted 跨块衔接判断：块内都有序但块尾>块头应判 False。
    （修复过 is_sorted 只验 chunk 内部、不比对 chunk 首尾的 bug）"""
    big_sorted = list(range(200_000))
    big_desc = big_sorted[::-1]
    # 跨块不衔接：前半升序后半升序，但整块尾部 > 整块头部
    half = 120_000  # 略大于 chunk_size，保证跨块
    bad_boundary = list(range(half)) + list(range(half))
    cases = [
        (big_sorted, True),
        (big_desc, False),
        (bad_boundary, False),
    ]
    for src, expected in cases:
        ba = BigArrayList(src)
        assert ba.is_sorted() == expected, f"len={len(src)} expected={expected}"


# ==================== 空数据 / 单元素边界 ====================


def test_edge_empty_and_single():
    """空数据 / 单元素：不报错。"""
    # 空
    assert len(BigArrayList([])) == 0
    assert list(BigArrayList([]).find(1)) == []
    assert list(BigArrayList([]).binary_search(1)) == []
    assert list(BigArrayList([]).sort()) == []
    # 单元素
    assert list(BigArrayList([42]).find(42)) == [(0, 42)]
    assert list(BigArrayList([42]).binary_search(42)) == [(0, 42)]
    assert list(BigArrayList([42]).sort()) == [42]


# ==================== generator 源：算子非空（回归） ====================


def test_generator_source_operators_not_empty():
    """generator 源构造后，所有算子调用 _fresh_iter 应产出非空数据。
    修复前：generator 被 list(source) 耗尽后，_fresh_iter 拿到空迭代器，
    所有算子全产出空数据。修复后：物化后 self.__source 更新为 list。"""
    data = [3, 1, 4, 1, 5, 9, 2, 6, 5, 3, 5, 1]

    # case 1：Generator 类型源
    ba = BigArrayList(x for x in data)
    assert len(ba) == len(data)  # 构造时已物化拿长度
    assert list(ba) == data  # for 循环（走 __next__ / self.__data）应非空

    # 构造新实例（上面 list(ba) 已耗尽 self.__data），验证所有算子非空
    assert list(BigArrayList(x for x in data).sort()) == sorted(data)

    ba3 = BigArrayList(x for x in data)
    hits = [(i, v) for i, v in ba3.find(5)]
    assert len(hits) == 3  # 5 出现在下标 4,8,10
    assert sorted(hits) == [(4, 5), (8, 5), (10, 5)]

    # reverse（generator 源 → 物化后走 reversed(list)）
    assert list(BigArrayList(x for x in data).reverse()) == data[::-1]
    # map
    assert list(BigArrayList(x for x in data).map(lambda x: x * 2)) == [x * 2 for x in data]
    # filter
    assert list(BigArrayList(x for x in data).filter(lambda x: x > 4)) == [x for x in data if x > 4]
    # __contains__
    ba7 = BigArrayList(x for x in data)
    assert 5 in ba7
    assert 999 not in ba7
    # copy
    assert list(BigArrayList(x for x in data).copy()) == data
    # append
    assert list(BigArrayList(x for x in data).append(99, 100)) == [*data, 99, 100]

    # case 2：无 __len__ 的可迭代对象（纯 Iterator，走 TypeError 物化分支）
    class _NoLenIter:
        """可迭代但无 __len__：触发 __init__ 的 TypeError 分支。"""

        def __init__(self, inner):
            self._inner = inner

        def __iter__(self):
            return iter(self._inner)

    ba_it = BigArrayList(_NoLenIter(data))
    assert len(ba_it) == len(data)
    assert list(ba_it.sort()) == sorted(data)  # sort 非空
    ba_it2 = BigArrayList(_NoLenIter(data))
    assert len([(i, v) for i, v in ba_it2.find(5)]) == 3  # find 非空
    assert list(BigArrayList(_NoLenIter(data)).reverse()) == data[::-1]  # reverse 非空


# ==================== 流式算子：map / filter / reduce ====================


def test_stream_operators_map_filter_reduce():
    """map / filter / reduce：基础函数式三件套。"""
    data = [1, 2, 3, 4, 5]
    ba = BigArrayList(list(data))

    # map：每个元素 *2
    assert list(ba.map(lambda x: x * 2)) == [2, 4, 6, 8, 10]
    # filter：保留偶数
    assert list(ba.filter(lambda x: x % 2 == 0)) == [2, 4]
    # reduce：求和
    assert ba.reduce(lambda a, b: a + b, 0) == 15
    # reduce：无 initial，用第一个元素作 acc
    assert ba.reduce(lambda a, b: a if a > b else b) == 5
    # reduce：空数据 + initial → 返回 initial
    assert BigArrayList([]).reduce(lambda a, b: a + b, 0) == 0
    # reduce：空数据 + 无 initial → 抛 TypeError
    with pytest.raises(TypeError):
        BigArrayList([]).reduce(lambda a, b: a + b)


# ==================== 流式算子：slice / distinct / count_if ====================


def test_stream_operators_slice_distinct_count_if(small):
    """slice / distinct / count_if：切片 / 去重 / 条件计数。"""
    ba = BigArrayList(list(small))

    # slice(start, stop, step)
    assert list(ba.slice(2, 8, 2)) == [4, 5, 2]
    # slice 全部默认
    assert list(BigArrayList([10, 20]).slice())[:2] == [10, 20]
    # slice 负数 start：抛 ValueError
    with pytest.raises(ValueError):
        list(BigArrayList([1, 2]).slice(-1, 2))
    # slice step=0：抛 ValueError
    with pytest.raises(ValueError):
        list(BigArrayList([1, 2]).slice(0, 2, 0))

    # distinct：保留首次出现
    assert list(ba.distinct()) == [3, 1, 4, 5, 9, 2, 6]

    # count_if：统计 > 4 的元素个数
    assert ba.count_if(lambda x: x > 4) == 5  # 5,9,6,5,5


# ==================== 流式算子：concat / chain / group_by ====================


def test_stream_operators_concat_chain_group_by():
    """concat / chain / group_by：合并 / 链式合并 / 分组。"""
    b1 = BigArrayList([1, 2, 3])
    b2 = BigArrayList([10, 20])

    # concat：BigArrayList + BigArrayList
    assert list(b1.concat(b2)) == [1, 2, 3, 10, 20]

    # concat：BigArrayList + 普通 Iterable（零副作用：b2 不被污染）
    assert list(b1.concat([100, 200])) == [1, 2, 3, 100, 200]
    # concat 后 b2 仍可正常迭代（零副作用保证）
    assert list(b2) == [10, 20]

    # chain：多个流
    b3 = BigArrayList([100])
    assert list(b1.chain(b2, b3, [7, 8])) == [1, 2, 3, 10, 20, 100, 7, 8]

    # group_by：小数据按 key 全局合并
    b4 = BigArrayList([1, 2, 3, 4, 5, 6])
    groups = dict(b4.group_by(lambda x: x % 2))
    assert groups[0] == [2, 4, 6]
    assert groups[1] == [1, 3, 5]


# ==================== 流式算子：参数校验立即执行 ====================


def test_stream_operators_validation_immediate():
    """流式算子参数校验在调用时立即执行（不推迟到迭代）。"""
    ba = BigArrayList([1, 2, 3])
    with pytest.raises(TypeError):
        ba.map("not_callable")
    with pytest.raises(TypeError):
        ba.filter(123)
    with pytest.raises(TypeError):
        ba.reduce(None)
    with pytest.raises(TypeError):
        ba.group_by([1, 2])
    with pytest.raises(TypeError):
        ba.count_if("nope")
    with pytest.raises(TypeError):
        ba.concat(123)
    with pytest.raises(TypeError):
        ba.chain([1, 2], 456)


# ==================== 外部排序（百万级）正确性 + 临时文件清理 ====================


def test_external_sort_correctness_and_cleanup(rng):
    """百万级数据走外部排序：结果非降 + 等于 sorted(data) + 临时文件清理干净。
    触发 _big_iter_sort 的百万级分支（_external_sort）。"""
    n = 1_000_000
    data = [rng.randint(0, 1000) for _ in range(n)]

    # 扫描临时目录基线：记录已有 big_sort_*.tmp 文件
    tmp_dir = tempfile.gettempdir()
    pattern = os.path.join(tmp_dir, "big_sort_*.tmp")
    before = set(glob.glob(pattern))

    try:
        out = list(BigArrayList(data).sort())
    finally:
        after = set(glob.glob(pattern))

    # 1) 长度对
    assert len(out) == n
    # 2) 严格非降
    assert all(out[i] <= out[i + 1] for i in range(len(out) - 1)), "外部排序结果不满足非降"
    # 3) 和内置 sorted 完全一致
    assert out == sorted(data)
    # 4) 临时文件清理干净（不应有新增的 big_sort_*.tmp 残留）
    leaked = after - before
    assert leaked == set(), f"外部排序泄漏临时文件：{leaked}"


# ==================== 分桶查找 bucket_find ====================


def test_bucket_find_small_correctness(small):
    """小数据 bucket_find：走线性分支，命中 (下标, 值) 全对。"""
    arr = BigArrayList(list(small))  # < 十万 → 走线性分支
    assert sorted(arr.bucket_find(5)) == [(4, 5), (8, 5), (10, 5)]
    # 找不到 target：返回空
    arr2 = BigArrayList(list(small))
    assert list(arr2.bucket_find(9999)) == []


def test_bucket_find_big_matches_find(rng, big_n):
    """大数据 bucket_find：命中集合等于 find() 的命中集合（原始下标对齐）。"""
    data = [rng.randint(0, 100) for _ in range(big_n)]
    arr = BigArrayList(data)
    # 用 find() 作为基准（线性扫，原始下标）
    baseline = dict(arr.find(50))

    # bucket_find 返回的也应该命中所有 50
    arr2 = BigArrayList(data)
    hits = list(arr2.bucket_find(50, num_buckets=64))
    bucket_result = dict(hits)
    assert bucket_result == baseline, "bucket_find 与 find 命中集合不一致"
    # 下标集合也必须完全一致
    assert sorted(bucket_result.keys()) == sorted(baseline.keys())


def test_bucket_find_key_func():
    """bucket_find + key_func：按元素属性分桶，命中正确。"""
    N = 200_000  # >= 十万，触发建桶分支
    users = [_User(i % 50, f"name_{i}") for i in range(N)]
    arr = BigArrayList(users)

    # 查 id=23 的用户（所有 id%50==23 的都应命中）
    target_user = _User(23, "target")
    hits = list(arr.bucket_find(target_user, key_func=lambda u: u.id, num_buckets=16))
    expected_count = sum(1 for u in users if u.id == 23)
    assert len(hits) == expected_count
    # 每个命中的 id 都是 23
    for _, u in hits:
        assert u.id == 23


def test_bucket_find_no_state_pollution():
    """bucket_find 不污染实例状态：调完后再 for 循环，数据完整。"""
    data = list(range(150_000))  # >= 十万，走建桶分支
    arr = BigArrayList(data)
    hits = list(arr.bucket_find(50, num_buckets=8))
    assert all(v == 50 for _, v in hits)
    # 实例状态未被污染：for 循环仍能产出全部 150000 条
    assert list(arr) == data


def test_bucket_find_temp_file_cleanup(rng):
    """bucket_find 的临时桶文件 finally 清理干净。"""
    tmp_dir = tempfile.gettempdir()
    pattern = os.path.join(tmp_dir, "big_bucket_*.tmp")
    before = set(glob.glob(pattern))

    data = [rng.randint(0, 1000) for _ in range(150_000)]
    try:
        hits = list(BigArrayList(data).bucket_find(500, num_buckets=8))
    finally:
        after = set(glob.glob(pattern))
    # 至少有命中（500 在 0~1000 范围内大概率出现）
    assert len(hits) > 0
    # 临时文件清理干净
    leaked = after - before
    assert leaked == set(), f"bucket_find 泄漏临时文件：{leaked}"


def test_bucket_find_validation_immediate():
    """bucket_find 参数校验在调用时立即执行。"""
    arr = BigArrayList([1, 2, 3])
    with pytest.raises(TypeError):
        arr.bucket_find(1, key_func=123)
    with pytest.raises(TypeError):
        arr.bucket_find(1, num_buckets=1.5)
    with pytest.raises(ValueError):
        arr.bucket_find(1, num_buckets=0)
    with pytest.raises(TypeError):
        arr.bucket_find(1, max_workers="abc")


# ==================== 蓄水池抽样 sample ====================


def test_sample_returns_k_elements():
    """sample(k)：返回 k 个元素，且都在原数据里。"""
    data = list(range(1000))
    arr = BigArrayList(data)
    k = 50
    result = list(arr.sample(k, seed=42))
    assert len(result) == k
    # 抽到的元素都在原数据里
    assert all(x in data for x in result)


def test_sample_k_greater_than_n():
    """sample(k) 当 k > N：返回全部 N 个元素。"""
    data = [10, 20, 30, 40]
    arr = BigArrayList(data)
    result = list(arr.sample(100, seed=42))
    assert len(result) == len(data)
    assert sorted(result) == sorted(data)


def test_sample_reproducible_with_seed():
    """sample(k, seed)：相同 seed 结果一致。"""
    data = list(range(5000))
    r1 = list(BigArrayList(data).sample(100, seed=42))
    r2 = list(BigArrayList(data).sample(100, seed=42))
    assert r1 == r2
    # 不同 seed 大概率不同
    r3 = list(BigArrayList(data).sample(100, seed=999))
    assert r1 != r3


def test_sample_roughly_uniform():
    """sample(k) 大致等概率：分 4 段，每段命中数应相近（统计性测试）。"""
    n = 10000
    data = list(range(n))
    result = list(BigArrayList(data).sample(1000, seed=42))  # 固定种子可复现
    # 把数据分 4 段，统计每段被抽中多少次
    quarter = n // 4
    counts = [0, 0, 0, 0]
    for x in result:
        seg = min(x // quarter, 3)
        counts[seg] += 1
    # 每段应该接近 250（1000/4），允许 ±60 的统计波动
    for c in counts:
        assert 190 < c < 310, f"某段抽样次数异常：{c}"


def test_sample_validation():
    """sample 参数校验：k 必须正整数。"""
    arr = BigArrayList([1, 2, 3])
    with pytest.raises(TypeError):
        arr.sample(1.5)
    with pytest.raises(ValueError):
        arr.sample(0)
    with pytest.raises(ValueError):
        arr.sample(-5)
    # bool 不该当 int 用
    with pytest.raises(TypeError):
        arr.sample(True)


# ==================== 二级索引 build_index ====================


def test_build_index_correctness():
    """build_index：dict[key] -> list[下标]，下标全对。"""
    data = [3, 1, 4, 1, 5, 9, 2, 6, 5, 3, 5, 1]
    arr = BigArrayList(data)
    idx = arr.build_index()
    # key=5 出现在下标 4, 8, 10
    assert idx[5] == [4, 8, 10]
    # key=1 出现在下标 1, 3, 11
    assert idx[1] == [1, 3, 11]
    # key=999 不在索引里
    assert 999 not in idx
    # 索引的 key 总数 = 不同元素数
    assert len(idx) == len(set(data))


def test_build_index_with_key_func():
    """build_index + key_func：按元素字段建索引。"""
    users = [_User(i % 10, f"name_{i}") for i in range(50)]
    arr = BigArrayList(users)
    idx = arr.build_index(key_func=lambda u: u.id)
    # id=3 的用户出现在哪些下标
    expected_indices = [i for i, u in enumerate(users) if u.id == 3]
    assert idx[3] == expected_indices
    # id 范围 0~9，共 10 个不同 key
    assert len(idx) == 10


def test_build_index_no_state_pollution():
    """build_index 不污染实例状态：建完索引后 for 循环数据完整。"""
    data = list(range(1000))
    arr = BigArrayList(data)
    idx = arr.build_index()
    assert 500 in idx
    # 实例状态未污染
    assert list(arr) == data


# ==================== 统一查找入口 auto_find ====================


def test_auto_find_small_uses_linear(small):
    """auto_find 小数据：走线性分支，命中等于 find。"""
    arr = BigArrayList(list(small))
    baseline = list(arr.find(5))
    arr2 = BigArrayList(list(small))
    result = list(arr2.auto_find(5))
    assert sorted(result) == sorted(baseline)


def test_auto_find_big_uses_bucket(rng, big_n):
    """auto_find 大数据：走分桶分支，命中等于 find。"""
    data = [rng.randint(0, 100) for _ in range(big_n)]
    arr = BigArrayList(data)
    baseline = dict(arr.find(50))
    arr2 = BigArrayList(data)
    result = dict(arr2.auto_find(50, num_buckets=32))
    assert result == baseline


def test_auto_find_no_state_pollution():
    """auto_find 不污染实例状态：调完后再 for 循环数据完整。"""
    data = list(range(150_000))  # >= 十万，走分桶分支
    arr = BigArrayList(data)
    hits = list(arr.auto_find(50, num_buckets=8))
    assert all(v == 50 for _, v in hits)
    assert list(arr) == data
