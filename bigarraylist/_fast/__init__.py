"""Cython 数值快路径：编译产物加载 + 可用性探测。

.pyd 编译成功时 FAST_BACKEND_AVAILABLE=True；
未编译（纯 Python 源码分发）时全部静默回退，行为与原版一致。

int 后端：Int64Array（list[int] 源）；
float 后端：Float64Array（list[int|float] 源，bool 被拒）。
"""

from __future__ import annotations

import functools
from collections.abc import Callable, Iterator
from typing import Any

__all__ = [
    "FAST_BACKEND_AVAILABLE",
    "FLOAT_BACKEND_AVAILABLE",
    "FRAME_IO_AVAILABLE",
    "NO_INITIAL",
    "NUMERIC_MERGE_AVAILABLE",
    "FastListResult",
    "Float64Array",
    "Int64Array",
    "coerce_target",
    "decode_float64",
    "decode_int64",
    "encode_float64",
    "encode_int64",
    "fast_contains",
    "fast_find_indices",
    "fast_float64",
    "fast_int64",
    "fast_numeric",
    "merge_numeric_files",
    "try_build_float64",
    "try_build_int64",
]

try:
    from bigarraylist._fast.int64_ops import (  # type: ignore[import-untyped]
        NO_INITIAL,
        Int64Array,
    )

    FAST_BACKEND_AVAILABLE = True
except ImportError:  # .pyd 未编译时回退纯 Python 后端
    Int64Array = None
    NO_INITIAL = object()
    FAST_BACKEND_AVAILABLE = False

try:
    from bigarraylist._fast.float64_ops import Float64Array  # type: ignore[import-untyped]

    FLOAT_BACKEND_AVAILABLE = True
except ImportError:  # .pyd 未编译时回退纯 Python 后端
    Float64Array = None
    FLOAT_BACKEND_AVAILABLE = False

try:
    from bigarraylist._fast.int64_ops import decode_int64, encode_int64

    FRAME_INT64_AVAILABLE = True
except ImportError:  # 旧 .pyd 未含帧 IO：仅关帧快路径，数组快路径不受影响
    encode_int64 = None
    decode_int64 = None
    FRAME_INT64_AVAILABLE = False

try:
    from bigarraylist._fast.float64_ops import decode_float64, encode_float64

    FRAME_FLOAT64_AVAILABLE = True
except ImportError:  # 旧 .pyd 未含帧 IO：仅关帧快路径
    encode_float64 = None
    decode_float64 = None
    FRAME_FLOAT64_AVAILABLE = False

# 两种编码都可用才启用二进制帧，避免写出的帧读不回来
FRAME_IO_AVAILABLE = FRAME_INT64_AVAILABLE and FRAME_FLOAT64_AVAILABLE

try:
    from bigarraylist._fast.merge_ops import merge_numeric_files  # type: ignore[import-untyped]

    NUMERIC_MERGE_AVAILABLE = True
except ImportError:  # 旧扩展不含 C 归并时回退 heapq.merge
    merge_numeric_files = None
    NUMERIC_MERGE_AVAILABLE = False


def try_build_int64(data: list[Any]) -> Any | None:
    """list 元素全为 int（且在 int64 范围内）时构建 Int64Array，否则返回 None。"""
    if not FAST_BACKEND_AVAILABLE:
        return None
    try:
        return Int64Array.from_list(data)
    except (TypeError, ValueError, OverflowError):
        return None


def try_build_float64(data: list[Any]) -> Any | None:
    """list 元素全为 int/float（bool 被拒）时构建 Float64Array，否则返回 None。"""
    if not FLOAT_BACKEND_AVAILABLE:
        return None
    try:
        return Float64Array.from_list(data)
    except (TypeError, ValueError, OverflowError):
        return None


class FastListResult:
    """快路径算子结果：包装 list，可迭代 + 支持链式 .map()/.filter()。

    与旧裸 generator 的关系：iter(obj) / list(obj) / for x in obj 全兼容；
    额外提供链式调用（旧裸 generator 不支持链式，这里只做增强不破坏）。
    """

    __slots__ = ("_items",)

    def __init__(self, items: list[Any]) -> None:
        self._items = items

    def __iter__(self) -> Iterator[Any]:
        return iter(self._items)

    def __len__(self) -> int:
        return len(self._items)

    def map(self, func: Callable[[Any], Any]) -> FastListResult:
        """链式 map（列表推导，快路径场景数据已在内存）。"""
        return FastListResult([func(x) for x in self._items])

    def filter(self, predicate: Callable[[Any], bool]) -> FastListResult:
        """链式 filter。"""
        return FastListResult([x for x in self._items if predicate(x)])

    def sum(self) -> Any:
        """终结求和（纯 Python，仅链式尾部用）。"""
        return functools.reduce(lambda a, b: a + b, self._items, 0)


def _fast_numeric(obj: Any, builder: Callable[[list[Any]], Any | None]) -> Any | None:
    """BigArrayList 的数值源（int/float 混合）→ 紧凑数组；不满足返回 None。

    优先按原顺序试 int 后端（更省内存），任一元素转 int64 失败再试 float 后端。
    """
    if not (FAST_BACKEND_AVAILABLE or FLOAT_BACKEND_AVAILABLE):
        return None
    src = obj._get_source()
    if isinstance(src, (list, tuple)):
        data = src if isinstance(src, list) else list(src)
        if FAST_BACKEND_AVAILABLE:
            fast = try_build_int64(data)
            if fast is not None:
                return fast
        return builder(data)
    return None


def fast_int64(obj: Any) -> Any | None:
    """BigArrayList 的 int 源 → Int64Array 快路径实例；不满足条件返回 None。

    供 _api.py 各算子入口统一调用：源必须是 list/tuple 且元素全为
    可放入 int64 的 int（bool 语义不同，from_list 转换时被拒）。
    """
    if not FAST_BACKEND_AVAILABLE:
        return None
    src = obj._get_source()
    if isinstance(src, (list, tuple)):
        return try_build_int64(src if isinstance(src, list) else list(src))
    return None


def fast_float64(obj: Any) -> Any | None:
    """BigArrayList 的数值源 → Float64Array 快路径实例；不满足条件返回 None。

    全 int 源已被 fast_int64 覆盖（调用方应先试 fast_int64）；
    本入口专门服务含 float 的混合数值源。
    """
    return _fast_numeric(obj, try_build_float64)


def fast_numeric(obj: Any) -> Any | None:
    """统一探测入口：int 源 → Int64Array，含 float 源 → Float64Array。

    sum/min/max/sort 等数值算子应统一走本入口（第四步起取代直接调 fast_int64）。

    结果按 (源id, 源长度) 缓存在实例上：连续调用多个数值算子只付一次
    O(n) 转换成本。源被替换或长度变化（增删元素）时缓存自动失效；
    同长度原地改值不失效——与库"构造后不改动源"的既有假设一致。
    缓存仅对 list/tuple 源生效，这类源本就全在内存中，紧凑数组的额外
    内存占用可接受（远小于 Python 对象开销）。
    """
    if not FAST_BACKEND_AVAILABLE:
        return None
    src = obj._get_source()
    if not isinstance(src, (list, tuple)):
        return None
    key = id(src)
    cached = obj._get_fast_cache()
    if cached is not None and cached[0] == key and cached[1] == len(src):
        return cached[2]
    data = src if isinstance(src, list) else list(src)
    fast = try_build_int64(data)
    if fast is None:
        fast = try_build_float64(data)
    obj._set_fast_cache((key, len(src), fast))
    return fast


# ==================== 等值查找快路门控（方案二）====================


def coerce_target(fast: Any, target: object) -> Any | None:
    """把 target 转成紧凑数组后端的 C 标量；类型不兼容返回 None。

    门控规则（与 from_list 的类型拒绝保持一致）：
        - Int64Array：只收真 int（``type(target) is int``）。
          bool 是 int 子类但语义不同，显式拒；float 也拒（避免静默截断）。
        - Float64Array：收 int / float（bool 由 exact type 排除）；
          int 升 float。
        - fast 为 None（非数值源或 .pyd 未编译）→ 返回 None。

    返回 None 时调用方必须回退纯 Python 路径，绝不能拿 None 去调 C 方法。
    """
    if fast is None:
        return None
    if Int64Array is not None and isinstance(fast, Int64Array):
        return target if type(target) is int else None
    if Float64Array is not None and isinstance(fast, Float64Array):
        if type(target) is int:
            return float(target)
        if type(target) is float:
            return target
    return None


def fast_contains(obj: Any, target: object) -> bool | None:
    """数值内存源 ``x in arr`` 快路。

    命中快路返回 True/False；不命中（非数值源、target 类型不符）返回 None，
    调用方（BigArrayList.__contains__）回退到流式 any() 路径。
    """
    fast = fast_numeric(obj)
    if fast is None:
        return None
    v = coerce_target(fast, target)
    if v is None:
        return None
    return fast.py_contains(v)  # type: ignore[no-any-return]


def fast_find_indices(obj: Any, target: object) -> list[int] | None:
    """数值内存源 ``find(target)`` 快路：返回全部命中下标 list。

    不命中快路返回 None，调用方回退到并行分块 / 分桶路径。
    返回的下标与源数组位置一一对应（升序），调用方负责包装成 (i, target)。
    """
    fast = fast_numeric(obj)
    if fast is None:
        return None
    v = coerce_target(fast, target)
    if v is None:
        return None
    return fast.py_find_indices(v)  # type: ignore[no-any-return]
