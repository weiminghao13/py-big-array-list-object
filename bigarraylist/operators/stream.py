"""流式算子 mixin + 链式 API（LazyChain）。

两种 API 共存（向后兼容 + 新功能）：
    1. BigArrayList.map(f) / .filter(p) / .reduce(...) 等
       返回**裸 generator**——重构前行为，旧调用方零改动。
       注意：这些方法返回裸 generator，不能 .filter().map() 链式调用。

    2. BigArrayList.lazy()  →  LazyChain
       .lazy().map(f).filter(p).distinct().slice(0, 10).collect()
       每个算子返回新的 LazyChain（包装变换后的生成器），
       最终 .collect() / list(...) / for x in chain 物化或迭代。
"""

from __future__ import annotations

import functools
import itertools
from collections.abc import Callable, Generator, Iterable, Iterator
from typing import Any, Generic, TypeVar

from ..config import BigArrayConfig
from ..utils.scale import HUNDRED_THOUSAND

T = TypeVar("T")
U = TypeVar("U")
K = TypeVar("K")


class StreamMixin(Generic[T]):
    """流式算子 mixin：变换 / 过滤 / 聚合 / 切片 / 去重 / 合并 / 分组 / 计数。

    全部基于 self._fresh_iter()，不碰 self.__data——可重复调用、零副作用。
    不声明 __slots__（多继承冲突）。

    继承 Generic[T] 仅用于类型推导（让 T 在 mixin 内被绑定）；
    运行时通过 cooperative multiple inheritance 与 _CoreBase 组合，
    _fresh_iter / __len__ / _get_config 实际由 _CoreBase 提供，
    _big_iter_sort 由 SortMixin 提供（MRO 中 SortMixin 在后，调用点用 type:ignore）。
    """

    # 协作契约：实际实现由 _CoreBase 通过 MRO 提供
    def __len__(self) -> int:  # pragma: no cover - 协作契约
        raise NotImplementedError

    def _fresh_iter(self) -> Iterator[T]:  # pragma: no cover - 协作契约
        raise NotImplementedError

    def _get_config(self) -> BigArrayConfig:  # pragma: no cover - 协作契约
        raise NotImplementedError

    # ==================== 变换 / 过滤 ====================
    def _map_iter(self, func: Callable[[T], U]) -> Generator[U, None, None]:
        """对每个元素应用 func，惰性产出结果。"""
        for item in self._fresh_iter():
            yield func(item)

    def _filter_iter(self, predicate: Callable[[T], bool]) -> Generator[T, None, None]:
        """过滤：只产出 predicate(item) 为真的元素。"""
        for item in self._fresh_iter():
            if predicate(item):
                yield item

    def _reduce(self, func: Callable[[U, T], U], initial: U | None = None) -> U:
        """聚合：acc = func(acc, item)，逐元素累加，返回最终 acc。

        - initial 为 None 时用第一个元素作初始 acc（空数据抛 TypeError）
        - initial 不为 None 时，空数据直接返回 initial
        - 内存 O(1)——只维护一个累加器
        """
        it = self._fresh_iter()
        try:
            acc = initial if initial is not None else next(it)
        except StopIteration:
            if initial is not None:
                return initial
            raise TypeError("reduce() of empty iterable with no initial value") from None
        for item in it:
            acc = func(acc, item)  # type: ignore[arg-type]  # acc 可能为 T（initial=None 用首个元素）
        return acc  # type: ignore[return-value]  # 同上，T 与 U 的兼容性由调用方保证

    def _slice_iter(
        self,
        start: int | None,
        stop: int | None,
        step: int | None,
    ) -> Generator[T, None, None]:
        """惰性切片：包装 itertools.islice。

        约束（来自 itertools.islice）：
            - start/stop/step 不允许负数（惰性流无法反向、无法预知总长倒数）
            - step 必须为正整数或 None
        """
        yield from itertools.islice(self._fresh_iter(), start, stop, step)

    def _distinct_iter(self) -> Generator[T, None, None]:
        """去重：保留每个元素的首次出现。

        按数据量分流：
            - 小数据（< 10 万）：set 记录已见元素，O(n) 时间 O(k) 内存
            - 大数据：先 sort 排序，再相邻去重
              O(n log n) 时间，内存可控（排序阶段同 sort）

        全程操作 _fresh_iter()，不碰 self.__data。
        """
        n = len(self)
        # 大数据走排序去重路径
        if n >= HUNDRED_THOUSAND:
            # _big_iter_sort 由 SortMixin 提供（MRO 中 SortMixin 在 StreamMixin 之后）
            sorted_iter = self._big_iter_sort()  # type: ignore[attr-defined]
            sentinel = object()
            prev: object = sentinel
            for item in sorted_iter:
                if prev is not sentinel and item == prev:
                    continue
                yield item
                prev = item
            return
        # 小数据用 set 记录
        seen: set[T] = set()
        for item in self._fresh_iter():
            if item not in seen:
                seen.add(item)
                yield item

    def _concat_iter(self, other: Iterable[T]) -> Generator[T, None, None]:
        """把 self 和 other 首尾相接，惰性产出。

        - other 是 BaseBigArrayList 实例：用 other._fresh_iter()，零副作用
        - 其他 Iterable：直接 yield from
        """
        yield from self._fresh_iter()
        # 延迟 import 避免循环
        try:
            from ..core.base import BaseBigArrayList

            if isinstance(other, BaseBigArrayList):
                yield from other._fresh_iter()
                return
        except ImportError:
            pass
        yield from other

    def _chain_iter(self, *others: Iterable[T]) -> Generator[T, None, None]:
        """把 self 和多个 others 首尾相接，惰性产出。

        对每个 other：BaseBigArrayList 实例走 _fresh_iter()（零副作用），
        其他 Iterable 直接 yield from。
        """
        yield from self._fresh_iter()
        try:
            from ..core.base import BaseBigArrayList
        except ImportError:
            BaseBigArrayList = None  # type: ignore[misc, assignment]
        for o in others:
            if BaseBigArrayList is not None and isinstance(o, BaseBigArrayList):
                yield from o._fresh_iter()
            else:
                yield from o

    def _group_by_iter(
        self, key_func: Callable[[T], K]
    ) -> Generator[tuple[K, list[T]], None, None]:
        """SQL 风格 GROUP BY：按 key_func 分组，yield (key, list[elements])。

        按数据量分流：
            - 小数据（< 10 万）：物化 + sorted(key=key_func) → 全局同 key 合并
            - 大数据：流式分组（语义同 itertools.groupby），只合并相邻同 key
              → 要求用户先 sort(key=key_func)，否则只合并相邻同 key 的局部

        ⚠️ 单组元素数量很大时，list(group) 会物化整组到内存。
        """
        n = len(self)
        if n < HUNDRED_THOUSAND:
            data: Iterable[T] = sorted(self._fresh_iter(), key=key_func)  # type: ignore[arg-type]
        else:
            data = self._fresh_iter()
        for k, group in itertools.groupby(data, key=key_func):
            yield (k, list(group))

    def _count_if(self, predicate: Callable[[T], bool]) -> int:
        """流式条件计数：扫一遍数据，对 predicate(item) 为真的元素累加。

        只维护一个计数器，O(1) 内存。
        """
        count = 0
        for item in self._fresh_iter():
            if predicate(item):
                count += 1
        return count

    # ==================== 聚合（单遍 O(n)，O(1) 内存）====================
    def _min(
        self,
        key: Callable[[T], Any] | None = None,
        default: T | None = None,
    ) -> T | None:
        """返回最小元素。空数据返回 default。

        key 不为 None 时比较 key(item)，但返回的是原始元素（不是 key）。
        """
        it = self._fresh_iter()
        if key is None:
            try:
                m: T = next(it)
            except StopIteration:
                return default
            for item in it:
                if item < m:  # type: ignore[operator]
                    m = item
            return m
        try:
            m = next(it)
            mk = key(m)
        except StopIteration:
            return default
        for item in it:
            ik = key(item)
            if ik < mk:
                m = item
                mk = ik
        return m

    def _max(
        self,
        key: Callable[[T], Any] | None = None,
        default: T | None = None,
    ) -> T | None:
        """返回最大元素。空数据返回 default。

        key 不为 None 时比较 key(item)，但返回的是原始元素（不是 key）。
        """
        it = self._fresh_iter()
        if key is None:
            try:
                m: T = next(it)
            except StopIteration:
                return default
            for item in it:
                if item > m:  # type: ignore[operator]
                    m = item
            return m
        try:
            m = next(it)
            mk = key(m)
        except StopIteration:
            return default
        for item in it:
            ik = key(item)
            if ik > mk:
                m = item
                mk = ik
        return m

    def _sum(self) -> T:
        """求和。元素需支持 + 运算符。空数据返回 0。"""
        total: Any = 0
        for item in self._fresh_iter():
            total += item
        return total  # type: ignore[no-any-return]

    def _avg(self) -> float:
        """求平均值。空数据抛 ValueError。"""
        n = len(self)
        if n == 0:
            raise ValueError("avg() of empty iterable")
        return self._sum() / n  # type: ignore[operator]

    # ==================== 链式 API 入口 ====================
    def lazy(self) -> LazyChain[T]:
        """切换到链式 API：返回包装 self._fresh_iter() 的 LazyChain。

        用法：
            chain = big.lazy()                      # 进入链式世界
            result = (chain
                      .filter(lambda x: x > 0)
                      .map(lambda x: x * 2)
                      .distinct()
                      .slice(0, 10)
                      .collect())                  # 物化为 list

        与旧 .map() / .filter() / ... 的区别：
            - 旧方法：返回裸 generator，不能 .filter().map() 链式
            - lazy()：每个算子返回新 LazyChain，链式组合
        """
        return LazyChain(self._fresh_iter())


# ==================== 链式 API（独立类，组合而非继承）====================
class LazyChain(Generic[T]):
    """惰性链式算子链。

    设计要点（"组合而非继承"理念）：
        - 不继承 BigArrayList，内部组合一个迭代器
        - 每个算子方法返回新 LazyChain（包装变换后的生成器）
        - 全程惰性：只有 .collect() / list(...) / for x in chain 才真正消费

    与 BigArrayList.map() 等方法的区别：
        - BigArrayList.map(f)   → 裸 generator（向后兼容，不可链式）
        - big.lazy().map(f)     → LazyChain（可链式，最后 .collect() 物化）

    用法：
        chain = big.lazy()
        out = (chain
               .filter(lambda x: x > 0)
               .map(lambda x: x * 2)
               .slice(0, 100)
               .collect())              # → list
    """

    __slots__ = ("__iter",)

    def __init__(self, iterable: Iterable[T]) -> None:
        # 保存迭代器（不保存 BigArrayList，避免循环依赖 + 真正"组合"）
        self.__iter = iter(iterable)

    # ===== 迭代协议（支持 for x in chain / list(chain)）=====
    def __iter__(self) -> Iterator[T]:
        return self.__iter

    # ===== 链式算子 =====
    def map(self, func: Callable[[T], U]) -> LazyChain[U]:
        """对每个元素应用 func，返回新 LazyChain。"""
        return LazyChain(func(x) for x in self)

    def filter(self, predicate: Callable[[T], bool]) -> LazyChain[T]:
        """过滤 predicate(item) 为真的元素，返回新 LazyChain。"""
        return LazyChain(x for x in self if predicate(x))

    def slice(
        self,
        start: int | None = None,
        stop: int | None = None,
        step: int | None = None,
    ) -> LazyChain[T]:
        """惰性切片，返回新 LazyChain。

        约束：start/stop/step 不允许负数（惰性流无法反向）。
        """
        return LazyChain(itertools.islice(self, start, stop, step))

    def take(self, n: int) -> LazyChain[T]:
        """取前 n 个元素，返回新 LazyChain。"""
        return LazyChain(itertools.islice(self, n))

    def drop(self, n: int) -> LazyChain[T]:
        """丢弃前 n 个元素，返回新 LazyChain。"""
        return LazyChain(itertools.islice(self, n, None))

    def distinct(self) -> LazyChain[T]:
        """去重（保留首次出现），返回新 LazyChain。

        ⚠️ 仅适合小数据：内部用 set 记录已见元素，内存 O(k)（k = 不同元素数），
        元素必须可哈希。当不同元素数达到 HUNDRED_THOUSAND（10 万）时主动抛
        RuntimeError 早停，避免大数据无声爆内存。

        大数据去重请改用 BigArrayList.distinct()（裸 generator 版，大数据走
        外部排序去重，内存可控）。
        """

        def _distinct_gen() -> Generator[T, None, None]:
            seen: set[T] = set()
            for x in self:
                if x in seen:
                    continue
                seen.add(x)
                if len(seen) >= HUNDRED_THOUSAND:
                    raise RuntimeError(
                        f"LazyChain.distinct 的 set 已达 {HUNDRED_THOUSAND} 个不同元素，"
                        "继续去重会爆内存；大数据请改用 BigArrayList.distinct()"
                        "（走外部排序去重）"
                    )
                yield x

        return LazyChain(_distinct_gen())

    def concat(self, other: Iterable[T]) -> LazyChain[T]:
        """首尾相接 self 和 other，返回新 LazyChain。"""
        return LazyChain(itertools.chain(self, other))

    def chain(self, *others: Iterable[T]) -> LazyChain[T]:
        """链式合并多个 others，返回新 LazyChain。"""
        return LazyChain(itertools.chain(self, *others))

    # ===== 终结算子（消费迭代器，返回值）=====
    def reduce(self, func: Callable[[U, T], U], initial: U | None = None) -> U:
        """聚合：acc = func(acc, item)，返回最终 acc。"""
        if initial is not None:
            return functools.reduce(func, self, initial)
        return functools.reduce(func, self)  # type: ignore[arg-type, return-value]

    def count_if(self, predicate: Callable[[T], bool]) -> int:
        """统计 predicate(item) 为真的元素个数。"""
        return sum(1 for x in self if predicate(x))

    def count(self) -> int:
        """统计元素总数。"""
        return sum(1 for _ in self)

    def min(
        self,
        key: Callable[[T], Any] | None = None,
        default: T | None = None,
    ) -> T | None:
        """返回最小元素。空数据返回 default。"""
        if key is None:
            it = iter(self)
            try:
                m: T = next(it)
            except StopIteration:
                return default
            for item in it:
                if item < m:  # type: ignore[operator]
                    m = item
            return m
        it = iter(self)
        try:
            m = next(it)
            mk = key(m)
        except StopIteration:
            return default
        for item in it:
            ik = key(item)
            if ik < mk:
                m = item
                mk = ik
        return m

    def max(
        self,
        key: Callable[[T], Any] | None = None,
        default: T | None = None,
    ) -> T | None:
        """返回最大元素。空数据返回 default。"""
        if key is None:
            it = iter(self)
            try:
                m: T = next(it)
            except StopIteration:
                return default
            for item in it:
                if item > m:  # type: ignore[operator]
                    m = item
            return m
        it = iter(self)
        try:
            m = next(it)
            mk = key(m)
        except StopIteration:
            return default
        for item in it:
            ik = key(item)
            if ik > mk:
                m = item
                mk = ik
        return m

    def sum(self) -> T:
        """求和。元素需支持 + 运算符。空数据返回 0。"""
        total: Any = 0
        for item in self:
            total += item
        return total  # type: ignore[no-any-return]

    def avg(self) -> float:
        """求平均值。空数据抛 ValueError。"""
        total: Any = 0
        count = 0
        for item in self:
            total += item
            count += 1
        if count == 0:
            raise ValueError("avg() of empty iterable")
        return total / count  # type: ignore[no-any-return]

    def collect(self) -> list[T]:
        """物化为 list（终结算子）。"""
        return list(self)

    def first(self, default: U | None = None) -> T | U | None:
        """取第一个元素；流为空时返回 default。"""
        for x in self:
            return x
        return default

    def group_by(self, key_func: Callable[[T], K]) -> LazyChain[tuple[K, list[T]]]:
        """SQL 风格 GROUP BY：按 key_func 分组。

        返回新 LazyChain，每项为 (key, list[elements])。
        要求上游已按 key_func 排序（语义同 itertools.groupby）。
        """
        return LazyChain((k, list(g)) for k, g in itertools.groupby(self, key=key_func))


__all__ = ["LazyChain", "StreamMixin"]
