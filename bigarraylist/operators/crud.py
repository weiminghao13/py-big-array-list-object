"""CRUD 算子 mixin：惰性增 / 删 / 改 / 插。

所有方法全程操作 self._fresh_iter() 返回的局部迭代器，不碰 self.__data——
算子调用零副作用、可重复调用。
"""

from __future__ import annotations

from collections.abc import Generator, Iterator
from typing import Generic, TypeVar

T = TypeVar("T")


class CRUDMixin(Generic[T]):
    """增删改插算子 mixin。

    不声明 __slots__：避免与 _CoreBase 的 __slots__ 多继承冲突；
    所有状态字段全部由 _CoreBase 持有，本 mixin 只读访问通过 self._fresh_iter()。

    继承 Generic[T] 仅用于类型推导（让 T 在 mixin 内被绑定）；
    运行时通过 cooperative multiple inheritance 与 _CoreBase 组合，
    _fresh_iter / __len__ 等方法实际由 _CoreBase 提供。
    """

    # 类型契约：实际实现由 _CoreBase 通过 MRO 提供
    def _fresh_iter(self) -> Iterator[T]:  # pragma: no cover - 协作契约，由 _CoreBase 实现
        raise NotImplementedError

    def _append_iter(self, *new_items: T) -> Generator[T, None, None]:
        """惰性追加：在原流末尾接上新元素，返回新生成器。

        全程操作 _fresh_iter() 返回的局部迭代器，不碰 self.__data——
        可以重复调用，不会污染实例状态。
        """
        data = self._fresh_iter()
        yield from data
        yield from new_items

    def _replace_iter(self, start: int, end: int, *new_items: T) -> Generator[T, None, None]:
        """惰性替换：把 [start, end) 范围的元素替换成 new_items。

        全程操作 _fresh_iter() 返回的局部迭代器，不碰 self.__data。
        """
        data = self._fresh_iter()
        # 产出 0~start-1
        for _ in range(start):
            yield next(data)
        # 丢弃 start ~ end-1
        drop_count = end - start
        for _ in range(drop_count):
            next(data)
        # 输出新内容
        yield from new_items
        # 输出剩余
        yield from data

    def _insert_iter(self, index: int, *new_items: T) -> Generator[T, None, None]:
        """在指定位置插入新元素的迭代器。

        全程操作 _fresh_iter() 返回的局部迭代器，不碰 self.__data。
        """
        data = self._fresh_iter()
        # 产出 0~index-1
        for _ in range(index):
            yield next(data)
        # 产出新增的元素
        yield from new_items
        # 产出剩余元素
        yield from data

    def _remove_iter(self, start: int, end: int) -> Generator[T, None, None]:
        """惰性删除：跳过 [start, end) 范围的元素，其余照常产出。

        全程操作 _fresh_iter() 返回的局部迭代器，不碰 self.__data。
        """
        data = self._fresh_iter()
        # 产出 0~start-1
        for _ in range(start):
            yield next(data)
        # 丢弃 start ~ end-1
        drop_count = end - start
        for _ in range(drop_count):
            next(data)
        # 产出剩余元素
        yield from data


__all__ = ["CRUDMixin"]
