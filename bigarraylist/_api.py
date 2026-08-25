"""BigArrayList 用户接口层。

设计要点（重构后）：
    - 仍继承 BaseBigArrayList（拆分为 mixin 后的核心 + 算子组合），
      所有公开方法签名 100% 向后兼容。
    - __init__ 新增可选 config 参数（BigArrayConfig），不传时用默认配置——
      行为与重构前完全一致。
    - 校验函数改用 utils.validation 模块；默认抛标准异常（向后兼容），
      设置 raise_custom=True 时抛自定义异常子类（仍继承对应标准异常）。
    - 删除死代码：__data_scale 字段 + get_scale_helper() 方法。
"""

from __future__ import annotations

from collections.abc import Callable, Generator, Iterable
from typing import Any, TypeVar

from .algorithms.bucket import BucketIndex
from .config import BigArrayConfig
from .core.base import BaseBigArrayList, _DiskSpillIterable
from .operators.stream import LazyChain
from .utils.scale import ScaleHelper
from .utils.validation import (
    validate_callable,
    validate_index,
    validate_iterable,
    validate_max_workers,
    validate_num_buckets,
    validate_range,
    validate_sample_k,
    validate_slice_arg,
)

T = TypeVar("T")
U = TypeVar("U")


class BigArrayList(BaseBigArrayList):
    """大数据用户接口层。

    继承自 BaseBigArrayList 的惰性流机制（核心 + CRUD/Sort/Search/Stream mixin），
    在此基础上提供 append / remove / replace / insert / find / sort /
    binary_search / reverse / copy 等用户级 API。

    所有公共方法在入口处做参数校验（类型 + 范围），非法参数抛
    TypeError / IndexError / ValueError，保证错误尽早暴露。
    想要更精确的异常类型可设 `raise_custom=True`，会改抛
    InvalidArgumentError / IndexOutOfRangeError / DataSourceError
    （均继承对应标准异常）。

    关于惰性与二分查找的冲突：
    - 线性查找 (find) 可保持惰性：流式分块、查到就 yield、不消费剩余
    - 二分查找 (binary_search) 要求"有序 + 随机访问"，因此必须先排好序
      并把结果物化到内存中。这是算法本身的约束，不是设计缺陷。
    - 建议：同一批数据要多次查找时，用 binary_search（一次排序 + 多次 O(log n)）；
      只查一次或少量几次，用 find（不用付排序成本）。

    关于 reverse / copy 的 source 类型限制：
    - int / list / tuple source：完全支持（list/tuple 走 reversed() 视图，不拷贝）
    - generator source：源在 __init__ 中已被 list(...) 消费耗尽，reverse / copy
      会抛 TypeError——请用 list/tuple 构造以支持这两个操作
    """

    # 子类新增字段：
    #   - __raise_custom：是否抛自定义异常（默认 False 保持向后兼容）
    # 注意：父类已声明 __data/__len/__index/__source/__config，
    # 子类不能再在自己的 __slots__ 里重复声明这些双下划线名字。
    __slots__ = ("__raise_custom",)

    def __init__(
        self,
        source: int | Iterable[T],
        *,
        config: BigArrayConfig | None = None,
        raise_custom: bool = False,
        length: int | None = None,
    ) -> None:
        """构造 BigArrayList。

        参数：
            source:       int / list / tuple / 任意可迭代对象
                          （int 表示造 N 个全 0 元素，大数据惰性测试用）
            config:       BigArrayConfig 实例；None 用默认配置
            raise_custom: True 时校验失败抛自定义异常子类
                          （InvalidArgumentError / IndexOutOfRangeError / DataSourceError，
                          均继承 ValueError/IndexError/TypeError），False 抛标准异常
            length:       当 source 没有 __len__（如自定义可迭代类、文件对象）时，
                          可手动指定长度以跳过 list() 物化。
                          要求 source 可重复迭代（非 generator/iterator）；
                          一次性流请用 from_stream() 类方法（磁盘落盘，内存 O(1)）。
        """
        # source 是 None 时父类会抛 TypeError；这里先给清晰错误
        if source is None:
            raise TypeError("source 不能为 None，请传入 int / list / tuple / 迭代器")
        self.__raise_custom = raise_custom
        super().__init__(source, config=config, length=length)

    # ==================== 替代构造器 ====================
    @classmethod
    def from_stream(
        cls,
        source: Iterable[T],
        *,
        config: BigArrayConfig | None = None,
        raise_custom: bool = False,
        length: int | None = None,
    ) -> BigArrayList:
        """从一次性流（generator/iterator）构造，自动磁盘落盘。

        与直接 BigArrayList(generator) 的区别：
            - 直接构造：list(source) 全量物化到内存，大数据会爆内存
            - from_stream：边读边写临时文件，内存 O(1)，可处理远超内存的数据

        落盘后数据可重复迭代（_fresh_iter 从磁盘读），所有算子正常工作。
        元素必须可 pickle（落盘要求）。

        参数：
            source:       任意可迭代对象（通常是 generator/iterator）
            config:       BigArrayConfig 实例；None 用默认配置
            raise_custom: True 时抛自定义异常子类
            length:       可选，已知数据总量；None 时落盘过程中自动统计
        """
        spilled = _DiskSpillIterable(source, length=length)
        return cls(spilled, config=config, raise_custom=raise_custom)  # type: ignore[arg-type]

    # ==================== 校验辅助（委托 utils.validation）====================
    def _v_index(self, name: str, value: object, length: int) -> None:
        validate_index(name, value, length, use_custom=self.__raise_custom)

    def _v_range(self, start: int, end: int) -> None:
        validate_range(start, end, len(self), use_custom=self.__raise_custom)

    def _v_max_workers(self, value: int | None) -> None:
        validate_max_workers(value, use_custom=self.__raise_custom)

    def _v_num_buckets(self, value: int | None) -> None:
        validate_num_buckets(value, use_custom=self.__raise_custom)

    def _v_sample_k(self, value: int) -> None:
        validate_sample_k(value, use_custom=self.__raise_custom)

    def _v_slice_arg(self, name: str, value: int | None) -> None:
        validate_slice_arg(name, value, use_custom=self.__raise_custom)

    def _v_callable(self, name: str, value: object) -> None:
        validate_callable(name, value, use_custom=self.__raise_custom)

    def _v_iterable(self, name: str, value: object) -> None:
        validate_iterable(name, value, use_custom=self.__raise_custom)

    # ===== 向后兼容：保留旧校验方法名（已迁移到 utils.validation） =====
    # 这些方法在重构前是 BigArrayList 的私有静态方法，部分外部代码可能直接 import 调用。
    @staticmethod
    def _validate_index(name: str, value: object, length: int) -> None:
        validate_index(name, value, length, use_custom=False)

    @staticmethod
    def _validate_max_workers(value: int | None) -> None:
        validate_max_workers(value, use_custom=False)

    def _validate_range(self, start: int, end: int) -> None:
        validate_range(start, end, len(self), use_custom=False)

    @staticmethod
    def _validate_callable(name: str, value: object) -> None:
        validate_callable(name, value, use_custom=False)

    @staticmethod
    def _validate_slice_arg(name: str, value: int | None, allow_none: bool = True) -> None:
        # allow_none 参数保留以兼容旧签名；当前实现 None 总是合法
        if value is None and not allow_none:
            raise TypeError(f"{name} 不能为 None")
        validate_slice_arg(name, value, use_custom=False)

    @staticmethod
    def _validate_iterable(name: str, value: object) -> None:
        validate_iterable(name, value, use_custom=False)

    @staticmethod
    def _validate_num_buckets(value: int | None) -> None:
        validate_num_buckets(value, use_custom=False)

    @staticmethod
    def _validate_sample_k(value: int) -> None:
        validate_sample_k(value, use_custom=False)

    # ==================== 增 ====================
    def append(self, *new_items: T) -> Generator[T, None, None]:
        """惰性追加：在原流末尾接上新元素，返回新生成器。

        注意：本方法是普通函数（非生成器函数），调用时立即完成参数处理
        并返回内部生成器对象。返回值仍是惰性流。
        """
        # new_items 为空也合法（等价于原样产出），不报错
        return self._append_iter(*new_items)

    # ==================== 删 ====================
    def remove(self, start: int, end: int) -> Generator[T, None, None]:
        """惰性删除：跳过 [start, end) 范围的元素，其余照常产出。"""
        self._v_range(start, end)
        return self._remove_iter(start, end)

    # ==================== 改 ====================
    def replace(self, start: int, end: int, *new_items: T) -> Generator[T, None, None]:
        """惰性替换：把 [start, end) 范围的元素替换成 new_items。"""
        self._v_range(start, end)
        return self._replace_iter(start, end, *new_items)

    def insert(self, index: int, *new_items: T) -> Generator[T, None, None]:
        """惰性插入：在 index 位置插入 new_items。"""
        self._v_index("index", index, len(self))
        return self._insert_iter(index, *new_items)

    # ==================== 线性查找（惰性并行）====================
    def find(
        self, target: T, max_workers: int | None = None
    ) -> Generator[tuple[int, T], None, None]:
        """惰性并行线性查找：返回生成器，每项为 (全局下标, 命中元素)。

        - 不预排序、不物化：流式分块、多进程并行
        - 查到即 yield，配合 islice 只取前 k 个命中时，可早停省资源
        - 单次 / 少量查找的首选
        """
        self._v_max_workers(max_workers)
        return self._parallel_find(target, max_workers=max_workers)

    # ==================== 二分查找（先排序，再 bisect）====================
    def binary_search(
        self,
        target: T,
        max_workers: int | None = None,
        key: Callable[[T], Any] | None = None,
    ) -> Generator[tuple[int, T], None, None]:
        """二分查找：返回生成器，每项为 (排序后下标, 命中元素)。

        流程：
            1) _big_iter_sort 并行分块排序 → 有序迭代器
            2) 物化为 list（bisect 需要 arr[mid] 随机访问）
            3) bisect_left / bisect_right 定位所有命中位置，逐个 yield

        参数：
            target:      要查找的元素；若 key 不为 None，则比较 key(target)
            max_workers: 排序阶段进程池并行度
            key:         排序键函数（类似 ``sorted(data, key=...)``）；
                         None 按元素本身排序/查找。
                         ⚠️ 多进程路径要求 key 可 pickle（模块级函数或 functools.partial）

        注意：返回的下标是"**排序后**的下标"，不是原始数据的下标。
        """
        self._v_max_workers(max_workers)
        if key is not None:
            self._v_callable("key", key)
        return self._binary_search(target, max_workers=max_workers, key=key)

    # ==================== 分桶查找（哈希分桶 + 单桶扫描）====================
    def bucket_find(
        self,
        target: T,
        key_func: Callable[[T], Any] | None = None,
        num_buckets: int | None = None,
        max_workers: int | None = None,
    ) -> Generator[tuple[int, T], None, None]:
        """分桶查找：按 key 哈希值把数据分到 N 个桶，只扫目标桶定位命中。

        适合"同一批数据要查很多次"的场景（建桶一次 O(n)，之后每次 O(n/N)）。
        与 find 的区别：find 每次全量扫；bucket_find 第一次建桶，后续每次只扫单桶。
        与 binary_search 的区别：返回原始下标（不是排序后下标），不需要排序。

        ⚠️ 本方法每次调用都重建桶（保持零副作用）。
        要真正复用建桶成本，请改用 build_buckets() 返回持久化 BucketIndex。
        """
        if key_func is not None:
            self._v_callable("key_func", key_func)
        self._v_num_buckets(num_buckets)
        self._v_max_workers(max_workers)
        return self._bucket_find(
            target,
            key_func=key_func,
            num_buckets=num_buckets,
            max_workers=max_workers,
        )

    def build_buckets(
        self,
        key_func: Callable[[T], Any] | None = None,
        num_buckets: int | None = None,
    ) -> BucketIndex:
        """持久化分桶：建桶一次，返回可多次查询的 BucketIndex。

        与 bucket_find 的区别：
            - bucket_find：建桶 + 查一次 + 销毁桶，仅适合单次查
            - build_buckets：建桶一次返回 BucketIndex，可多次 find(target)

        使用：
            with big.build_buckets(key_func=lambda u: u.id) as idx:
                for tgt in targets:
                    hits = list(idx.find(tgt))
        """
        if key_func is not None:
            self._v_callable("key_func", key_func)
        self._v_num_buckets(num_buckets)
        return self._build_buckets(
            key_func=key_func,
            num_buckets=num_buckets,
        )

    # ==================== 蓄水池抽样 ====================
    def sample(self, k: int, seed: int | None = None) -> Generator[T, None, None]:
        """蓄水池抽样：从总量未知的流里随机等概率抽 k 个元素。

        一遍流式扫描，内存 O(k)，不需要预先知道总量 N。
        每个元素被选中的概率都是 k/N。
        """
        self._v_sample_k(k)
        return self._sample(k, seed=seed)

    # ==================== 二级索引 ====================
    def build_index(self, key_func: Callable[[T], Any] | None = None) -> dict:
        """建二级索引：dict[key] -> list[原始下标]。

        建好的索引可反复用：
            - 判断 key 在不在：O(1)
            - 拿到所有命中下标列表：O(1)

        注意：惰性流不支持随机访问（arr[i] 抛 NotImplementedError），
        索引存的是下标，要拿元素仍需流式扫。
        """
        if key_func is not None:
            self._v_callable("key_func", key_func)
        return self._build_index(key_func=key_func)

    # ==================== 统一查找入口 ====================
    def auto_find(
        self,
        target: T,
        key_func: Callable[[T], Any] | None = None,
        num_buckets: int | None = None,
        max_workers: int | None = None,
    ) -> Generator[tuple[int, T], None, None]:
        """统一查找入口：根据数据量自动选策略。

        策略：
            - 小数据（< config.linear_parallel_threshold）：线性查找
            - 大数据（>= threshold）：分桶查找

        不做 binary_search（要排序+返回排序后下标，语义不同）。
        不缓存预处理结果（零副作用；查很多次请直接用 build_buckets）。
        """
        if key_func is not None:
            self._v_callable("key_func", key_func)
        self._v_num_buckets(num_buckets)
        self._v_max_workers(max_workers)
        return self._auto_find(
            target,
            key_func=key_func,
            num_buckets=num_buckets,
            max_workers=max_workers,
        )

    # ==================== 排序 ====================
    def sort(
        self,
        max_workers: int | None = None,
        key: Callable[[T], Any] | None = None,
        reverse: bool = False,
    ) -> Generator[T, None, None]:
        """惰性并行排序：返回生成器，按升序（或降序）逐个产出。

        参数：
            max_workers: 进程池并行度；None 用默认
            key:         排序键函数（类似 ``sorted(data, key=...)``）；
                         None 按元素本身排序。
                         ⚠️ 多进程路径（中数据/大数据）要求 key 可 pickle，
                         即模块级函数或 functools.partial；lambda 在 spawn
                         模式下不可 pickle，仅小数据路径（单进程）可用 lambda。
            reverse:     True 降序，False 升序（默认）。全链路支持（小/中/大数据）。

        默认 streaming 模式：归并阶段逐条 load，内存峰值 = O(块数)。
        设 BigArrayConfig(external_sort_streaming_merge=False) 走整块模式
        （速度快 3~5 倍，但内存峰值 ≈ 数据总量）。
        """
        self._v_max_workers(max_workers)
        if key is not None:
            self._v_callable("key", key)
        return self._big_iter_sort(max_workers=max_workers, key=key, reverse=reverse)

    # ==================== 反转 ====================
    def reverse(self) -> Generator[T, None, None]:
        """惰性反转：从尾到头逐个产出元素。"""
        return self._reverse_iter()

    # ==================== 拷贝 ====================
    def copy(self) -> BigArrayList:
        """返回一个包含相同数据的新 BigArrayList 实例（独立迭代器状态）。

        实现策略（按 source 类型分流，与 reverse 一致）：
            - int source：直接用同样的 int 构造
            - list/tuple source：直接复用源容器（list/tuple 可重复迭代）
            - generator source：源已耗尽无法复制，抛 TypeError
        """
        src = self._get_source()  # 通过 _CoreBase 提供的保护方法访问
        if isinstance(src, int):
            return BigArrayList(src)
        if isinstance(src, (list, tuple)):
            return BigArrayList(src)
        raise TypeError(
            "无法对 generator source 执行 copy（源已耗尽）；"
            "请用 list/tuple 构造 BigArrayList(...) 以支持 copy"
        )

    # ==================== 流式算子（裸 generator，向后兼容）====================
    def map(self, func: Callable[[T], U]) -> Generator[U, None, None]:
        """惰性变换：对每个元素应用 func，产出结果流（裸 generator）。"""
        self._v_callable("func", func)
        return self._map_iter(func)

    def filter(self, predicate: Callable[[T], bool]) -> Generator[T, None, None]:
        """惰性过滤：只产出 predicate(item) 为真的元素（裸 generator）。"""
        self._v_callable("predicate", predicate)
        return self._filter_iter(predicate)

    def reduce(self, func: Callable[[U, T], U], initial: U | None = None) -> U:
        """流式聚合：acc = func(acc, item)，逐元素累加，返回最终 acc。"""
        self._v_callable("func", func)
        return self._reduce(func, initial=initial)

    def slice(
        self,
        start: int | None = None,
        stop: int | None = None,
        step: int | None = None,
    ) -> Generator[T, None, None]:
        """惰性切片：包装 itertools.islice，支持步长。

        约束：start/stop/step 不允许负数（惰性流无法反向）。
        """
        self._v_slice_arg("start", start)
        self._v_slice_arg("stop", stop)
        self._v_slice_arg("step", step)
        if step is not None and step == 0:
            raise ValueError("step 不能为 0")
        return self._slice_iter(start, stop, step)

    def distinct(self) -> Generator[T, None, None]:
        """惰性去重：保留每个元素的首次出现（裸 generator）。"""
        return self._distinct_iter()

    def concat(self, other: Iterable[T]) -> Generator[T, None, None]:
        """惰性合并：把 self 和 other 首尾相接产出（裸 generator）。"""
        self._v_iterable("other", other)
        return self._concat_iter(other)

    def chain(self, *others: Iterable[T]) -> Generator[T, None, None]:
        """惰性链式合并：把 self 和多个 others 首尾相接产出（裸 generator）。"""
        for i, o in enumerate(others):
            self._v_iterable(f"others[{i}]", o)
        return self._chain_iter(*others)

    def group_by(self, key_func: Callable[[T], Any]) -> Generator[tuple[Any, list[T]], None, None]:
        """SQL 风格 GROUP BY：按 key_func 分组，yield (key, list[elements])。"""
        self._v_callable("key_func", key_func)
        return self._group_by_iter(key_func)

    def count_if(self, predicate: Callable[[T], bool]) -> int:
        """流式条件计数：统计 predicate(item) 为真的元素个数。"""
        self._v_callable("predicate", predicate)
        return self._count_if(predicate)

    # ==================== 聚合（单遍 O(n)，O(1) 内存）====================
    def min(
        self,
        key: Callable[[T], Any] | None = None,
        default: T | None = None,
    ) -> T | None:
        """返回最小元素。空数据返回 default。

        参数：
            key:     比较键函数；None 按元素本身比较
            default: 数据为空时的返回值
        """
        if key is not None:
            self._v_callable("key", key)
        return self._min(key=key, default=default)

    def max(
        self,
        key: Callable[[T], Any] | None = None,
        default: T | None = None,
    ) -> T | None:
        """返回最大元素。空数据返回 default。

        参数：
            key:     比较键函数；None 按元素本身比较
            default: 数据为空时的返回值
        """
        if key is not None:
            self._v_callable("key", key)
        return self._max(key=key, default=default)

    def sum(self) -> Any:
        """求和。元素需支持 + 运算符（数值等）。空数据返回 0。"""
        return self._sum()

    def avg(self) -> float:
        """求平均值。空数据抛 ValueError。"""
        return self._avg()

    def count(self) -> int:
        """返回元素总数（等价于 len(self)）。"""
        return len(self)

    # ==================== 链式 API（新功能，不破坏旧 API）====================
    def lazy(self) -> LazyChain[T]:
        """切换到链式 API：返回 LazyChain 包装的 self._fresh_iter()。

        用法：
            result = (big.lazy()
                        .filter(lambda x: x > 0)
                        .map(lambda x: x * 2)
                        .distinct()
                        .slice(0, 10)
                        .collect())  # 物化为 list

        与旧 .map() / .filter() 的区别：
            - 旧方法返回裸 generator，不能 .filter().map() 链式
            - .lazy() 返回 LazyChain，每个算子返回新 LazyChain，可链式组合
        """
        return super().lazy()


__all__ = ["BigArrayConfig", "BigArrayList", "BucketIndex", "LazyChain", "ScaleHelper"]
