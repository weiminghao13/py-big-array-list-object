"""BaseBigArrayList：迭代协议 + _fresh_iter + __slots__。

通过 cooperative multiple inheritance 组合各算子 mixin：
    BaseBigArrayList(_CoreBase, CRUDMixin, StreamMixin, SortMixin, SearchMixin)

各 mixin 只声明方法、不声明字段（避免 __slots__ 多继承冲突），
通过 self._fresh_iter() 和 len(self) 与 _CoreBase 协作。
蓄水池抽样 _sample 直接写在 _CoreBase 里（依赖 reservoir_sample 函数即可，无需 mixin）。
"""

from __future__ import annotations

import os
import tempfile
from collections.abc import Generator, Iterable, Iterator
from typing import Any, Generic, TypeVar

from ..algorithms._pool import shutdown_pool  # noqa: F401  # 确保模块导入即注册 atexit
from ..algorithms.fileio import IO_BUFFER_SIZE, dump_frame, frames_iter
from ..algorithms.sample import reservoir_sample
from ..config import BigArrayConfig
from ..operators.crud import CRUDMixin
from ..operators.search import SearchMixin
from ..operators.sort import SortMixin
from ..operators.stream import StreamMixin

T = TypeVar("T")


class _DiskSpillIterable(Iterable[T]):
    """把一次性迭代器（generator/iterator）的数据全量落盘到临时文件。

    构造时消费迭代器并按自描述帧格式落盘（每批一个 list，内存 O(batch_size)）：
    整批全 int → int64 紧凑二进制帧，全 float → float64 帧，否则 pickle 帧
    （P0-1：批量比逐条快 10x+；方案②：数值流全程零 pickle，写读更省）。
    之后 __iter__ 从磁盘读，可重复迭代——解决 generator 源无法被
    _fresh_iter() 反复消费的问题。

    与 list(source) 的区别：数据放磁盘而非内存，能处理远超内存的数据量。
    代价：构造有磁盘 IO；非数值元素仍需可 pickle（数值批不需要）。

    生命周期：持有临时文件路径，__del__ / close() 时删除。
    """

    __slots__ = ("_batch_size", "_length", "_path")

    def __init__(
        self,
        source: Iterable[T],
        *,
        length: int | None = None,
        batch_size: int = 4096,
    ) -> None:
        self._length = length
        self._batch_size = batch_size
        # 先占位 None：构造中途抛异常（如元素不可 pickle）时 __del__/close() 依赖它
        self._path: str | None = None
        tmp = tempfile.NamedTemporaryFile(
            suffix=".tmp",
            prefix="big_stream_",
            delete=False,
            buffering=IO_BUFFER_SIZE,
        )
        # 边写边数（source 可能是一次性 iterator，只能消费一次）
        count = 0
        try:
            buf: list[T] = []
            for item in source:
                buf.append(item)
                if len(buf) >= batch_size:
                    dump_frame(tmp, buf)
                    count += len(buf)
                    buf = []
            if buf:
                dump_frame(tmp, buf)
                count += len(buf)
            tmp.close()
        except BaseException:
            tmp.close()
            try:
                os.unlink(tmp.name)
            except OSError:
                pass
            raise
        self._path = tmp.name
        if self._length is None:
            self._length = count

    def __iter__(self) -> Iterator[T]:
        # __init__ 成功才可能有迭代；构造失败时对象不会活到被迭代
        assert self._path is not None, "_DiskSpillIterable 构造未完成"
        with open(self._path, "rb", buffering=IO_BUFFER_SIZE) as f:
            yield from frames_iter(f)

    def __len__(self) -> int:
        return self._length  # type: ignore[return-value]  # __init__ 保证非 None

    def close(self) -> None:
        """删除临时文件，幂等；构造失败（_path 为 None）时为空操作。"""
        path = self._path
        self._path = None
        if path is None:
            return
        try:
            os.unlink(path)
        except OSError:
            pass

    def __del__(self) -> None:
        self.close()


def _is_reiterable(obj: object) -> bool:
    """判断对象是否可重复迭代（不是 iterator/generator 这种一次性的）。

    iter(obj) is iter(obj) 为 True → 是 iterator（一次性）；为 False → 是
    可重复迭代的 container（list/tuple/range/file object 等）。
    对标准类型无副作用（iter() 不消费元素）。
    """
    try:
        return iter(obj) is not iter(obj)  # type: ignore[call-overload]
    except TypeError:
        return False


class _CoreBase(Generic[T]):
    """核心：__slots__ + 迭代协议 + _fresh_iter。

    所有算子方法（_append_iter / find / sort / ...）由各 mixin 提供，
    它们统一通过本类的 _fresh_iter() 和 len(self) 与核心交互，
    不直接访问 self.__source / self.__len 等私有字段——
    避免 name mangling 在 mixin 模式下找不到属性。

    继承 Generic[T] 让 T 在本类内被绑定，使 _fresh_iter / __next__ 等方法
    的返回类型 T 能被 mypy 推导；运行时 Generic 不引入额外开销。
    """

    __slots__ = ("__config", "__data", "__fast_cache", "__index", "__len", "__source")

    # 类型标注（mypy 用，运行时被 __slots__ 覆盖）
    __config: BigArrayConfig
    __data: Iterator[Any]
    __fast_cache: tuple[int, int, Any] | None
    __index: int
    __len: int
    __source: int | Iterable[Any]

    def __init__(
        self,
        source: int | Iterable[T],
        *,
        config: BigArrayConfig | None = None,
        length: int | None = None,
    ) -> None:
        """三种构造方式：
        - source 是 int：用生成器造 source 个全 0 数据（大数据惰性测试用）
        - source 是 Sized Iterable（list/tuple 等有 __len__ 的）：直接用其 len
        - source 是纯 Iterator/Generator（没 len 的）：
            * 若传了 length 且源可重复迭代：用 length，零物化
            * 否则：list(source) 物化拿长度（大数据不推荐，建议用 from_stream）
        """
        # 保存配置（None 用默认）
        self.__config = config if config is not None else BigArrayConfig.default()

        # 保存原始 source，供重置迭代器用
        self.__source = source
        if isinstance(source, int):
            self.__data = iter(0 for _ in range(source))
            self.__len = source
        else:
            try:
                # 优先走 __len__ 路径（list/tuple/str 等容器，零物化）
                data_len = len(source)  # type: ignore[arg-type]
            except TypeError:
                # 没 __len__ 的可迭代对象
                if length is not None and _is_reiterable(source):
                    # 用户提供了长度且源可重复迭代：跳过物化，真正惰性
                    self.__data = iter(source)
                    self.__len = length
                else:
                    # 没提供长度，或源是一次性 iterator：必须物化拿长度
                    materialized = list(source)
                    # ✅ 关键修复：source 已在 list() 时耗尽，必须更新 __source 为物化后的 list，
                    # 否则 _fresh_iter() 会返回空迭代器，所有算子产出空数据
                    self.__source = materialized
                    self.__data = iter(materialized)
                    self.__len = len(materialized)
            else:
                # 有 __len__：list/tuple 等可重复迭代容器，零物化
                self.__data = iter(source)
                self.__len = data_len
        self.__index = 0  # 当前已消费的位置（惰性迭代用）
        self.__fast_cache = None  # 数值快路径数组缓存（_fast 层写入）

    # ===== 长度 / 索引 =====
    def __len__(self) -> int:
        return self.__len

    def __repr__(self) -> str:
        """友好表示：BigArrayList(len=1000000, source=list)。"""
        src = self.__source
        if isinstance(src, int):
            src_type = "int"
        elif isinstance(src, _DiskSpillIterable):
            src_type = "disk-spilled"
        elif isinstance(src, list):
            src_type = "list"
        elif isinstance(src, tuple):
            src_type = "tuple"
        else:
            src_type = type(src).__name__
        return f"{type(self).__name__}(len={self.__len}, source={src_type})"

    def __getitem__(self, index: int) -> T:
        # 大数据不支持随机访问：随机访问要求把数据全部物化进内存，
        # 这与"惰性流"的核心设计冲突。要用第 N 个元素，请用 itertools.islice 流式跳过
        raise NotImplementedError(
            "Index access is not supported for big data, use itertools.islice instead"
        )

    # ===== 迭代协议 =====
    def __iter__(self) -> Iterator[T]:
        # 惰性迭代：返回 self，让 for 循环调用 __next__ 逐个取值
        # 不预读任何数据，next 一次产出一个
        return self

    def __next__(self) -> T:
        # 标准 Python 迭代器协议：耗尽抛 StopIteration。
        # 这样 `for x in big` / `list(big)` 都能正常结束，
        # 不会因为循环语义而卡死。
        try:
            item = next(self.__data)
        except StopIteration:
            raise
        self.__index += 1
        return item  # type: ignore[no-any-return]  # __data 持 Any，运行时为 T

    # ===== 配置访问（mixin 用）=====
    def _get_config(self) -> BigArrayConfig:
        """返回实例的 BigArrayConfig（供算子读取阈值）。"""
        return self.__config

    # ===== 核心机制：独立迭代器 =====
    def _fresh_iter(self) -> Iterator[T]:
        """返回一份基于原始 source 的全新独立迭代器。

        所有算子（_append_iter / _remove_iter / find / sort / ...）
        统一通过本方法拿一份**独立**的局部迭代器，全程操作局部变量，
        绝不碰 self.__data——避免算子之间互相污染实例状态。

        self.__data 只留给 __next__ 对外 for 循环用。
        """
        src = self.__source
        if isinstance(src, int):
            # int source：生成全 0 流（int 兼容 T 由调用方语义保证，类型层无法表达）
            return iter(0 for _ in range(src))  # type: ignore[misc]
        return iter(src)

    def cycle(self) -> Generator[T, None, None]:
        """无限循环迭代器：数据耗尽后从头再来，永不停止。

        想要"耗尽不停止"的循环语义时用这个，而不是直接 for x in big。
        用法：
            for x in itertools.islice(big.cycle(), 100):  # 只取 100 个
                ...
            for x in big.cycle():  # 真无限循环，必须自己 break
                if cond: break
        """
        while True:
            yield from self._fresh_iter()

    def __contains__(self, target: object) -> bool:
        """x in big_arr：线性查找 target 是否存在于数据中。

        ⚠️ 关键设计：用 _fresh_iter() 拿**独立**迭代器查找，
        不消费 self.__data——算子调用零副作用，可以重复调用。
        找不到时返回 False（耗尽的是临时迭代器，不影响实例状态）。
        """
        return any(item == target for item in self._fresh_iter())

    # ===== 反向 / 复制（与 source 类型强耦合，放核心层）=====
    def _reverse_iter(self) -> Generator[T, None, None]:
        """反向迭代器：从尾到头逐个产出元素。

        按 source 类型分流：
            - int source（全 0 测试源）：反转仍是全 0，直接生成 N 个 0
            - 支持 reversed() 的容器（list/tuple/str/range 等）：零拷贝反向视图
            - 其余（set、from_stream 落盘流等）：抛 TypeError

        ⚠️ 本实现不物化源数据：对 from_stream 的落盘流反向需先把整份数据
        读进内存，与"惰性低内存"设计冲突，故直接拒绝而非静默物化——这是
        有意的安全选择。要反向就先自行物化（list(big)）再构造。
        """
        src = self.__source
        if isinstance(src, int):
            for _ in range(src):
                yield 0  # type: ignore[misc]  # int 兼容 T 由调用方语义保证
            return
        try:
            yield from reversed(src)  # type: ignore[call-overload]  # src 运行时须支持 __reversed__/__getitem__
        except TypeError as e:
            if isinstance(src, _DiskSpillIterable):
                raise TypeError(
                    "from_stream 的落盘流不支持 reverse（反向需把整份数据读入内存，"
                    "与低内存设计冲突）；请先 list(big) 物化后再构造，或改用其他方案"
                ) from e
            raise TypeError(
                f"{type(src).__name__} 源不支持 reverse（reversed() 需要 list/tuple/str/"
                "range 等实现了 __reversed__ 或 __getitem__ 的类型）；请用 list/tuple 构造"
            ) from e

    def _copy_iter(self) -> Generator[T, None, None]:
        """复制迭代器：从头到尾逐个产出元素。

        全程操作 _fresh_iter()，不碰 self.__data。
        """
        yield from self._fresh_iter()

    # ===== 供子类（_api.BigArrayList）访问原始 source（copy/构造新实例） =====
    def _get_source(self) -> int | Iterable[Any]:
        """返回原始 source（int / list / tuple / ...）。

        供 BigArrayList.copy() 等需要构造新实例的场景使用。
        返回的是原始引用，不复制——调用方请勿修改。
        """
        return self.__source

    # ===== 数值快路径缓存（_fast 层读取/写入，避免每次算子重转换）=====
    def _get_fast_cache(self) -> tuple[int, int, Any] | None:
        """返回 (源id, 源长度, 紧凑数值数组) 缓存；无缓存时为 None。

        本层只负责存取不透明值，不依赖 _fast（避免循环导入）。
        """
        return self.__fast_cache

    def _set_fast_cache(self, value: tuple[int, int, Any] | None) -> None:
        """写入数值快路径缓存（None 表示清空）。"""
        self.__fast_cache = value

    # ===== 蓄水池抽样（直接放核心层，无需 mixin）=====
    def _sample(self, k: int, seed: int | None = None) -> Generator[T, None, None]:
        """蓄水池抽样：从总量未知的流里随机等概率抽 k 个元素。

        数学保证：每个元素被选中的概率都是 k/N（N 为实际总量，无需预先知道）。
        内存 O(k)，无需预知 N。

        参数：
            k:    要抽取的样本数（必须 >= 1，调用方校验）
            seed: 随机种子，None 表示系统随机；传 int 可复现结果
        """
        yield from reservoir_sample(self._fresh_iter(), k, seed=seed)


class BaseBigArrayList(_CoreBase[T], CRUDMixin[T], StreamMixin[T], SortMixin[T], SearchMixin[T]):
    """大数据惰性流基类：组合核心协议 + 所有算子 mixin。

    对外 API 由 _api.BigArrayList 提供（继承本类 + 参数校验）。
    本类供"需要直接访问底层算子、不要参数校验"的场景使用。
    """

    pass


__all__ = [
    "BaseBigArrayList",
    "BigArrayConfig",
    "_CoreBase",
    "_DiskSpillIterable",
    "reservoir_sample",
]
