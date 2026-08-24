"""参数校验工具：所有校验在调用时立即执行（参数非法立刻抛错，不等迭代）。

向后兼容：默认仍抛 TypeError / IndexError / ValueError；
当 raise_big_array_list_error=True 时改抛自定义异常子类（仍继承对应标准异常）。
"""

from __future__ import annotations

from collections.abc import Iterable

from ..exceptions import (
    DataSourceError,
    IndexOutOfRangeError,
    InvalidArgumentError,
)


def _raise(
    err_cls: type[Exception],
    std_cls: type[Exception],
    msg: str,
    use_custom: bool,
) -> None:
    """统一抛错：use_custom=True 走自定义异常（仍继承标准异常），
    False 抛标准异常保持向后兼容。"""
    if use_custom:
        raise err_cls(msg) from None
    raise std_cls(msg) from None


def validate_index(name: str, value: object, length: int, *, use_custom: bool = False) -> None:
    """校验单个索引参数：必须是 int（排除 bool）、范围 [0, length]。"""
    if isinstance(value, bool) or not isinstance(value, int):
        _raise(
            InvalidArgumentError,
            TypeError,
            f"{name} 必须是整数，得到 {type(value).__name__}",
            use_custom,
        )
    elif value < 0 or value > length:
        _raise(
            IndexOutOfRangeError,
            IndexError,
            f"{name}={value} 越界，有效范围 [0, {length}]",
            use_custom,
        )


def validate_range(
    start: int,
    end: int,
    length: int,
    *,
    use_custom: bool = False,
) -> None:
    """校验 [start, end) 区间：类型对、0 <= start <= end <= length。"""
    validate_index("start", start, length, use_custom=use_custom)
    validate_index("end", end, length, use_custom=use_custom)
    if start > end:
        _raise(
            InvalidArgumentError,
            ValueError,
            f"start={start} 不能大于 end={end}",
            use_custom,
        )


def validate_max_workers(value: int | None, *, use_custom: bool = False) -> None:
    """校验 max_workers：None 或正整数（排除 bool）。"""
    if value is None:
        return
    if isinstance(value, bool) or not isinstance(value, int):
        _raise(
            InvalidArgumentError,
            TypeError,
            f"max_workers 必须是正整数或 None，得到 {type(value).__name__}",
            use_custom,
        )
    elif value < 1:
        _raise(
            InvalidArgumentError,
            ValueError,
            f"max_workers 必须 >= 1，得到 {value}",
            use_custom,
        )


def validate_num_buckets(value: int | None, *, use_custom: bool = False) -> None:
    """校验 num_buckets：None 或正整数（排除 bool）。"""
    if value is None:
        return
    if isinstance(value, bool) or not isinstance(value, int):
        _raise(
            InvalidArgumentError,
            TypeError,
            f"num_buckets 必须是正整数或 None，得到 {type(value).__name__}",
            use_custom,
        )
    elif value < 1:
        _raise(
            InvalidArgumentError,
            ValueError,
            f"num_buckets 必须 >= 1，得到 {value}",
            use_custom,
        )


def validate_sample_k(value: int, *, use_custom: bool = False) -> None:
    """校验 sample 的 k：正整数（排除 bool）。"""
    if isinstance(value, bool) or not isinstance(value, int):
        _raise(
            InvalidArgumentError,
            TypeError,
            f"k 必须是正整数，得到 {type(value).__name__}",
            use_custom,
        )
    elif value < 1:
        _raise(
            InvalidArgumentError,
            ValueError,
            f"k 必须 >= 1，得到 {value}",
            use_custom,
        )


def validate_slice_arg(name: str, value: int | None, *, use_custom: bool = False) -> None:
    """校验 slice 的 start/stop/step 参数。

    - 允许 None（表示"不限制"）
    - 不允许负数（惰性流无法反向、无法预知总长倒数）
    - bool 排除（语义不该当索引用）
    """
    if value is None:
        return
    if isinstance(value, bool) or not isinstance(value, int):
        _raise(
            InvalidArgumentError,
            TypeError,
            f"{name} 必须是整数或 None，得到 {type(value).__name__}",
            use_custom,
        )
    elif value < 0:
        _raise(
            InvalidArgumentError,
            ValueError,
            f"{name}={value} 不允许为负数（惰性流不支持反向切片）",
            use_custom,
        )


def validate_callable(name: str, value: object, *, use_custom: bool = False) -> None:
    """校验一个参数必须可调用（callable）。None 不视为 callable。"""
    if not callable(value):
        _raise(
            InvalidArgumentError,
            TypeError,
            f"{name} 必须是可调用对象（函数/lambda/可调用类实例），得到 {type(value).__name__}",
            use_custom,
        )


def validate_iterable(name: str, value: object, *, use_custom: bool = False) -> None:
    """校验一个参数必须是可迭代对象（Iterable）。"""
    if isinstance(value, Iterable):
        return
    _raise(
        DataSourceError,
        TypeError,
        f"{name} 必须是可迭代对象（Iterable），得到 {type(value).__name__}",
        use_custom,
    )
