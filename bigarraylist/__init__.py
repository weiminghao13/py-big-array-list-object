"""BigArrayList：面向大数据量场景的惰性流式列表工具包。

公共 API：
    BigArrayList   —— 用户接口层（增删改插 / 查找 / 排序 / 流式算子 / 链式 API）
    BigArrayConfig  —— 阈值配置对象（可调外部排序模式、桶上限等）
    LazyChain       —— 链式算子（big.lazy().filter().map().collect()）
    BucketIndex     —— 持久化分桶索引（build_buckets 返回，多次 find 复用）
    exceptions      —— 自定义异常层次（继承标准异常，向后兼容）

    ScaleHelper / ScaleValue / choose_chunk_size / choose_external_sort_chunk_size /
    chunk_wise_processing —— 向后兼容保留

典型用法：
    from bigarraylist import BigArrayList
    arr = BigArrayList([3, 1, 4, 1, 5, 9, 2, 6])
    for x in arr.sort():
        print(x)

    # 链式 API（新功能）
    result = (arr.lazy()
                 .filter(lambda x: x > 2)
                 .map(lambda x: x * 10)
                 .distinct()
                 .collect())

    # 持久化分桶（多次查询复用）
    with arr.build_buckets() as idx:
        for tgt in targets:
            hits = list(idx.find(tgt))
"""

from ._api import BigArrayList
from .algorithms.bucket import BucketIndex, build_buckets
from .config import BigArrayConfig
from .exceptions import (
    BigArrayListError,
    DataSourceError,
    IndexOutOfRangeError,
    InvalidArgumentError,
)
from .operators.stream import LazyChain
from .utils.scale import (
    ScaleHelper,
    ScaleValue,
    choose_chunk_size,
    choose_external_sort_chunk_size,
    chunk_wise_processing,
)

__all__ = [
    "BigArrayConfig",
    # 主类
    "BigArrayList",
    # 异常
    "BigArrayListError",
    "BucketIndex",
    "DataSourceError",
    "IndexOutOfRangeError",
    "InvalidArgumentError",
    "LazyChain",
    # 向后兼容：量级助手
    "ScaleHelper",
    "ScaleValue",
    "build_buckets",
    "choose_chunk_size",
    "choose_external_sort_chunk_size",
    "chunk_wise_processing",
]

__version__ = "0.3.0"
