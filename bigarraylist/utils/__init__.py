"""工具层：量级判定 + 参数校验。"""

from .scale import (
    HUNDRED_MILLION,
    HUNDRED_THOUSAND,
    MILLION,
    TEN_MILLION,
    ScaleHelper,
    ScaleValue,
    choose_chunk_size,
    choose_external_sort_chunk_size,
    chunk_wise_processing,
)
from .validation import (
    validate_callable,
    validate_index,
    validate_iterable,
    validate_max_workers,
    validate_num_buckets,
    validate_range,
    validate_sample_k,
    validate_slice_arg,
)

__all__ = [
    # 量级
    "HUNDRED_MILLION",
    "HUNDRED_THOUSAND",
    "MILLION",
    "TEN_MILLION",
    "ScaleHelper",
    "ScaleValue",
    "choose_chunk_size",
    "choose_external_sort_chunk_size",
    "chunk_wise_processing",
    # 校验
    "validate_callable",
    "validate_index",
    "validate_iterable",
    "validate_max_workers",
    "validate_num_buckets",
    "validate_range",
    "validate_sample_k",
    "validate_slice_arg",
]
