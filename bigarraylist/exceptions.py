"""自定义异常层次。

向后兼容策略：所有自定义异常都继承自 ValueError / TypeError / IndexError，
现有 `except ValueError:` / `except TypeError:` / `except IndexError:` 调用方代码
无需任何修改即可继续工作；想精确捕获的调用方可改用 BigArrayListError 子类。

异常层次：

    BigArrayListError                ← 所有自定义异常的根
    ├── InvalidArgumentError(ValueError)   参数类型/取值不合法
    ├── IndexOutOfRangeError(IndexError)   索引越界（start/end/index）
    └── DataSourceError(TypeError)        source 类型不支持某操作（reverse/copy generator 源等）

未来如有更细的需求，可继续在此层次下加子类。
"""

from __future__ import annotations


class BigArrayListError(Exception):
    """所有 BigArrayList 自定义异常的根。

    继承 Exception 而非 ValueError/TypeError：本类是包内统一标识。
    具体子类按语义分别继承 ValueError/IndexError/TypeError 以保证向后兼容。
    """


class InvalidArgumentError(BigArrayListError, ValueError):
    """参数类型/取值不合法（如 max_workers < 1、num_buckets 为负数、step=0）。"""


class IndexOutOfRangeError(BigArrayListError, IndexError):
    """索引越界（start/end/index 超出 [0, len] 范围）。"""


class DataSourceError(BigArrayListError, TypeError):
    """source 类型不支持某操作。

    例：
        - generator source 已被耗尽，无法 reverse / copy
        - 不可哈希元素走小数据 distinct 分支
    """


__all__ = [
    "BigArrayListError",
    "DataSourceError",
    "IndexOutOfRangeError",
    "InvalidArgumentError",
]
