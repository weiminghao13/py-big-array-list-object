# API 参考

> 公共入口：
> ```python
> from bigarraylist import (
>     BigArrayList, BigArrayConfig, LazyChain, BucketIndex, build_buckets,
>     ScaleHelper,
>     BigArrayListError, InvalidArgumentError, IndexOutOfRangeError, DataSourceError,
> )
> ```

## 构造

```python
BigArrayList(source, *, config=None, raise_custom=False)
```

- `source` 为 `int`：生成 N 个 0（大数据惰性测试用）。
- `source` 为 `list` / `tuple`（有 `__len__`）：直接用其长度，惰性产出。
- `source` 为 generator / 无 `__len__` 的迭代器：构造时**物化**拿长度（大数据不推荐）。
- `config: BigArrayConfig | None`：阈值配置；`None` 用 `BigArrayConfig.default()`。
- `raise_custom: bool`：`True` 时校验失败抛自定义异常子类（仍继承标准异常）；
  默认 `False` 抛标准异常（向后兼容）。

## 迭代

| 方法 | 说明 |
| --- | --- |
| `__len__()` | 元素总数 |
| `__iter__()` / `__next__()` | 惰性逐个产出；耗尽抛 `StopIteration` |
| `__contains__(target)` | `x in arr`；用 `_fresh_iter()`，零副作用 |
| `__getitem__(i)` | **不支持**（抛 `NotImplementedError`），用 `itertools.islice` |
| `cycle()` | 无限循环生成器 |

## CRUD（惰性，返回生成器）

| 方法 | 签名 |
| --- | --- |
| `append(*items)` | 末尾接上新元素 |
| `remove(start, end)` | 跳过 `[start, end)` |
| `replace(start, end, *items)` | 替换 `[start, end)` 为 items |
| `insert(index, *items)` | 在 index 处插入 |

## 查找（返回生成器 `(下标, 值)`）

| 方法 | 签名 | 说明 |
| --- | --- | --- |
| `find(target, max_workers=None)` | 线性并行，原始下标，惰性可早停 |
| `binary_search(target, max_workers=None)` | 排序后下标 |
| `bucket_find(target, key_func=None, num_buckets=None, max_workers=None)` | 哈希分桶，原始下标，**每次重建桶** |
| `build_buckets(key_func=None, num_buckets=None) -> BucketIndex` | 建桶一次，多次 `find` 复用（见下） |
| `sample(k, seed=None)` | 蓄水池抽样 k 个 |
| `build_index(key_func=None) -> dict` | `dict[key] -> list[下标]` |
| `auto_find(target, key_func=None, num_buckets=None, max_workers=None)` | 自动选策略 |

> `bucket_find` vs `build_buckets`：
> - `bucket_find`：建桶 + 查一次 + 销毁桶，**仅适合单次查**。
> - `build_buckets`：建桶一次返回 `BucketIndex`，可多次 `find`，**真正复用建桶成本**。

## 排序 / 反转 / 拷贝

| 方法 | 说明 |
| --- | --- |
| `sort(max_workers=None)` | 惰性升序生成器，三级分流；默认 streaming 外部排序（低内存） |
| `reverse()` | 反转生成器（list/tuple 源走 `reversed()` 视图，generator 源抛 `TypeError`） |
| `copy() -> BigArrayList` | 独立实例（generator 源抛 `TypeError`） |

## 流式算子（裸 generator，向后兼容）

| 方法 | 说明 |
| --- | --- |
| `map(func)` | 对每元素应用 func |
| `filter(predicate)` | 保留 predicate 为真的 |
| `reduce(func, initial=None)` | 流式聚合，O(1) 内存 |
| `slice(start=None, stop=None, step=None)` | 包装 `itertools.islice`，不支持负数 |
| `distinct()` | 保留首次出现（小数据 set / 大数据排序去重） |
| `concat(other)` | 首尾相接 |
| `chain(*others)` | 多流首尾相接 |
| `group_by(key_func)` | `yield (key, list)`，小数据全局合并 |
| `count_if(predicate) -> int` | 条件计数，O(1) 内存 |

> ⚠️ 这些方法返回**裸 generator**，不能 `.filter().map()` 链式调用。
> 要链式组合请用下面的 `.lazy()`。

## 链式 API（新）：`.lazy()` → `LazyChain`

```python
result = (arr.lazy()
             .filter(lambda x: x > 0)
             .map(lambda x: x * 2)
             .distinct()
             .slice(0, 10)
             .collect())           # 物化为 list
```

`LazyChain` 内部组合一个迭代器（不继承 `BigArrayList`），每个算子返回新 `LazyChain`，
全程惰性，只有终结方法才真正消费。

### 链式算子（返回新 `LazyChain`）

| 方法 | 说明 |
| --- | --- |
| `map(func)` | 对每元素应用 func |
| `filter(predicate)` | 过滤 |
| `slice(start=None, stop=None, step=None)` | 惰性切片（不支持负数） |
| `take(n)` | 取前 n 个 |
| `drop(n)` | 丢弃前 n 个 |
| `distinct()` | 去重（保留首次出现，内存 O(k)，元素须可哈希） |
| `concat(other)` | 首尾相接 |
| `chain(*others)` | 多流合并 |
| `group_by(key_func)` | `yield (key, list)`，要求上游已按 key 排序 |

### 终结算子（消费迭代器，返回值）

| 方法 | 返回 | 说明 |
| --- | --- | --- |
| `collect()` | `list` | 物化 |
| `reduce(func, initial=None)` | 聚合值 | 流式聚合 |
| `count_if(predicate)` | `int` | 条件计数 |
| `count()` | `int` | 总数 |
| `first(default=None)` | 元素 / default | 取第一个；空流返回 default |

`LazyChain` 也支持迭代协议：`for x in chain` / `list(chain)` 等价于 `.collect()`。

## `BucketIndex`（持久化分桶索引）

`build_buckets()` 返回的对象，建桶一次后可多次 `find`。

```python
with arr.build_buckets(key_func=lambda u: u.id) as idx:
    for tgt in targets:
        hits = list(idx.find(tgt))   # 每次只扫目标桶 O(n/N)
```

| 成员 | 说明 |
| --- | --- |
| `find(target) -> Generator[(下标, 值)]` | 扫目标桶定位所有命中 |
| `num_buckets` | 桶数（只读属性） |
| `close()` | 清理所有桶临时文件（幂等） |
| `__enter__` / `__exit__` | 上下文管理器，退出自动 `close()` |

> 也可脱离 `with` 使用，但需自行 `close()` 释放临时文件。

## `BigArrayConfig`（配置对象）

`frozen` dataclass，所有阈值集中可配。不传 `config` 时用 `BigArrayConfig.default()`，
所有字段值与 0.1.0 硬编码一致。

```python
cfg = BigArrayConfig(external_sort_streaming_merge=False)  # 走整块模式（快 3~5 倍）
arr = BigArrayList(data, config=cfg)
```

| 字段 | 默认 | 说明 |
| --- | --- | --- |
| `linear_parallel_threshold` | `100_000` | 数据量 ≥ 此值才走多进程查找 |
| `chunk_size_map` | `None` | 内存排序分块映射表；`None` 用内置默认 |
| `external_sort_chunk_target` | `32` | 外部排序目标块数（32 路归并） |
| `external_sort_min_chunk` | `100_000` | 每块至少 10 万条 |
| `external_sort_max_chunk` | `5_000_000` | 每块最多 500 万条 |
| `external_sort_threshold` | `1_000_000` | 数据量 ≥ 此值才走外部排序 |
| `external_sort_streaming_merge` | `True` | **归并模式**：`True`=逐条 load（低内存，默认）；`False`=整块 load（快 3~5 倍，内存 ≈ 总量） |
| `bucket_max_count` | `1024` | 桶数上限（文件描述符约束） |

类级方法：

| 方法 | 说明 |
| --- | --- |
| `BigArrayConfig.default() -> BigArrayConfig` | 当前生效的默认配置 |
| `BigArrayConfig.set_default(**kwargs) -> BigArrayConfig` | 类级覆盖默认值，影响后续所有未传 config 的实例 |
| `BigArrayConfig.reset_default()` | 恢复内置默认 |

## 异常层次（新）

```
BigArrayListError                          ← 根（继承 Exception）
├── InvalidArgumentError(ValueError)     参数类型/取值不合法
├── IndexOutOfRangeError(IndexError)     索引越界
└── DataSourceError(TypeError)           source 类型不支持某操作
```

**向后兼容**：子类同时继承标准异常，现有 `except ValueError:` 等代码无需改动。
默认抛标准异常；`BigArrayList(..., raise_custom=True)` 启用自定义异常子类。

```python
from bigarraylist import BigArrayList, InvalidArgumentError

arr = BigArrayList([1, 2, 3], raise_custom=True)
try:
    arr.remove(1, 999)          # 越界
except InvalidArgumentError as e:
    ...                         # 精确捕获（也能被 except ValueError 捕获）
```

## 校验约定

所有公共方法在**调用时立即**校验参数（类型 + 范围），非法参数当场抛 `TypeError` /
`IndexError` / `ValueError`（或 `raise_custom=True` 时的对应自定义子类），不推迟到迭代。
例如 `remove(1, 3)` 越界会立刻报错，不会等 `next()` 才暴露。

## scale 模块（向后兼容保留）

| 名称 | 说明 |
| --- | --- |
| `ScaleHelper(value)` | 量级助手，`get_scale(n)` 返回档位 `ScaleValue` |
| `ScaleValue` | 量级档位（向后兼容保留） |
| `choose_chunk_size(total_count)` | 内存排序分块（100~1000/块） |
| `choose_external_sort_chunk_size(total_count, *, target=32, min_chunk=100_000, max_chunk=5_000_000)` | 外部排序分块 |
| `chunk_wise_processing(iterable)` | 按 `choose_chunk_size` 分块产出（要求 `Sized` 输入） |
