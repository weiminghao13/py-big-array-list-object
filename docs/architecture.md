# 架构说明

> 版本 0.2.0：从单文件 908 行 `baseBigArrayList.py` 重构为四层包结构。

## 四层架构

```
┌──────────────────────────────────────────────────────────────┐
│  _api.py  · BigArrayList（公共 API + 参数校验）                  │  ← 用户只接触这一层
├──────────────────────────────────────────────────────────────┤
│  operators/  · CRUDMixin / StreamMixin / SortMixin / SearchMixin │  ← 算子 mixin 层
├──────────────────────────────────────────────────────────────┤
│  core/base.py · _CoreBase(__slots__+迭代协议+_fresh_iter)        │  ← 核心机制
│                BaseBigArrayList(_CoreBase, *所有mixin) 组合        │
├──────────────────────────────────────────────────────────────┤
│  algorithms/  · parallel / external_sort / bucket / sample      │  ← 算法实现层
│  utils/       · scale / validation                              │  ← 工具层
│  config.py    · BigArrayConfig                                  │
│  exceptions.py · BigArrayListError + 子类                        │
└──────────────────────────────────────────────────────────────┘
```

| 层 | 职责 | 不做什么 |
| --- | --- | --- |
| `_api.py` | 公共方法签名 + 入口参数校验 + 用户级语义 | 不实现算法细节，转调 mixin |
| `operators/` | 用 `_fresh_iter()` 实现各类算子（CRUD / 查找 / 排序 / 流式） | 不校验参数、不声明字段 |
| `core/base.py` | `__slots__` + 迭代协议 + `_fresh_iter` + 蓄水池抽样 | 不实现算子（除 `_sample` 直接依赖算法函数） |
| `algorithms/` | 可独立调用的算法实现（external_sort / build_buckets / sort_chunk / reservoir_sample） | 不持有状态、不访问实例字段 |
| `utils/` + `config` + `exceptions` | 量级判定 / 参数校验 / 阈值配置 / 异常层次 | 无业务逻辑 |

## cooperative multiple inheritance（mixin 组合）

`BaseBigArrayList` 通过多继承把四个算子 mixin 与核心组合到一起：

```python
class BaseBigArrayList(_CoreBase, CRUDMixin, StreamMixin, SortMixin, SearchMixin):
    pass
```

**mixin 协作约定**：
- 各 mixin 只声明方法、不声明字段（避免 `__slots__` 多继承冲突）。
- mixin 内部统一通过三个公开协议与核心交互，**绝不直接访问** `self.__source` /
  `self.__data` 等双下划线字段——name mangling 在 mixin 里会找不到这些属性：
  - `self._fresh_iter()` —— 拿一份独立局部迭代器
  - `len(self)` —— 元素总数
  - `self._get_config()` —— 读 `BigArrayConfig` 阈值
- 算法实现（`algorithms/`）更进一步：完全脱离实例，只接收 `Iterable` 与
  `total_count` 作参数，可在任意调用方独立使用。

> 这套设计让算法层（`algorithms/`）可单元测试、可被其它数据结构复用；
> mixin 层（`operators/`）只负责"把算法接到惰性流上"。

## 惰性流与 `_fresh_iter()`

实例内部只存一份 `self.__data` 迭代器，**专供 `__next__` / `for` 循环对外用**。
所有算子（`find` / `sort` / `map` / ...）一律通过 `_fresh_iter()` 拿一份**全新独立**
的局部迭代器，全程操作局部变量，绝不碰 `self.__data`。

> 这条是项目的硬约束。历史上出现过"算子直接消费 `self.__data` 导致实例状态被污染"
> 的 bug，因此现在强制走 `_fresh_iter()`，并在测试里用 `test_*_no_state_pollution`
> 系列锁定。

`_fresh_iter()` 按 source 类型分流返回全新迭代器：
- `int` source：`iter(0 for _ in range(source))`
- `list` / `tuple` / `str` 等：`iter(source)`（容器可重复迭代，零物化）
- generator source：构造时已被 `list(...)` 物化并写回 `self.__source`，
  因此 `_fresh_iter()` 拿的是物化后的 list（不再耗尽）。

## 迭代协议

- `__next__` 遵循标准 Python 协议：耗尽抛 `StopIteration`，`for` / `list` / `in` 正常结束。
- 想要"耗尽不停止"的循环语义，显式调用 `.cycle()`（内部循环 `_fresh_iter()`）。

## 查找策略

| 方法 | 是否排序 | 返回下标 | 建桶 | 适合场景 |
| --- | --- | --- | --- | --- |
| `find` | 否 | 原始下标 | 否 | 单次 / 少量查找，惰性、可早停 |
| `binary_search` | 是（先排序） | 排序后下标 | 否 | 同批数据多次查找（一次排序 + 多次 O(log n)） |
| `bucket_find` | 否 | 原始下标 | **每次重建** | 单次分桶查找（语义同 find，哈希分桶加速） |
| `build_buckets` → `BucketIndex.find` | 否 | 原始下标 | **建一次复用** | 同批数据多次查找，建桶一次 O(n) 后每次 O(n/N) |
| `auto_find` | 否 | 原始下标 | 按量自动 | 不想自己选策略 |

> `bucket_find` 与 `build_buckets` 的关键区别：`bucket_find` 每次调用都重建桶
> （保持零副作用），多次查询无加速；`build_buckets` 返回持久化 `BucketIndex`，
> 建桶一次后可任意多次 `find`，是真正的"预处理一次、查询多次"。

`BucketIndex` 支持上下文管理器，退出时自动清理桶临时文件：

```python
with arr.build_buckets(key_func=lambda u: u.id) as idx:
    for tgt in targets:
        hits = list(idx.find(tgt))   # 每次只扫目标桶
```

## 排序三级分流

| 数据量 | 策略 | 内存 |
| --- | --- | --- |
| < 十万 | `sorted()` 直排 | O(n) |
| 十万 ~ 百万 | 多进程分块排序 + 归并 | O(块大小 × 进程数) |
| ≥ 百万 | 外部排序（分块排序 + 落盘 + `heapq.merge`） | 见下 |

### 外部排序两种归并模式

由 `BigArrayConfig.external_sort_streaming_merge` 控制：

| 模式 | 阶段 1 落盘 | 阶段 2 归并 | 内存峰值 | 速度 | 适合 |
| --- | --- | --- | --- | --- | --- |
| **streaming（默认）** | 逐条 `pickle.dump` | 每文件 lazy 迭代器逐条 `pickle.load` | O(块数) 句柄 + O(块数) 元素（≈32） | 较慢 | 数据量远超内存 |
| block | 整块 `pickle.dump(list)` | 每文件一次 `pickle.load` 读整 list | ≈ 数据总量 | 快 3~5 倍 | 数据放得下内存 |

> 0.1.0 默认走 block 模式（归并峰值 ≈ 数据总量，违背"外部排序"初衷）。
> 0.2.0 默认改为 streaming 模式，真正符合外部排序语义；要恢复旧行为显式设
> `BigArrayConfig(external_sort_streaming_merge=False)`。

外部排序分块由 `choose_external_sort_chunk_size` 决定：目标 32 块、单块 100k~5M，
块数少 → 阶段 1 调度少、阶段 2 归并堆小 → 更快。

## 配置对象

所有阈值集中在 `BigArrayConfig`（frozen dataclass）：

```python
cfg = BigArrayConfig(
    linear_parallel_threshold=100_000,   # 并行查找触发阈值
    external_sort_threshold=1_000_000,    # 外部排序触发阈值
    external_sort_streaming_merge=True,   # 外部排序归并模式
    external_sort_chunk_target=32,        # 目标块数
    bucket_max_count=1024,                # 桶数上限（文件描述符约束）
)
arr = BigArrayList(data, config=cfg)
```

不传 `config` 时用 `BigArrayConfig.default()`，所有字段值与 0.1.0 硬编码一致——
**零参数改动，行为完全一致**。也可类级别覆盖：`BigArrayConfig.set_default(...)`。

## 异常层次

```
BigArrayListError                       ← 所有自定义异常的根（继承 Exception）
├── InvalidArgumentError(ValueError)   参数类型/取值不合法
├── IndexOutOfRangeError(IndexError)   索引越界
└── DataSourceError(TypeError)         source 类型不支持某操作
```

向后兼容策略：子类同时继承标准异常（`ValueError`/`IndexError`/`TypeError`），
现有 `except ValueError:` 调用方代码无需改动。默认仍抛标准异常；
`BigArrayList(..., raise_custom=True)` 启用自定义异常子类。

## 临时文件

- 外部排序写 `big_sort_*.tmp`
- 分桶查找写 `big_bucket_{idx}_*.tmp`（文件名含桶下标，`BucketIndex` 据此定位目标桶）

均在 `finally` 中清理；`BucketIndex.close()` 幂等。测试用 `glob` 扫描临时目录，
确保无泄漏（`test_external_sort_correctness_and_cleanup`、
`test_bucket_find_temp_file_cleanup`）。
