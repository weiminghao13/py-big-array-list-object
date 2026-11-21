# BigArrayList

> 面向大数据量场景的惰性流式列表：惰性迭代 / 并行查找 / 分块排序 / 低内存外部排序 /
> 持久化分桶 / 蓄水池抽样 / 二级索引 / 链式算子。

零第三方运行时依赖（只用标准库）。核心思想是**惰性流**——数据不一次性物化进内存，
算子组合、按需产出，从而在内存可控的前提下处理远超内存的数据量。

> 版本：0.3.0（构造函数惰性化 + from_stream 磁盘落盘 + find 真正惰性早停 +
> sort reverse + min/max/sum/avg 聚合 + __repr__；旧 API 100% 向后兼容）

## 安装

开发环境（含测试 + 质量工具）：

```bash
pip install -e ".[dev]"
```

仅需运行时则零依赖，直接 `import` 即可。

## 快速开始

```python
from bigarraylist import BigArrayList

arr = BigArrayList([3, 1, 4, 1, 5, 9, 2, 6, 5, 3, 5, 1])

# 1) 惰性排序（生成器，按需产出）
top3 = list(__import__("itertools").islice(arr.sort(), 3))   # [1, 1, 3]

# 2) 线性并行查找（返回 (下标, 值)，惰性、可早停）
for idx, val in arr.find(5):
    print(idx, val)   # 4 5 / 8 5 / 10 5

# 3) 链式 API（新）：.lazy() 进入 LazyChain，每个算子返回新 LazyChain
result = (arr.lazy()
             .filter(lambda x: x > 2)
             .map(lambda x: x * 10)
             .distinct()
             .slice(0, 5)
             .collect())           # [30, 40, 90, 60, 50]

# 4) 持久化分桶（新）：建桶一次，多次 find 复用，O(n/N) 每次查询
with arr.build_buckets() as idx:
    for tgt in [3, 5, 9]:
        hits = list(idx.find(tgt))
        print(tgt, hits)
```

## 特性

| 能力 | 方法 | 说明 |
| --- | --- | --- |
| 惰性迭代 | `__next__` / `cycle` | 标准协议耗尽即停；`.cycle()` 无限循环 |
| 增删改插 | `append` / `remove` / `replace` / `insert` | 全惰性、不物化 |
| 线性查找 | `find` | 数值内存源 C 扫描；其他源线性/并行分块 |
| 二分查找 | `binary_search` | 数值源复用 C 排序副本后 bisect，返回排序后下标 |
| 分桶查找（一次性） | `bucket_find` | 哈希分桶 + 单桶扫描，每次重建桶 |
| **持久化分桶（新）** | `build_buckets` | 建桶一次返回 `BucketIndex`，多次 `find` 复用 |
| 蓄水池抽样 | `sample` | O(k) 内存，无需预知总量 |
| 二级索引 | `build_index` | `dict[key] -> list[下标]`，O(1) 判定 |
| 统一查找 | `auto_find` | 单次查询直接线性/并行查找，不建桶 |
| 排序 | `sort` | 数值内存源 C 排序；其他源按阈值内存/外部排序；数值磁盘帧 C 多路归并 |
| **流式构造（新）** | `from_stream` / `length=` | generator 不再强制物化；`from_stream()` 磁盘落盘内存 O(1)；`length=` 跳过 list() |
| **聚合（新）** | `min`/`max`/`sum`/`avg`/`count` | 数值源 C 快路；`avg()` 复用 `sum()/len()` |
| 流式算子（裸生成器） | `map`/`filter`/`reduce`/`slice`/`distinct`/`concat`/`chain`/`group_by`/`count_if` | 数值去重使用 C 哈希并保留首次出现顺序 |
| **链式算子（新）** | `.lazy()` → `LazyChain` | `.filter().map().distinct().collect()` 链式组合 |
| **配置对象（新）** | `BigArrayConfig` | 阈值、外部排序、桶数与桶文件缓冲均可调 |
| **自定义异常（新）** | `exceptions` | 继承标准异常，向后兼容；`raise_custom=True` 启用 |

## 项目结构

```
PyBigArrayListObject/
├── bigarraylist/                # 包
│   ├── __init__.py              # 暴露公共 API
│   ├── _api.py                  # BigArrayList 用户接口层（参数校验）
│   ├── config.py                # BigArrayConfig（所有阈值可配置）
│   ├── exceptions.py            # 自定义异常层次（继承标准异常）
│   ├── core/
│   │   └── base.py              # _CoreBase(__slots__+迭代协议+_fresh_iter)
│   │                            #   + BaseBigArrayList(组合各 mixin)
│   ├── operators/               # 算子 mixin 层
│   │   ├── crud.py              #   append/remove/replace/insert
│   │   ├── search.py            #   find/binary_search/bucket_find/build_buckets
│   │   ├── sort.py              #   sort/is_sorted
│   │   └── stream.py            #   map/filter/.../count_if + LazyChain
│   ├── algorithms/              # 算法实现层（可独立调用）
│   │   ├── parallel.py          #   模块级可 pickle 函数（子进程安全）
│   │   ├── external_sort.py     #   低内存外部排序（streaming/block 两种）
│   │   ├── bucket.py            #   BucketIndex + build_buckets 持久化分桶
│   │   └── sample.py            #   蓄水池抽样
│   └── utils/
│       ├── scale.py             # ScaleHelper/choose_chunk_size/...
│       └── validation.py        # 参数校验（标准异常 or 自定义异常切换）
├── tests/                       # pytest 套件（test_core/test_api/test_scale + conftest）
├── docs/                        # 架构说明 / API 参考
├── examples/                    # 使用示例
├── pyproject.toml               # 项目配置 + ruff/mypy/pytest/coverage
├── .pre-commit-config.yaml
└── README.md
```

## 质量工具

```bash
pytest                          # 跑测试（46 个）
ruff check bigarraylist tests   # Lint
ruff format bigarraylist tests  # 格式化
mypy bigarraylist               # 静态类型检查（moderate，见 pyproject 注释）
pytest --cov=bigarraylist       # 覆盖率
pre-commit run --all-files      # 提交前检查
```

## 设计要点

- **`_fresh_iter()`**：所有算子统一通过它拿一份**独立**的局部迭代器，全程操作局部变量，
  绝不碰 `self.__data`——算子调用零副作用、可重复调用。
- **三级排序分流**：按数据量选 `sorted()` / 内存多进程分块 / 外部排序，兼顾速度与内存。
- **低内存外部排序（新）**：默认 streaming 模式——阶段 1 逐条 `pickle.dump` 落盘，
  阶段 2 每个文件用 lazy 迭代器逐条 `pickle.load`，`heapq.merge` 同时驱动。
  内存峰值 = O(块数) 文件句柄 + O(块数) 临时元素（≈ 32），**真正符合"外部排序"语义**，
  能处理远超内存的数据。需要快 3~5 倍但内存放得下时，设
  `BigArrayConfig(external_sort_streaming_merge=False)` 走整块模式。
- **cooperative multiple inheritance**：`BaseBigArrayList` 多继承 `_CoreBase + CRUDMixin +
  StreamMixin + SortMixin + SearchMixin` 组合各算子；mixin 不声明字段，通过
  `self._fresh_iter()` / `len(self)` / `self._get_config()` 与核心协作，避免 `__slots__`
  多继承冲突与 name mangling 问题。
- **不支持随机访问 `[]`**：与惰性流设计冲突；要第 N 个用 `itertools.islice` 流式跳过。

详见 [docs/architecture.md](docs/architecture.md) 与 [docs/api_reference.md](docs/api_reference.md)。

## License

MIT
