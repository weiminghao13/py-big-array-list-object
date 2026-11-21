# AGENTS.md — AI 助手 / 新贡献者上手指南

本项目是一个面向大数据量场景的惰性流式列表库。读这份文档能避免踩到几个非显而易见的约束。

## 1. 常用命令（沙箱）

本地工具装在 `.deps/`（不进 Git），跑命令前设 PYTHONPATH：

```powershell
# 一次性设置环境（当前 shell 生效）
$env:PYTHONPATH = ".deps;."

# Lint + 格式化检查
.deps\bin\ruff.exe check bigarraylist tests examples
.deps\bin\ruff.exe format --check bigarraylist tests examples
# 自动修复格式
.deps\bin\ruff.exe format bigarraylist tests examples

# 类型检查（严格模式：disallow_untyped_defs + warn_return_any 已开）
python -m mypy bigarraylist

# 测试（含覆盖率报告）
python -m pytest tests --tb=short -q
python -m pytest tests --cov=bigarraylist --cov-report=term-missing

# 端到端 demo（8 个场景，覆盖所有公开 API）
python examples/demo.py

# 全套验证（一行跑完）
$env:PYTHONPATH=".deps;."; python -m mypy bigarraylist; .deps\bin\ruff.exe check bigarraylist tests examples; .deps\bin\ruff.exe format --check bigarraylist tests examples; python -m pytest tests --tb=short -q; python examples/demo.py
```

CI（`.github/workflows/ci.yml`）在 Python 3.10/3.11/3.12 (ubuntu) + 3.12 (windows) 上跑同样的 ruff/mypy/pytest + 覆盖率。

Cython 快路径（`_fast/*.pyx` → `*.pyd`）改了 `.pyx` 后必须重编，否则 `_fast` 静默回退纯 Python：

```powershell
# PATH 上的 python 是沙箱内嵌解释器（无 Python.h），编译必须用带头文件的解释器
C:\Users\浩浩\AppData\Local\Programs\Python\Python314\python.exe setup_fast.py build_ext --inplace
```

## 2. 架构总览

四层包结构，自下而上：

```
bigarraylist/
├── core/base.py          # _CoreBase + BaseBigArrayList：__slots__/迭代协议/_fresh_iter
├── _api.py               # BigArrayList：对外公开 API + 参数校验
├── operators/            # 算子 mixin（CRUD / Stream / Sort / Search）
├── algorithms/           # 可独立复用的算法（parallel / external_sort / bucket / sample）
├── utils/                # scale（量级判定+分块） / validation（索引校验）
├── config.py             # BigArrayConfig：所有阈值集中可配
└── exceptions.py         # 自定义异常体系
```

**cooperative multiple inheritance** 是核心设计模式：

```python
class BaseBigArrayList(_CoreBase[T], CRUDMixin[T], StreamMixin[T], SortMixin[T], SearchMixin[T]):
    ...
```

- `_CoreBase` 持有所有状态字段（`__slots__`）+ 核心机制（`_fresh_iter`、`__iter__`、`__next__`、`_get_config`）
- 各 Mixin **只声明方法、不声明字段**，通过 `self._fresh_iter()` / `len(self)` / `self._get_config()` 与 `_CoreBase` 协作
- Mixin 继承 `Generic[T]` 仅用于 mypy 类型推导，运行时不引入开销

## 3. 非显而易见的约束（改代码前必读）

### 3.1 `__slots__` 多继承规则
- **只有 `_CoreBase` 声明 `__slots__`**，所有 mixin **禁止**声明 `__slots__`
- 原因：多继承时多个 `__slots__` 类组合会冲突；mixin 不带 `__slots__` 才能被 `_CoreBase` 组合
- mixin 通过 `self._fresh_iter()` 访问数据，**绝不**直接读写 `self.__data` 等双下划线字段（name mangling 在 mixin 类里找不到 `_CoreBase` 的私有字段）

### 3.2 `_fresh_iter()` 纪律（最高优先级约束）
- 所有算子（`_append_iter` / `find` / `_big_iter_sort` / ...）**必须**调 `self._fresh_iter()` 拿独立局部迭代器
- **绝不**消费 `self.__data`（那是留给 `__next__` 对外 for 循环用的）
- 违反这条会导致算子之间互相污染实例状态——历史上踩过这个坑

### 3.3 MRO stub 陷阱
- Mixin **不要**给"由后置 mixin 提供的方法"加 `NotImplementedError` stub
- 例：`StreamMixin._distinct_iter` 调 `self._big_iter_sort()`，但 `_big_iter_sort` 由 `SortMixin` 提供（MRO 中 `SortMixin` 在 `StreamMixin` 之后）。如果在 `StreamMixin` 加 stub，stub 会覆盖真实实现，运行时报 `NotImplementedError`
- 正确做法：在调用点用 `# type: ignore[attr-defined]` 抑制跨 mixin 的类型检查缺口
- 仅对"由 `_CoreBase` 通过 MRO 提供"的方法（`__len__` / `_fresh_iter` / `_get_config`）才加 stub（因为 `_CoreBase` 在 MRO 最前，真实实现会覆盖 stub）

### 3.4 模块级函数 pickle 要求
- `algorithms/parallel.py` 里的 `sort_chunk` / `chunk_is_sorted` / `linear_find_in_chunk` / `bisect_find_in_chunk` **必须**是模块级函数
- ProcessPoolExecutor 子进程用 spawn 方式时，pickle 找不到嵌套函数/lambda 的定义
- 同理，`tests/test_api.py` 里供多进程 pickle 用的类要放在模块级，不能嵌套在测试函数里

### 3.5 惰性流不随机访问
- `__getitem__` 主动抛 `NotImplementedError`——大数据不支持随机访问（随机访问要求全部物化进内存）
- 要取第 N 个元素用 `itertools.islice(stream, N, None)` 流式跳过
- 反向迭代 `_reverse_iter` **不物化**：仅支持 int 与 `reversed()` 可用的容器（list/tuple/str/range）；set、`from_stream` 落盘流等直接抛 TypeError（反向需全量读入内存，与低内存设计冲突，拒绝优于静默物化）。`copy()` 则支持任何可重复迭代的源

### 3.6 向后兼容红线
- `from bigarraylist import BigArrayList` 不能变
- 旧 `.map()` / `.filter()` / `.sort()` 返回**裸 generator**（不可链式）——重构前行为，零改动
- 新功能（`.lazy()` / `BigArrayConfig` / `build_buckets` / 异常体系）层叠在旧 API 之上，不破坏旧调用

### 3.7 排序与数值快路
- list/tuple 纯数值源优先复用 `_fast` 紧凑数组：排序、`binary_search`、`sum`/`avg`、`distinct`、`is_sorted` 不进入通用阈值分流
- 通用路径默认 `< 1M` 直接 `sorted()`；`memory_parallel_sort_enabled=True` 时才启用内存并行分块；`>= 1M` 走外部排序
- 外部排序默认 streaming 模式；同类型纯数值块用 `merge_ops.pyx` C 多路归并，混合/非数值块回退 `heapq.merge`
- block 模式（内存 ≈ 数据总量，快 3~5 倍）通过 `BigArrayConfig(external_sort_streaming_merge=False)` opt-in
- 块大小由 `choose_external_sort_chunk_size` 决定：目标 32 块，单块 100k~5M
- 分桶文件缓冲独立使用 `bucket_file_buffer_size`（默认 64KB），不要改回通用 `IO_BUFFER_SIZE`（8MB），否则数百桶会占数 GB
- 数值 `distinct()` 返回首次出现下标对应的原对象；含 NaN 必须回退 Python，不能用 C 相等语义覆盖对象身份语义

### 3.8 mypy 严格模式 + 已知 type:ignore 码
- `disallow_untyped_defs=true` + `warn_return_any=true` 已开，所有函数必须全标注
- 新增/修改函数时记得补类型标注
- 已知真实类型缺口（标注在源码处，不要随便删）：
  - `bisect_left/right` 在泛型 T 上 → `# type: ignore[call-overload]`（T 未必 SupportsRichComparison）
  - `sorted()` / `heapq.merge` 在泛型 T 上 → `# type: ignore[type-var]`
  - T 上的比较运算符（`<=` / `>`）→ `# type: ignore[operator]`
  - `iter(0 for ...)` 产 int 兼容 T → `# type: ignore[misc]`
  - `__next__` 返 Any 但声明返 T → `# type: ignore[no-any-return]`
  - `reversed(Iterable)` → `# type: ignore[call-overload]`
  - "Cannot assign to a type"（如 `BaseBigArrayList = None` 兜底）→ `# type: ignore[misc, assignment]`（需同时给两个码）

## 4. 改完代码的验证清单

1. `ruff check` + `ruff format --check` 通过
2. `mypy bigarraylist` 0 errors（注意 `warn_unused_ignores=true`：删代码后 type:ignore 可能变 unused，要清掉）
3. `pytest tests` 全绿（130+ 测试）
4. `python examples/demo.py` 8 个场景全跑通
5. 改了 mixin/`_CoreBase` 时，额外验证多进程路径（bucket_find / external_sort）——Windows spawn 会暴露 pickle 问题
6. 改了 `.pyx` 后重编并验证磁盘数值流；性能复测用 `python benchmarks/benchmark.py --hotspots`（计时不启用 tracemalloc，Python 内存独立复测）

## 5. 关键文档

- `README.md` — 用户视角的功能介绍 + 用法
- `docs/architecture.md` — 详细架构设计（四层 + 协作契约 + 数据流）
- `docs/api_reference.md` — 公开 API 完整签名
- `examples/demo.py` — 8 个可运行场景，覆盖所有公开 API
