# Changelog

本项目所有重要变更都会记录在此文件中。
格式参考 [Keep a Changelog](https://keepachangelog.com/zh-CN/)。

## [0.3.0] - 2026-08-25

### 新增
- **`from_stream()` 类方法**：从一次性流（generator/iterator）构造，自动磁盘落盘，
  内存 O(1)，可处理远超内存的数据量。落盘后数据可重复迭代，所有算子正常工作。
- **构造函数 `length` 参数**：当 source 没有 `__len__` 但可重复迭代时（如文件对象、
  自定义可迭代类），手动指定长度可跳过 `list()` 物化，真正惰性构造。
- **`sort(reverse=True)`**：全链路支持降序，包括小数据（sorted）、中数据（分块并行+
  多路归并）、大数据（external_sort）。归并阶段通过 `_ReverseWrapper` 取反比较实现。
- **聚合方法**：`min(key=, default=)` / `max(key=, default=)` / `sum()` / `avg()` /
  `count()`，单遍 O(n) O(1) 内存。BigArrayList 和 LazyChain 均支持。
- **`__repr__`**：`print(arr)` 显示 `BigArrayList(len=1000000, source=list)`，
  不再是裸内存地址。

### 优化
- **`find()` 真正惰性早停**：改用滑动窗口（workers*2 个飞行任务），按块顺序 yield，
  调用方 break/close() 时取消所有未启动的 chunk 任务。之前是一次性提交全部 chunk，
  早停时剩余 chunk 白跑。
- **构造函数简化**：移除冗余的 Generator 二次检查分支（generator 不可能有 `__len__`，
  原分支永远走不到），逻辑更清晰。

### 向后兼容
- 所有旧 API 签名不变，`BigArrayList(source)` 行为与 0.2.0 完全一致。
- `sort_chunk` 函数兼容旧的 2 元组/1 元组调用格式。

## [Unreleased]

### 工程化
- 项目结构从"脚本项目"重构为标准 Python 包：新增 `bigarraylist/` 包，源码拆为
  `core.py`（BaseBigArrayList）/ `api.py`（BigArrayList）/ `scale.py`（ScaleHelper）。
- 新增 `pyproject.toml`：声明项目元信息与 Python 版本要求。
- 测试迁移到 `tests/`，并从 `unittest` 改写为 `pytest` 函数风格 + `conftest` 共享
  fixtures；拆为 `test_core` / `test_api` / `test_scale` 三个模块，新增 3 个用例，
  共 46 个测试全过。
- 引入代码质量护栏：`ruff`（格式化 + Lint，全绿）、`mypy`（静态类型，moderate 配置）、
  `pre-commit`（提交前自动 ruff）。
- 新增 `.gitignore`、`LICENSE`(MIT)、`docs/`、`examples/`。

### 代码质量（由 ruff / mypy 驱动）
- `core.py`：把 `_extract_bucket_idx` 里的内联 `import re` 提到模块顶部。
- `core.py`：`cycle()` / `_append_iter()` 的 `for ...: yield` 改为 `yield from`。
- `core.py`：`__contains__` 改用 `any()` 表达。
- `core.py`：外部排序阶段 2 的 `for ...: files.append(open(...))` 改为
  `files.extend(...)`。

## [0.1.0] - 2026-08-23

### 新增
- **惰性流核心**：`BaseBigArrayList` 提供 `_fresh_iter()` 独立迭代器机制，所有算子
  零副作用、可重复调用；`__next__` 遵循标准 `StopIteration` 协议，`.cycle()` 提供无限循环。
- **CRUD 算子**：`append` / `remove` / `replace` / `insert`，全部惰性、不物化。
- **查找**：
  - `find`：线性并行查找（多进程分块），惰性、可早停。
  - `binary_search`：先排序后 bisect，返回排序后下标。
  - `bucket_find`：哈希分桶 + 单桶扫描，返回原始下标，临时桶文件 finally 清理。
  - `sample`：蓄水池抽样，O(k) 内存，无需预知总量。
  - `build_index`：二级索引 dict[key] -> list[下标]。
  - `auto_find`：按数据量自动选线性 / 分桶策略。
- **排序**：三级分流——<10w 直接 `sorted()`、10w~100w 内存多进程分块排序、≥100w
  外部排序（整块 pickle 落盘 + heapq.merge 多路归并，临时文件清理）。
- **流式算子**：`map` / `filter` / `reduce` / `slice` / `distinct` / `concat` /
  `chain` / `group_by` / `count_if`，全部惰性、非物化。
- **量级助手**：`ScaleHelper`（十万 / 百万 / 千万 / 亿）+ `choose_chunk_size`
  （内存排序，100~1000/块）+ `choose_external_sort_chunk_size`（外部排序，目标 32 块、
  单块 100k~5M）。
