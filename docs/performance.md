# 性能优化方案（PyBigArrayListObject）

> 基于对当前实现的完整代码审查 + 本机实测数据（Windows, Python 3.14）。
> 所有方案均遵守 AGENTS.md 红线：`from bigarraylist import BigArrayList` 不变、
> 旧 `.map()/.filter()/.sort()` 仍返回裸 generator、`_fresh_iter()` 纪律不破。

## 0. 实测现状（问题定位的证据）

| 场景 | 现状 | 对照 | 差距 |
|---|---|---|---|
| pickle 逐条 dump 20 万元素 | 0.124s | 按 4096/批 dump：0.003s | **36.7x** |
| pickle 逐条 load 20 万元素 | 0.072s | 按批 load：0.006s | **12.7x** |
| 30 万元素排序（中数据路径） | 1.139s | 直接 `sorted()`：0.046s | **慢 24.9x** |

三个最大的性能黑洞全部确认：
1. **逐条 pickle**（外部排序 streaming 模式、`_DiskSpillIterable`、分桶建桶全是逐条）
2. **并行任务粒度过小**（100k 数据分 100 元素/块 → 1000 次跨进程 IPC）
3. **中数据"并行排序"起反效果**（IPC + 进程启动开销远超并行收益）

---

## P0-1 批量 pickle 化（预期收益最大的单项改动）

### 问题
逐条 `pickle.dump(item, f)` / `pickle.load(f)` 的开销是批量模式（`pickle.dump(list)`）
的 **13~37 倍**，且产生大量小 IO 调用。涉及三处：

| 位置 | 文件 | 现状 |
|---|---|---|
| 外部排序 streaming 落盘/归并 | `algorithms/external_sort.py` `_dump_streaming` / `_streaming_file_iter` | 逐条 dump/load |
| generator 源落盘 | `core/base.py` `_DiskSpillIterable` | 逐条 dump/load |
| 分桶建桶/查桶 | `algorithms/bucket.py` `build_buckets` / `BucketIndex.find` | 逐条 dump (i, item) 元组 |

### 方案
统一引入**批量帧格式**（内部临时文件格式，不是公开 API，可自由改）：

```python
_BATCH = 4096  # 可调，进 BigArrayConfig

def _dump_batches(items, f):
    buf = []
    for x in items:
        buf.append(x)
        if len(buf) >= _BATCH:
            pickle.dump(buf, f, protocol=pickle.HIGHEST_PROTOCOL)
            buf = []
    if buf:
        pickle.dump(buf, f, protocol=pickle.HIGHEST_PROTOCOL)

def _batched_file_iter(f):
    """对外仍是逐条 yield 的生成器——heapq.merge 等上层接口零改动。"""
    while True:
        try:
            batch = pickle.load(f)
        except EOFError:
            return
        yield from batch
```

关键点：`_batched_file_iter` 保持"逐条产出"的生成器接口，所以 `heapq.merge`、
`BucketIndex.find` 等上层代码**一行不用改**。

内存影响：streaming 归并从 O(块数 × 1 条) 变为 O(块数 × 4096 条)。
32 块 × 4096 × 假设 100B/条 ≈ 13MB，依然远小于 block 模式，不破坏
"streaming 省内存"的语义。

附带收益：
- `pickle.HIGHEST_PROTOCOL`（当前用默认协议）
- 落盘/读盘文件句柄加 `buffering=8*1024*1024`（NamedTemporaryFile 默认缓冲偏小）

预期：**外部排序整体 2~5x；generator 源落盘 10x+；分桶建桶 5~10x。**

---

## P0-2 中数据排序路径：删掉或修好"负优化"（实测慢 24.9x）

### 问题
`SortMixin._big_iter_sort` 中数据分支（100k ~ 1M）：
- `choose_chunk_size(n)` 给出 **100~300 元素/块** → 30 万数据 = 3000 个 future
- 每次 `pool.submit` 是一次 pickle + IPC 往返
- 主进程最后对 3000 个 list 做 `heapq.merge`，O(n log 3000)
- Windows spawn 下进程池启动本身就要 1s+

实测 30 万 float：库路径 1.139s vs `sorted()` 0.046s。

### 方案（两步，第一步即可解决 90% 问题）

**Step 1 — 收紧阈值**：把内存并行排序的适用区间整体上移。
1M 以内直接 `sorted(self._fresh_iter(), ...)`（纯 C 的 Timsort，1M float ≈ 0.15s）。
即：`linear_parallel_threshold` 语义改为"内存 sorted 上限"，默认提到 1M；
`external_sort_threshold` 之下的并行分支只保留给"CPU 核数多 + 元素比较贵"
的场景，通过 config opt-in。

**Step 2 — 若保留并行分支，修任务粒度**：
块大小不再用 `choose_chunk_size` 的 100~1000，改为
`max(65_536, n // (workers * 4))`——30 万数据在 8 核上 = 8~32 块，
每块 1~4 万元素，IPC 次数从 3000 降到几十。归并路数随之从 3000 降到
几十，`heapq.merge` 开销同步下降。

预期：中数据排序 **1.1s → 0.05s（20x+）**。

---

## P0-3 并行查找 / is_sorted / 外部排序阶段 1 的任务粒度与流水线

### 问题
1. `_parallel_find`、`is_sorted` 同样用 `choose_chunk_size`（100~1000 元素/块），
   IPC 次数爆炸（同 P0-2）。
2. `chunk_is_sorted` 用纯 Python 索引循环
   `all(chunk[i] <= chunk[i+1] ...)`——逐元素字节码解释。
3. `external_sort` 阶段 1 的 `_BUFFER_DEPTH = 2` 硬编码：无论多少 worker，
   飞行中的排序任务最多 2 个，8 核机器 6 核空转。

### 方案
1. 块大小统一改为 `max(65_536, n // (workers * 4))`（与 P0-2 同一个 helper）。
2. `chunk_is_sorted` 改为 `chunk == sorted(chunk)`——Timsort 对近似有序
   输入是 O(n)，且全程 C 速度，比纯 Python 循环快一个数量级以上。
3. `_BUFFER_DEPTH` 改为 `max(2, 2 * (max_workers or os.cpu_count() or 4))`，
   内存峰值 = O(深度 × chunk_size)，仍可控（chunk_size 由
   `choose_external_sort_chunk_size` 保证 ≤ 5M，深度 × 块数乘积用 config 封顶）。

预期：并行查找 / is_sorted 大数据下 **3~10x**；外部排序阶段 1 接近核数线性加速。

---

## P1-1 大数据 distinct：排序去重 O(n log n) → 哈希分桶去重 O(n)

### 问题
`_distinct_iter` 大数据路径（≥10 万）走 `_big_iter_sort()` 再相邻去重，
外部排序区间是 O(n log n) + 全部数据落盘一次。

### 方案
复用已有的 `build_buckets` 基础设施做**哈希分桶去重**：
1. 按 `hash(item) % N` 流式分桶落盘（配合 P0-1 批量写，O(n)）
2. 每桶独立 load 进内存用 set 去重（桶大小 ≈ n/N，内存可控）
3. 按桶顺序产出（注意：产出顺序不再是"首次出现顺序"，
   若需保序则每桶内记录最小原始下标，最后按下标归并——多一次 O(u) 排序，
   u 为去重后条数）

预期：1M+ 数据去重 **5~20x**（省掉完整外部排序）。

---

## P1-2 进程池复用（Windows 收益最大）

### 问题
每次调用 `sort/find/is_sorted/external_sort` 都新建 `ProcessPoolExecutor`。
Windows spawn 模式下每个 worker 要重新启动解释器 + import 包，
8 worker 池冷启动 1~2s，且这笔开销每次调用都付一遍。

### 方案
模块级懒加载共享池（`algorithms/_pool.py`）：

```python
_pool: ProcessPoolExecutor | None = None

def get_pool(max_workers=None) -> ProcessPoolExecutor:
    global _pool
    if _pool is None:
        _pool = ProcessPoolExecutor(max_workers=max_workers)
    return _pool
```

配合 `atexit` 注册 shutdown。注意约束（AGENTS.md 3.4）：worker 函数仍必须
模块级可 pickle——这条不变。

风险点：共享池后"调用方 break 早停取消 future"的语义要保留
（现有 `_parallel_find` 的 GeneratorExit 处理逻辑不变，只是池不 shutdown）。

预期：大数据场景每次调用省 **1~2s 固定开销**（Windows）；Linux fork 收益小但仍为正。

---

## P1-3 分桶查找的两处小优化

1. `BucketIndex.find` 目前对桶内每条元素重算 `key_func(item)`。
   建桶时把 `(i, key, item)` 一起批量落盘，find 时直接比 key，
   省掉重复 key 计算（key 贵时收益大，如字符串处理）。
2. `_extract_bucket_idx` 用正则从文件名解析桶号——建桶后直接维护
   `dict[int, str]`（桶号 → 路径）放进 BucketIndex（`__slots__` 加一个字段），
   find 从 O(桶数 × 正则) 变 O(1)。桶数大（数百）且 find 频繁时可感知。

---

## P2（可选 / 需评估兼容性）

1. **`_map_iter` / `_filter_iter` 用内建 `map`/`filter`**：C 速度，比 Python
   生成器逐条 yield 快 ~30%。但返回对象从 generator 变成 map/filter 对象——
   AGENTS.md 红线只承诺"裸 generator 不可链式"，需先查测试是否断言
   `isinstance(r, GeneratorType)`。若断言存在则放弃此项。
2. **数值快路径（opt-in）**：元素全为 int/float 时用 `array.array` 或 numpy
   做块存储与排序，内存降 4~8x、排序再快 2~5x。建议做成
   `BigArrayConfig(numeric_fastpath=True)` opt-in，不改变默认行为。
3. **基准测试基建**：新增 `benchmarks/`（不进 CI 断言，只出报告），
   覆盖 sort/find/distinct/spill 四条主路径 × 3 个量级（100k/1M/10M），
   防止后续重构把 P0 的收益改回去。

---

## 实施顺序与验证

| 顺序 | 项目 | 预期收益 | 风险 |
|---|---|---|---|
| 1 | P0-2 中数据排序阈值/粒度 | 20x+（实测） | 低：纯分流逻辑 |
| 2 | P0-1 批量 pickle | 2~10x（IO 路径） | 低：内部格式，接口不变 |
| 3 | P0-3 粒度 + 流水线深度 | 3~10x（并行路径） | 中：注意早停取消语义 |
| 4 | P1-2 共享进程池 | 省 1~2s/次（Win） | 中：池生命周期管理 |
| 5 | P1-1 分桶去重 | 5~20x（distinct） | 中：产出顺序语义 |
| 6 | P1-3 / P2 | 增量 | 低 |

每项改完按 AGENTS.md 第 4 节清单验证：
`ruff check` / `ruff format --check` / `mypy`（注意清理 unused type:ignore）/
`pytest` 全绿 / `examples/demo.py` 8 场景 / 动了并行路径必须实跑多进程
（Windows spawn 会暴露 pickle 问题）。

---

## Cython 数值快路径：第三步实测（prange 多线程 + GIL 释放）

2026-10-04 实测（10M 个 int64，Windows / MSVC `/openmp`，5 轮取最优）：

| 操作 | 纯 Python | C 单线程 | C 多线程（4） | vs Python |
|---|---|---|---|---|
| sum | 930.6 ms | 5.65 ms | **2.52 ms** | ~370x |
| min | — | 11.33 ms | **2.91 ms** | — |
| max | — | 11.04 ms | **2.77 ms** | — |

- `py_sum_parallel`：prange 自动归约（`schedule='guided'`），inner loop 全程 nogil
- `py_min_parallel` / `py_max_parallel`：按线程分块求局部极值，
  空分块（线程数 > 有效块数）写哨兵 `_I64_MAX` / `_I64_MIN`，再串行归约部分结果
- sum 在 4 线程后触及内存带宽瓶颈（8 线程无进一步收益）；min/max 4 线程约 3.9x，近线性
- API 分流：`BigArrayConfig(fast_parallel_threshold=1_000_000, fast_parallel_threads=0)`，
  int 源数据量 >= 阈值时 `sum()/min()/max()` 自动切多线程；threads=0 用
  `omp_get_max_threads()`（逻辑核数），可显式覆盖


---

## 方案一/二/三 评审（2026-10-04 代码走查 + 本机实测复核）

> 对三个深度方案（二进制帧 / Cython 扩展 find·contains·count / 线程替换进程池）
> 的逐项复核。结论：**方案二直接做；方案一改用 Cython 零拷贝、不引入 numpy；
> 方案三方向对但论据有两处事实错误，且被"sort() 入口分流"大幅简化。**
> 实测环境：Windows, Python 3.14.2, numpy 2.4.0（仅环境里有，项目 dependencies 仍为空）。

### 实测修正的四个事实（原方案的论据错误）

| 原论断 | 实测 | 影响 |
|---|---|---|
| "numpy `arr.sort()` 底层 OpenBLAS/Accelerate 多线程" | **错。np.sort/arr.sort 是单线程 introsort**：1M int64 = 21ms（vs py `sorted()` 261ms，12x） | 结论方向不变（不需要进程池），但理由应是"单线程已够快"，不是"numpy 多线程" |
| "归并迭代器 `yield from arr` 快于 list" | **反了**：heapq.merge 10k 元素，np.int64 标量 1.4ms vs Python int 1.0ms（np 标量比较慢 ~40%） | 正确做法：块排序后 `py_to_list()`（1M 仅 13ms）再进 heapq.merge |
| "1M 外部排序 = 9.0s" | **基线过期**：P0 批量 pickle + 共享池落地后，实测 `big.sort()` 1M ≈ **1.04s** | 收益预期下调为 ~3~5x（1s → 0.2~0.3s），不是 10~50x |
| "MSVC OpenMP 链接标志是剩余工作量" | 已解决：`int64_ops.pyd` 已带 `/openmp` 编译（prange sum/min/max 在跑） | 方案三的编译风险项实际为零 |

二进制帧收益本身**确认**：1M int 写盘 raw-binary 0.050s vs 批量 pickle 0.232s（4.6x），
读盘 0.013s vs 0.088s（6.8x）。

### 被三个方案共同漏掉的最优解：list/tuple 数值源根本不需要外部排序

关键事实：`fast_numeric()` 命中意味着**源数据已经完整在内存里**（list/tuple）。
外部排序/分桶落盘存在的唯一理由是不落地的流；对内存数值源：

- **sort() 全程可在 Int64Array 内完成**（`py_sorted_copy`，C introsort，1M ≈ 21ms 级），
  任何量级都不走 `external_sort` / `_big_iter_sort` / 进程池。这一条单独就把
  1M sort 从 1.04s 打到 ~50ms，**且是 `_api.py` 入口几行的分流改动**，零文件格式工作。
- 同理 find/contains/count 全量级走 C 扫描（方案二）。
- 二进制帧只在**数值流源**（`from_stream` 落盘流等 fast_numeric 不命中的场景）才有意义 —
  而这正是方案一的正确适用范围（方案原文已写明，保留）。

### 方案一（二进制帧）修订版

1. **不引入 numpy**。`Int64Array` 已持有连续 `int64_t*` 且 `py_view()` 零拷贝，
   在 .pyx 里加两个 C 方法即可：`write_frame(f)`（fwrite 原始字节，8B magic + dtype tag 头）
   和类方法 `read_frame(f)`。`dependencies = []` 红线不破，方案三的 optional-extra 风险归零。
2. 适用范围收窄为：数值**流源**的外部排序 / `_hash_bucket_distinct` 落盘 / 分桶建桶。
   数值 list/tuple 源被上面的 sort() 分流覆盖，不再落盘。
3. float64 帧注意 NaN 语义对齐（C `==` 与 Python 一致：NaN 不命中任何值，加测试钉死；
   排序路径 NaN 位置与 Timsort 的 `<` 语义一致，加对拍测试）。
4. 文件头格式：8 字节 magic（如 `BAL1` + dtype tag + reserved），与 pickle 帧互斥 —
   一个临时文件只走一条路，读侧按 magic 分流，旧 pickle 帧格式保留兼容。

### 方案二（Cython find/contains/count）评审：最干净，直接实施

- 两个 .pyx 各加 nogil C 循环方法：`find_indices(value) -> list[int]`（全部命中下标，
  或先只做 find_first，见下）、`contains(value) -> bool`、`count_equal/less/greater -> int`。
  1M 纯 C 扫描 ≈ 1ms 级，对 Python 路径 1.3s 是 ~100x，预期收益成立。
- 语义对齐点（必须钉测试）：
  - `find` 现有契约是**惰性 generator 产出全部 (index, item)**。快路一次性扫描收全部
    下标再 yield —— 早停收益丧失（调用方 `next()` 一次就停的场景多扫了），但 1M 也就
    几 ms，可接受；文档写明语义差异。若在意，加 `find_first` 快路 + `find` 保持原样。
  - `x in big` → `__contains__` 快路直转 C contains，语义完全一致（`==`）。
  - target 类型/范围检查：非 int/float 或超 int64 范围 → 回退 Python 路径（与
    `fast_numeric` 既有约束同规则）。
  - NaN：`float('nan') in big` Python 语义为 False，C `==` 天然一致 —— 测试钉死即可，无需特判。
- 快路拦截位置：`_auto_find` / `__contains__` 入口**先于**阈值分流（分桶建桶 7s 的
  场景就是数值源走了桶路径，快路应直接绕过）。

### 方案三（线程替换进程池）修订版：删繁就简

原案"线程 + numpy + prange 重写 parallel.py"过于庞大。按上面的事实修正，收敛为：

1. **外部排序阶段 1 进程内化（数值流源）**：配合方案一的二进制帧，
   `sort_chunk` 改为 `Int64Array.from_list(chunk).py_sort()` 后直接写帧。
   单线程 21ms/1M 已快于任何 IPC 方案，**不需要后台落盘线程、不需要流水线**。
2. **进程池保留**：非数值对象 + 用户 opt-in 的 `memory_parallel_sort_enabled` 分支
   （AGENTS.md 3.6 红线不动）；`parallel.py` 模块级函数不动（pickle 约束继续满足）。
3. **is_sorted / 并行 find 的 prange 化降级为可选**：P0-3 粒度修复 + P1-2 共享池后
   这两个路径收益边际，不值得先动。

### 修订后的实施顺序

| 顺序 | 项目 | 预期收益 | 风险 |
|---|---|---|---|
| 1 | sort()/find/contains 入口 fast_numeric 分流（含方案二 .pyx 方法） | sort 1M 1.04s→~50ms；find 1M 1.3s→ms 级 | 低：纯增量，回退路径不变 |
| 2 | 方案一修订版：.pyx 二进制帧 IO + 数值流源外部排序/分桶落盘接入 | 数值流外部排序 3~5x | 中：帧格式 + NaN 对拍测试 |
| 3 | 方案三修订版：阶段 1 进程内化 | 冷启动 + IPC 全消 | 低：只动数值分支 |
| 4 | prange 化 is_sorted / 并行 find（可选） | 边际 | 低 |

验证红线不变：AGENTS.md 第 4 节全套 + 动了并行路径实跑多进程；
新增 NaN / 混合类型 / 越界回退 / 数值流源对拍测试。
