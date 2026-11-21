# cython: language_level=3, boundscheck=False, wraparound=False, cdivision=True, initializedcheck=False

"""int64 紧凑数组后端（Cython 快路径）。

第二步：map/filter/reduce/min/max 全 C 级循环。
第三步：sum/min/max prange 多线程 + GIL 释放。
第四步：C 级 introsort + 切片视图 view()（零拷贝，不拥有数据）。

map/filter/count_if 接受 Python 回调（逐元素调用仍是 PyObject 级），但循环骨架、
索引推进、结果容器预分配都在 C 层——比 Python 版省掉生成器协议
（next/StopIteration）与逐元素动态查找的开销。
sum/min/max/reduce/sort 无回调，纯 C 算术/比较循环。
"""

from cpython.bytes cimport PyBytes_FromStringAndSize
from libc.stdint cimport int64_t, uint64_t, uint8_t
from libc.stdlib cimport calloc, free, malloc
cimport openmp
from cython.parallel cimport prange

__all__ = ["Int64Array"]

# reduce"无初值"哨兵：普通模块属性（cdef 变量 Python 不可见），_api.py import 同一对象
NO_INITIAL = object()

# prange 手动归约的中性元：空分块（线程数多于有效块数）写哨兵值，归约时是恒等元
cdef int64_t _I64_MAX = <int64_t>0x7FFFFFFFFFFFFFFF
cdef int64_t _I64_MIN = -_I64_MAX - 1

# introsort 小分区阈值：以下用插入排序（n 小时常数项小）
cdef int _SORT_CUTOFF = 16


cdef inline void _insertion_sort(int64_t* a, Py_ssize_t lo, Py_ssize_t hi) noexcept nogil:
    """插入排序 [lo, hi)（introsort 小分区终点）。"""
    cdef Py_ssize_t i, j
    cdef int64_t key
    for i in range(lo + 1, hi):
        key = a[i]
        j = i - 1
        while j >= lo and a[j] > key:
            a[j + 1] = a[j]
            j -= 1
        a[j + 1] = key


cdef inline int64_t _median3(int64_t a, int64_t b, int64_t c) noexcept nogil:
    """三数取中（防排序输入有序时快排退化 O(n²)）。"""
    if a < b:
        if b < c:
            return b
        return a if a > c else c
    if a < c:
        return a
    return b if b > c else c


cdef void _introsort_impl(int64_t* a, Py_ssize_t lo, Py_ssize_t hi, int depth) noexcept nogil:
    """introsort 主体：快排（三数取中）+ 递归过深切堆排 + 小分区插排。"""
    cdef:
        Py_ssize_t i, j
        int64_t pivot, tmp
    while hi - lo > _SORT_CUTOFF:
        if depth <= 0:
            # 递归过深：切堆排序（保证 O(n log n) 最坏情况）
            _heapsort(a, lo, hi)
            return
        pivot = _median3(a[lo], a[lo + (hi - lo) // 2], a[hi - 1])
        i = lo - 1
        j = hi
        # Hoare 划分
        while True:
            i += 1
            while a[i] < pivot:
                i += 1
            j -= 1
            while a[j] > pivot:
                j -= 1
            if i >= j:
                break
            tmp = a[i]
            a[i] = a[j]
            a[j] = tmp
        depth -= 1
        # 尾递归优化：先递归小的一半，循环处理大的一半
        if j - lo < hi - (j + 1):
            _introsort_impl(a, lo, j + 1, depth)
            lo = j + 1
        else:
            _introsort_impl(a, j + 1, hi, depth)
            hi = j + 1
    _insertion_sort(a, lo, hi)


cdef void _sift_down(int64_t* a, Py_ssize_t start, Py_ssize_t end) noexcept nogil:
    """堆排序下滤：[start, end) 内以 start 为根下沉。"""
    cdef:
        Py_ssize_t root, child
        int64_t tmp
    root = start
    while root * 2 + 1 < end:
        child = root * 2 + 1
        if child + 1 < end and a[child] < a[child + 1]:
            child += 1
        if a[root] < a[child]:
            tmp = a[root]
            a[root] = a[child]
            a[child] = tmp
            root = child
        else:
            return


cdef void _heapsort(int64_t* a, Py_ssize_t lo, Py_ssize_t hi) noexcept nogil:
    """堆排序 [lo, hi)（introsort 深度兜底）。"""
    cdef:
        Py_ssize_t start, end
        int64_t tmp
    start = (hi - lo) // 2
    end = hi - lo
    while start > 0:
        start -= 1
        _sift_down(a + lo, start, end)
    while end > 1:
        end -= 1
        tmp = a[lo]
        a[lo] = a[lo + end]
        a[lo + end] = tmp
        _sift_down(a + lo, 0, end)


cdef inline int _depth_limit(Py_ssize_t n) noexcept nogil:
    """introsort 深度上限：2 * floor(log2(n))。"""
    cdef int d = 0
    while n > 1:
        n >>= 1
        d += 1
    return 2 * d if d > 0 else 1


cdef inline int _resolve_threads(Py_ssize_t num_threads) noexcept nogil:
    """num_threads <= 0 时用 OpenMP 默认线程数（通常 = 逻辑核数）。"""
    if num_threads <= 0:
        return openmp.omp_get_max_threads()
    return <int>num_threads


cdef class Int64Array:
    """紧凑连续 int64_t[] 数组，对标 NumPy 的 data 指针 + OWNDATA 语义。"""

    cdef:
        int64_t* data
        Py_ssize_t n
        bint owns_data

    def __cinit__(self):
        self.data = NULL
        self.n = 0
        self.owns_data = False

    def __dealloc__(self):
        if self.owns_data and self.data != NULL:
            free(self.data)

    @classmethod
    def from_list(cls, list src):
        """list[int] → 紧凑 C 数组（一次性转换）。

        元素不是 int 或超出 int64 范围时抛 TypeError/OverflowError，
        调用方（_fast.__init__.try_build_int64）捕获后回退 Python 后端。

        注意必须显式 `type(item) is not int`：Cython 把 object 赋给 int64_t
        走 nb_int 转换，float 会被静默截断（bool 也是 int 子类，同样要拒）。
        """
        cdef:
            Int64Array a = cls.__new__(cls)
            Py_ssize_t i, n = len(src)
            object item
        a.n = n
        if n > 0:
            a.data = <int64_t*>malloc(n * sizeof(int64_t))
            if a.data == NULL:
                raise MemoryError(f"无法分配 {n} 个 int64")
            for i in range(n):
                item = src[i]
                if type(item) is not int:
                    raise TypeError(f"Int64Array 只接受 int，第 {i} 个元素是 {type(item).__name__}")
                a.data[i] = item
        a.owns_data = True
        return a

    def __len__(self):
        return self.n

    # ==================== 纯 C 算术（无回调）====================

    def py_sum(self):
        """C 级求和：纯 CPU add 循环，无 Python 对象开销。"""
        cdef:
            int64_t total = 0
            Py_ssize_t i
        with nogil:
            for i in range(self.n):
                total += self.data[i]
        return total

    def py_min(self):
        """C 级最小值：空数据返回 None（对齐 _min(default=None)）。"""
        cdef:
            int64_t m
            Py_ssize_t i
        if self.n == 0:
            return None
        m = self.data[0]
        with nogil:
            for i in range(1, self.n):
                if self.data[i] < m:
                    m = self.data[i]
        return m

    def py_max(self):
        """C 级最大值：空数据返回 None（对齐 _max(default=None)）。"""
        cdef:
            int64_t m
            Py_ssize_t i
        if self.n == 0:
            return None
        m = self.data[0]
        with nogil:
            for i in range(1, self.n):
                if self.data[i] > m:
                    m = self.data[i]
        return m

    def py_reduce(self, object func, object initial):
        """C 级聚合骨架：acc = func(acc, x)。initial 传 NO_INITIAL 表示无初值。"""
        cdef:
            Py_ssize_t i
            object acc
        if initial is NO_INITIAL:
            if self.n == 0:
                raise TypeError("reduce() of empty iterable with no initial value")
            acc = self.data[0]
            for i in range(1, self.n):
                acc = func(acc, self.data[i])
        else:
            acc = initial
            for i in range(self.n):
                acc = func(acc, self.data[i])
        return acc

    # ==================== C 循环骨架 + Python 回调 ====================

    def py_map(self, object func):
        """C 级 map：预分配结果列表，循环推进在 C 层。返回 list[int]（已物化）。"""
        cdef:
            list out
            Py_ssize_t i
        if self.n == 0:
            return []
        out = [0] * self.n
        for i in range(self.n):
            out[i] = func(self.data[i])
        return out

    def py_filter(self, object predicate):
        """C 级 filter：命中才 append。返回 list[int]（已物化）。"""
        cdef:
            list out = []
            Py_ssize_t i
            object x
        for i in range(self.n):
            x = self.data[i]
            if predicate(x):
                out.append(x)
        return out

    def py_count_if(self, object predicate):
        """C 级条件计数：循环骨架 C 级，谓词逐元素回调。"""
        cdef:
            Py_ssize_t i, count = 0
        for i in range(self.n):
            if predicate(self.data[i]):
                count += 1
        return count

    def py_map_filter_sum(self, object map_fn, object pred):
        """融合 map+filter+sum：单遍扫描（对齐方案第④招）。"""
        cdef:
            int64_t total = 0
            Py_ssize_t i
            object x
        for i in range(self.n):
            x = self.data[i]
            if pred(x):
                total += map_fn(x)
        return total

    # ==================== 多线程（prange + 释放 GIL，第三步） ====================

    def py_sum_parallel(self, Py_ssize_t num_threads=0):
        """多线程求和：prange 自动归约（+），inner loop 全程释放 GIL。

        num_threads <= 0 时用 OpenMP 默认线程数（通常 = 逻辑核数）。
        """
        cdef:
            int64_t total = 0
            Py_ssize_t i
            int nt = _resolve_threads(num_threads)
        if self.n == 0:
            return 0
        with nogil:
            for i in prange(self.n, schedule='guided', num_threads=nt):
                total += self.data[i]
        return total

    def py_min_parallel(self, Py_ssize_t num_threads=0):
        """多线程最小值：按线程分块求局部 min，再串行归约部分结果。

        空分块（线程数 > 有效块数）写哨兵 _I64_MAX，归约时是恒等元。
        空数据返回 None（对齐 py_min / _min(default=None)）。
        """
        cdef:
            Py_ssize_t i, t, lo, hi, chunk
            int nt
            int64_t* partials = NULL
            int64_t local, m
        if self.n == 0:
            return None
        nt = _resolve_threads(num_threads)
        partials = <int64_t*>malloc(nt * sizeof(int64_t))
        if partials == NULL:
            raise MemoryError(f"无法分配 {nt} 个线程局部结果")
        chunk = (self.n + nt - 1) // nt
        with nogil:
            for t in prange(nt, num_threads=nt):
                lo = t * chunk
                hi = lo + chunk
                if hi > self.n:
                    hi = self.n
                local = _I64_MAX
                for i in range(lo, hi):
                    if self.data[i] < local:
                        local = self.data[i]
                partials[t] = local
        m = partials[0]
        for t in range(1, nt):
            if partials[t] < m:
                m = partials[t]
        free(partials)
        return m

    def py_max_parallel(self, Py_ssize_t num_threads=0):
        """多线程最大值：同 py_min_parallel，哨兵用 _I64_MIN。空数据返回 None。"""
        cdef:
            Py_ssize_t i, t, lo, hi, chunk
            int nt
            int64_t* partials = NULL
            int64_t local, m
        if self.n == 0:
            return None
        nt = _resolve_threads(num_threads)
        partials = <int64_t*>malloc(nt * sizeof(int64_t))
        if partials == NULL:
            raise MemoryError(f"无法分配 {nt} 个线程局部结果")
        chunk = (self.n + nt - 1) // nt
        with nogil:
            for t in prange(nt, num_threads=nt):
                lo = t * chunk
                hi = lo + chunk
                if hi > self.n:
                    hi = self.n
                local = _I64_MIN
                for i in range(lo, hi):
                    if self.data[i] > local:
                        local = self.data[i]
                partials[t] = local
        m = partials[0]
        for t in range(1, nt):
            if partials[t] > m:
                m = partials[t]
        free(partials)
        return m

    # ==================== 排序（C 级 introsort，第四步）====================

    def py_sort(self, bint reverse=False):
        """C 级 introsort：原地排序，返回 None（对齐 list.sort 语义）。

        快排（三数取中 + Hoare 划分）+ 递归过深切堆排（保证 O(n log n)
        最坏情况）+ 小分区（<=16）插排。全程释放 GIL。
        """
        cdef:
            int64_t tmp
            Py_ssize_t i, j
        if self.n < 2:
            return None
        with nogil:
            _introsort_impl(self.data, 0, self.n, _depth_limit(self.n))
            if reverse:
                j = self.n - 1
                for i in range(self.n // 2):
                    tmp = self.data[i]
                    self.data[i] = self.data[j]
                    self.data[j] = tmp
                    j -= 1
        return None

    def py_sorted_copy(self, bint reverse=False):
        """C 级 introsort：排序到新数组（不动原数据），返回新 Int64Array。"""
        cdef:
            Int64Array out
            int64_t tmp
            Py_ssize_t i, j
        out = Int64Array.__new__(Int64Array)
        out.n = self.n
        if self.n == 0:
            return out
        out.data = <int64_t*>malloc(self.n * sizeof(int64_t))
        if out.data == NULL:
            raise MemoryError(f"无法分配 {self.n} 个 int64")
        out.owns_data = True
        with nogil:
            for i in range(self.n):
                out.data[i] = self.data[i]
            _introsort_impl(out.data, 0, self.n, _depth_limit(self.n))
            if reverse:
                j = self.n - 1
                for i in range(self.n // 2):
                    tmp = out.data[i]
                    out.data[i] = out.data[j]
                    out.data[j] = tmp
                    j -= 1
        return out

    # ==================== 切片视图（零拷贝，第四步）====================

    def py_view(self, Py_ssize_t start, Py_ssize_t stop, Py_ssize_t step=1):
        """切片视图：零拷贝（共享底层 data 指针，步长 != 1 时物化拷贝）。

        返回不拥有数据（owns_data=False）的 Int64Array；在宿主存活期内有效。
        Python 语义对齐 list 切片：负数索引/None/越界自动裁剪。
        """
        cdef:
            Int64Array out
            Py_ssize_t i, k, lo, hi
        # 对齐 Python 切片语义：负索引折算 + 越界裁剪
        if start < 0:
            start += self.n
        if stop < 0:
            stop += self.n
        if start < 0:
            start = 0
        if stop > self.n:
            stop = self.n
        if start > stop:
            start = stop
        if step == 1:
            out = Int64Array.__new__(Int64Array)
            out.n = stop - start
            out.data = self.data + start  # 零拷贝：指针偏移
            out.owns_data = False
            return out
        # 步长 != 1：无法用连续指针表达，物化拷贝
        k = 0
        for i in range(start, stop, step):
            k += 1
        out = Int64Array.__new__(Int64Array)
        out.n = k
        if k == 0:
            return out
        out.data = <int64_t*>malloc(k * sizeof(int64_t))
        if out.data == NULL:
            raise MemoryError(f"无法分配 {k} 个 int64")
        out.owns_data = True
        i = 0
        lo = start
        while lo < stop:
            out.data[i] = self.data[lo]
            i += 1
            lo += step
        return out

    def py_to_list(self):
        """视图物化：int64_t[] → Python list[int]（map/filter/列表化共用）。"""
        cdef:
            list out
            Py_ssize_t i
        out = [0] * self.n
        for i in range(self.n):
            out[i] = self.data[i]
        return out

    def py_to_bytes(self):
        """二进制物化：int64_t[] → bytes（原生字节序）。

        与 py_to_list 同为一次拷贝，但不创建任何 Python 对象——外部排序
        数值块落盘直接拿这段字节写 int64 帧载荷，零逐元素解释开销。
        """
        cdef:
            Py_ssize_t nbytes = self.n * sizeof(int64_t)
            bytes out
        if nbytes == 0:
            return b""
        # 一次 memcpy 直接产出 bytes（无中间 bytearray / 逐元素 Python 对象）
        out = PyBytes_FromStringAndSize(<const char*>self.data, nbytes)
        return out

    # ==================== 查找 / 计数（C 扫描，方案二）====================

    def py_contains(self, int64_t target):
        """C 扫描：target 是否在数组中，命中即返回（短路径，全程释放 GIL）。"""
        cdef:
            Py_ssize_t i
            bint found = False
        with nogil:
            for i in range(self.n):
                if self.data[i] == target:
                    found = True
                    break
        return found

    def py_find_indices(self, int64_t target):
        """C 扫描：返回所有命中下标 list（保持原序）。

        与 find() 惰性契约的差异：本方法一次扫完整个数组再 yield——
        结果与线性扫描完全一致，只是不支持"取前 k 个就早停"。
        调用方（_api.BigArrayList.find）负责把下标翻译成 (i, target) 流。
        """
        cdef:
            list out = []
            Py_ssize_t i
        for i in range(self.n):
            if self.data[i] == target:
                out.append(i)
        return out

    def py_count_equal(self, int64_t target):
        """C 计数：== target 的元素个数（全程释放 GIL）。"""
        cdef:
            Py_ssize_t i, c = 0
        with nogil:
            for i in range(self.n):
                if self.data[i] == target:
                    c += 1
        return c

    def py_count_less(self, int64_t target):
        """C 计数：< target 的元素个数（全程释放 GIL）。"""
        cdef:
            Py_ssize_t i, c = 0
        with nogil:
            for i in range(self.n):
                if self.data[i] < target:
                    c += 1
        return c

    def py_count_greater(self, int64_t target):
        """C 计数：> target 的元素个数（全程释放 GIL）。"""
        cdef:
            Py_ssize_t i, c = 0
        with nogil:
            for i in range(self.n):
                if self.data[i] > target:
                    c += 1
        return c

    def py_is_sorted(self):
        """单遍 C 检查非降序，发现逆序对立即退出。"""
        cdef:
            Py_ssize_t i
            bint ordered = True
        with nogil:
            for i in range(1, self.n):
                if self.data[i - 1] > self.data[i]:
                    ordered = False
                    break
        return ordered

    def py_distinct_indices(self):
        """开放寻址哈希去重，返回各值首次出现的下标。"""
        cdef:
            Py_ssize_t i, count = 0, capacity = 8, mask, slot
            int64_t value
            uint64_t h
            int64_t* values = NULL
            uint8_t* used = NULL
            Py_ssize_t* indices = NULL
            list out
        if self.n == 0:
            return []
        while capacity < self.n * 2:
            capacity *= 2
        mask = capacity - 1
        values = <int64_t*>malloc(capacity * sizeof(int64_t))
        used = <uint8_t*>calloc(capacity, sizeof(uint8_t))
        indices = <Py_ssize_t*>malloc(self.n * sizeof(Py_ssize_t))
        if values == NULL or used == NULL or indices == NULL:
            free(values)
            free(used)
            free(indices)
            raise MemoryError(f"无法为 {self.n} 个 int64 建立去重哈希表")
        with nogil:
            for i in range(self.n):
                value = self.data[i]
                h = <uint64_t>value
                h ^= h >> 30
                h *= <uint64_t>0xBF58476D1CE4E5B9
                h ^= h >> 27
                h *= <uint64_t>0x94D049BB133111EB
                h ^= h >> 31
                slot = <Py_ssize_t>(h & <uint64_t>mask)
                while used[slot]:
                    if values[slot] == value:
                        break
                    slot = (slot + 1) & mask
                if not used[slot]:
                    used[slot] = 1
                    values[slot] = value
                    indices[count] = i
                    count += 1
        free(values)
        free(used)
        out = [0] * count
        for i in range(count):
            out[i] = indices[i]
        free(indices)
        return out

    def __iter__(self):
        """支持 for x in arr：委托 py_to_list 的列表迭代器。"""
        return iter(self.py_to_list())


# ==================== 帧 IO（方案②：替代 pickle，服务 from_stream 数值流）====================
# 只负责"一批元素 ↔ 一段二进制载荷"：帧头/回退由 algorithms/fileio.py 决定。
# 载荷用原生字节序（临时文件同机写读，不跨机器），省掉逐元素 struct 打包开销。
# encode_* 返回 None 表示"本批不适合该编码"，调用方回退 pickle——绝不写半截帧。


def encode_int64(list items):
    """把一批元素编码成 int64 载荷（n*8 字节）；非 int 或越界返回 None。

    拒绝 bool（type(x) is not int 同时挡掉 bool 与 int 子类），与
    Int64Array.from_list 的严格性一致，避免静默截断/类型漂移。
    """
    cdef:
        Py_ssize_t i, n = len(items)
        object item
        bytearray payload
        unsigned char[::1] mv
        int64_t* dst
    payload = bytearray(n * 8)
    if n:
        mv = payload
        dst = <int64_t*>&mv[0]
        for i in range(n):
            item = items[i]
            if type(item) is not int:
                return None
            if item < -9223372036854775808 or item > 9223372036854775807:
                return None
            dst[i] = item
    return payload


def decode_int64(bytes raw):
    """把 int64 载荷解码成 list[int]（raw 长度须为 8 的倍数）。"""
    cdef:
        Py_ssize_t i, n
        const unsigned char[::1] mv
        const int64_t* src
        list out
    if len(raw) % 8 != 0:
        raise ValueError("int64 载荷长度不是 8 的倍数")
    n = len(raw) // 8
    out = [0] * n
    if n:
        mv = raw
        src = <const int64_t*>&mv[0]
        for i in range(n):
            out[i] = src[i]
    return out
