# cython: language_level=3, boundscheck=False, wraparound=False, cdivision=True, initializedcheck=False

"""float64 紧凑数组后端（Cython 快路径，第四步新增）。

与 Int64Array 同骨架：malloc 紧凑数组 + OWNDATA 语义 + C 级循环。
覆盖数值最常用算子：sum/min/max/sort/view；含 NaN 的数据
min/max 返回 Python float('nan')（对齐 min()/max() 传播 NaN 的语义）。

元素必须可转 float（int/float/bool 除外——bool 语义不同被拒）；
转换失败抛 TypeError/OverflowError，调用方捕获后回退 Python 后端。
"""

from cpython.bytes cimport PyBytes_FromStringAndSize
from libc.stdint cimport uint64_t, uint8_t
from libc.stdlib cimport calloc, free, malloc

__all__ = ["Float64Array"]

# introsort 小分区阈值（与 int64_ops 保持一致）
cdef int _SORT_CUTOFF = 16


cdef inline void _insertion_sort_f(double* a, Py_ssize_t lo, Py_ssize_t hi) noexcept nogil:
    """插入排序 [lo, hi)。"""
    cdef Py_ssize_t i, j
    cdef double key
    for i in range(lo + 1, hi):
        key = a[i]
        j = i - 1
        while j >= lo and a[j] > key:
            a[j + 1] = a[j]
            j -= 1
        a[j + 1] = key


cdef inline double _median3_f(double a, double b, double c) noexcept nogil:
    """三数取中。"""
    if a < b:
        if b < c:
            return b
        return a if a > c else c
    if a < c:
        return a
    return b if b > c else c


cdef void _introsort_impl_f(double* a, Py_ssize_t lo, Py_ssize_t hi, int depth) noexcept nogil:
    """introsort 主体（快排 + 小分区插排；浮点无堆排兜底——数据来自
    Python float，最坏退化仅影响极小分区后的插排段）。"""
    cdef:
        Py_ssize_t i, j
        double pivot, tmp
    while hi - lo > _SORT_CUTOFF:
        pivot = _median3_f(a[lo], a[lo + (hi - lo) // 2], a[hi - 1])
        i = lo - 1
        j = hi
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
        if j - lo < hi - (j + 1):
            _introsort_impl_f(a, lo, j + 1, depth)
            lo = j + 1
        else:
            _introsort_impl_f(a, j + 1, hi, depth)
            hi = j + 1
    _insertion_sort_f(a, lo, hi)


cdef class Float64Array:
    """紧凑连续 double[] 数组，对标 NumPy 的 data 指针 + OWNDATA 语义。"""

    cdef:
        double* data
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
        """list[float|int] → 紧凑 C 数组（一次性转换）。"""
        cdef:
            Float64Array a = cls.__new__(cls)
            Py_ssize_t i, n = len(src)
            object item
        a.n = n
        if n > 0:
            a.data = <double*>malloc(n * sizeof(double))
            if a.data == NULL:
                raise MemoryError(f"无法分配 {n} 个 float64")
            for i in range(n):
                item = src[i]
                if item is True or item is False:
                    raise TypeError("bool 语义与数值不同，不走 float 快路径")
                a.data[i] = item
        a.owns_data = True
        return a

    def __len__(self):
        return self.n

    # ==================== 纯 C 算术（无回调）====================

    def py_sum(self):
        """C 级求和。"""
        cdef:
            double total = 0.0
            Py_ssize_t i
        with nogil:
            for i in range(self.n):
                total += self.data[i]
        return total

    def py_min(self):
        """C 级最小值：空数据返回 None。含 NaN 时返回 NaN（对齐 min() 传播语义）。"""
        cdef:
            double m
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
        """C 级最大值：空数据返回 None。含 NaN 时返回 NaN（对齐 max() 传播语义）。"""
        cdef:
            double m
            Py_ssize_t i
        if self.n == 0:
            return None
        m = self.data[0]
        with nogil:
            for i in range(1, self.n):
                if self.data[i] > m:
                    m = self.data[i]
        return m

    # ==================== 排序（C 级 introsort）====================

    def py_sort(self, bint reverse=False):
        """C 级 introsort：原地排序，返回 None。全程释放 GIL。"""
        cdef:
            double tmp
            Py_ssize_t i, j
        if self.n < 2:
            return None
        with nogil:
            _introsort_impl_f(self.data, 0, self.n, _depth_limit(self.n))
            if reverse:
                j = self.n - 1
                for i in range(self.n // 2):
                    tmp = self.data[i]
                    self.data[i] = self.data[j]
                    self.data[j] = tmp
                    j -= 1
        return None

    def py_sorted_copy(self, bint reverse=False):
        """C 级 introsort：排序到新数组（不动原数据），返回新 Float64Array。"""
        cdef:
            Float64Array out
            double tmp
            Py_ssize_t i, j
        out = Float64Array.__new__(Float64Array)
        out.n = self.n
        if self.n == 0:
            return out
        out.data = <double*>malloc(self.n * sizeof(double))
        if out.data == NULL:
            raise MemoryError(f"无法分配 {self.n} 个 float64")
        out.owns_data = True
        with nogil:
            for i in range(self.n):
                out.data[i] = self.data[i]
            _introsort_impl_f(out.data, 0, self.n, _depth_limit(self.n))
            if reverse:
                j = self.n - 1
                for i in range(self.n // 2):
                    tmp = out.data[i]
                    out.data[i] = out.data[j]
                    out.data[j] = tmp
                    j -= 1
        return out

    # ==================== 切片视图（零拷贝）====================

    def py_view(self, Py_ssize_t start, Py_ssize_t stop, Py_ssize_t step=1):
        """切片视图：零拷贝（步长 1 指针偏移；步长 != 1 物化拷贝）。

        负索引/越界对齐 Python 切片语义。
        """
        cdef:
            Float64Array out
            Py_ssize_t i, k, lo
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
            out = Float64Array.__new__(Float64Array)
            out.n = stop - start
            out.data = self.data + start
            out.owns_data = False
            return out
        k = 0
        for i in range(start, stop, step):
            k += 1
        out = Float64Array.__new__(Float64Array)
        out.n = k
        if k == 0:
            return out
        out.data = <double*>malloc(k * sizeof(double))
        if out.data == NULL:
            raise MemoryError(f"无法分配 {k} 个 float64")
        out.owns_data = True
        i = 0
        lo = start
        while lo < stop:
            out.data[i] = self.data[lo]
            i += 1
            lo += step
        return out

    def py_to_list(self):
        """物化：double[] → Python list[float]。"""
        cdef:
            list out
            Py_ssize_t i
        out = [0.0] * self.n
        for i in range(self.n):
            out[i] = self.data[i]
        return out

    def py_to_bytes(self):
        """二进制物化：double[] → bytes（原生字节序）。

        与 py_to_list 同为一次拷贝，但不创建任何 Python 对象——外部排序
        数值块落盘直接拿这段字节写 float64 帧载荷，零逐元素解释开销。
        """
        cdef:
            Py_ssize_t nbytes = self.n * sizeof(double)
            bytes out
        if nbytes == 0:
            return b""
        # 一次 memcpy 直接产出 bytes（无中间 bytearray / 逐元素 Python 对象）
        out = PyBytes_FromStringAndSize(<const char*>self.data, nbytes)
        return out

    # ==================== 查找 / 计数（C 扫描，方案二）====================
    # NaN 语义：==、<、> 与 NaN 比较恒为 False，与 Python float() 一致，
    # 不做特判。

    def py_contains(self, double target):
        """C 扫描：target 是否在数组中，命中即返回（全程释放 GIL）。"""
        cdef:
            Py_ssize_t i
            bint found = False
        with nogil:
            for i in range(self.n):
                if self.data[i] == target:
                    found = True
                    break
        return found

    def py_find_indices(self, double target):
        """C 扫描：返回所有命中下标 list（保持原序）。"""
        cdef:
            list out = []
            Py_ssize_t i
        for i in range(self.n):
            if self.data[i] == target:
                out.append(i)
        return out

    def py_count_equal(self, double target):
        """C 计数：== target 的元素个数（全程释放 GIL）。"""
        cdef:
            Py_ssize_t i, c = 0
        with nogil:
            for i in range(self.n):
                if self.data[i] == target:
                    c += 1
        return c

    def py_count_less(self, double target):
        """C 计数：< target 的元素个数（全程释放 GIL）。"""
        cdef:
            Py_ssize_t i, c = 0
        with nogil:
            for i in range(self.n):
                if self.data[i] < target:
                    c += 1
        return c

    def py_count_greater(self, double target):
        """C 计数：> target 的元素个数（全程释放 GIL）。"""
        cdef:
            Py_ssize_t i, c = 0
        with nogil:
            for i in range(self.n):
                if self.data[i] > target:
                    c += 1
        return c

    def py_is_sorted(self):
        """单遍 C 检查非降序；NaN 比较语义与 Python 一致。"""
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
        """稳定哈希去重；含 NaN 时返回 None，由 Python 保留对象身份语义。"""
        cdef:
            Py_ssize_t i, count = 0, capacity = 8, mask, slot
            double value
            uint64_t h
            double* values = NULL
            uint8_t* used = NULL
            Py_ssize_t* indices = NULL
            bint has_nan = False
            list out
        if self.n == 0:
            return []
        while capacity < self.n * 2:
            capacity *= 2
        mask = capacity - 1
        values = <double*>malloc(capacity * sizeof(double))
        used = <uint8_t*>calloc(capacity, sizeof(uint8_t))
        indices = <Py_ssize_t*>malloc(self.n * sizeof(Py_ssize_t))
        if values == NULL or used == NULL or indices == NULL:
            free(values)
            free(used)
            free(indices)
            raise MemoryError(f"无法为 {self.n} 个 float64 建立去重哈希表")
        with nogil:
            for i in range(self.n):
                value = self.data[i]
                if value != value:
                    has_nan = True
                    break
                if value == 0.0:
                    h = 0
                else:
                    h = (<uint64_t*>&value)[0]
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
        if has_nan:
            free(indices)
            return None
        out = [0] * count
        for i in range(count):
            out[i] = indices[i]
        free(indices)
        return out

    def __iter__(self):
        return iter(self.py_to_list())


cdef inline int _depth_limit(Py_ssize_t n) noexcept nogil:
    """introsort 深度上限：2 * floor(log2(n))。"""
    cdef int d = 0
    while n > 1:
        n >>= 1
        d += 1
    return 2 * d if d > 0 else 1


# ==================== 帧 IO（方案②：替代 pickle，服务 from_stream 数值流）====================
# 与 int64_ops 同协议；严格只收 float（type(x) is float），int 走 int64 帧，
# 混排批整体回退 pickle，保证 list(big) 的元素类型与源一致、无精度漂移。


def encode_float64(list items):
    """把一批 float 编码成 float64 载荷（n*8 字节）；含非 float 返回 None。"""
    cdef:
        Py_ssize_t i, n = len(items)
        object item
        bytearray payload
        unsigned char[::1] mv
        double* dst
    payload = bytearray(n * 8)
    if n:
        mv = payload
        dst = <double*>&mv[0]
        for i in range(n):
            item = items[i]
            if type(item) is not float:
                return None
            dst[i] = item
    return payload


def decode_float64(bytes raw):
    """把 float64 载荷解码成 list[float]（raw 长度须为 8 的倍数）。"""
    cdef:
        Py_ssize_t i, n
        const unsigned char[::1] mv
        const double* src
        list out
    if len(raw) % 8 != 0:
        raise ValueError("float64 载荷长度不是 8 的倍数")
    n = len(raw) // 8
    out = [0.0] * n
    if n:
        mv = raw
        src = <const double*>&mv[0]
        for i in range(n):
            out[i] = src[i]
    return out
