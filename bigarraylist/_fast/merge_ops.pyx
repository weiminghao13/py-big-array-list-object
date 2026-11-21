# cython: language_level=3, boundscheck=False, wraparound=False, initializedcheck=False

"""同类型 int64/float64 二进制帧文件的流式 C 多路归并。"""

from libc.stdint cimport int64_t
from libc.stdlib cimport free, malloc


def _read_exact(file, Py_ssize_t size):
    cdef list parts = []
    cdef bytes part
    cdef Py_ssize_t remaining = size
    while remaining:
        part = file.read(remaining)
        if not part:
            raise EOFError("数值帧被截断")
        parts.append(part)
        remaining -= len(part)
    return b"".join(parts)


cdef class _NumericReader:
    cdef:
        object file
        int kind
        const unsigned char[::1] view
        Py_ssize_t pos
        Py_ssize_t count
        bint done
        int64_t int_value
        double float_value

    def __init__(self, file, int kind):
        self.file = file
        self.kind = kind
        self.pos = 0
        self.count = 0
        self.done = False

    cdef void advance(self) except *:
        cdef:
            bytes tag
            bytes raw
            Py_ssize_t size
            const int64_t* ints
            const double* floats
        while self.pos >= self.count and not self.done:
            tag = self.file.read(1)
            if not tag:
                self.done = True
                return
            if (self.kind == 0 and tag != b"\x00") or (self.kind == 1 and tag != b"\x01"):
                raise ValueError("C 归并只接受同类型数值帧")
            self.count = int.from_bytes(_read_exact(self.file, 4), "little")
            if self.count == 0:
                continue
            raw = _read_exact(self.file, self.count * 8)
            self.view = raw
            self.pos = 0
        if self.done:
            return
        if self.kind == 0:
            ints = <const int64_t*>&self.view[0]
            self.int_value = ints[self.pos]
        else:
            floats = <const double*>&self.view[0]
            self.float_value = floats[self.pos]
        self.pos += 1


cdef inline bint _comes_before(
    _NumericReader left,
    _NumericReader right,
    int kind,
    bint reverse,
) noexcept:
    if kind == 0:
        if reverse:
            return left.int_value > right.int_value
        return left.int_value < right.int_value
    if reverse:
        return left.float_value > right.float_value
    return left.float_value < right.float_value


cdef void _sift_down(
    Py_ssize_t* heap,
    Py_ssize_t heap_size,
    Py_ssize_t start,
    list readers,
    int kind,
    bint reverse,
) except *:
    cdef:
        Py_ssize_t root = start
        Py_ssize_t child
        Py_ssize_t tmp
        _NumericReader root_reader
        _NumericReader child_reader
    while root * 2 + 1 < heap_size:
        child = root * 2 + 1
        if child + 1 < heap_size:
            if _comes_before(readers[heap[child + 1]], readers[heap[child]], kind, reverse):
                child += 1
        root_reader = readers[heap[root]]
        child_reader = readers[heap[child]]
        if not _comes_before(child_reader, root_reader, kind, reverse):
            return
        tmp = heap[root]
        heap[root] = heap[child]
        heap[child] = tmp
        root = child


def merge_numeric_files(list files, int kind, bint reverse=False):
    """归已排序的同类型数值帧文件；kind=0 为 int64，kind=1 为 float64。"""
    cdef:
        Py_ssize_t i
        Py_ssize_t heap_size = len(files)
        Py_ssize_t* heap = NULL
        _NumericReader reader
        list readers
    if kind not in (0, 1):
        raise ValueError("kind 必须为 0 (int64) 或 1 (float64)")
    if heap_size == 0:
        return
    readers = [_NumericReader(file, kind) for file in files]
    heap = <Py_ssize_t*>malloc(heap_size * sizeof(Py_ssize_t))
    if heap == NULL:
        raise MemoryError("无法分配数值归并堆")
    try:
        for i in range(heap_size):
            reader = readers[i]
            reader.advance()
            heap[i] = i
        for i in range(heap_size // 2 - 1, -1, -1):
            _sift_down(heap, heap_size, i, readers, kind, reverse)
        while heap_size:
            reader = readers[heap[0]]
            if kind == 0:
                yield reader.int_value
            else:
                yield reader.float_value
            reader.advance()
            if reader.done:
                heap_size -= 1
                heap[0] = heap[heap_size]
            if heap_size > 1:
                _sift_down(heap, heap_size, 0, readers, kind, reverse)
    finally:
        free(heap)
