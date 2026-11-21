"""Cython 扩展构建脚本（开发用，不进包元数据）。
用法：
    python setup_fast.py build_ext --inplace
"""

import sys

from Cython.Build import cythonize
from setuptools import Extension, setup

if sys.platform == "win32":
    # MSVC：/openmp 启用 OpenMP（prange 多线程），/O2 优化
    extra_compile_args = ["/O2", "/openmp"]
    extra_link_args: list[str] = []
else:
    # GCC/Clang：-fopenmp 编译 + 链接
    extra_compile_args = ["-O3", "-fopenmp"]
    extra_link_args = ["-fopenmp"]

extensions = [
    Extension(
        "bigarraylist._fast.int64_ops",
        ["bigarraylist/_fast/int64_ops.pyx"],
        extra_compile_args=extra_compile_args,
        extra_link_args=extra_link_args,
    ),
    Extension(
        "bigarraylist._fast.float64_ops",
        ["bigarraylist/_fast/float64_ops.pyx"],
        extra_compile_args=extra_compile_args,
        extra_link_args=extra_link_args,
    ),
    Extension(
        "bigarraylist._fast.merge_ops",
        ["bigarraylist/_fast/merge_ops.pyx"],
        extra_compile_args=extra_compile_args,
        extra_link_args=extra_link_args,
    ),
]

setup(
    ext_modules=cythonize(extensions, compiler_directives={"language_level": "3"}),
)
