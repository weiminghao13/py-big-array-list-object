"""BigArrayList 使用示例（0.2.0：含链式 API + 持久化分桶 + 低内存外部排序）。

运行：
    python examples/demo.py
"""

import itertools

from bigarraylist import BigArrayConfig, BigArrayList


def main() -> None:
    # 1) 构造 + 惰性排序
    arr = BigArrayList([3, 1, 4, 1, 5, 9, 2, 6, 5, 3, 5, 1])
    print("sorted:", list(arr.sort()))

    # 2) 线性查找（惰性，只取前 2 个命中）
    gen = arr.find(5)
    print("find 5 (前2个):", list(itertools.islice(gen, 2)))
    gen.close()  # 早停，释放剩余

    # 3) 链式 API（新）：.lazy() 进入 LazyChain，每个算子返回新 LazyChain
    result = (
        arr.lazy().filter(lambda x: x > 2).map(lambda x: x * 10).distinct().slice(0, 5).collect()
    )  # 终结：物化为 list
    print("chain .filter.map.distinct.slice.collect:", result)
    # [30, 40, 90, 60, 50]

    # 4) 蓄水池抽样：从 10 万条流里随机抽 5 个
    sample = list(BigArrayList(range(100_000)).sample(5, seed=42))
    print("sample 5 (seed=42):", sample)

    # 5) 二级索引：建好后 O(1) 拿到所有命中下标
    idx = BigArrayList([3, 1, 4, 1, 5, 9, 2, 6, 5, 3, 5, 1]).build_index()
    print("index[5] 出现下标:", idx[5])  # [4, 8, 10]

    # 6) 持久化分桶（新）：建桶一次，多次 find 复用
    #    每次只扫目标桶 O(n/N)，避免 bucket_find 每次重建桶
    targets = [3, 5, 9]
    with arr.build_buckets() as bidx:
        for tgt in targets:
            hits = list(bidx.find(tgt))
            print(f"  bucket find {tgt}:", hits)

    # 7) 自定义异常（新）：raise_custom=True 时改抛自定义异常子类（仍继承标准异常）
    #    IndexOutOfRangeError 继承 IndexError，向后兼容——现有 except IndexError 仍能捕获
    arr2 = BigArrayList([1, 2, 3], raise_custom=True)
    try:
        arr2.remove(1, 999)  # 越界
    except IndexError as e:  # 向后兼容：仍能被 IndexError 捕获
        print("  raise_custom 越界（被 IndexError 捕获）:", type(e).__name__)

    # 8) BigArrayConfig（新）：阈值可配置
    #    例：外部排序走整块模式（快 3~5 倍，内存 ≈ 数据总量；适合数据放得下内存）
    cfg = BigArrayConfig(external_sort_streaming_merge=False)
    big = BigArrayList(range(1_500_000), config=cfg)
    top3 = list(itertools.islice(big.sort(), 3))
    print("block-mode external sort 前3:", top3)  # [0, 0, 0]


if __name__ == "__main__":
    main()
