#!/usr/bin/env python3
"""优化 Lance 数据集以提升 Ray 并行度

通过增加 fragments 数量，让 Ray 可以并行处理更多 batch。
建议: 每个 fragment 500-1000 行
"""
import lance
import argparse


def optimize_dataset(source_uri, target_uri, rows_per_fragment=500):
    """重写数据集增加 fragments"""
    print(f"读取: {source_uri}")
    ds = lance.dataset(source_uri)
    total_rows = ds.count_rows()
    old_frags = len(list(ds.get_fragments()))
    
    print(f"  总行数: {total_rows}")
    print(f"  原始 fragments: {old_frags}")
    print()
    
    expected_frags = total_rows // rows_per_fragment
    print(f"优化配置:")
    print(f"  rows_per_fragment: {rows_per_fragment}")
    print(f"  预期 fragments: {expected_frags}")
    print()
    
    table = ds.to_table()
    
    print(f"写入: {target_uri}")
    lance.write_dataset(
        table,
        target_uri,
        max_rows_per_file=rows_per_fragment,
        mode="overwrite"
    )
    
    # 验证
    new_ds = lance.dataset(target_uri)
    new_frags = len(list(new_ds.get_fragments()))
    
    print()
    print(f"✅ 完成!")
    print(f"  总行数: {new_ds.count_rows()}")
    print(f"  Fragments: {old_frags} → {new_frags} ({new_frags}x 并行度)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Optimize Lance dataset for Ray parallelism")
    parser.add_argument("source", help="源 Lance 表 URI")
    parser.add_argument("target", help="目标 Lance 表 URI")
    parser.add_argument("--rows-per-fragment", type=int, default=500,
                        help="每个 fragment 的行数 (默认 500)")
    args = parser.parse_args()
    
    optimize_dataset(args.source, args.target, args.rows_per_fragment)
