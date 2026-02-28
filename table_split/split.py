#!/usr/bin/env python3
"""按 video_id 切分 Lance 表，保证每个 video_id 的所有 segment 完整。"""
import lance
import pyarrow as pa
import pyarrow.compute as pc
import argparse


def split_table(source: str, target: str, fraction: int = 20):
    ds = lance.dataset(source)
    tbl = ds.to_table()
    total_rows = tbl.num_rows

    unique_vids = sorted(pc.unique(tbl['video_id']).to_pylist())
    n = len(unique_vids) // fraction
    selected = pa.array(unique_vids[:n], type=tbl.schema.field('video_id').type)

    filtered = tbl.filter(pc.is_in(tbl['video_id'], value_set=selected))

    lance.write_dataset(filtered, target)

    print(f"Source: {total_rows} rows, {len(unique_vids)} video_ids")
    print(f"Target: {filtered.num_rows} rows, {n} video_ids (1/{fraction})")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Split Lance table by video_id')
    parser.add_argument('source', help='Source Lance table URI')
    parser.add_argument('target', help='Target Lance table URI')
    parser.add_argument('--fraction', type=int, default=20, help='Split fraction (default: 1/20)')
    args = parser.parse_args()
    split_table(args.source, args.target, args.fraction)
