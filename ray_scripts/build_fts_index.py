#!/usr/bin/env python3
"""为 Lance 表的 description 和 tags 字段构建倒排索引，支持全文检索。

使用方式:
  python3 build_fts_index.py s3://bucket/dataset.lance

验证:
  python3 build_fts_index.py s3://bucket/dataset.lance --query 猫
"""
import lance
import argparse
import os

STORAGE_OPTS = {"region": "ap-northeast-1", "aws_region": "ap-northeast-1"}


def build_index(uri, columns):
    os.environ["AWS_DEFAULT_REGION"] = "ap-northeast-1"
    os.environ["AWS_REGION"] = "ap-northeast-1"

    ds = lance.dataset(uri, storage_options=STORAGE_OPTS)
    print(f"Table: {uri} ({ds.count_rows()} rows)")

    existing = {idx["name"] for idx in ds.list_indices()}
    for col in columns:
        idx_name = f"{col}_idx"
        if idx_name in existing:
            print(f"  Skip {col}: index already exists")
            continue
        print(f"  Creating inverted index on '{col}'...")
        ds.create_scalar_index(col, index_type="INVERTED")
        print(f"  Done: {idx_name}")

    ds = lance.dataset(uri, storage_options=STORAGE_OPTS)
    print(f"\nIndices: {[idx['name'] for idx in ds.list_indices()]}")
    return ds


def test_query(ds, query):
    print(f"\n=== Full-text search: '{query}' ===")
    results = ds.scanner(
        full_text_query=query,
        columns=["video_id", "segment_id", "description", "tags", "_score"],
    ).to_table()
    for i in range(min(5, results.num_rows)):
        vid = results.column("video_id")[i].as_py()
        sid = results.column("segment_id")[i].as_py()
        desc = (results.column("description")[i].as_py() or "")[:60]
        tags = results.column("tags")[i].as_py() or ""
        score = results.column("_score")[i].as_py()
        print(f"  [{score:.2f}] video_id={vid}, segment_id={sid}")
        print(f"         desc: {desc}...")
        print(f"         tags: {tags}")
    print(f"Total: {results.num_rows} results")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build inverted index for full-text search on Lance table")
    parser.add_argument("uri", help="Lance table URI")
    parser.add_argument("--columns", nargs="+", default=["description", "tags"],
                        help="Columns to index (default: description tags)")
    parser.add_argument("--query", help="Optional: run a test query after indexing")
    args = parser.parse_args()

    ds = build_index(args.uri, args.columns)
    if args.query:
        test_query(ds, args.query)
