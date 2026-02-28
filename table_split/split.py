#!/usr/bin/env python3
"""按 video_id 切分 Lance 表，保证每个 video_id 的所有 segment 完整。支持自动注册到 Glue Data Catalog。"""
import lance
import pyarrow as pa
import pyarrow.compute as pc
import argparse
import boto3

LANCE_TYPE_TO_GLUE = {'int64': 'int', 'double': 'double', 'string': 'string', 'float': 'float'}


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
    return ds.schema


def register_glue_table(table_name: str, location: str, schema, database: str = 'multilake', region: str = 'ap-northeast-1'):
    columns = [{'Name': f.name, 'Type': LANCE_TYPE_TO_GLUE.get(str(f.type), 'string')} for f in schema]
    glue = boto3.client('glue', region_name=region)
    glue.create_table(
        DatabaseName=database,
        TableInput={
            'Name': table_name,
            'StorageDescriptor': {
                'Columns': columns,
                'Location': location,
                'InputFormat': 'lance',
                'OutputFormat': 'lance',
                'Compressed': False,
            },
            'TableType': 'EXTERNAL_TABLE',
            'Parameters': {'classification': 'lance'},
        }
    )
    print(f"Registered Glue table: {database}.{table_name}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Split Lance table by video_id')
    parser.add_argument('source', help='Source Lance table URI')
    parser.add_argument('target', help='Target Lance table URI')
    parser.add_argument('--fraction', type=int, default=20, help='Split fraction (default: 1/20)')
    parser.add_argument('--register-glue', metavar='TABLE_NAME', help='Register target to Glue Data Catalog')
    parser.add_argument('--database', default='multilake', help='Glue database name (default: multilake)')
    parser.add_argument('--region', default='ap-northeast-1', help='AWS region (default: ap-northeast-1)')
    args = parser.parse_args()

    schema = split_table(args.source, args.target, args.fraction)
    if args.register_glue:
        register_glue_table(args.register_glue, args.target, schema, args.database, args.region)
