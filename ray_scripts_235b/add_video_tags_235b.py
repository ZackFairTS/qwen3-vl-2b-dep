#!/usr/bin/env python3
"""使用 Ray 分布式计算 + AWS Bedrock Qwen3-VL-235B 为 Lance 表中的视频生成描述和标签。

通过 Bedrock Converse API 调用 us-east-1 的 qwen.qwen3-vl-235b-a22b 模型，
将推理结果作为 des_235b 和 tags_235b 两列添加到 Lance 表中。

使用方式:
  # 自动从 Glue Catalog 获取 Lance 表 URI
  python3 add_video_tags_235b.py

  # 手动指定 URI
  python3 add_video_tags_235b.py --uri s3://bucket/dataset.lance

  # 调整并发（根据 Bedrock 配额调整）
  python3 add_video_tags_235b.py --concurrency 2 --threads 4 --batch-size 32
"""
import pyarrow as pa
import boto3
import json
import re
import os
import tempfile
import time
from botocore.config import Config
from botocore.exceptions import ClientError
from concurrent.futures import ThreadPoolExecutor, as_completed
from lance_ray import add_columns
import argparse

PROMPT = "用中文简洁描述这个视频并打标签。直接输出JSON，包含两个key：description（字符串，100字以内），tags（字符串列表）。不要输出markdown格式。"

MODEL_ID = "qwen.qwen3-vl-235b-a22b"
BEDROCK_REGION = "us-east-1"

# Retry config for Bedrock throttling
MAX_RETRIES = 6
BASE_DELAY = 2.0


def infer_video(s3_path, bedrock_client, s3_client):
    """使用 Bedrock Converse API 推理单个视频"""
    parts = s3_path.replace("s3://", "").split("/", 1)
    tmp = tempfile.mktemp(suffix=".mp4")
    try:
        s3_client.download_file(parts[0], parts[1], tmp)
        with open(tmp, "rb") as f:
            video_bytes = f.read()

        # Bedrock Converse API with manual retry for throttling
        last_err = None
        for attempt in range(MAX_RETRIES):
            try:
                response = bedrock_client.converse(
                    modelId=MODEL_ID,
                    messages=[{
                        "role": "user",
                        "content": [
                            {
                                "video": {
                                    "format": "mp4",
                                    "source": {"bytes": video_bytes}
                                }
                            },
                            {"text": PROMPT}
                        ]
                    }],
                    inferenceConfig={
                        "maxTokens": 350,
                        "temperature": 0.1
                    }
                )
                break
            except ClientError as e:
                code = e.response["Error"]["Code"]
                if code in ("ThrottlingException", "TooManyRequestsException",
                            "ServiceUnavailableException", "ModelTimeoutException"):
                    last_err = e
                    if attempt < MAX_RETRIES - 1:
                        delay = BASE_DELAY * (2 ** attempt)
                        print(f"    Bedrock {code}, retry {attempt+1}/{MAX_RETRIES} after {delay:.0f}s")
                        time.sleep(delay)
                    else:
                        raise
                else:
                    raise

        content = response["output"]["message"]["content"][0]["text"]

        # Strip Qwen3 thinking tags if present
        content = re.sub(r"<think>[\s\S]*?</think>", "", content).strip()

        # Parse JSON from response
        try:
            return json.loads(content)
        except json.JSONDecodeError:
            m = re.search(r"\{.*\}", content, re.DOTALL)
            if m:
                return json.loads(m.group())
            return {"description": content, "tags": []}
    except Exception as e:
        return {"description": f"ERROR: {e}", "tags": []}
    finally:
        if os.path.exists(tmp):
            os.remove(tmp)


def make_transform(threads, bedrock_region):
    """创建 batch 处理函数，每个 Ray Actor 内部创建 boto3 client"""
    def add_video_tags_235b(batch: pa.RecordBatch) -> pa.RecordBatch:
        bedrock_config = Config(
            region_name=bedrock_region,
            retries={"max_attempts": 3, "mode": "adaptive"}
        )
        bedrock_client = boto3.client("bedrock-runtime", config=bedrock_config)
        s3_client = boto3.client("s3")

        df = batch.to_pandas()
        results = [None] * len(df)

        def process(idx, s3_path):
            return idx, infer_video(s3_path, bedrock_client, s3_client)

        with ThreadPoolExecutor(max_workers=threads) as executor:
            futures = [executor.submit(process, i, row["video_src"])
                       for i, row in df.iterrows()]
            done = 0
            for f in as_completed(futures):
                idx, res = f.result()
                results[idx] = res
                done += 1
                if done % 10 == 0:
                    print(f"  Batch progress: {done}/{len(df)}")

        descriptions = [r.get("description", "") for r in results]
        tags_list = [json.dumps(r.get("tags", []), ensure_ascii=False) for r in results]
        print(f"  Batch complete: {len(df)} rows")
        return pa.RecordBatch.from_pydict({
            "des_235b": descriptions,
            "tags_235b": tags_list
        })

    return add_video_tags_235b


def get_table_uri(table_name, database="multilake", region="ap-northeast-1"):
    """从 Glue Data Catalog 获取 Lance 表的 S3 URI"""
    glue = boto3.client("glue", region_name=region)
    resp = glue.get_table(DatabaseName=database, Name=table_name)
    return resp["Table"]["StorageDescriptor"]["Location"]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Add video des_235b & tags_235b to Lance table via Ray + Bedrock Qwen3-VL-235B"
    )
    parser.add_argument("--uri", help="Lance table URI (S3)，不指定则从 Glue Catalog 自动获取")
    parser.add_argument("--table-name", default="microlens_large_segment_videos_part_20frags",
                        help="Glue table name (default: microlens_large_segment_videos_part_20frags)")
    parser.add_argument("--database", default="multilake",
                        help="Glue database (default: multilake)")
    parser.add_argument("--glue-region", default="ap-northeast-1",
                        help="Glue catalog region (default: ap-northeast-1)")
    parser.add_argument("--bedrock-region", default="us-east-1",
                        help="Bedrock model region (default: us-east-1)")
    parser.add_argument("--concurrency", type=int, default=2,
                        help="Ray Actor 并发数 (default: 2)")
    parser.add_argument("--threads", type=int, default=4,
                        help="每个 Actor 的线程数 (default: 4)")
    parser.add_argument("--batch-size", type=int, default=32,
                        help="每个 batch 的行数 (default: 32)")
    args = parser.parse_args()

    # Get Lance table URI
    if args.uri:
        uri = args.uri
    else:
        print(f"Looking up table {args.database}.{args.table_name} in Glue ({args.glue_region})...")
        uri = get_table_uri(args.table_name, args.database, args.glue_region)
        print(f"  Found: {uri}")

    print(f"\nProcessing {uri}")
    print(f"  Model: {MODEL_ID} ({args.bedrock_region})")
    print(f"  max_tokens: 350, temperature: 0.1")
    print(f"  New columns: des_235b, tags_235b")
    print(f"  concurrency: {args.concurrency}")
    print(f"  threads: {args.threads}")
    print(f"  batch_size: {args.batch_size}")
    print(f"  理论并发: {args.concurrency} × {args.threads} = {args.concurrency * args.threads}")
    print()

    add_columns(
        uri=uri,
        transform=make_transform(args.threads, args.bedrock_region),
        concurrency=args.concurrency,
        batch_size=args.batch_size
    )

    import lance
    ds = lance.dataset(uri)
    print(f"\n✅ Done! Rows: {ds.count_rows()}")
    print(f"Columns: {ds.schema.names}")
