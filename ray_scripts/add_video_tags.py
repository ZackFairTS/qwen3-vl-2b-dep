#!/usr/bin/env python3
"""使用 Ray 分布式计算 + vLLM 为 Lance 表中的视频生成描述和标签。

优化配置:
- max_tokens: 350 (从 512 优化，加速 2.7x)
- 支持自定义 batch_size 和并发度
- 建议使用多个 fragments 提升并行度
"""
import pyarrow as pa
import boto3, base64, requests, json, re, os, tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from lance_ray import add_columns
import argparse

PROMPT = "用中文简洁描述这个视频并打标签。直接输出JSON，包含两个key：description（字符串，100字以内），tags（字符串列表）。不要输出markdown格式。"


def infer_video(s3_path, vllm_url):
    """推理单个视频"""
    parts = s3_path.replace("s3://", "").split("/", 1)
    s3 = boto3.client("s3")
    tmp = tempfile.mktemp(suffix=".mp4")
    try:
        s3.download_file(parts[0], parts[1], tmp)
        with open(tmp, "rb") as f:
            video_b64 = base64.b64encode(f.read()).decode()
        resp = requests.post(f"{vllm_url}/v1/chat/completions", json={
            "model": "Qwen/Qwen3-VL-2B-Instruct",
            "messages": [{"role": "user", "content": [
                {"type": "video_url", "video_url": {"url": f"data:video/mp4;base64,{video_b64}"}},
                {"type": "text", "text": PROMPT}
            ]}],
            "max_tokens": 350,
            "temperature": 0.3,
            "mm_processor_kwargs": {
                "fps": 0.5,
                "size": {"shortest_edge": 131072, "longest_edge": 360448}
            }
        }, timeout=120)
        content = resp.json()["choices"][0]["message"]["content"]
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


def make_transform(vllm_url, threads):
    """创建 batch 处理函数"""
    def add_video_tags(batch: pa.RecordBatch) -> pa.RecordBatch:
        df = batch.to_pandas()
        results = [None] * len(df)
        
        def process(idx, s3_path):
            return idx, infer_video(s3_path, vllm_url)
        
        with ThreadPoolExecutor(max_workers=threads) as executor:
            futures = [executor.submit(process, i, row["video_src"]) for i, row in df.iterrows()]
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
        return pa.RecordBatch.from_pydict({"description": descriptions, "tags": tags_list})
    
    return add_video_tags


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Add video description & tags to Lance table via Ray + vLLM")
    parser.add_argument("uri", help="Lance table URI (支持 S3)")
    parser.add_argument("--vllm-url", default="http://qwen3-vl-2b.vllm-inference.svc.cluster.local:8000",
                        help="vLLM 服务地址")
    parser.add_argument("--concurrency", type=int, default=4,
                        help="Ray Actor 并发数 (不超过集群 CPU 数)")
    parser.add_argument("--threads", type=int, default=6,
                        help="每个 Actor 的线程数")
    parser.add_argument("--batch-size", type=int, default=128,
                        help="每个 batch 的行数 (建议 128-256)")
    args = parser.parse_args()
    
    print(f"Processing {args.uri}")
    print(f"  max_tokens: 350")
    print(f"  concurrency: {args.concurrency}")
    print(f"  threads: {args.threads}")
    print(f"  batch_size: {args.batch_size}")
    print(f"  理论并发: {args.concurrency} × {args.threads} = {args.concurrency * args.threads}")
    print()
    
    add_columns(
        uri=args.uri,
        transform=make_transform(args.vllm_url, args.threads),
        concurrency=args.concurrency,
        batch_size=args.batch_size
    )
    
    import lance
    ds = lance.dataset(args.uri)
    print(f"\n✅ Done! Rows: {ds.count_rows()}")
