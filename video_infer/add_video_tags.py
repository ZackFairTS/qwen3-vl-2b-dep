#!/usr/bin/env python3
"""使用 Ray 分布式计算 + vLLM 为 Lance 表中的视频生成描述和标签。"""
import pyarrow as pa
import boto3, base64, requests, json, re, os, tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from lance_ray import add_columns
import argparse

PROMPT = "用中文描述这个视频并打标签。直接输出JSON，包含两个key：description（字符串），tags（字符串列表）。不要输出markdown格式。"


def infer_video(s3_path, vllm_url):
    parts = s3_path.replace('s3://', '').split('/', 1)
    s3 = boto3.client('s3')
    tmp = tempfile.mktemp(suffix='.mp4')
    try:
        s3.download_file(parts[0], parts[1], tmp)
        with open(tmp, 'rb') as f:
            video_b64 = base64.b64encode(f.read()).decode()
        resp = requests.post(f'{vllm_url}/v1/chat/completions', json={
            'model': 'Qwen/Qwen3-VL-2B-Instruct',
            'messages': [{'role': 'user', 'content': [
                {'type': 'video_url', 'video_url': {'url': f'data:video/mp4;base64,{video_b64}'}},
                {'type': 'text', 'text': PROMPT}
            ]}],
            'max_tokens': 512
        }, timeout=180)
        content = resp.json()['choices'][0]['message']['content']
        try:
            return json.loads(content)
        except json.JSONDecodeError:
            m = re.search(r'\{.*\}', content, re.DOTALL)
            if m:
                return json.loads(m.group())
            return {'description': content, 'tags': []}
    except Exception as e:
        return {'description': f'ERROR: {e}', 'tags': []}
    finally:
        if os.path.exists(tmp):
            os.remove(tmp)


def make_transform(vllm_url, threads):
    def add_video_tags(batch: pa.RecordBatch) -> pa.RecordBatch:
        df = batch.to_pandas()
        results = [None] * len(df)

        def process(idx, s3_path):
            return idx, infer_video(s3_path, vllm_url)

        with ThreadPoolExecutor(max_workers=threads) as executor:
            futures = [executor.submit(process, i, row['video_src']) for i, row in df.iterrows()]
            done = 0
            for f in as_completed(futures):
                idx, res = f.result()
                results[idx] = res
                done += 1
                if done % 10 == 0:
                    print(f"  Batch progress: {done}/{len(df)}")

        descriptions = [r.get('description', '') for r in results]
        tags_list = [json.dumps(r.get('tags', []), ensure_ascii=False) for r in results]
        print(f"  Batch complete: {len(df)} rows")
        return pa.RecordBatch.from_pydict({'description': descriptions, 'tags': tags_list})
    return add_video_tags


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Add video description & tags to Lance table via Ray + vLLM')
    parser.add_argument('uri', help='Lance table URI')
    parser.add_argument('--vllm-url', default='http://qwen3-vl-2b.vllm-inference.svc.cluster.local:8000')
    parser.add_argument('--concurrency', type=int, default=4, help='Ray pool concurrency')
    parser.add_argument('--threads', type=int, default=4, help='Threads per batch for vLLM requests')
    args = parser.parse_args()

    print(f"Processing {args.uri} (concurrency={args.concurrency}, threads={args.threads})")
    add_columns(uri=args.uri, transform=make_transform(args.vllm_url, args.threads), concurrency=args.concurrency)

    import lance
    ds = lance.dataset(args.uri)
    print(f"Done! Schema: {ds.schema}, Rows: {ds.count_rows()}")
