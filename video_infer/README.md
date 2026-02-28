# Video Infer

使用 Ray 分布式计算 + vLLM 为 Lance 表中每个视频 segment 生成中文描述和标签。

## 用法

```bash
python add_video_tags.py <lance_uri> [--vllm-url URL] [--concurrency N] [--threads N]
```

- `lance_uri` - Lance 表路径（支持 S3）
- `--vllm-url` - vLLM 服务地址（默认 `http://qwen3-vl-2b.vllm-inference.svc.cluster.local:8000`）
- `--concurrency` - Ray 并发 Actor 数（默认 4，不超过集群 CPU 数）
- `--threads` - 每个 Actor 内并发请求 vLLM 的线程数（默认 4）

## 示例

```bash
# 在 Ray head pod 中执行
python add_video_tags.py \
  s3://tang-emr-tokyo/multilake/microlens_large_segment_videos_part.lance \
  --concurrency 4 --threads 4
```

## 新增列

| 列名 | 类型 | 说明 |
|------|------|------|
| `description` | string | 视频中文描述 |
| `tags` | string | JSON 格式标签列表 |

## 依赖

```
pylance
pyarrow
lance-ray
requests
boto3
```
