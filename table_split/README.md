# Lance Table Split & Video Tagging

Lance 表切分与视频标注工具集。

## 用法

```bash
python split.py <source> <target> [--fraction N] [--register-glue TABLE_NAME] [--database DB] [--region REGION]
```

- `source` - 源 Lance 表路径（支持 S3）
- `target` - 目标 Lance 表路径
- `--fraction` - 切分比例，默认 20（取 1/20）
- `--register-glue` - 注册到 Glue Data Catalog 的表名
- `--database` - Glue 数据库名（默认 `multilake`）
- `--region` - AWS 区域（默认 `ap-northeast-1`）

## 示例

切分并注册到 Glue：

```bash
python split.py \
  s3://tang-emr-tokyo/multilake/microlens_large_segment_videos.lance \
  s3://tang-emr-tokyo/multilake/microlens_large_segment_videos_part.lance \
  --fraction 20 \
  --register-glue microlens_large_segment_videos_part
```

输出：
```
Source: 214118 rows, 19220 video_ids
Target: 10726 rows, 961 video_ids (1/20)
Registered Glue table: multilake.microlens_large_segment_videos_part
```

## 依赖

```
pylance
pyarrow
boto3
```

## 切分逻辑 (split.py)

1. 读取源表所有 `video_id` 并排序
2. 取前 `1/N` 的 `video_id`
3. 筛选这些 `video_id` 对应的全部行（包含所有 `segment_id`）
4. 写入新的 Lance 表

---

## 视频标注 (add_video_tags.py)

使用 Ray 分布式计算 + vLLM 为 Lance 表中每个视频 segment 生成中文描述和标签，通过 `lance_ray.add_columns` 将结果写回表中。

### 用法

```bash
python add_video_tags.py <lance_uri> [--vllm-url URL] [--concurrency N] [--threads N]
```

- `lance_uri` - Lance 表路径（支持 S3）
- `--vllm-url` - vLLM 服务地址（默认 `http://qwen3-vl-2b.vllm-inference.svc.cluster.local:8000`）
- `--concurrency` - Ray 并发 Actor 数（默认 4，不超过集群 CPU 数）
- `--threads` - 每个 Actor 内并发请求 vLLM 的线程数（默认 4）

### 示例

```bash
# 在 Ray head pod 中执行
python add_video_tags.py \
  s3://tang-emr-tokyo/multilake/microlens_large_segment_videos_part.lance \
  --concurrency 4 --threads 4
```

### 新增列

| 列名 | 类型 | 说明 |
|------|------|------|
| `description` | string | 视频中文描述 |
| `tags` | string | JSON 格式标签列表 |

### 依赖

```
pylance
pyarrow
lance-ray
requests
boto3
```
