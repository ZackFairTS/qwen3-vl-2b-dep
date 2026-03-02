# Ray + Bedrock Qwen3-VL-235B 视频推理

使用 Ray 分布式 + AWS Bedrock `qwen.qwen3-vl-235b-a22b` 模型为 Lance 表中的视频生成描述和标签，添加 `des_235b` 和 `tags_235b` 两列。

## 文件说明

| 文件 | 说明 |
|------|------|
| `add_video_tags_235b.py` | 主推理脚本，通过 Bedrock Converse API 调用 235B 模型 |

## 使用方法

### 安装依赖

```bash
pip install lance lance-ray boto3 pyarrow
```

### 运行推理

```bash
# 自动从 Glue Catalog 获取 Lance 表 URI
python3 add_video_tags_235b.py

# 手动指定 URI
python3 add_video_tags_235b.py --uri s3://bucket/dataset.lance

# 调整并发（根据 Bedrock 配额）
python3 add_video_tags_235b.py --concurrency 2 --threads 4 --batch-size 32
```

### 部署到 Ray 集群

```bash
HEAD_POD=$(kubectl get pods -l ray.io/node-type=head -o jsonpath='{.items[0].metadata.name}')

# 安装依赖
kubectl exec $HEAD_POD -c ray-head -- pip install lance lance-ray boto3 pyarrow -q

# 复制脚本
kubectl cp ray_scripts_235b/add_video_tags_235b.py $HEAD_POD:/tmp/ -c ray-head

# 运行
kubectl exec $HEAD_POD -c ray-head -- \
  nohup python3 -u /tmp/add_video_tags_235b.py \
    --concurrency 2 --threads 4 --batch-size 32 \
    > /tmp/job_235b.log 2>&1 &
```

### 监控进度

```bash
kubectl exec $HEAD_POD -c ray-head -- bash -c "
  rows=\$(grep 'Batch complete' /tmp/job_235b.log | grep -oP '\d+(?= rows)' | awk '{s+=\$1}END{print s+0}')
  echo \"Progress: \$rows / TOTAL\"
"
```

## 参数说明

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--uri` | 从 Glue 自动获取 | Lance 表 S3 URI |
| `--table-name` | `microlens_large_segment_videos_part_20frags` | Glue 表名 |
| `--database` | `multilake` | Glue 数据库 |
| `--glue-region` | `ap-northeast-1` | Glue Catalog 区域 |
| `--bedrock-region` | `us-east-1` | Bedrock 模型区域 |
| `--concurrency` | 2 | Ray Actor 数量 |
| `--threads` | 4 | 每 Actor 并发线程数 |
| `--batch-size` | 32 | 每批处理行数 |

实际并发 = concurrency × threads

## 与 2B 版本的区别

| | `ray_scripts/add_video_tags.py` | `ray_scripts_235b/add_video_tags_235b.py` |
|---|---|---|
| 模型 | Qwen3-VL-2B (vLLM 自建) | Qwen3-VL-235B (Bedrock) |
| 推理方式 | vLLM OpenAI API | Bedrock Converse API |
| 输出列 | `description`, `tags` | `des_235b`, `tags_235b` |
| temperature | 0.3 | 0.1 |
| 默认并发 | 4×6=24 | 2×4=8 |
| 表 URI | 手动指定 | 自动从 Glue 获取 |
