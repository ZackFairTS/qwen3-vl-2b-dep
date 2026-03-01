# Ray 视频推理脚本

使用 Ray + vLLM 为 Lance 表中的视频批量生成中文描述和标签。

## 文件说明

| 文件 | 说明 |
|------|------|
| `add_video_tags.py` | 主推理脚本，添加 `description` 和 `tags` 列 |
| `optimize_dataset.py` | Lance 表 fragment 拆分工具，提升并行度 |
| `monitor.sh` | Ray 任务监控脚本 |

## 使用方法

### 部署到 Ray 集群

```bash
HEAD_POD=$(kubectl get pods -l ray.io/node-type=head -o jsonpath='{.items[0].metadata.name}')
WORKER_POD=$(kubectl get pods -l ray.io/node-type=worker -o jsonpath='{.items[0].metadata.name}')

# 两个节点都需要安装依赖
kubectl exec $HEAD_POD -c ray-head -- pip install lance lance-ray boto3 pyarrow -q
kubectl exec $WORKER_POD -- pip install lance lance-ray boto3 pyarrow -q

# 复制脚本
kubectl cp ray_scripts/add_video_tags.py $HEAD_POD:/tmp/ -c ray-head
```

### 运行推理

```bash
kubectl exec $HEAD_POD -c ray-head -- \
  nohup python3 -u /tmp/add_video_tags.py \
    s3://bucket/dataset.lance \
    --concurrency 4 --threads 12 --batch-size 256 \
    > /tmp/job.log 2>&1 &
```

### 监控进度

```bash
kubectl exec $HEAD_POD -c ray-head -- bash -c "
  rows=\$(grep 'Batch complete' /tmp/job.log | grep -oP '\d+(?= rows)' | awk '{s+=\$1}END{print s+0}')
  echo \"Progress: \$rows / TOTAL\"
"
```

## 参数说明

### add_video_tags.py

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `uri` (必需) | - | Lance 表 URI |
| `--vllm-url` | cluster 内部地址 | vLLM 服务地址 |
| `--concurrency` | 4 | Ray Actor 数量（≤集群 CPU 数） |
| `--threads` | 6 | 每 Actor 并发线程数 |
| `--batch-size` | 128 | 每批处理行数 |

实际并发 = concurrency × threads

### optimize_dataset.py

```bash
python3 optimize_dataset.py SOURCE_URI TARGET_URI --rows-per-fragment 500
```

增加 fragment 数量以提升 Ray 并行度。建议每 fragment 500 行。

## 实测性能

在 5× A10G + 4 actors × 12 threads 下：

- 10,726 视频 → 25 分钟完成
- ~350 行/分钟
- 99.99% 成功率
