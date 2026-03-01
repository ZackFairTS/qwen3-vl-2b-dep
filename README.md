# Qwen3-VL-2B Video Tagging Pipeline

使用 vLLM + Ray 在 EKS 上批量处理视频，生成中文描述和标签，写入 Lance 表。

## 项目结构

```
.
├── serve.sh                  # 本地 vLLM 启动脚本
├── eks/                      # EKS 部署配置
│   ├── DEPLOY.md
│   ├── deployment.yaml       # vLLM Deployment + Service + HPA
│   └── gpu-nodegroup.yaml    # GPU 节点组
├── ray_scripts/              # Ray 分布式推理
│   ├── add_video_tags.py     # 主推理脚本
│   ├── optimize_dataset.py   # Lance 表 fragment 优化
│   └── monitor.sh            # 任务监控
└── table_split/              # Lance 表分割工具
```

## 运行结果

在 5× g5.xlarge (A10G) 上处理 10,726 个视频片段：

| 指标 | 值 |
|------|-----|
| 成功率 | 99.99% (10,725/10,726) |
| 总耗时 | ~25 分钟 |
| 吞吐量 | ~350 行/分钟 |
| GPU 利用率 | 89-99% |
| 平均 prompt tokens | ~272 |

## 快速开始

### 1. 部署 vLLM 到 EKS

```bash
eksctl create nodegroup -f eks/gpu-nodegroup.yaml
kubectl apply -f eks/deployment.yaml
```

### 2. 运行推理

```bash
# 复制脚本到 Ray head pod
HEAD_POD=$(kubectl get pods -l ray.io/node-type=head -o jsonpath='{.items[0].metadata.name}')
kubectl cp ray_scripts/add_video_tags.py $HEAD_POD:/tmp/ -c ray-head

# 在 head pod 上执行
kubectl exec $HEAD_POD -c ray-head -- \
  python3 -u /tmp/add_video_tags.py \
    s3://bucket/dataset.lance \
    --concurrency 4 --threads 12 --batch-size 256
```

## 技术栈

- **推理框架**: vLLM 0.15 (V1 engine)
- **模型**: Qwen/Qwen3-VL-2B-Instruct
- **分布式**: Ray 2.46 + lance-ray
- **存储**: Lance (on S3)
- **GPU**: NVIDIA A10G (g5.xlarge)

---

## 最佳实践与注意事项

### 1. Qwen3-VL 视频处理参数 (关键)

Qwen3-VL 的视频处理参数与 Qwen2-VL **不兼容**，参数名已变更：

| Qwen2-VL (旧) | Qwen3-VL (新) | 说明 |
|---|---|---|
| `min_pixels` | `size.shortest_edge` | 最小像素数 |
| `max_pixels` | `size.longest_edge` | 最大像素数 |
| `patch_size=14` | `patch_size=16` | 视觉 patch 大小 |

正确的 `mm_processor_kwargs` 配置：

```json
{
  "fps": 0.5,
  "size": {"shortest_edge": 131072, "longest_edge": 360448}
}
```

**服务端**（`--mm-processor-kwargs`）在 vLLM 0.15 V1 引擎中可能不生效。
必须在**请求级别**传递 `mm_processor_kwargs`：

```python
requests.post(f"{vllm_url}/v1/chat/completions", json={
    "model": "Qwen/Qwen3-VL-2B-Instruct",
    "messages": [...],
    "max_tokens": 350,
    "mm_processor_kwargs": {
        "fps": 0.5,
        "size": {"shortest_edge": 131072, "longest_edge": 360448}
    }
})
```

不传此参数时，默认 `fps=2`、`longest_edge=786432`，
一个 15 秒 720p 视频的 prompt tokens 可达 **11,000+**，远超 `max_model_len`。

### 2. vLLM 部署配置

```yaml
# 推荐的 vLLM 启动参数
python -m vllm.entrypoints.openai.api_server \
  --model Qwen/Qwen3-VL-2B-Instruct \
  --dtype auto \
  --max-model-len 4096 \
  --trust-remote-code \
  --mm-processor-kwargs '{"fps": 0.5, "size": {"shortest_edge": 131072, "longest_edge": 360448}}'
```

关键点：
- `max-model-len 4096` 足够（请求级 mm_processor_kwargs 生效后 prompt ~272 tokens）
- `VLLM_USE_V1=0` 在 vLLM 0.15 中**不生效**，V1 引擎是强制的
- 模型权重仅 ~4GB，但 vLLM 双进程架构 (APIServer + EngineCore) 总共需要 ~6-8GB 系统内存

### 3. Kubernetes 探针配置

vLLM 在高并发下 `/health` 响应变慢，必须增大探针超时：

```yaml
livenessProbe:
  httpGet:
    path: /health
    port: 8000
  initialDelaySeconds: 240   # CUDA graph warmup 需要 2-3 分钟
  periodSeconds: 30
  timeoutSeconds: 30          # 默认 1s，高并发下必超时
  failureThreshold: 5         # 容忍连续 5 次失败
readinessProbe:
  httpGet:
    path: /health
    port: 8000
  initialDelaySeconds: 180
  periodSeconds: 10
  timeoutSeconds: 10
  failureThreshold: 3
```

**不设置 `timeoutSeconds` 会导致 pod 在高并发下反复被杀重启。**

### 4. Ray 集群配置

- Head 节点 `--num-cpus=N` 决定是否参与计算调度。如果 head 内存不足，设 `num-cpus=0` 避免调度 actor 到 head
- Head 内存建议 **8Gi+**（4Gi 在 concurrency=4 时 OOM）
- Worker 节点也需要安装 `lance`、`lance-ray`、`boto3` 等依赖
- `lance-ray.add_columns()` 是**原子操作**，中途崩溃不会保存任何结果

### 5. 并发调优

| 参数 | 推荐值 | 说明 |
|------|--------|------|
| `--concurrency` | 4 | Ray actor 数，不超过集群 CPU 总数 |
| `--threads` | 12 | 每 actor 的并发线程数 |
| `--batch-size` | 256 | 每 batch 行数 |
| 总并发 | 48 | concurrency × threads |
| vLLM pods | 5 | 每 pod 约承接 10 并发 |

### 6. 常见问题

| 问题 | 原因 | 解决 |
|------|------|------|
| 400 Bad Request | prompt tokens 超 max_model_len | 请求级传 mm_processor_kwargs 降低 tokens |
| Pod OOMKilled | vLLM 双进程架构内存开销大 | 降低 max_model_len 或增加节点内存 |
| Pod 反复重启 | liveness probe timeout 默认 1s | 设 timeoutSeconds=30, failureThreshold=5 |
| Ray head OOM | actor 调度到内存不足的 head 上 | 增加 head 内存到 8Gi+ |
| 滚动更新卡住 | GPU 资源不足，新旧 pod 死锁 | 手动删除旧 pod 或缩旧 ReplicaSet 到 0 |
| Worker 报错 ModuleNotFoundError | Worker 节点未安装依赖 | 在 worker 上也执行 pip install |
| 数据全是 ERROR | 请求返回 400 但脚本 catch 后继续 | 先小批量测试确认 200 再全量运行 |
