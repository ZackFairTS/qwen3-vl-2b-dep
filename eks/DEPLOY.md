# Qwen3-VL-2B EKS 部署文档

## 概述

在 Amazon EKS 上使用 vLLM 部署 Qwen3-VL-2B-Instruct 多模态大语言模型，支持图片和视频推理。

## 架构

- 集群: `eks-ray-cluster` (ap-northeast-1)
- GPU 节点组: g5/g6 实例，支持自动扩缩容 (0~5 节点)
- 推理服务: vLLM OpenAI 兼容 API，运行在 `vllm-inference` 命名空间
- HPA: 基于 CPU 利用率自动扩缩 Pod (1~5 副本)

## 前置条件

- AWS CLI 已配置，具有 EKS 操作权限
- 已安装 `eksctl`、`kubectl`
- EKS 集群 `eks-ray-cluster` 已创建
- 集群已安装 NVIDIA Device Plugin（用于 GPU 调度）

## 部署步骤

### 1. 创建 GPU 节点组

```bash
eksctl create nodegroup -f eks/gpu-nodegroup.yaml
```

节点组配置要点:
- 实例类型: g5.xlarge / g5.2xlarge / g6.xlarge / g6.2xlarge
- 自动扩缩: 0~5 节点，初始 1 节点
- 磁盘: 100GB
- 标签: `role=gpu-inference`（用于 Pod nodeSelector 调度）
- 子网: 私有网络部署

### 2. 部署推理服务

```bash
kubectl apply -f eks/deployment.yaml
```

该文件包含以下资源:

| 资源 | 名称 | 说明 |
|------|------|------|
| Namespace | `vllm-inference` | 专用命名空间 |
| Deployment | `qwen3-vl-2b` | vLLM 推理服务，1 GPU |
| Service | `qwen3-vl-2b` | ClusterIP，端口 8000 |
| HPA | `qwen3-vl-2b` | CPU 70% 触发扩容，1~5 副本 |

### 3. 验证部署

```bash
# 查看 Pod 状态（首次启动约需 2 分钟加载模型）
kubectl get pods -n vllm-inference -w

# 查看日志
kubectl logs -n vllm-inference -l app=qwen3-vl-2b -f

# 健康检查
kubectl exec -it -n vllm-inference deploy/qwen3-vl-2b -- curl localhost:8000/health
```

### 4. 访问服务

集群内通过 Service DNS 访问:

```bash
curl http://qwen3-vl-2b.vllm-inference.svc.cluster.local:8000/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{
    "model": "Qwen/Qwen3-VL-2B-Instruct",
    "messages": [{"role": "user", "content": "Hello"}]
  }'
```

如需集群外访问，可通过 port-forward 临时测试:

```bash
kubectl port-forward -n vllm-inference svc/qwen3-vl-2b 8000:8000
```

## 关键配置说明

- 镜像: `public.ecr.aws/deep-learning-containers/vllm:0.15-gpu-py312`
- 模型最大上下文: 4096 tokens（配合 mm_processor_kwargs 降低视觉 tokens 后足够）
- 视频处理参数: `fps=0.5`, `size.longest_edge=360448`（Qwen3-VL 专用参数名）
- 注意: `VLLM_USE_V1=0` 在 vLLM 0.15 中不生效，V1 引擎是强制的
- 资源请求: 4 CPU / 16Gi 内存 / 1 GPU
- 健康探针: liveness 240s 初始延迟 + 30s 超时，readiness 180s 初始延迟 + 10s 超时

## HPA 扩缩策略

- 扩容: CPU > 70% 时触发，每 120s 最多增加 1 个 Pod，稳定窗口 60s
- 缩容: 每 300s 最多减少 1 个 Pod，稳定窗口 300s

## 文件清单

```
eks/
├── deployment.yaml      # K8s 部署清单（Namespace + Deployment + Service + HPA）
├── gpu-nodegroup.yaml   # eksctl GPU 节点组配置
├── requirements.txt     # vLLM 运行时 Python 依赖（参考用）
└── DEPLOY.md            # 本文档
```
