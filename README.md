# Qwen3-VL-2B Deployment

Qwen3-VL-2B-Instruct 多模态大语言模型部署项目，支持图片和视频推理。

## 项目结构

```
.
├── analyze_video_final.py   # 视频分析脚本
├── serve.sh                 # 本地服务启动脚本
├── video_result.json        # 示例输出结果
└── eks/                     # EKS 部署配置
    ├── DEPLOY.md            # 详细部署文档
    ├── deployment.yaml      # K8s 部署清单
    ├── gpu-nodegroup.yaml   # GPU 节点组配置
    └── requirements.txt     # Python 依赖
```

## 快速开始

### 本地部署

```bash
./serve.sh
```

### 视频分析

```bash
python analyze_video_final.py s3://bucket/path/to/video.mp4 output.json
```

### EKS 部署

详见 [eks/DEPLOY.md](eks/DEPLOY.md)

## 功能特性

- 支持图片和视频多模态推理
- OpenAI 兼容 API
- S3 视频自动下载和分析
- JSON 格式标签输出
- EKS 自动扩缩容部署

## 技术栈

- vLLM 0.15
- Qwen3-VL-2B-Instruct
- Amazon EKS
- GPU: g5/g6 实例
