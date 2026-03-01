#!/bin/bash
python -m vllm.entrypoints.openai.api_server \
    --model Qwen/Qwen3-VL-2B-Instruct \
    --dtype auto \
    --max-model-len 4096 \
    --port 8000 \
    --host 0.0.0.0 \
    --trust-remote-code \
    --mm-processor-kwargs '{"fps": 0.5, "size": {"shortest_edge": 131072, "longest_edge": 360448}}'
