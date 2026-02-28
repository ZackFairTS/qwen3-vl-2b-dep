#!/bin/bash
python -m vllm.entrypoints.openai.api_server \
    --model Qwen/Qwen3-VL-2B-Instruct \
    --dtype auto \
    --max-model-len 32768 \
    --port 8000 \
    --host 0.0.0.0 \
    --trust-remote-code \
    --limit-mm-per-prompt '{"image": 5, "video": 2}'
