#!/usr/bin/env python3
import boto3
import requests
import base64
import sys
import json
import re
from datetime import datetime

def analyze_video(s3_path, output_file=None):
    parts = s3_path.replace('s3://', '').split('/', 1)
    bucket, key = parts[0], parts[1]
    
    print(f"下载视频: {s3_path}")
    s3 = boto3.client('s3')
    s3.download_file(bucket, key, '/tmp/video.mp4')
    
    with open('/tmp/video.mp4', 'rb') as f:
        video_data = base64.b64encode(f.read()).decode('utf-8')
    
    print("推理中...")
    resp = requests.post('http://localhost:8000/v1/chat/completions', json={
        'model': 'Qwen/Qwen3-VL-2B-Instruct',
        'messages': [{
            'role': 'user',
            'content': [
                {'type': 'video_url', 'video_url': {'url': f'data:video/mp4;base64,{video_data}'}},
                {'type': 'text', 'text': '描述这个视频并进行打标,使用json输出标签'}
            ]
        }],
        'max_tokens': 2048
    })
    
    result = resp.json()
    if 'choices' not in result:
        print(f"错误: {result.get('error')}")
        return
    
    content = result['choices'][0]['message']['content']
    
    # 提取 JSON
    json_match = re.search(r'```json\s*(\{.*?\})\s*```', content, re.DOTALL)
    if json_match:
        parsed = json.loads(json_match.group(1))
        print(f"\n原始 JSON:\n{json.dumps(parsed, ensure_ascii=False, indent=2)}")
        desc = parsed.get('视频描述') or parsed.get('video_description', '')
        tags = parsed.get('标签') or parsed.get('tags', [])
        print(f"\n视频描述:\n{desc}")
        print(f"\n标签:\n{', '.join(tags)}")
    else:
        print(f"\n{content}")
    
    print(f"\nToken 使用: {result['usage']}")
    
    if output_file:
        output = {
            's3_path': s3_path,
            'timestamp': datetime.now().isoformat(),
            'raw_response': content,
            'tokens': result['usage']
        }
        if json_match:
            output['parsed'] = parsed
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(output, f, ensure_ascii=False, indent=2)
        print(f"\n结果已保存到: {output_file}")

if __name__ == '__main__':
    s3_path = sys.argv[1] if len(sys.argv) > 1 else 's3://tang-emr-tokyo/microlens/videos_large_segments/1/1.mp4'
    output = sys.argv[2] if len(sys.argv) > 2 else 'video_result.json'
    analyze_video(s3_path, output)
