# Lance Table Split

按 `video_id` 对 Lance 表进行切分，保证每个 `video_id` 下的所有 `segment` 完整保留。

## 用法

```bash
python split.py <source> <target> [--fraction N]
```

- `source` - 源 Lance 表路径（支持 S3）
- `target` - 目标 Lance 表路径
- `--fraction` - 切分比例，默认 20（取 1/20）

## 示例

```bash
python split.py \
  s3://tang-emr-tokyo/multilake/microlens_large_segment_videos.lance \
  s3://tang-emr-tokyo/multilake/microlens_large_segment_videos_part.lance \
  --fraction 20
```

输出：
```
Source: 214118 rows, 19220 video_ids
Target: 10726 rows, 961 video_ids (1/20)
```

## 依赖

```
pylance
pyarrow
```

## 切分逻辑

1. 读取源表所有 `video_id` 并排序
2. 取前 `1/N` 的 `video_id`
3. 筛选这些 `video_id` 对应的全部行（包含所有 `segment_id`）
4. 写入新的 Lance 表
