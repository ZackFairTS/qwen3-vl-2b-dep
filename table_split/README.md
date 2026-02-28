# Lance Table Split

按 `video_id` 对 Lance 表进行切分，保证每个 `video_id` 下的所有 `segment` 完整保留。

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

## 切分逻辑

1. 读取源表所有 `video_id` 并排序
2. 取前 `1/N` 的 `video_id`
3. 筛选这些 `video_id` 对应的全部行（包含所有 `segment_id`）
4. 写入新的 Lance 表
