# 视频片段检索 Web 服务

基于 Lance 全文检索 + Streamlit 的视频片段搜索与预览工具。

## 功能

- 从 AWS Glue Data Catalog 选择 Lance 表
- 关键字全文检索（基于 description / tags 倒排索引）
- 搜索结果分页展示（video_id、segment_id、description、tags、relevance score）
- 视频在线预览（通过 S3 presigned URL）

## 前置条件

- Lance 表已通过 `build_fts_index.py` 构建了 description / tags 的倒排索引
- Lance 表已注册到 AWS Glue Data Catalog（参考 `table_split/split.py --register-glue`）
- EC2 实例拥有 Glue 和 S3 的访问权限（IAM Role 或 AWS credentials）

## 安装与运行

```bash
cd web
pip install -r requirements.txt
streamlit run app.py --server.port 8501 --server.address 0.0.0.0 --server.headless true
```

外网访问需要确保 EC2 安全组放行 TCP 8501 入站。

## 使用流程

1. 左侧边栏配置 AWS Region 和 Glue Database
2. 下拉选择目标 Lance 表，点击「连接」
3. 主区域输入搜索关键字（如「猫」），点击「搜索」
4. 浏览搜索结果，展开条目可预览对应视频片段
