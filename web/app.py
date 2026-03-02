#!/usr/bin/env python3
"""视频片段检索 Web 服务 — 基于 Lance 全文检索 + Streamlit"""
import os
import math
from urllib.parse import urlparse

import streamlit as st
import lance
import boto3

STORAGE_OPTS = {"region": "ap-northeast-1", "aws_region": "ap-northeast-1"}
SEARCH_COLUMNS = ["video_id", "segment_id", "description", "tags", "video_src", "_score"]
PAGE_SIZE = 10
PRESIGNED_EXPIRY = 3600  # 1 hour


# ── helpers ──────────────────────────────────────────────────────────────────

@st.cache_data(ttl=300)
def list_glue_tables(database: str, region: str) -> list[str]:
    glue = boto3.client("glue", region_name=region)
    tables = []
    paginator = glue.get_paginator("get_tables")
    for page in paginator.paginate(DatabaseName=database):
        for t in page["TableList"]:
            tables.append(t["Name"])
    return sorted(tables)


@st.cache_data(ttl=300)
def get_table_location(database: str, table_name: str, region: str) -> str:
    glue = boto3.client("glue", region_name=region)
    resp = glue.get_table(DatabaseName=database, Name=table_name)
    return resp["Table"]["StorageDescriptor"]["Location"]


def open_lance_dataset(location: str):
    os.environ["AWS_DEFAULT_REGION"] = "ap-northeast-1"
    os.environ["AWS_REGION"] = "ap-northeast-1"
    return lance.dataset(location, storage_options=STORAGE_OPTS)


def generate_presigned_url(s3_uri: str, region: str) -> str:
    parsed = urlparse(s3_uri)
    bucket = parsed.netloc
    key = parsed.path.lstrip("/")
    s3 = boto3.client("s3", region_name=region)
    return s3.generate_presigned_url(
        "get_object",
        Params={"Bucket": bucket, "Key": key},
        ExpiresIn=PRESIGNED_EXPIRY,
    )


def search(ds, keyword: str):
    available = {f.name for f in ds.schema}
    columns = [c for c in SEARCH_COLUMNS if c in available or c == "_score"]
    results = ds.scanner(full_text_query=keyword, columns=columns).to_table()
    return results


# ── sidebar: connection ──────────────────────────────────────────────────────

st.set_page_config(page_title="视频片段检索", layout="wide")
st.title("视频片段检索")

with st.sidebar:
    st.header("连接配置")
    region = st.text_input("AWS Region", value="ap-northeast-1")
    database = st.text_input("Glue Database", value="multilake")

    table_names = []
    try:
        table_names = list_glue_tables(database, region)
    except Exception as e:
        st.error(f"获取表列表失败: {e}")

    table_name = st.selectbox("Glue Table", options=table_names) if table_names else None

    connect_clicked = st.button("连接", disabled=not table_name)

if connect_clicked and table_name:
    try:
        location = get_table_location(database, table_name, region)
        ds = open_lance_dataset(location)
        st.session_state["ds"] = ds
        st.session_state["location"] = location
        st.session_state["region"] = region
        st.sidebar.success(f"已连接: {location} ({ds.count_rows()} rows)")
    except Exception as e:
        st.sidebar.error(f"连接失败: {e}")

# ── main: search ─────────────────────────────────────────────────────────────

ds = st.session_state.get("ds")

if ds is None:
    st.info("请在左侧选择 Glue 表并点击「连接」")
    st.stop()

col1, col2 = st.columns([4, 1])
with col1:
    keyword = st.text_input("关键字", placeholder="输入搜索关键字，如「猫」")
with col2:
    st.write("")  # spacer
    search_clicked = st.button("搜索", use_container_width=True)

if not search_clicked and "results" not in st.session_state:
    st.stop()

if search_clicked:
    if not keyword.strip():
        st.warning("请输入关键字")
        st.stop()
    with st.spinner("搜索中..."):
        results = search(ds, keyword.strip())
    st.session_state["results"] = results
    st.session_state["page"] = 0

results = st.session_state.get("results")
if results is None or results.num_rows == 0:
    st.warning("未找到结果")
    st.stop()

# ── pagination ───────────────────────────────────────────────────────────────

total = results.num_rows
total_pages = math.ceil(total / PAGE_SIZE)
page = st.session_state.get("page", 0)

st.write(f"共 **{total}** 条结果，第 **{page + 1}** / **{total_pages}** 页")

start = page * PAGE_SIZE
end = min(start + PAGE_SIZE, total)

for i in range(start, end):
    row = {col: results.column(col)[i].as_py() for col in results.column_names}
    score = row.get("_score", "")
    video_id = row.get("video_id", "")
    segment_id = row.get("segment_id", "")
    desc = row.get("description", "") or ""
    tags = row.get("tags", "") or ""
    video_src = row.get("video_src", "")

    with st.expander(f"[{score:.2f}] video={video_id}  segment={segment_id}", expanded=(i == start)):
        st.markdown(f"**Description:** {desc}")
        st.markdown(f"**Tags:** {tags}")
        if video_src:
            try:
                url = generate_presigned_url(video_src, st.session_state.get("region", region))
                st.video(url)
            except Exception as e:
                st.error(f"视频预览失败: {e}")

# ── page nav ─────────────────────────────────────────────────────────────────

nav_col1, nav_col2, nav_col3 = st.columns([1, 1, 1])
with nav_col1:
    if page > 0 and st.button("⬅ 上一页"):
        st.session_state["page"] = page - 1
        st.rerun()
with nav_col3:
    if page < total_pages - 1 and st.button("下一页 ➡"):
        st.session_state["page"] = page + 1
        st.rerun()
