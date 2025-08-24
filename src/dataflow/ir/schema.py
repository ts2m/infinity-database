# 统一 IR：可兼容“散表”和“成库”；字段取自你期望的 california_schools 格式
from __future__ import annotations
from typing import Dict, Any

def new_ir(dataset_id: str) -> Dict[str, Any]:
    return {
        "dataset_id": dataset_id,
        "source": "unknown",
        "type": "logical",
        "table_header": {},     # {table: [col, ...]}
        "table_schema": {},     # {table: {"columns":[{"name":"", "type":""}], "primary_key":[], "foreign_keys":[...]} }
        "table_content": {},    # {table: {"samples":[...], "row_count": int, "data_uri": "file:///..."} }
        "meta": {}
    }
