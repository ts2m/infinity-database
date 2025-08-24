"""Operator for consolidating schemas from multiple data sources."""

from __future__ import annotations
from typing import Dict, Any
import json
from ..core.operator import Operator
from ..core.registry import register
from ..core.artifact import Artifact

@register
class ConsolidateSchema(Operator):
    name = "ConsolidateSchema"
    input_kinds = ["IR", "ClusterMap"]
    output_kinds = ["LogicalDB"]

    def run(self, inputs: Dict[str, Artifact], provider: str="llm_http",
            prompt_template_path: str="", workdir: str="", **kwargs):
        # 简化：不强依赖 LLM，基于 cluster 将同簇表合并成“数据库”
        ir = inputs["IR"].data
        cmap = inputs["ClusterMap"].data

        final = {}
        for cid, table_ids in cmap.items():
            dbid = f"db_{cid}"
            table_meta = {}
            for t in table_ids:
                # 简化生成建表语句（真实项目中可调用 LLM 生成/修复）
                header = ir["table_header"].get(t, [])
                cols = ",\n  ".join([f"\"{c}\" TEXT" for c in header]) or "\"id\" INTEGER"
                create_sql = f'CREATE TABLE "{t}" (\n  {cols}\n);'
                table_meta[t] = create_sql

            final[dbid] = {
                "source": "consolidated",
                "type": "sqlite",
                "unique_id": dbid,
                "table_meta": table_meta,
                "table_header": {t: ir["table_header"].get(t, []) for t in table_ids},
                "table_content": {t: { "content": "", "is_empty": True } for t in table_ids}
            }

        return {"LogicalDB": Artifact(kind="LogicalDB", data=final).save_json(f"{workdir}/consolidated_database.json")}
