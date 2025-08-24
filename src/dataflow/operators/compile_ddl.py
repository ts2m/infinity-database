"""Operator for compiling DDL statements from schemas."""

from __future__ import annotations
from typing import Dict
from ..core.operator import Operator
from ..core.registry import register
from ..core.artifact import Artifact

@register
class CompileDDL(Operator):
    name = "CompileDDL"
    input_kinds = ["LogicalDB"]
    output_kinds = ["DDLBundle"]

    def run(self, inputs: Dict[str, Artifact], workdir: str="", **_):
        dbs = inputs["LogicalDB"].data
        ddl_bundle = {}
        for dbid, meta in dbs.items():
            ddl_bundle[dbid] = meta["table_meta"]
        return {"DDLBundle": Artifact(kind="DDLBundle", data=ddl_bundle).save_json(f"{workdir}/ddl_bundle.json")}
