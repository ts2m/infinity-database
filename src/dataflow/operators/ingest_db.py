"""Operator for ingesting data from databases."""

from __future__ import annotations
from typing import Dict, Any
from sqlalchemy import create_engine, inspect, text
import pandas as pd
from ..core.operator import Operator
from ..core.registry import register
from ..core.artifact import Artifact
from ..ir.schema import new_ir

@register
class IngestDB(Operator):
    name = "IngestDB"
    input_kinds = []
    output_kinds = ["IR"]

    def run(self, inputs: Dict[str, Artifact], uri: str, dataset_id: str, workdir: str, **_):
        engine = create_engine(uri)
        insp = inspect(engine)
        ir = new_ir(dataset_id)
        ir["source"] = uri
        ir["type"] = "db"

        tables = insp.get_table_names()
        for t in tables:
            cols = insp.get_columns(t)
            fks = insp.get_foreign_keys(t)
            pks = insp.get_pk_constraint(t).get("constrained_columns") or []

            ir["table_header"][t] = [c["name"] for c in cols]
            ir["table_schema"][t] = {
                "columns": [{"name": c["name"], "type": str(c.get("type"))} for c in cols],
                "primary_key": pks,
                "foreign_keys": [{"column": fk["constrained_columns"][0],
                                  "ref_table": fk["referred_table"],
                                  "ref_column": (fk["referred_columns"] or ["id"])[0]} for fk in fks]
            }

            with engine.connect() as conn:
                sample = conn.execute(text(f"SELECT * FROM \"{t}\" LIMIT 20")).fetchall()
                cnt = conn.execute(text(f"SELECT COUNT(*) FROM \"{t}\"")).scalar()
            ir["table_content"][t] = {
                "samples": [list(row) for row in sample],
                "row_count": int(cnt),
                "data_uri": uri
            }

        return {"IR": Artifact(kind="IR", data=ir)}
