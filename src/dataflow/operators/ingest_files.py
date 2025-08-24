"""Operator for ingesting data from files."""

from __future__ import annotations
from typing import Dict, Any, List
from pathlib import Path
import pandas as pd
from slugify import slugify
from ..core.operator import Operator
from ..core.registry import register
from ..core.artifact import Artifact
from ..ir.schema import new_ir

@register
class IngestFiles(Operator):
    name = "IngestFiles"
    input_kinds = []
    output_kinds = ["IR"]

    def run(self, inputs: Dict[str, Artifact], input_globs: List[str], dataset_id: str, workdir: str, **_):
        ir = new_ir(dataset_id)
        ir["source"] = "files"
        ir["type"] = "tables"

        tables = {}
        tmp_dir = Path(workdir) / "staging" / dataset_id
        tmp_dir.mkdir(parents=True, exist_ok=True)

        for pattern in input_globs:
            for p in Path().glob(pattern):
                try:
                    df = (pd.read_excel(p) if p.suffix.lower() in [".xlsx", ".xls"]
                          else pd.read_json(p) if p.suffix.lower() == ".json"
                          else pd.read_parquet(p) if p.suffix.lower() == ".parquet"
                          else pd.read_csv(p))
                except Exception:
                    continue

                tname = slugify(p.stem)
                tables.setdefault(tname, []).append((p, df))

        # 简化：同名表合并列头（取最大覆盖）
        for tname, items in tables.items():
            header = []
            for _, df in items:
                header = list({*header, *map(str, df.columns)})
            ir["table_header"][tname] = header

            sample_rows = []
            total_rows = 0
            # 落地 parquet 作为 data_uri
            table_dir = tmp_dir / tname
            table_dir.mkdir(parents=True, exist_ok=True)
            parts = []
            for idx, (p, df) in enumerate(items):
                total_rows += len(df)
                sample_rows += df.head(5).values.tolist()
                outp = table_dir / f"part_{idx}.parquet"
                df.to_parquet(outp)
                parts.append(str(outp))

            ir["table_content"][tname] = {
                "samples": sample_rows[:50],
                "row_count": int(total_rows),
                "data_uri": str(table_dir.resolve())
            }

        return {"IR": Artifact(kind="IR", data=ir)}
