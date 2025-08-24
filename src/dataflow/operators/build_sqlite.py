"""Operator for building SQLite databases from data and DDL."""

from __future__ import annotations
from typing import Dict
import sqlite3, os, json
from pathlib import Path
from tqdm import tqdm
from ..core.operator import Operator
from ..core.registry import register
from ..core.artifact import Artifact

@register
class BuildSQLite(Operator):
    name = "BuildSQLite"
    input_kinds = ["LogicalDB"]
    output_kinds = ["SQLiteDB", "AgentReadyMeta"]

    def run(self, inputs: Dict[str, Artifact], workdir: str="", **_):
        logical = inputs["LogicalDB"].data
        out_dir = Path(workdir) / "sqlite_dbs"
        out_dir.mkdir(parents=True, exist_ok=True)

        agent_meta = {}
        for dbid, meta in tqdm(list(logical.items()), desc="Create SQLite"):
            db_path = out_dir / f"{dbid}.sqlite"
            if db_path.exists(): db_path.unlink()
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            for t, ddl in meta["table_meta"].items():
                cur.execute(ddl)
            conn.commit(); conn.close()

            db_meta = dict(meta)
            db_meta["sqlite_path"] = str(db_path.resolve())
            # 标记为空内容（后续由 LLM 插入）
            tc = {}
            for t in meta["table_meta"].keys():
                tc[t] = {"content": "| column1 | column2 |\n|---|---|\n", "is_empty": True}
            db_meta["table_content"] = tc

            agent_meta[dbid] = db_meta

        agent_path = Path(workdir) / "agent_ready_metadata.json"
        json.dump(agent_meta, open(agent_path, "w", encoding="utf-8"), indent=2, ensure_ascii=False)
        return {
            "SQLiteDB": Artifact(kind="SQLiteDB", data={"db_paths":[m["sqlite_path"] for m in agent_meta.values()]}),
            "AgentReadyMeta": Artifact(kind="AgentReadyMeta", uri=str(agent_path)).load_json()
        }
