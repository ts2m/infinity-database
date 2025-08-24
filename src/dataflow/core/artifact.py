"""Artifact class for managing data products in the pipeline."""

from __future__ import annotations
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional, Dict
import json, xxhash

@dataclass
class Artifact:
    kind: str                           # e.g., "IR", "Embeddings", "ClusterMap", "DDL", "SQLiteDB", "QCReport"
    uri: Optional[str] = None           # 路径或连接串，可选
    data: Optional[Any] = None          # 小型内存对象（JSON-able）
    meta: Dict[str, Any] = field(default_factory=dict)

    def save_json(self, path: str | Path):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(self.data, f, indent=2, ensure_ascii=False)
        self.uri = str(path)
        return self

    def load_json(self):
        if not self.uri:
            raise ValueError("No uri to load from")
        with open(self.uri, "r", encoding="utf-8") as f:
            self.data = json.load(f)
        return self

    def hash(self) -> str:
        base = json.dumps({
            "kind": self.kind, "data": self.data, "meta": self.meta, "uri": self.uri
        }, sort_keys=True, ensure_ascii=False)
        return xxhash.xxh3_64_hexdigest(base)
