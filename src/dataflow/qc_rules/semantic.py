from __future__ import annotations
from typing import Dict, Any
import sqlite3, re

class ZipCodeRule:
    id = "semantic_zipcode"
    def __init__(self, column: str = "Zip", country: str = "US"):
        self.column = column
        self.country = country
        self.pat = re.compile(r"^\d{5}(-\d{4})?$") if country=="US" else re.compile(r".*")

    def run(self, db_path: str, ctx: Dict[str, Any]):
        out = []
        with sqlite3.connect(db_path) as conn:
            cur = conn.cursor()
            for t in ctx["tables"]:
                cur.execute(f"PRAGMA table_info('{t}')")
                cols = [c[1] for c in cur.fetchall()]
                if self.column not in cols: continue
                cur.execute(f"SELECT COUNT(*) FROM '{t}'")
                total = cur.fetchone()[0]
                cur.execute(f"SELECT COUNT(*) FROM '{t}' WHERE \"{self.column}\" IS NOT NULL AND \"{self.column}\" <> ''")
                nonnull = cur.fetchone()[0]
                cur.execute(f"SELECT COUNT(*) FROM '{t}' WHERE \"{self.column}\" REGEXP '.*'")  # sqlite 无原生 regex，这里只是示意
                # 简化：抽样检查（真实实现可注册 REGEXP 或拉到 Python 侧检查）
                passed = True
                out.append({"rule_id": self.id, "table": t, "passed": passed,
                            "severity":"info", "message": f"{t}.{self.column} semantic check placeholder",
                            "details":{"total": total, "nonnull": nonnull}})
        return out
