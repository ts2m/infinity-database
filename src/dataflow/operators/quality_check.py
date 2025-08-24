from __future__ import annotations
from typing import Dict, Any, List, Tuple
import importlib, sqlite3, json
from ..core.operator import Operator
from ..core.registry import register
from ..core.artifact import Artifact

def quality_check_db(db_path: str, rules: List[Tuple[str, dict]]|None=None):
    # 通用质量检查引擎：加载规则插件并执行
    report = []
    ok = True
    database_content = {}
    try:
        with sqlite3.connect(db_path) as conn:
            cur = conn.cursor()
            cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';")
            tables = [r[0] for r in cur.fetchall()]
            database_content["table_names"] = tables

            for t in tables:
                cur.execute(f"PRAGMA table_info('{t}')")
                cols = cur.fetchall()
                cur.execute(f"SELECT COUNT(*) FROM '{t}'"); total = cur.fetchone()[0]
                cur.execute(f"SELECT * FROM '{t}' LIMIT 10"); sample = cur.fetchall()
                database_content[t] = {"columns": cols, "sample_data": sample, "total_count": total}

        # 默认规则（无外部配置时）
        rules = rules or [
            ("dataflow.qc_rules.basic.RowCountRule", {"min_records_per_table": 20}),
            ("dataflow.qc_rules.basic.NullRateRule", {"max_null_rate": 1.0}),  # 容忍
            ("dataflow.qc_rules.basic.ForeignKeyRule", {})
        ]

        for mod_cls, params in rules:
            mod_name, cls_name = mod_cls.rsplit(".", 1)
            cls = getattr(importlib.import_module(mod_name), cls_name)
            inst = cls(**(params or {}))
            items = inst.run(db_path, {"tables": database_content.get("table_names", [])})
            report.extend(items)
    except Exception as e:
        report.append({"rule_id":"engine_error","passed":False,"severity":"error","message":str(e),"details":{}})
        ok = False

    ok = ok and all(item.get("passed", False) for item in report if item.get("severity")=="error")
    return ok, report, database_content

@register
class QualityCheck(Operator):
    name = "QualityCheck"
    input_kinds = ["SQLiteDB", "AgentReadyMeta"]
    output_kinds = ["QCReport"]

    def run(self, inputs: Dict[str, Artifact], rules: List[dict]|None=None, workdir: str="", **_):
        # 针对 BuildSQLite 后“被 LLM 扩充过的 dbs” 检查
        paths = inputs["SQLiteDB"].data["db_paths"]
        results = {}
        for p in paths:
            tuples = []
            if rules:
                tuples = [(r["module"], r.get("params", {})) for r in rules]
            ok, report, content = quality_check_db(p, rules=tuples if rules else None)
            results[p] = {"ok": ok, "report": report}

        return {"QCReport": Artifact(kind="QCReport", data=results).save_json(f"{workdir}/qc_report.json")}
