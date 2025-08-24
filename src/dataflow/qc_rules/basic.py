from __future__ import annotations
from typing import List, Dict, Any
import sqlite3

class RowCountRule:
    id = "row_count"
    def __init__(self, min_records_per_table: int = 20):
        self.min_records = min_records_per_table
    def run(self, db_path: str, ctx: Dict[str, Any]):
        out = []
        with sqlite3.connect(db_path) as conn:
            cur = conn.cursor()
            for t in ctx["tables"]:
                cur.execute(f"SELECT COUNT(*) FROM '{t}'")
                cnt = cur.fetchone()[0]
                passed = cnt >= self.min_records
                out.append({"rule_id": self.id, "table": t, "passed": passed,
                            "severity": "error" if not passed else "info",
                            "message": f"{t} has {cnt} rows (min={self.min_records})", "details": {"count": cnt}})
        return out

class NullRateRule:
    id = "null_rate"
    def __init__(self, max_null_rate: float = 0.3):
        self.max_null_rate = max_null_rate
    def run(self, db_path: str, ctx: Dict[str, Any]):
        out = []
        with sqlite3.connect(db_path) as conn:
            cur = conn.cursor()
            for t in ctx["tables"]:
                cur.execute(f"PRAGMA table_info('{t}')"); cols = cur.fetchall()
                for _, name, *_ in cols:
                    cur.execute(f"SELECT COUNT(*) FROM '{t}'")
                    total = cur.fetchone()[0]
                    cur.execute(f"SELECT COUNT(*) FROM '{t}' WHERE \"{name}\" IS NULL OR \"{name}\" = ''")
                    nulls = cur.fetchone()[0]
                    rate = (nulls / total) if total else 0.0
                    passed = rate <= self.max_null_rate
                    out.append({"rule_id": self.id, "table": t, "column": name, "passed": passed,
                                "severity": "warn" if not passed else "info",
                                "message": f"{t}.{name} null/empty rate={rate:.3f} (max={self.max_null_rate})",
                                "details": {"total": total, "nulls": nulls, "rate": rate}})
        return out

class ForeignKeyRule:
    id = "fk_integrity"
    def run(self, db_path: str, ctx: Dict[str, Any]):
        out = []
        with sqlite3.connect(db_path) as conn:
            cur = conn.cursor()
            for t in ctx["tables"]:
                cur.execute(f"PRAGMA foreign_key_list('{t}')")
                fks = cur.fetchall()
                for fk in fks:
                    # fk: (id, seq, table, from, to, on_update, on_delete, match)
                    _, _, ref_table, fk_col, ref_col, *_ = fk
                    q = f"""
                        SELECT COUNT(*) FROM '{t}' a
                        LEFT JOIN '{ref_table}' b ON a."{fk_col}" = b."{ref_col}"
                        WHERE a."{fk_col}" IS NOT NULL AND b."{ref_col}" IS NULL
                    """
                    cur.execute(q); bad = cur.fetchone()[0]
                    passed = bad == 0
                    out.append({"rule_id": self.id, "table": t, "passed": passed,
                                "severity": "error" if not passed else "info",
                                "message": f"{t}.{fk_col} -> {ref_table}.{ref_col} broken={bad}",
                                "details": {"violations": bad}})
        return out
