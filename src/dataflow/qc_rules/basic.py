from __future__ import annotations
from typing import List, Dict, Any
import sqlite3
import re

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

class VarcharLengthRule:
    id = "varchar_length"
    def __init__(self, min_length: int = 255):
        self.min_length = min_length
    def run(self, db_path: str, ctx: Dict[str, Any]):
        out = []
        with sqlite3.connect(db_path) as conn:
            cur = conn.cursor()
            for t in ctx["tables"]:
                cur.execute(f"PRAGMA table_info('{t}')")
                cols = cur.fetchall()
                for _, name, col_type, *_ in cols:
                    col_type_str = str(col_type).upper()
                    if col_type_str.startswith("VARCHAR"):
                        length_match = re.search(r'VARCHAR\((\d+)\)', col_type_str)
                        if length_match:
                            length = int(length_match.group(1))
                            passed = length >= self.min_length
                            out.append({
                                "rule_id": self.id,
                                "table": t,
                                "column": name,
                                "passed": passed,
                                "severity": "warn" if not passed else "info",
                                "message": f"{t}.{name} VARCHAR length={length} (min={self.min_length})",
                                "details": {"length": length}
                            })
        return out

class PrimaryKeyAutoincrementRule:
    id = "primary_key_autoincrement"
    def __init__(self):
        pass
    def run(self, db_path: str, ctx: Dict[str, Any]):
        out = []
        with sqlite3.connect(db_path) as conn:
            cur = conn.cursor()
            for t in ctx["tables"]:
                cur.execute(f"PRAGMA table_info('{t}')")
                cols = cur.fetchall()
                pk_cols = [col for col in cols if col[5] == 1]  # pk flag is in the 6th position
                if not pk_cols:
                    out.append({
                        "rule_id": self.id,
                        "table": t,
                        "passed": False,
                        "severity": "error",
                        "message": f"{t} has no primary key defined",
                        "details": {}
                    })
                else:
                    pk_col = pk_cols[0]
                    col_type = str(pk_col[2]).upper()
                    is_autoincrement = "INTEGER" in col_type  # SQLite autoincrement requires INTEGER
                    passed = is_autoincrement and len(pk_cols) == 1
                    out.append({
                        "rule_id": self.id,
                        "table": t,
                        "passed": passed,
                        "severity": "error" if not passed else "info",
                        "message": f"{t} primary key {pk_col[1]} type={col_type}, autoincrement={'supported' if is_autoincrement else 'not supported'}",
                        "details": {"pk_count": len(pk_cols), "type": col_type}
                    })
        return out

class DataIsolationRule:
    id = "data_isolation"
    def __init__(self):
        pass
    def run(self, db_path: str, ctx: Dict[str, Any]):
        out = []
        with sqlite3.connect(db_path) as conn:
            cur = conn.cursor()
            tables = ctx["tables"]
            referenced_tables = set()
            referencing_tables = set()
            for t in tables:
                cur.execute(f"PRAGMA foreign_key_list('{t}')")
                fks = cur.fetchall()
                if fks:
                    referencing_tables.add(t)
                    for fk in fks:
                        referenced_tables.add(fk[2])  # referred_table is in the 3rd position
            isolated_tables = set(tables) - referenced_tables - referencing_tables
            for t in isolated_tables:
                out.append({
                    "rule_id": self.id,
                    "table": t,
                    "passed": False,
                    "severity": "warn",
                    "message": f"{t} is an isolated table with no foreign key relationships",
                    "details": {}
                })
            if not isolated_tables:
                out.append({
                    "rule_id": self.id,
                    "table": "all",
                    "passed": True,
                    "severity": "info",
                    "message": "No isolated tables found in the database",
                    "details": {}
                })
        return out

class CompositePrimaryKeyRule:
    id = "composite_primary_key"
    def __init__(self):
        pass
    def run(self, db_path: str, ctx: Dict[str, Any]):
        out = []
        with sqlite3.connect(db_path) as conn:
            cur = conn.cursor()
            for t in ctx["tables"]:
                cur.execute(f"PRAGMA table_info('{t}')")
                cols = cur.fetchall()
                pk_cols = [col for col in cols if col[5] == 1]  # pk flag is in the 6th position
                if len(pk_cols) > 1:
                    out.append({
                        "rule_id": self.id,
                        "table": t,
                        "passed": False,
                        "severity": "error",
                        "message": f"{t} has a composite primary key with {len(pk_cols)} fields",
                        "details": {"pk_fields": [col[1] for col in pk_cols]}
                    })
                elif len(pk_cols) == 1:
                    out.append({
                        "rule_id": self.id,
                        "table": t,
                        "passed": True,
                        "severity": "info",
                        "message": f"{t} has a single primary key field",
                        "details": {"pk_field": pk_cols[0][1]}
                    })
                else:
                    out.append({
                        "rule_id": self.id,
                        "table": t,
                        "passed": False,
                        "severity": "error",
                        "message": f"{t} has no primary key defined",
                        "details": {}
                    })
        return out

class MultipleForeignKeyReferenceRule:
    id = "multiple_foreign_key_reference"
    def __init__(self):
        pass
    def run(self, db_path: str, ctx: Dict[str, Any]):
        out = []
        with sqlite3.connect(db_path) as conn:
            cur = conn.cursor()
            for t in ctx["tables"]:
                cur.execute(f"PRAGMA foreign_key_list('{t}')")
                fks = cur.fetchall()
                ref_count = {}
                for fk in fks:
                    ref_table = fk[2]  # referred_table is in the 3rd position
                    ref_count[ref_table] = ref_count.get(ref_table, 0) + 1
                for ref_table, count in ref_count.items():
                    if count > 1:
                        out.append({
                            "rule_id": self.id,
                            "table": t,
                            "passed": False,
                            "severity": "error",
                            "message": f"{t} references {ref_table} with {count} foreign keys",
                            "details": {"reference_count": count}
                        })
                    else:
                        out.append({
                            "rule_id": self.id,
                            "table": t,
                            "passed": True,
                            "severity": "info",
                            "message": f"{t} references {ref_table} with a single foreign key",
                            "details": {"reference_count": count}
                        })
        return out
