from __future__ import annotations
from typing import Dict, Any
import sqlite3, re, datetime

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

class DateFormatRule:
    id = "semantic_date"
    def __init__(self, column: str = "Date", format: str = "%Y-%m-%d"):
        self.column = column
        self.format = format

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
                # 拉取数据到 Python 侧进行检查
                cur.execute(f"SELECT \"{self.column}\" FROM '{t}' WHERE \"{self.column}\" IS NOT NULL AND \"{self.column}\" <> '' LIMIT 100")
                values = [row[0] for row in cur.fetchall()]
                invalid_count = 0
                for val in values:
                    try:
                        datetime.datetime.strptime(val, self.format)
                    except ValueError:
                        invalid_count += 1
                passed = invalid_count == 0
                out.append({
                    "rule_id": self.id,
                    "table": t,
                    "passed": passed,
                    "severity": "warn" if not passed else "info",
                    "message": f"{t}.{self.column} date format check: {invalid_count} invalid out of {len(values)} sampled",
                    "details": {"total": total, "nonnull": nonnull, "invalid_sampled": invalid_count}
                })
        return out

class DatetimeFormatRule:
    id = "semantic_datetime"
    def __init__(self, column: str = "Datetime", format: str = "%Y-%m-%d %H:%M:%S"):
        self.column = column
        self.format = format

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
                # 拉取数据到 Python 侧进行检查
                cur.execute(f"SELECT \"{self.column}\" FROM '{t}' WHERE \"{self.column}\" IS NOT NULL AND \"{self.column}\" <> '' LIMIT 100")
                values = [row[0] for row in cur.fetchall()]
                invalid_count = 0
                for val in values:
                    try:
                        datetime.datetime.strptime(val, self.format)
                    except ValueError:
                        invalid_count += 1
                passed = invalid_count == 0
                out.append({
                    "rule_id": self.id,
                    "table": t,
                    "passed": passed,
                    "severity": "warn" if not passed else "info",
                    "message": f"{t}.{self.column} datetime format check: {invalid_count} invalid out of {len(values)} sampled",
                    "details": {"total": total, "nonnull": nonnull, "invalid_sampled": invalid_count}
                })
        return out

class StringTypeRule:
    id = "semantic_string_type"
    def __init__(self, columns: list = None):
        self.columns = columns or ["Name", "Description", "Title", "Text"]

    def run(self, db_path: str, ctx: Dict[str, Any]):
        out = []
        with sqlite3.connect(db_path) as conn:
            cur = conn.cursor()
            for t in ctx["tables"]:
                cur.execute(f"PRAGMA table_info('{t}')")
                cols = cur.fetchall()
                col_names = [c[1] for c in cols]
                col_types = {c[1]: str(c[2]).upper() for c in cols}
                for col in col_names:
                    if col in self.columns or any(col.lower().find(term.lower()) != -1 for term in self.columns):
                        col_type = col_types.get(col, "")
                        passed = "VARCHAR" in col_type or "TEXT" in col_type
                        out.append({
                            "rule_id": self.id,
                            "table": t,
                            "column": col,
                            "passed": passed,
                            "severity": "warn" if not passed else "info",
                            "message": f"{t}.{col} type={col_type}, expected VARCHAR or TEXT",
                            "details": {"type": col_type}
                        })
        return out

class DecimalTypeRule:
    id = "semantic_decimal_type"
    def __init__(self, columns: list = None):
        self.columns = columns or ["Price", "Amount", "Rate", "Cost"]

    def run(self, db_path: str, ctx: Dict[str, Any]):
        out = []
        with sqlite3.connect(db_path) as conn:
            cur = conn.cursor()
            for t in ctx["tables"]:
                cur.execute(f"PRAGMA table_info('{t}')")
                cols = cur.fetchall()
                col_names = [c[1] for c in cols]
                col_types = {c[1]: str(c[2]).upper() for c in cols}
                for col in col_names:
                    if col in self.columns or any(col.lower().find(term.lower()) != -1 for term in self.columns):
                        col_type = col_types.get(col, "")
                        passed = "DECIMAL" in col_type or "REAL" in col_type or "FLOAT" in col_type
                        out.append({
                            "rule_id": self.id,
                            "table": t,
                            "column": col,
                            "passed": passed,
                            "severity": "warn" if not passed else "info",
                            "message": f"{t}.{col} type={col_type}, expected DECIMAL, REAL or FLOAT",
                            "details": {"type": col_type}
                        })
        return out

class IntegerTypeRule:
    id = "semantic_integer_type"
    def __init__(self, columns: list = None):
        self.columns = columns or ["Count", "Number", "Quantity", "ID"]

    def run(self, db_path: str, ctx: Dict[str, Any]):
        out = []
        with sqlite3.connect(db_path) as conn:
            cur = conn.cursor()
            for t in ctx["tables"]:
                cur.execute(f"PRAGMA table_info('{t}')")
                cols = cur.fetchall()
                col_names = [c[1] for c in cols]
                col_types = {c[1]: str(c[2]).upper() for c in cols}
                for col in col_names:
                    if col in self.columns or any(col.lower().find(term.lower()) != -1 for term in self.columns):
                        col_type = col_types.get(col, "")
                        passed = "INT" in col_type
                        out.append({
                            "rule_id": self.id,
                            "table": t,
                            "column": col,
                            "passed": passed,
                            "severity": "warn" if not passed else "info",
                            "message": f"{t}.{col} type={col_type}, expected INT",
                            "details": {"type": col_type}
                        })
        return out

class EmailFormatRule:
    id = "semantic_email"
    def __init__(self, column: str = "Email"):
        self.column = column
        self.pat = re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")

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
                # 拉取数据到 Python 侧进行检查
                cur.execute(f"SELECT \"{self.column}\" FROM '{t}' WHERE \"{self.column}\" IS NOT NULL AND \"{self.column}\" <> '' LIMIT 100")
                values = [row[0] for row in cur.fetchall()]
                invalid_count = sum(1 for val in values if not self.pat.match(val))
                passed = invalid_count == 0
                out.append({"rule_id": self.id, "table": t, "passed": passed,
                            "severity": "warn" if not passed else "info",
                            "message": f"{t}.{self.column} email format check: {invalid_count} invalid out of {len(values)} sampled",
                            "details": {"total": total, "nonnull": nonnull, "invalid_sampled": invalid_count}})
        return out

class PunctuationEndingRule:
    id = "semantic_punctuation_ending"
    def __init__(self, columns: list = None, punctuation: str = ".!?,;:"):
        self.columns = columns or []
        self.punctuation = punctuation

    def run(self, db_path: str, ctx: Dict[str, Any]):
        out = []
        with sqlite3.connect(db_path) as conn:
            cur = conn.cursor()
            for t in ctx["tables"]:
                cur.execute(f"PRAGMA table_info('{t}')")
                cols = [c[1] for c in cur.fetchall()]
                check_cols = self.columns if self.columns else cols
                for col in check_cols:
                    if col not in cols: continue
                    cur.execute(f"SELECT COUNT(*) FROM '{t}'")
                    total = cur.fetchone()[0]
                    cur.execute(f"SELECT COUNT(*) FROM '{t}' WHERE \"{col}\" IS NOT NULL AND \"{col}\" <> ''")
                    nonnull = cur.fetchone()[0]
                    # 拉取数据到 Python 侧进行检查
                    cur.execute(f"SELECT \"{col}\" FROM '{t}' WHERE \"{col}\" IS NOT NULL AND \"{col}\" <> '' LIMIT 100")
                    values = [row[0] for row in cur.fetchall()]
                    invalid_count = sum(1 for val in values if val and any(val.strip().endswith(p) for p in self.punctuation))
                    passed = invalid_count == 0
                    out.append({
                        "rule_id": self.id,
                        "table": t,
                        "column": col,
                        "passed": passed,
                        "severity": "warn" if not passed else "info",
                        "message": f"{t}.{col} punctuation ending check: {invalid_count} invalid out of {len(values)} sampled",
                        "details": {"total": total, "nonnull": nonnull, "invalid_sampled": invalid_count}
                    })
        return out
