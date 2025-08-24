"""Operator for augmenting data using LLM."""

from __future__ import annotations
from typing import Dict
from pathlib import Path
import json, re, shutil
from ..core.operator import Operator
from ..core.registry import register
from ..core.artifact import Artifact
from ..providers.llm import HTTPClient
from ..operators.quality_check import quality_check_db
from ..utils.sqlite_exec import exec_python_code

def extract_python_block(text: str):
    m = re.findall(r"```python(.*?)```", text, flags=re.S)
    return m[0].strip() if m else ""

@register
class AugmentWithLLM(Operator):
    name = "AugmentWithLLM"
    input_kinds = ["AgentReadyMeta"]
    output_kinds = ["AugmentResult"]

    def run(self, inputs: Dict[str, Artifact], provider: str="llm_http",
            init_prompt_path: str="", react_prompt_path: str="", table_react_prompt_path: str="",
            max_iterations: int=20, workdir: str="", **cfg):

        meta = inputs["AgentReadyMeta"].data
        client = HTTPClient(url=cfg.get("url","http://localhost:8000"), token=cfg.get("token",""))

        log_dir = Path(workdir) / "augment_logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        results = {}

        for dbid, schema_meta in meta.items():
            sqlite_path = schema_meta["sqlite_path"]
            # 用空库起步（仅 schema）
            work_db = Path(workdir) / "augment_dbs" / f"{dbid}.sqlite"
            work_db.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(sqlite_path, work_db)

            # 组 prompt（简化）
            schema_info = "\n".join(schema_meta["table_meta"].values())
            init_prompt = Path(init_prompt_path).read_text("utf-8") if init_prompt_path else "Write extend_database()"
            full_prompt = f"{init_prompt}\n<SCHEMA>\n{schema_info}\n</SCHEMA>"
            rsp = client.complete(full_prompt)
            code = extract_python_block(rsp) + "\n\nif __name__=='__main__':\n\tprint(extend_database())"

            success = False
            for it in range(max_iterations):
                # 执行代码
                stdout, stderr = exec_python_code(code, env={
                    "SQLITE_PATH": str(work_db.resolve()),
                    "PYTHONNOUSERSITE": "1",
                    "MAX_ID": "1200",
                })
                if stdout and stdout.strip() == "True":
                    # 质量检查
                    ok, report, _ = quality_check_db(str(work_db))
                    if ok:
                        results[dbid] = {"success": True, "code": code}
                        success = True
                        break
                    else:
                        # 用质量错误+代码+schema 让模型修复
                        qtext = json.dumps(report, ensure_ascii=False, indent=2)
                        react_prompt = Path(table_react_prompt_path).read_text("utf-8") if table_react_prompt_path else "Fix quality issues"
                        fix_query = f"{react_prompt}\n<SCHEMA>\n{schema_info}\n</SCHEMA>\n<ERRORS>\n{qtext}\n</ERRORS>\n<PREV>\n```python\n{code}\n```\n</PREV>"
                        rsp = client.complete(fix_query)
                        code = extract_python_block(rsp) + "\n\nif __name__=='__main__':\n\tprint(extend_database())"
                else:
                    # 运行报错，走 react_prompt 修复
                    react = Path(react_prompt_path).read_text("utf-8") if react_prompt_path else "Fix error"
                    query = f"{react}\n<SCHEMA>\n{schema_info}\n</SCHEMA>\n<ERROR>\n{stderr}\n</ERROR>\n<PREV>\n```python\n{code}\n```\n</PREV>"
                    rsp = client.complete(query)
                    code = extract_python_block(rsp) + "\n\nif __name__=='__main__':\n\tprint(extend_database())"

            if not success:
                results[dbid] = {"success": False, "code": code}

        out = Artifact(kind="AugmentResult", data=results).save_json(f"{workdir}/augment_result.json")
        return {"AugmentResult": out}
