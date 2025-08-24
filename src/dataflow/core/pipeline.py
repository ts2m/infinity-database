"""Pipeline class for orchestrating data processing steps."""

from __future__ import annotations
from typing import Dict, Any, List, Type
from pathlib import Path
from .artifact import Artifact
from .operator import Operator
from .registry import OP_REGISTRY
from .config import load_yaml
from ..utils.logging import get_logger

log = get_logger(__name__)

class Pipeline:
    def __init__(self, workdir: str):
        self.workdir = Path(workdir)
        self.workdir.mkdir(parents=True, exist_ok=True)
        self.ctx: Dict[str, Artifact] = {}   # 最新产物（按 kind 存放）

    def run_steps(self, steps: List[Dict[str, Any]]):
        for i, step in enumerate(steps):
            op_name = step["op"]
            params = step.get("params", {})
            op_cls: Type[Operator] = OP_REGISTRY[op_name]
            op = op_cls()
            log.info(f"[{i+1}/{len(steps)}] Run Operator: {op_name} params={params}")

            # 匹配输入：从 ctx 里取算子声明的 input_kinds（缺就给空 Artifact）
            inputs: Dict[str, Artifact] = {}
            for k in op.input_kinds:
                if k in self.ctx:
                    inputs[k] = self.ctx[k]

            outputs = op.run(inputs, workdir=str(self.workdir), **params)
            # 合并产物到 ctx，允许同 kind 覆盖
            for kind, art in outputs.items():
                self.ctx[kind] = art

        return self.ctx

def run_from_config(path: str):
    cfg = load_yaml(path)
    pl = Pipeline(workdir=cfg.get("workdir", "./workdir"))
    return pl.run_steps(cfg["steps"])
