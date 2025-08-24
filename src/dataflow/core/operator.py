"""Base Operator class for data processing steps."""

from __future__ import annotations
from typing import Dict, Any, List
from .artifact import Artifact

class Operator:
    name: str = "Operator"
    input_kinds: List[str] = []   # 允许的上游产物类型
    output_kinds: List[str] = []  # 产物类型（通常 1 个，也可多产物）

    def run(self, inputs: Dict[str, Artifact], **params) -> Dict[str, Artifact]:
        """子类实现核心逻辑。inputs 的 key = kind，value = Artifact"""
        raise NotImplementedError

    # 可选：统一的缓存键（基于输入 hash + params）
    @classmethod
    def cache_key(cls, inputs: Dict[str, Artifact], params: Dict[str, Any]) -> str:
        base = cls.__name__ + "|" + "|".join(
            f"{k}:{v.hash()}" for k, v in sorted(inputs.items())
        ) + "|" + str(sorted(params.items()))
        import xxhash
        return xxhash.xxh3_64_hexdigest(base)
