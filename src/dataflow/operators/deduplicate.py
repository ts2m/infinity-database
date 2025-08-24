"""Operator for deduplicating data records."""

from __future__ import annotations
from typing import Dict
import json, hashlib
from ..core.operator import Operator
from ..core.registry import register
from ..core.artifact import Artifact

@register
class Deduplicate(Operator):
    name = "Deduplicate"
    input_kinds = ["IR"]
    output_kinds = ["IR"]

    def run(self, inputs: Dict[str, Artifact], **_):
        ir = json.loads(json.dumps(inputs["IR"].data))  # deep copy
        seen = {}
        for t, header in list(ir["table_header"].items()):
            stable = json.dumps({
                "header": header,
                "samples": ir["table_content"].get(t, {}).get("samples", [])
            }, sort_keys=True)
            h = hashlib.md5(stable.encode("utf-8")).hexdigest()
            if h in seen:
                # remove duplicate table
                ir["table_header"].pop(t, None)
                ir["table_schema"].pop(t, None)
                ir["table_content"].pop(t, None)
            else:
                seen[h] = t
        return {"IR": Artifact(kind="IR", data=ir)}
