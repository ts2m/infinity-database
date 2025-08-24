"""Operator for generating embeddings from text data."""

from __future__ import annotations
from typing import Dict, Any, List
import numpy as np
from ..core.operator import Operator
from ..core.registry import register
from ..core.artifact import Artifact
from ..providers.embedding import get_embedding_provider

@register
class EmbedTables(Operator):
    name = "EmbedTables"
    input_kinds = ["IR"]
    output_kinds = ["Embeddings"]

    def run(self, inputs: Dict[str, Artifact], provider: str="dummy", model: str="", parallelism: int=8, workdir: str="", **kwargs):
        ir = inputs["IR"].data
        ids, texts = [], []
        for t in ir["table_header"].keys():
            title = t
            header = ", ".join(ir["table_header"][t])
            rep = f"Table Title: {title}. Column Names: {header}."
            ids.append(t); texts.append(rep)

        prov = get_embedding_provider(provider, model=model, **kwargs)
        vecs = prov.embed(texts)

        emb = {"ids": ids, "vectors": vecs.tolist()}
        art = Artifact(kind="Embeddings", data=emb).save_json(f"{workdir}/embeddings.json")
        return {"Embeddings": art}
