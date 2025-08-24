from __future__ import annotations
from typing import List, Dict, Any
import numpy as np, requests, time

class EmbeddingProvider:
    def embed(self, texts: List[str], **kwargs) -> np.ndarray:
        raise NotImplementedError

class DummyEmbedding(EmbeddingProvider):
    def embed(self, texts: List[str], **_):
        rng = np.random.default_rng(42)
        return rng.normal(size=(len(texts), 128))

class QianfanEmbedding(EmbeddingProvider):
    def __init__(self, api_url: str, token: str, model: str):
        self.api_url = api_url
        self.token = token
        self.model = model

    def embed(self, texts: List[str], **_):
        headers = {"Content-Type": "application/json", "Authorization": f"Bearer {self.token}"}
        out = []
        for t in texts:
            payload = {"model": self.model, "input": [t]}
            for _ in range(5):
                try:
                    r = requests.post(self.api_url, headers=headers, json=payload, timeout=20)
                    r.raise_for_status()
                    res = r.json()
                    out.append(res["data"][0]["embedding"])
                    break
                except Exception:
                    time.sleep(1)
            else:
                out.append([0.0]*768)
        return np.array(out, dtype=float)

def get_embedding_provider(name: str, **cfg) -> EmbeddingProvider:
    if name == "qianfan":
        return QianfanEmbedding(api_url=cfg.get("api_url","https://qianfan.baidubce.com/v2/embeddings"),
                                token=cfg.get("token",""), model=cfg.get("model","tao-8k"))
    return DummyEmbedding()
