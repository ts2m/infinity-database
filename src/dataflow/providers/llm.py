from __future__ import annotations
from typing import Dict, Any
import requests, time

class LLMClient:
    def complete(self, prompt: str) -> str:
        raise NotImplementedError

class HTTPClient(LLMClient):
    def __init__(self, url: str, token: str):
        self.url = url; self.token = token

    def complete(self, prompt: str) -> str:
        headers = {"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"}
        payload = {"query": prompt, "inputs": {"__system__": ""}}
        for _ in range(3):
            try:
                r = requests.post(self.url, headers=headers, json=payload, timeout=60)
                r.raise_for_status()
                return r.json().get("answer", "")
            except Exception:
                time.sleep(2)
        return ""
