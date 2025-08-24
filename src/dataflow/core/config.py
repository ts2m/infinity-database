"""Configuration management for pipeline settings."""

from __future__ import annotations
import yaml

def load_yaml(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)
