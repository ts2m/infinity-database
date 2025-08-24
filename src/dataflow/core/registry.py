"""Registry for operator classes."""

from __future__ import annotations
from typing import Dict, Type
from .operator import Operator

OP_REGISTRY: Dict[str, Type[Operator]] = {}

def register(cls: Type[Operator]):
    OP_REGISTRY[cls.__name__] = cls
    return cls
