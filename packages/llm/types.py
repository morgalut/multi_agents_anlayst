from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass(frozen=True, slots=True)
class LLMMessage:
    role: str  # "system" | "user"
    content: str


@dataclass(frozen=True, slots=True)
class LLMResult:
    """
    Generic LLM call result.
    """
    text: str
    model: str
    usage: Dict[str, Any] = field(default_factory=dict)
    raw: Optional[Dict[str, Any]] = None