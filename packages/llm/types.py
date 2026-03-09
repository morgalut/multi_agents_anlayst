from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Literal, Optional


LLMRole = Literal["system", "user", "assistant"]


@dataclass(frozen=True, slots=True)
class LLMMessage:
    """
    One chat message sent to the LLM.
    """
    role: LLMRole
    content: str


@dataclass(frozen=True, slots=True)
class LLMResult:
    """
    Normalized LLM response returned by the client.

    text:
      primary response text

    model:
      deployment/model identifier used by the client

    usage:
      best-effort token/accounting payload from provider SDK

    raw:
      optional provider-native raw payload if the client chooses to keep it
    """
    text: str
    model: str
    usage: Dict[str, Any] = field(default_factory=dict)
    raw: Optional[Any] = None