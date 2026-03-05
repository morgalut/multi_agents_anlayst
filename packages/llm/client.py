from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from openai import AzureOpenAI

from .types import LLMMessage, LLMResult


@dataclass(frozen=True, slots=True)
class LLMConfig:
    endpoint_env: str = "AZURE_OPENAI_GPT_5_ENDPOINT"
    api_key_env: str = "AZURE_OPENAI_GPT_5_API_KEY"
    api_version_env: str = "AZURE_OPENAI_GPT_5_API_VERSION"
    deployment_env: str = "AZURE_OPENAI_GPT_5_DEPLOYMENT_NAME"
    timeout_seconds: float = 60.0


class LLMClient:
    """
    Azure OpenAI LLM client.
    Reads credentials from environment variables (.env).

    IMPORTANT:
    - Never log secrets.
    """

    def __init__(self, config: Optional[LLMConfig] = None):
        self.config = config or LLMConfig()

        endpoint = os.getenv(self.config.endpoint_env)
        api_key = os.getenv(self.config.api_key_env)
        api_version = os.getenv(self.config.api_version_env)
        deployment = os.getenv(self.config.deployment_env)

        if not endpoint:
            raise RuntimeError(f"Missing env var: {self.config.endpoint_env}")
        if not api_key:
            raise RuntimeError(f"Missing env var: {self.config.api_key_env}")
        if not api_version:
            raise RuntimeError(f"Missing env var: {self.config.api_version_env}")
        if not deployment:
            raise RuntimeError(f"Missing env var: {self.config.deployment_env}")

        self.deployment = deployment
        self.client = AzureOpenAI(
            azure_endpoint=endpoint,
            api_key=api_key,
            api_version=api_version,
            timeout=self.config.timeout_seconds,
        )

    def chat(self, messages: List[LLMMessage]) -> LLMResult:
        resp = self.client.chat.completions.create(
            model=self.deployment,
            messages=[{"role": m.role, "content": m.content} for m in messages],
            temperature=0.0,
        )

        text = resp.choices[0].message.content or ""
        usage: Dict[str, Any] = {}
        try:
            usage = resp.usage.model_dump()  # type: ignore[attr-defined]
        except Exception:
            usage = {}

        return LLMResult(text=text, model=self.deployment, usage=usage, raw=None)