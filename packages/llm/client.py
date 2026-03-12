# Multi_agen\packages\llm\client.py
from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from openai import AzureOpenAI, APIStatusError, APITimeoutError, RateLimitError
from .types import LLMMessage, LLMResult

logger = logging.getLogger("multi_agen.llm.client")


def _clean_env(name: str) -> str:
    value = (os.getenv(name) or "").strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        value = value[1:-1].strip()
    return value


@dataclass(frozen=True, slots=True)
class LLMConfig:
    endpoint_env: str     = "AZURE_OPENAI_GPT54_ENDPOINT"
    api_key_env: str      = "AZURE_OPENAI_GPT54_API_KEY"
    api_version_env: str  = "AZURE_OPENAI_GPT54_API_VERSION"
    deployment_env: str   = "AZURE_OPENAI_GPT54_DEPLOYMENT_NAME"

    # ── Timeouts ────────────────────────────────────────────────────────────
    # GPT-5.4 is a large model. Under TPM pressure it can take 90–180s.
    # connect_timeout: time to establish TCP connection
    # read_timeout:    time to wait for response bytes after sending request
    # The SDK's single `timeout=` sets BOTH — we need them separate via httpx.
    connect_timeout_seconds: float = 10.0
    read_timeout_seconds: float    = 180.0   # was 60/120 — too short post-429 retry

    max_completion_tokens: int = 16384

    # ── Retry policy ────────────────────────────────────────────────────────
    max_retries: int             = 4
    retry_base_delay: float      = 35.0    # 429: Azure retry-after is ~30s
    retry_timeout_delay: float   = 15.0    # timeout: shorter wait, then retry


class LLMClient:
    def __init__(self, config: Optional[LLMConfig] = None) -> None:
        self.config = config or LLMConfig()

        endpoint    = _clean_env(self.config.endpoint_env).rstrip("/")
        api_key     = _clean_env(self.config.api_key_env)
        api_version = _clean_env(self.config.api_version_env)
        deployment  = _clean_env(self.config.deployment_env)

        missing = [
            name for name, val in [
                (self.config.endpoint_env,    endpoint),
                (self.config.api_key_env,     api_key),
                (self.config.api_version_env, api_version),
                (self.config.deployment_env,  deployment),
            ]
            if not val
        ]
        if missing:
            raise RuntimeError(
                f"Missing environment variables: {missing}\n"
                f"Check Multi_agen/.env — variable names must use underscores, not dots."
            )

        self.endpoint    = endpoint
        self.api_version = api_version
        self.deployment  = deployment

        # ── Split connect vs read timeout via httpx.Timeout ─────────────────
        # The SDK accepts httpx.Timeout directly — this is the only way to set
        # connect and read timeouts independently without patching httpx.
        import httpx
        timeout = httpx.Timeout(
            connect=self.config.connect_timeout_seconds,
            read=self.config.read_timeout_seconds,
            write=30.0,
            pool=10.0,
        )

        # max_retries=0 — we own retry logic for full observability
        self.client = AzureOpenAI(
            azure_endpoint=endpoint,
            api_key=api_key,
            api_version=api_version,
            timeout=timeout,
            max_retries=0,
        )

        logger.info(
            "LLMClient ready  deployment=%r  endpoint=%r  "
            "read_timeout=%.0fs  max_retries=%d",
            self.deployment, self.endpoint,
            self.config.read_timeout_seconds, self.config.max_retries,
        )

    # ── Public: chat ─────────────────────────────────────────────────────────
    def chat(self, messages: List[LLMMessage]) -> LLMResult:
        """
        Send a chat request to GPT-5.4 with retry on 429 and timeout errors.

        Retry matrix
        ────────────
        RateLimitError  (429) → wait retry-after header OR base_delay (35s)
        APITimeoutError       → wait timeout_delay (15s), then retry
        APIStatusError  (5xx) → re-raise immediately (server error, not transient)
        Other Exception       → re-raise immediately
        """
        formatted    = [{"role": m.role, "content": m.content} for m in messages]
        total_chars  = sum(len(m["content"]) for m in formatted)
        total_tokens = total_chars // 4

        logger.info(
            "LLMClient:chat  deployment=%r  messages=%d  "
            "prompt_chars=%d  prompt_tokens~=%d",
            self.deployment, len(formatted), total_chars, total_tokens,
        )

        last_exc: Optional[Exception] = None

        for attempt in range(1, self.config.max_retries + 1):
            try:
                resp = self.client.chat.completions.create(
                    model=self.deployment,
                    messages=formatted,
                    max_completion_tokens=self.config.max_completion_tokens,
                    # temperature intentionally omitted — not supported by GPT-5.4
                )

                # ── Success ─────────────────────────────────────────────────
                text: str = resp.choices[0].message.content or ""

                usage: Dict[str, Any] = {}
                try:
                    if resp.usage is not None:
                        usage = (
                            resp.usage.model_dump()
                            if hasattr(resp.usage, "model_dump")
                            else vars(resp.usage)
                        )
                except Exception:
                    usage = {}

                logger.info(
                    "LLMClient:chat OK  attempt=%d/%d  usage=%s",
                    attempt, self.config.max_retries, usage,
                )
                return LLMResult(text=text, model=self.deployment, usage=usage, raw=None)

            # ── 429 Rate Limit ───────────────────────────────────────────────
            except RateLimitError as exc:
                last_exc = exc
                wait     = self._parse_retry_after(exc) or self.config.retry_base_delay

                if attempt < self.config.max_retries:
                    logger.warning(
                        "LLMClient:rate_limited  attempt=%d/%d  waiting=%.0fs  "
                        "prompt_tokens~=%d  "
                        "tip='Lower max_total_preview_tokens if this repeats'",
                        attempt, self.config.max_retries, wait, total_tokens,
                    )
                    time.sleep(wait)
                else:
                    logger.error(
                        "LLMClient:rate_limit_exhausted  attempts=%d  prompt_tokens~=%d",
                        self.config.max_retries, total_tokens,
                    )

            # ── Timeout ──────────────────────────────────────────────────────
            except APITimeoutError as exc:
                last_exc = exc
                wait     = self.config.retry_timeout_delay

                if attempt < self.config.max_retries:
                    logger.warning(
                        "LLMClient:timeout  attempt=%d/%d  waiting=%.0fs  "
                        "read_timeout_cfg=%.0fs  "
                        "tip='Increase read_timeout_seconds in LLMConfig if this repeats'",
                        attempt, self.config.max_retries, wait,
                        self.config.read_timeout_seconds,
                    )
                    time.sleep(wait)
                else:
                    logger.error(
                        "LLMClient:timeout_exhausted  attempts=%d  "
                        "read_timeout_cfg=%.0fs",
                        self.config.max_retries, self.config.read_timeout_seconds,
                    )

            # ── Structured API error (4xx/5xx) — don't retry ────────────────
            except APIStatusError as exc:
                raise RuntimeError(
                    f"Azure OpenAI API error  deployment={self.deployment!r}\n"
                    f"  status={exc.status_code}  message={exc.message}"
                ) from exc

            # ── Unknown error — don't retry ──────────────────────────────────
            except Exception as exc:
                raise RuntimeError(
                    f"Azure OpenAI call failed  deployment={self.deployment!r}\n"
                    f"  endpoint={self.endpoint!r}  api_version={self.api_version!r}\n"
                    f"  error={type(exc).__name__}: {exc}"
                ) from exc

        # All retries exhausted
        raise RuntimeError(
            f"Azure OpenAI: all {self.config.max_retries} retries exhausted  "
            f"deployment={self.deployment!r}  last_error={type(last_exc).__name__}: {last_exc}"
        ) from last_exc

    # ── Helpers ──────────────────────────────────────────────────────────────
    @staticmethod
    def _parse_retry_after(exc: RateLimitError) -> Optional[float]:
        """Extract Retry-After seconds from response headers, with +2s buffer."""
        try:
            headers = exc.response.headers
            val = headers.get("retry-after") or headers.get("Retry-After")
            return float(val) + 2.0 if val else None
        except Exception:
            return None