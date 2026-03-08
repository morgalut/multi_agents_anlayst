from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, Optional
from urllib import request
from urllib.error import HTTPError, URLError

from .logger import logger


@dataclass(frozen=True, slots=True)
class HttpMCPTransportConfig:
    timeout_seconds: float = 60.0
    max_error_body_chars: int = 4000


class HttpMCPTransport:


    def __init__(
        self,
        server_base_urls: Dict[str, str],
        config: Optional[HttpMCPTransportConfig] = None,
    ):
        self._base = dict(server_base_urls)
        self._cfg = config or HttpMCPTransportConfig()

    def call_tool(
        self,
        server_id: str,
        tool_name: str,
        args: Dict[str, Any],
        ctx: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:

        logger.info("[MCP CALL] server=%s tool=%s", server_id, tool_name)

        if server_id not in self._base:
            raise KeyError(f"Unknown server_id: {server_id}")

        url = self._base[server_id].rstrip("/") + "/call"

        payload = {
            "tool_name": tool_name,
            "args": args or {},
            "ctx": ctx or {},
        }

        logger.debug("[MCP REQUEST] %s", payload)

        data = json.dumps(payload).encode("utf-8")

        req = request.Request(
            url=url,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )

        try:
            with request.urlopen(req, timeout=self._cfg.timeout_seconds) as resp:
                body = resp.read().decode("utf-8")
                out = json.loads(body) if body else {}

                logger.info("[MCP SUCCESS] tool=%s", tool_name)
                return out

        except HTTPError as e:
            body_text: Optional[str] = None
            try:
                raw = e.read()
                if isinstance(raw, bytes):
                    body_text = raw.decode("utf-8", errors="replace")
                elif raw is not None:
                    body_text = str(raw)
                if body_text and len(body_text) > self._cfg.max_error_body_chars:
                    body_text = body_text[: self._cfg.max_error_body_chars] + "...[truncated]"
            except Exception:
                body_text = None

            logger.error("[MCP ERROR] HTTPError %s tool=%s body=%s", e.code, tool_name, body_text)

            # FIX: Attach the already-read body onto the exception so that
            # excel_client._read_http_error_body() can retrieve it without
            # trying to read the now-exhausted stream a second time.
            e._cached_body = body_text  # type: ignore[attr-defined]

            raise

        except URLError as e:
            logger.error("[MCP ERROR] URLError tool=%s error=%s", tool_name, e)
            raise