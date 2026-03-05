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


class HttpMCPTransport:

    def __init__(self, server_base_urls: Dict[str, str], config: Optional[HttpMCPTransportConfig] = None):
        self._base = dict(server_base_urls)
        self._cfg = config or HttpMCPTransportConfig()

    def call_tool(self, server_id: str, tool_name: str, args: Dict[str, Any], ctx: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:

        logger.info(f"[MCP CALL] server={server_id} tool={tool_name}")

        if server_id not in self._base:
            raise KeyError(f"Unknown server_id: {server_id}")

        url = self._base[server_id].rstrip("/") + "/call"

        payload = {
            "tool_name": tool_name,
            "args": args or {},
            "ctx": ctx or {},
        }

        logger.debug(f"[MCP REQUEST] {payload}")

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

                logger.info(f"[MCP SUCCESS] tool={tool_name}")

                return out

        except HTTPError as e:
            logger.error(f"[MCP ERROR] HTTPError {e.code} tool={tool_name}")
            raise

        except URLError as e:
            logger.error(f"[MCP ERROR] URLError tool={tool_name} error={e}")
            raise