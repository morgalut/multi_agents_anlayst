from __future__ import annotations

import json
import socket
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional
from urllib import request
from urllib.error import HTTPError, URLError

from .logger import logger


@dataclass(frozen=True, slots=True)
class HttpMCPTransportConfig:
    timeout_seconds: float = 120.0
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

        if server_id not in self._base:
            raise KeyError(f"Unknown server_id: {server_id}")

        url = self._base[server_id].rstrip("/") + "/call"
        payload = {
            "tool_name": tool_name,
            "args": args or {},
            "ctx": ctx or {},
        }

        data = json.dumps(payload).encode("utf-8")
        req = request.Request(
            url=url,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )

        started = time.perf_counter()
        sheet_name = (args or {}).get("sheet_name")
        workbook_path = (ctx or {}).get("workbook_path")

        logger.info(
            "[MCP CALL] server=%s tool=%s timeout_s=%.1f sheet=%s workbook=%r",
            server_id,
            tool_name,
            self._cfg.timeout_seconds,
            sheet_name,
            workbook_path,
        )

        try:
            with request.urlopen(req, timeout=self._cfg.timeout_seconds) as resp:
                body = resp.read().decode("utf-8")
                out = json.loads(body) if body else {}

                elapsed_ms = (time.perf_counter() - started) * 1000
                logger.info(
                    "[MCP SUCCESS] server=%s tool=%s elapsed_ms=%.1f",
                    server_id,
                    tool_name,
                    elapsed_ms,
                )
                return out

        except HTTPError as e:
            elapsed_ms = (time.perf_counter() - started) * 1000
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

            logger.error(
                "[MCP ERROR] HTTPError server=%s tool=%s code=%s elapsed_ms=%.1f body=%s",
                server_id,
                tool_name,
                getattr(e, "code", None),
                elapsed_ms,
                body_text,
            )

            e._cached_body = body_text  # type: ignore[attr-defined]
            raise

        except (TimeoutError, socket.timeout) as e:
            elapsed_ms = (time.perf_counter() - started) * 1000
            logger.error(
                "[MCP ERROR] Timeout server=%s tool=%s timeout_s=%.1f elapsed_ms=%.1f sheet=%s workbook=%r error=%s",
                server_id,
                tool_name,
                self._cfg.timeout_seconds,
                elapsed_ms,
                sheet_name,
                workbook_path,
                type(e).__name__,
            )
            raise

        except URLError as e:
            elapsed_ms = (time.perf_counter() - started) * 1000
            logger.error(
                "[MCP ERROR] URLError server=%s tool=%s elapsed_ms=%.1f error=%s",
                server_id,
                tool_name,
                elapsed_ms,
                e,
            )
            raise