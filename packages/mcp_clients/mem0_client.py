from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional


class MCPTransport:
    def call_tool(self, server_id: str, tool_name: str, args: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError


@dataclass(frozen=True, slots=True)
class Mem0ClientConfig:
    server_id: str = "mem0-mcp"


class Mem0MCPClient:
    """
    Optional memory tool client.
    """

    def __init__(self, transport: MCPTransport, config: Optional[Mem0ClientConfig] = None):
        self._t = transport
        self._cfg = config or Mem0ClientConfig()

    def get(self, key: str) -> Dict[str, Any]:
        return self._t.call_tool(self._cfg.server_id, "memory.get", {"key": key})

    def put(self, key: str, value: Any) -> Dict[str, Any]:
        return self._t.call_tool(self._cfg.server_id, "memory.put", {"key": key, "value": value})