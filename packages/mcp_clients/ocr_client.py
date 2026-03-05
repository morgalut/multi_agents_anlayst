from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional


class MCPTransport:
    def call_tool(self, server_id: str, tool_name: str, args: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError


@dataclass(frozen=True, slots=True)
class OcrClientConfig:
    server_id: str = "ocr-mcp"


class OCRMCPClient:
    def __init__(self, transport: MCPTransport, config: Optional[OcrClientConfig] = None):
        self._t = transport
        self._cfg = config or OcrClientConfig()

    def render_sheet_image(self, workbook_path: str, sheet_name: str) -> Dict[str, Any]:
        """
        Returns: { "image_path": "...", "width": int, "height": int }
        """
        args = {"workbook_path": workbook_path, "sheet_name": sheet_name}
        return self._t.call_tool(self._cfg.server_id, "ocr.render_sheet_image", args)

    def extract_text(self, image_path: str) -> Dict[str, Any]:
        """
        Returns: { "text": "...", "blocks": [...] }
        """
        return self._t.call_tool(self._cfg.server_id, "ocr.extract_text", {"image_path": image_path})