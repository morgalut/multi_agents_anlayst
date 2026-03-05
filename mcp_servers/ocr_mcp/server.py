from __future__ import annotations

from typing import Any, Dict, List

from .types import ToolSpec
from .tools.render_sheet_image import tool_render_sheet_image
from .tools.extract_text import tool_extract_text


class OCRMCPServer:
    def __init__(self, ocr_backend):
        self.ocr_backend = ocr_backend
        self._tools: Dict[str, ToolSpec] = {t.name: t for t in self._build_tools()}

    def _build_tools(self) -> List[ToolSpec]:
        return [
            tool_render_sheet_image(self.ocr_backend),
            tool_extract_text(self.ocr_backend),
        ]

    def list_tools(self) -> List[Dict[str, Any]]:
        return [
            {
                "name": t.name,
                "description": t.description,
                "input_schema": t.input_schema,
                "output_schema": t.output_schema,
                "capabilities": t.capabilities,
            }
            for t in self._tools.values()
        ]

    def dispatch(self, tool_name: str, args: Dict[str, Any], ctx: Dict[str, Any]) -> Dict[str, Any]:
        if tool_name not in self._tools:
            raise KeyError(f"Unknown tool: {tool_name}")
        return self._tools[tool_name].handler(args=args, ctx=ctx)