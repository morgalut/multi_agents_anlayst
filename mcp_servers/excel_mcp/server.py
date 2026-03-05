from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List

from .tools.list_sheets import tool_list_sheets
from .tools.read_range import tool_read_sheet_range
from .tools.write_cells import tool_write_cells
from .tools.merged_cells import tool_detect_merged_cells
from .tools.formulas import tool_get_formulas
from .tools.find_text import tool_find_text


@dataclass(frozen=True, slots=True)
class ToolSpec:
    name: str
    description: str
    input_schema: Dict[str, Any]
    output_schema: Dict[str, Any]
    capabilities: List[str]
    handler: Any  # callable(args, ctx) -> dict


class ExcelMCPServer:
    """
    Minimal MCP server skeleton.

    Integrate with actual MCP runtime by:
      - listing tools via list_tools()
      - calling dispatch(tool_name, args, ctx)
    """

    def __init__(self, workbook_provider):
        """
        workbook_provider: object that can open workbook path / provide workbook handle.
        Keep server stateless; ctx should carry workbook_path, etc.
        """
        self.workbook_provider = workbook_provider
        self._tools: Dict[str, ToolSpec] = {t.name: t for t in self._build_tools()}

    def _build_tools(self) -> List[ToolSpec]:
        return [
            tool_list_sheets(self.workbook_provider),
            tool_read_sheet_range(self.workbook_provider),
            tool_write_cells(self.workbook_provider),
            tool_detect_merged_cells(self.workbook_provider),
            tool_get_formulas(self.workbook_provider),
            tool_find_text(self.workbook_provider),
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
        tool = self._tools[tool_name]
        return tool.handler(args=args, ctx=ctx)