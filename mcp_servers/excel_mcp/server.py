from __future__ import annotations

from typing import Any, Dict, List

from .types import ToolSpec
from .tools.list_sheets import tool_list_sheets
from .tools.read_range import tool_read_sheet_range
from .tools.write_cells import tool_write_cells
from .tools.merged_cells import tool_detect_merged_cells
from .tools.formulas import tool_get_formulas
from .tools.find_text import tool_find_text


class ExcelMCPServer:
    def __init__(self, workbook_provider):
        self.workbook_provider = workbook_provider

        built_tools = self._build_tools()
        self._tools: Dict[str, ToolSpec] = {t.name: t for t in built_tools}

    def _build_tools(self) -> List[ToolSpec]:
        factories = [
            ("excel.list_sheets", tool_list_sheets),
            ("excel.read_sheet_range", tool_read_sheet_range),
            ("excel.write_cells", tool_write_cells),
            ("excel.detect_merged_cells", tool_detect_merged_cells),
            ("excel.get_formulas", tool_get_formulas),
            ("excel.find_text", tool_find_text),
        ]

        built: List[ToolSpec] = []
        for name, factory in factories:
            tool = factory(self.workbook_provider)
            if tool is None:
                raise RuntimeError(f"Tool factory returned None: {name}")
            if not isinstance(tool, ToolSpec):
                raise TypeError(
                    f"Tool factory {name} returned {type(tool).__name__}, expected ToolSpec"
                )
            built.append(tool)

        return built

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