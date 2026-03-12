from __future__ import annotations

from typing import Any, Dict

from ..types import ToolSpec


def tool_list_sheets(workbook_provider) -> ToolSpec:
    def handler(args: Dict[str, Any], ctx: Dict[str, Any]) -> Dict[str, Any]:
        workbook_path = ctx.get("workbook_path", "")
        if not workbook_path:
            raise ValueError("Missing required context: workbook_path")

        wb = workbook_provider.open_for_read(workbook_path, data_only=False)
        try:
            return {"sheets": list(wb.sheetnames)}
        finally:
            workbook_provider.close_quietly(wb)

    return ToolSpec(
        name="excel.list_sheets",
        description="List sheet names in the workbook.",
        input_schema={
            "type": "object",
            "properties": {},
            "required": [],
            "additionalProperties": False,
        },
        output_schema={
            "type": "object",
            "properties": {"sheets": {"type": "array", "items": {"type": "string"}}},
            "required": ["sheets"],
            "additionalProperties": False,
        },
        capabilities=["excel.list_sheets"],
        handler=handler,
    )