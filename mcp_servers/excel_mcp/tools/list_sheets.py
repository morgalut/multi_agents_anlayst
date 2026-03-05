from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List

from ..server import ToolSpec


def tool_list_sheets(workbook_provider) -> ToolSpec:
    def handler(args: Dict[str, Any], ctx: Dict[str, Any]) -> Dict[str, Any]:
        wb = workbook_provider.open(ctx["workbook_path"])
        return {"sheets": list(wb.sheetnames)}

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