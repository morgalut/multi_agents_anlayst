from __future__ import annotations

from typing import Any, Dict, List

from ..server import ToolSpec


def tool_write_cells(workbook_provider) -> ToolSpec:
    def handler(args: Dict[str, Any], ctx: Dict[str, Any]) -> Dict[str, Any]:
        sheet_name = args["sheet_name"]
        cells = args["cells"]  # list[{row,col,value}] 0-based

        wb = workbook_provider.open(ctx["workbook_path"])
        ws = wb[sheet_name]

        changed = 0
        for item in cells:
            r = int(item["row"])
            c = int(item["col"])
            v = item.get("value")
            ws.cell(row=r + 1, column=c + 1).value = v
            changed += 1

        workbook_provider.save(wb, ctx["workbook_path"])
        return {"written": changed}

    return ToolSpec(
        name="excel.write_cells",
        description="Write values to specific cells (0-based indices).",
        input_schema={
            "type": "object",
            "properties": {
                "sheet_name": {"type": "string"},
                "cells": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "row": {"type": "integer", "minimum": 0},
                            "col": {"type": "integer", "minimum": 0},
                            "value": {},
                        },
                        "required": ["row", "col", "value"],
                        "additionalProperties": False,
                    },
                },
            },
            "required": ["sheet_name", "cells"],
            "additionalProperties": False,
        },
        output_schema={
            "type": "object",
            "properties": {"written": {"type": "integer", "minimum": 0}},
            "required": ["written"],
            "additionalProperties": False,
        },
        capabilities=["excel.write_cells"],
        handler=handler,
    )