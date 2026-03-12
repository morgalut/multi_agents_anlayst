from __future__ import annotations

from typing import Any, Dict, List

from ..types import ToolSpec


def tool_detect_merged_cells(workbook_provider) -> ToolSpec:
    def handler(args: Dict[str, Any], ctx: Dict[str, Any]) -> Dict[str, Any]:
        sheet_name = args["sheet_name"]

        workbook_path = ctx.get("workbook_path", "")
        if not workbook_path:
            raise ValueError("Missing required context: workbook_path")

        wb = workbook_provider.open_for_metadata(workbook_path)
        try:
            if sheet_name not in wb.sheetnames:
                available = ", ".join(repr(s) for s in wb.sheetnames[:20])
                raise ValueError(
                    f"Worksheet {sheet_name!r} does not exist. Available sheets: {available}"
                )

            ws = wb[sheet_name]

            merged_ranges: List[Dict[str, Any]] = []
            for mr in ws.merged_cells.ranges:
                merged_ranges.append(
                    {
                        "min_row": mr.min_row - 1,
                        "min_col": mr.min_col - 1,
                        "max_row": mr.max_row - 1,
                        "max_col": mr.max_col - 1,
                        "range": str(mr),
                    }
                )

            return {"merged_ranges": merged_ranges}
        finally:
            workbook_provider.close_quietly(wb)

    return ToolSpec(
        name="excel.detect_merged_cells",
        description="Return merged cell ranges (0-based bounds).",
        input_schema={
            "type": "object",
            "properties": {
                "sheet_name": {"type": "string", "minLength": 1},
            },
            "required": ["sheet_name"],
            "additionalProperties": False,
        },
        output_schema={
            "type": "object",
            "properties": {
                "merged_ranges": {
                    "type": "array",
                    "items": {"type": "object"},
                },
            },
            "required": ["merged_ranges"],
            "additionalProperties": False,
        },
        capabilities=["excel.detect_merged_cells"],
        handler=handler,
    )