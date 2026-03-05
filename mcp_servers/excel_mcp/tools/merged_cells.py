from __future__ import annotations

from typing import Any, Dict, List

from ..server import ToolSpec


def tool_detect_merged_cells(workbook_provider) -> ToolSpec:
    def handler(args: Dict[str, Any], ctx: Dict[str, Any]) -> Dict[str, Any]:
        sheet_name = args["sheet_name"]
        wb = workbook_provider.open(ctx["workbook_path"])
        ws = wb[sheet_name]

        merged_ranges: List[Dict[str, Any]] = []
        for mr in ws.merged_cells.ranges:
            # mr bounds are 1-based: min_row, min_col, max_row, max_col
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

    return ToolSpec(
        name="excel.detect_merged_cells",
        description="Return merged cell ranges (0-based bounds).",
        input_schema={
            "type": "object",
            "properties": {"sheet_name": {"type": "string"}},
            "required": ["sheet_name"],
            "additionalProperties": False,
        },
        output_schema={
            "type": "object",
            "properties": {
                "merged_ranges": {"type": "array", "items": {"type": "object"}},
            },
            "required": ["merged_ranges"],
            "additionalProperties": False,
        },
        capabilities=["excel.detect_merged_cells"],
        handler=handler,
    )