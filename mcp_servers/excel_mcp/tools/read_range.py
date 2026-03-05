from __future__ import annotations

from typing import Any, Dict, List

from ..server import ToolSpec


def tool_read_sheet_range(workbook_provider) -> ToolSpec:
    def handler(args: Dict[str, Any], ctx: Dict[str, Any]) -> Dict[str, Any]:
        sheet_name = args["sheet_name"]
        row0 = int(args["row0"])
        col0 = int(args["col0"])
        nrows = int(args["nrows"])
        ncols = int(args["ncols"])

        wb = workbook_provider.open(ctx["workbook_path"])
        ws = wb[sheet_name]

        grid: List[List[Any]] = []
        for r in range(row0, row0 + nrows):
            row_out: List[Any] = []
            for c in range(col0, col0 + ncols):
                # openpyxl is 1-based for cell access
                cell = ws.cell(row=r + 1, column=c + 1)
                row_out.append(cell.value)
            grid.append(row_out)

        return {"grid": grid}

    return ToolSpec(
        name="excel.read_sheet_range",
        description="Read a rectangular grid from a sheet (0-based indices).",
        input_schema={
            "type": "object",
            "properties": {
                "sheet_name": {"type": "string"},
                "row0": {"type": "integer", "minimum": 0},
                "col0": {"type": "integer", "minimum": 0},
                "nrows": {"type": "integer", "minimum": 1},
                "ncols": {"type": "integer", "minimum": 1},
            },
            "required": ["sheet_name", "row0", "col0", "nrows", "ncols"],
            "additionalProperties": False,
        },
        output_schema={
            "type": "object",
            "properties": {"grid": {"type": "array", "items": {"type": "array"}}},
            "required": ["grid"],
            "additionalProperties": False,
        },
        capabilities=["excel.read_sheet_range"],
        handler=handler,
    )