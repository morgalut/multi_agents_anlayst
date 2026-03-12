from __future__ import annotations

from typing import Any, Dict, List

from ..types import ToolSpec


def tool_read_sheet_range(workbook_provider) -> ToolSpec:
    def handler(args: Dict[str, Any], ctx: Dict[str, Any]) -> Dict[str, Any]:
        sheet_name = args["sheet_name"]
        row0 = int(args["row0"])
        col0 = int(args["col0"])
        nrows = int(args["nrows"])
        ncols = int(args["ncols"])

        workbook_path = ctx.get("workbook_path", "")
        if not workbook_path:
            raise ValueError("Missing required context: workbook_path")

        if row0 < 0 or col0 < 0:
            raise ValueError("row0 and col0 must be >= 0")
        if nrows < 1 or ncols < 1:
            raise ValueError("nrows and ncols must be >= 1")

        wb = workbook_provider.open_for_read(workbook_path, data_only=False)
        try:
            if sheet_name not in wb.sheetnames:
                available = ", ".join(repr(s) for s in wb.sheetnames[:20])
                raise ValueError(
                    f"Worksheet {sheet_name!r} does not exist. Available sheets: {available}"
                )

            ws = wb[sheet_name]

            grid: List[List[Any]] = []
            for row in ws.iter_rows(
                min_row=row0 + 1,
                max_row=row0 + nrows,
                min_col=col0 + 1,
                max_col=col0 + ncols,
            ):
                grid.append([cell.value for cell in row])

            while len(grid) < nrows:
                grid.append([None] * ncols)

            for row in grid:
                if len(row) < ncols:
                    row.extend([None] * (ncols - len(row)))
                elif len(row) > ncols:
                    del row[ncols:]

            return {"grid": grid}
        finally:
            workbook_provider.close_quietly(wb)

    return ToolSpec(
        name="excel.read_sheet_range",
        description="Read a rectangular grid from a sheet (0-based indices).",
        input_schema={
            "type": "object",
            "properties": {
                "sheet_name": {"type": "string", "minLength": 1},
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
            "properties": {
                "grid": {
                    "type": "array",
                    "items": {"type": "array"},
                }
            },
            "required": ["grid"],
            "additionalProperties": False,
        },
        capabilities=["excel.read_sheet_range"],
        handler=handler,
    )