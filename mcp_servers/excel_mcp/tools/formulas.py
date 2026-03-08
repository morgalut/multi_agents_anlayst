from __future__ import annotations

from typing import Any

from ..server import ToolSpec

_MAX_DIMENSION = 10_000
_FORMULA_PREFIX = "="

_INPUT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "sheet_name": {"type": "string", "minLength": 1},
        "row0": {"type": "integer", "minimum": 0},
        "col0": {"type": "integer", "minimum": 0},
        "nrows": {"type": "integer", "minimum": 1, "maximum": _MAX_DIMENSION},
        "ncols": {"type": "integer", "minimum": 1, "maximum": _MAX_DIMENSION},
    },
    "required": ["sheet_name", "row0", "col0", "nrows", "ncols"],
    "additionalProperties": False,
}

_OUTPUT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "formulas": {
            "type": "array",
            "items": {
                "type": "array",
                "items": {"type": ["string", "null"]},
            },
        },
        "formula_count": {"type": "integer"},
    },
    "required": ["formulas", "formula_count"],
    "additionalProperties": False,
}


def _is_formula(value: Any) -> bool:
    return isinstance(value, str) and value.startswith(_FORMULA_PREFIX)


def tool_get_formulas(workbook_provider) -> ToolSpec:
    def handler(args: dict[str, Any], ctx: dict[str, Any]) -> dict[str, Any]:
        sheet_name: str = args["sheet_name"]
        row0: int = int(args["row0"])
        col0: int = int(args["col0"])
        nrows: int = int(args["nrows"])
        ncols: int = int(args["ncols"])

        workbook_path: str = ctx.get("workbook_path", "")
        if not workbook_path:
            raise ValueError("Missing required context: workbook_path")

        wb = workbook_provider.open(workbook_path)

        if sheet_name not in wb.sheetnames:
            available = ", ".join(f"'{s}'" for s in wb.sheetnames)
            raise KeyError(f"Sheet '{sheet_name}' not found. Available sheets: {available}")

        ws = wb[sheet_name]
        max_row, max_col = ws.max_row or 0, ws.max_column or 0

        effective_nrows = min(nrows, max(0, max_row - row0))
        effective_ncols = min(ncols, max(0, max_col - col0))

        formulas: list[list[str | None]] = []
        formula_count = 0

        for r in range(row0, row0 + effective_nrows):
            row_out: list[str | None] = []
            for c in range(col0, col0 + effective_ncols):
                value = ws.cell(row=r + 1, column=c + 1).value
                if _is_formula(value):
                    row_out.append(value)
                    formula_count += 1
                else:
                    row_out.append(None)

            row_out.extend([None] * (ncols - len(row_out)))
            formulas.append(row_out)

        empty_row: list[None] = [None] * ncols
        for _ in range(nrows - len(formulas)):
            formulas.append(list(empty_row))

        return {"formulas": formulas, "formula_count": formula_count}

    return ToolSpec(
        name="excel.get_formulas",
        description=(
            "Read a rectangular grid of cells from *sheet_name* and return each "
            "cell's formula or null if the cell does not contain a formula."
        ),
        input_schema=_INPUT_SCHEMA,
        output_schema=_OUTPUT_SCHEMA,
        capabilities=["excel.get_formulas"],
        handler=handler,
    )