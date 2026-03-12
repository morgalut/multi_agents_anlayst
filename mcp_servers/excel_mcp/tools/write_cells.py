from __future__ import annotations


from typing import Any, Dict

from ..types import ToolSpec

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

            formulas: list[list[str | None]] = []
            formula_count = 0

            for row in ws.iter_rows(
                min_row=row0 + 1,
                max_row=row0 + nrows,
                min_col=col0 + 1,
                max_col=col0 + ncols,
            ):
                row_out: list[str | None] = []
                for cell in row:
                    value = cell.value
                    if _is_formula(value):
                        row_out.append(value)
                        formula_count += 1
                    else:
                        row_out.append(None)
                formulas.append(row_out)

            while len(formulas) < nrows:
                formulas.append([None] * ncols)

            for row in formulas:
                if len(row) < ncols:
                    row.extend([None] * (ncols - len(row)))
                elif len(row) > ncols:
                    del row[ncols:]

            return {"formulas": formulas, "formula_count": formula_count}
        finally:
            workbook_provider.close_quietly(wb)

    return ToolSpec(
        name="excel.get_formulas",
        description=(
            "Read a rectangular grid of cells from sheet_name and return each "
            "cell's formula or null if the cell does not contain a formula."
        ),
        input_schema=_INPUT_SCHEMA,
        output_schema=_OUTPUT_SCHEMA,
        capabilities=["excel.get_formulas"],
        handler=handler,
    )
    


def tool_write_cells(workbook_provider) -> ToolSpec:
    def handler(args: Dict[str, Any], ctx: Dict[str, Any]) -> Dict[str, Any]:
        sheet_name = args["sheet_name"]
        cells = args["cells"]

        workbook_path = ctx.get("workbook_path", "")
        if not workbook_path:
            raise ValueError("Missing required context: workbook_path")

        wb = workbook_provider.open_for_write(workbook_path)
        try:
            if sheet_name not in wb.sheetnames:
                available = ", ".join(repr(s) for s in wb.sheetnames[:20])
                raise ValueError(
                    f"Worksheet {sheet_name!r} does not exist. Available sheets: {available}"
                )

            ws = wb[sheet_name]

            changed = 0
            for item in cells:
                r = int(item["row"])
                c = int(item["col"])
                v = item.get("value")
                if r < 0 or c < 0:
                    raise ValueError("row and col must be >= 0")
                ws.cell(row=r + 1, column=c + 1).value = v
                changed += 1

            workbook_provider.save(wb, workbook_path)
            return {"written": changed}
        finally:
            workbook_provider.close_quietly(wb)

    return ToolSpec(
        name="excel.write_cells",
        description="Write values to specific cells (0-based indices).",
        input_schema={
            "type": "object",
            "properties": {
                "sheet_name": {"type": "string", "minLength": 1},
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