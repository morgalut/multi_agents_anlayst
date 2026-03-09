from __future__ import annotations

from dataclasses import dataclass
from typing import List

from Multi_agen.packages.core import (
    ColumnMapping,
    FinalRenderOutput,
    SheetExtractionResult,
    WorkbookExtractionResult,
)


@dataclass(frozen=True, slots=True)
class OutputRendererConfig:
    """
    Deterministic renderer for the required workbook-structure output format.

    Important:
    - Renders the exact human-readable structure shown in the examples.
    - Does not invent values.
    - Assumes inputs are already resolved and validated.
    """
    line_break: str = "\n"


class OutputRenderer:
    """
    Renders workbook structural analysis into the final deterministic text format.

    Input:
      WorkbookExtractionResult

    Output:
      FinalRenderOutput(text=...)

    This replaces the old per-row summary-sheet renderer.
    """

    def __init__(self, config: OutputRendererConfig | None = None):
        self.config = config or OutputRendererConfig()

    def render(
        self,
        workbook_result: WorkbookExtractionResult,
    ) -> FinalRenderOutput:
        lines: List[str] = []

        total_columns = 0
        sorted_sheets = workbook_result.sorted_sheets()

        for idx, sheet in enumerate(sorted_sheets):
            if idx > 0:
                lines.append("")

            lines.extend(self._render_sheet(sheet))
            total_columns += len(sheet.columns)

        if lines:
            lines.append("")

        lines.extend(self._render_workbook_footer(workbook_result))

        return FinalRenderOutput(
            text=self.config.line_break.join(lines).rstrip(),
            sheets_count=len(sorted_sheets),
            columns_count=total_columns,
            entities_count=len(workbook_result.entities),
        )

    def _render_sheet(self, sheet: SheetExtractionResult) -> List[str]:
        lines: List[str] = []

        unit = sheet.unit or "units"
        data_row_start = self._fmt_int(sheet.data_row_start)
        data_row_end = self._fmt_int(sheet.data_row_end)

        lines.append(
            f"SHEET: {sheet.sheet_name} | Contains: {sheet.contains} | Unit: {unit} | Data rows: {data_row_start}-{data_row_end}"
        )
        lines.append("")

        for col in sheet.sorted_columns():
            lines.append(self._render_column(col))

        return lines

    def _render_column(self, col: ColumnMapping) -> str:
        return (
            f"COLUMN: {col.col_letter}({col.col_idx + 1})"
            f" | Role: {col.role}"
            f" | Entity: {col.entity}"
            f" | Currency: {col.currency}"
            f" | Period: {col.period}"
            f" | Header: {col.header_text}"
            f" | Formula: {col.formula_pattern}"
            f" | RowStart: {self._fmt_int(col.row_start)}"
            f" | RowEnd: {self._fmt_int(col.row_end)}"
            f" | SheetName: {col.sheet_name}"
        )

    def _render_workbook_footer(self, workbook_result: WorkbookExtractionResult) -> List[str]:
        entities = [str(x).strip() for x in workbook_result.entities if str(x).strip()]
        entities_text = ", ".join(entities)

        if entities:
            entities_line = f"ENTITIES: {len(entities)} entities: {entities_text}"
        else:
            entities_line = "ENTITIES: 0 entities:"

        consolidated_line = (
            f"CONSOLIDATED: {'yes' if workbook_result.has_consolidated else 'no'}"
            f" - formula pattern: {workbook_result.consolidated_formula_pattern}"
        )

        aje_types = [str(x).strip() for x in workbook_result.aje_types if str(x).strip()]
        aje_types_text = ", ".join(aje_types)

        aje_line = (
            f"AJE: {'yes' if workbook_result.has_aje else 'no'}"
            f" - types found: {aje_types_text}"
        )

        nis_line = f"NIS: {'yes' if workbook_result.has_nis else 'no'}"

        return [
            entities_line,
            consolidated_line,
            aje_line,
            nis_line,
        ]

    def _fmt_int(self, value: int | None) -> str:
        return "" if value is None else str(int(value))