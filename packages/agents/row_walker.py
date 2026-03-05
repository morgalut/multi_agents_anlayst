from __future__ import annotations

from dataclasses import dataclass
from typing import List

from Multi_agen.packages.core import MainSheetSchema, MissingValueSentinel, OutputColumns, PipelineState, RowTask


@dataclass(frozen=True, slots=True)
class RowWalkerConfig:
    max_rows_scan: int = 2000  # safety cap


class RowWalkerAgent:
    """
    Reads the main Summary sheet and creates RowTask objects:
      RowTask { row_index, sheet_name }
    """

    def __init__(self, config: RowWalkerConfig | None = None):
        self.config = config or RowWalkerConfig()

    def build_tasks(self, state: PipelineState, tools) -> List[RowTask]:
        if state.main_sheet is None:
            raise RuntimeError("MainSheetSchema missing")

        schema: MainSheetSchema = state.main_sheet
        filename_col = schema.columns.get(OutputColumns.FILENAME.value)
        if filename_col is None:
            raise RuntimeError("Filename column not found in main sheet schema")

        # Read a vertical slice: header+rows in Filename column
        # Tool contract assumed: read_sheet_range(sheet, row0, col0, nrows, ncols) -> grid
        row0 = schema.header_row_index + 1
        grid = tools.excel_read_sheet_range(
            sheet_name=schema.name,
            row0=row0,
            col0=filename_col,
            nrows=self.config.max_rows_scan,
            ncols=1,
        )

        tasks: List[RowTask] = []
        for i, row in enumerate(grid):
            cell = row[0] if row else None
            sheet_name = (str(cell).strip() if cell is not None else "")
            if not sheet_name:
                # stop at first empty by default (common summary layout)
                break
            tasks.append(RowTask(row_index=row0 + i, sheet_name=sheet_name))
        return tasks