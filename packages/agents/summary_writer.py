from __future__ import annotations

from dataclasses import dataclass
from typing import List, Tuple, Optional
import logging

from Multi_agen.packages.core import MainSheetSchema, OutputColumns, PipelineState, RowResult

logger = logging.getLogger("multi_agen.agents.summary_writer")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)


@dataclass(frozen=True, slots=True)
class SummaryWriterConfig:
    """
    Controls how we write results back to the Summary sheet.
    """
    dry_run: bool = False

    # By default we DO NOT create missing optional columns
    # (matches your spec: "If optional columns are not present, we don’t create them by default.")
    create_missing_optional_columns: bool = False

    # If True, log the full payload (row/col/value)
    log_payload: bool = False


class SummaryRowWriterAgent:
    """
    Writes RowResolvedOutput into the Summary sheet row.

    Tool contract assumed:
      tools.excel_write_cells(sheet_name, cells=[(row_idx, col_idx, value), ...])
    """

    def __init__(self, config: Optional[SummaryWriterConfig] = None):
        self.config = config or SummaryWriterConfig()

    def write_row(self, state: PipelineState, row_result: RowResult, tools) -> None:
        if state.main_sheet is None:
            raise RuntimeError("MainSheetSchema missing")

        schema: MainSheetSchema = state.main_sheet
        r = row_result.row_index
        res = row_result.resolved

        logger.info("SummaryWriter: start row_index=%s sheet=%s", r, schema.name)

        # Only write columns that exist in schema.columns.
        # Optional column creation (if enabled) would happen elsewhere (schema migration),
        # not in this writer (writer stays simple & safe).
        payload: List[Tuple[int, int, str]] = []

        def put(header_text: str, value: str, *, optional: bool = False) -> None:
            col_idx = schema.columns.get(header_text)
            if col_idx is None:
                if optional and self.config.create_missing_optional_columns:
                    # Writer does NOT create columns; we log that it was requested but skipped.
                    logger.warning(
                        "SummaryWriter: optional column missing and create_missing_optional_columns=True "
                        "but writer does not create columns (header=%s).",
                        header_text,
                    )
                return
            payload.append((r, col_idx, value))

        # Required columns
        put(OutputColumns.FILENAME.value, res.filename)
        put(OutputColumns.BS.value, res.bs)
        put(OutputColumns.PL.value, res.pl)
        put(OutputColumns.MAIN_COMPANY_DOLLAR.value, res.main_company_dollar)
        put(OutputColumns.SUB_COMPANY.value, res.sub_company)

        # Optional columns (write only if present)
        put(OutputColumns.MAIN_COMPANY_IL.value, res.main_company_il, optional=True)
        put(OutputColumns.AJE.value, res.aje, optional=True)
        put(OutputColumns.CONSOLIDATED.value, res.consolidated, optional=True)

        if not payload:
            logger.info("SummaryWriter: nothing to write row_index=%s", r)
            return

        if self.config.log_payload:
            logger.info("SummaryWriter: payload row_index=%s payload=%s", r, payload)
        else:
            logger.info("SummaryWriter: writing %d cells row_index=%s", len(payload), r)

        if self.config.dry_run:
            logger.info("SummaryWriter: dry_run=True, skipping excel_write_cells")
            return

        tools.excel_write_cells(sheet_name=schema.name, cells=payload)

        logger.info("SummaryWriter: done row_index=%s", r)