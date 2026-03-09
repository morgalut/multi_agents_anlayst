from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional
import logging

from Multi_agen.packages.core import (
    MainSheetSchema,
    MissingValueSentinel,
    OutputColumns,
    PipelineState,
    RowTask,
)
from Multi_agen.packages.agents.sheet_name_resolver import SheetNameResolver, SheetNameResolution

logger = logging.getLogger("multi_agen.agents.row_walker")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)


@dataclass(frozen=True, slots=True)
class RowWalkerConfig:
    max_rows_scan: int = 2000
    exclude_hidden_summary_like_sheets: bool = True
    allow_sheet_name_resolution: bool = True


class RowWalkerAgent:
    """
    Builds RowTask objects for downstream analysis.

    Primary mode:
      - Use the Filename column from the main summary/output sheet.

    Fallback mode:
      - If the main sheet schema does not contain a Filename column,
        derive tasks from workbook sheet names.

    Improvement:
      - Candidate sheet names taken from cells are resolved against the
        real workbook sheet names before RowTask objects are created.
      - Provenance is attached to state.task_provenance so the API can
        serialize where each task came from:
            workbook file
            source sheet
            source row / column
            source A1 cell
            raw cell value
            normalized candidate
            resolved sheet name
            matched_by
    """

    def __init__(self, config: RowWalkerConfig | None = None):
        self.config = config or RowWalkerConfig()
        self._resolver = SheetNameResolver()

    def build_tasks(self, state: PipelineState, tools) -> List[RowTask]:
        if state.main_sheet is None:
            raise RuntimeError("MainSheetSchema missing")

        schema: MainSheetSchema = state.main_sheet
        all_sheets = tools.excel_list_sheets()
        filename_col = schema.columns.get(OutputColumns.FILENAME.value)

        task_provenance: List[Dict[str, Any]] = []

        # Preferred mode: explicit Filename column on the main sheet
        if filename_col is not None:
            logger.info(
                "RowWalker:using filename column mode main_sheet=%s header_row=%d filename_col=%d",
                schema.name,
                schema.header_row_index,
                filename_col,
            )
            tasks = self._build_from_filename_column(
                state=state,
                schema=schema,
                filename_col=filename_col,
                all_sheets=all_sheets,
                tools=tools,
                task_provenance=task_provenance,
            )
            if tasks:
                self._attach_task_provenance(state, task_provenance)
                logger.info("RowWalker:built tasks from filename column count=%d", len(tasks))
                return tasks

            logger.warning(
                "RowWalker:filename column mode produced zero valid tasks; falling back to workbook-sheet mode"
            )

        # Fallback mode: no Filename column found
        logger.info(
            "RowWalker:using workbook-sheet fallback mode main_sheet=%s",
            schema.name,
        )
        tasks = self._build_from_workbook_sheets(
            state=state,
            schema=schema,
            all_sheets=all_sheets,
            task_provenance=task_provenance,
        )

        if not tasks:
            raise RuntimeError(
                "Failed to build row tasks: no Filename column in main sheet schema and no usable workbook sheets found"
            )

        self._attach_task_provenance(state, task_provenance)
        logger.info("RowWalker:built tasks from workbook-sheet fallback count=%d", len(tasks))
        return tasks

    def _build_from_filename_column(
        self,
        state: PipelineState,
        schema: MainSheetSchema,
        filename_col: int,
        all_sheets: List[str],
        tools,
        task_provenance: List[Dict[str, Any]],
    ) -> List[RowTask]:
        """
        Build tasks from the explicit Filename column on the main sheet.

        Provenance source:
          - workbook sheet: schema.name
          - workbook row: row0 + i
          - workbook col: filename_col
        """
        row0 = schema.header_row_index + 1
        grid = tools.excel_read_sheet_range(
            sheet_name=schema.name,
            row0=row0,
            col0=filename_col,
            nrows=self.config.max_rows_scan,
            ncols=1,
        )

        tasks: List[RowTask] = []
        workbook_path = self._get_workbook_path(state)

        for i, row in enumerate(grid):
            cell = row[0] if row else None
            raw_cell_value = "" if cell is None else str(cell)
            normalized_candidate = self._normalize_sheet_name_cell(cell)

            if not normalized_candidate:
                # Common summary layout: stop on first empty row
                break

            resolution: SheetNameResolution = self._resolver.resolve(normalized_candidate, all_sheets)
            if resolution.resolved_name is None:
                logger.warning(
                    "RowWalker:unresolved sheet reference raw=%r row_index=%d available_count=%d",
                    normalized_candidate,
                    row0 + i,
                    len(all_sheets),
                )
                task_provenance.append(
                    self._make_task_provenance(
                        source_workbook_file=workbook_path,
                        source_sheet=schema.name,
                        source_row_0_based=row0 + i,
                        source_col_0_based=filename_col,
                        raw_cell_value=raw_cell_value,
                        normalized_candidate=normalized_candidate,
                        resolved_sheet_name=None,
                        matched_by=None,
                        source_kind="filename_column",
                    )
                )
                continue

            if resolution.resolved_name == schema.name:
                logger.info(
                    "RowWalker:skipping self-reference raw=%r resolved=%r row_index=%d",
                    normalized_candidate,
                    resolution.resolved_name,
                    row0 + i,
                )
                task_provenance.append(
                    self._make_task_provenance(
                        source_workbook_file=workbook_path,
                        source_sheet=schema.name,
                        source_row_0_based=row0 + i,
                        source_col_0_based=filename_col,
                        raw_cell_value=raw_cell_value,
                        normalized_candidate=normalized_candidate,
                        resolved_sheet_name=resolution.resolved_name,
                        matched_by=resolution.matched_by,
                        source_kind="filename_column_self_reference",
                    )
                )
                continue

            tasks.append(RowTask(row_index=row0 + i, sheet_name=resolution.resolved_name))
            task_provenance.append(
                self._make_task_provenance(
                    source_workbook_file=workbook_path,
                    source_sheet=schema.name,
                    source_row_0_based=row0 + i,
                    source_col_0_based=filename_col,
                    raw_cell_value=raw_cell_value,
                    normalized_candidate=normalized_candidate,
                    resolved_sheet_name=resolution.resolved_name,
                    matched_by=resolution.matched_by,
                    source_kind="filename_column",
                )
            )

        return tasks

    def _build_from_workbook_sheets(
        self,
        state: PipelineState,
        schema: MainSheetSchema,
        all_sheets: List[str],
        task_provenance: List[Dict[str, Any]],
    ) -> List[RowTask]:
        """
        Build tasks directly from workbook sheet names when no explicit
        Filename column exists in the detected main-sheet schema.

        Provenance source:
          - not a worksheet cell
          - comes from workbook tab list
        """
        main_sheet_name = (schema.name or "").strip()
        workbook_path = self._get_workbook_path(state)

        tasks: List[RowTask] = []
        synthetic_row_index = schema.header_row_index + 1

        for sheet_name in all_sheets:
            normalized = (sheet_name or "").strip()
            if not normalized:
                continue

            if normalized == main_sheet_name:
                continue

            if self.config.exclude_hidden_summary_like_sheets and self._should_skip_sheet_name(normalized):
                continue

            tasks.append(
                RowTask(
                    row_index=synthetic_row_index,
                    sheet_name=sheet_name,  # keep exact workbook tab name
                )
            )
            task_provenance.append(
                {
                    "row_index": synthetic_row_index,
                    "sheet_name": sheet_name,
                    "provenance": {
                        "source_kind": "workbook_sheet_list",
                        "source_workbook_file": workbook_path,
                        "source_sheet": None,
                        "source_row_0_based": None,
                        "source_row_1_based": None,
                        "source_col_0_based": None,
                        "source_col_1_based": None,
                        "source_col_letter": None,
                        "source_cell_a1": None,
                        "raw_cell_value": sheet_name,
                        "normalized_candidate": normalized,
                        "resolved_sheet_name": sheet_name,
                        "matched_by": "workbook_sheet_list",
                        "source_code": {
                            "file": "Multi_agen/packages/agents/row_walker.py",
                            "line_start": 156,
                            "line_end": 205,
                        },
                    },
                }
            )
            synthetic_row_index += 1

        return tasks

    def _normalize_sheet_name_cell(self, cell) -> str:
        """
        Normalize raw cell content into a candidate sheet-name string.

        Important:
        - This is a normalized candidate, not the exact source cell text.
        - Exact raw workbook text should be preserved separately.
        """
        if cell is None or cell is MissingValueSentinel:
            return ""

        text = str(cell).strip()
        if not text:
            return ""

        lowered = text.lower()
        if lowered in {"nan", "none", "null"}:
            return ""

        return text

    def _make_task_provenance(
        self,
        *,
        source_workbook_file: Optional[str],
        source_sheet: Optional[str],
        source_row_0_based: Optional[int],
        source_col_0_based: Optional[int],
        raw_cell_value: str,
        normalized_candidate: str,
        resolved_sheet_name: Optional[str],
        matched_by: Optional[str],
        source_kind: str,
    ) -> Dict[str, Any]:
        col_letter = None
        cell_a1 = None

        if source_row_0_based is not None and source_col_0_based is not None:
            col_letter = self._column_index_to_letter(source_col_0_based)
            cell_a1 = f"{col_letter}{source_row_0_based + 1}"

        return {
            "row_index": source_row_0_based,
            "sheet_name": resolved_sheet_name,
            "provenance": {
                "source_kind": source_kind,
                "source_workbook_file": source_workbook_file,
                "source_sheet": source_sheet,
                "source_row_0_based": source_row_0_based,
                "source_row_1_based": None if source_row_0_based is None else source_row_0_based + 1,
                "source_col_0_based": source_col_0_based,
                "source_col_1_based": None if source_col_0_based is None else source_col_0_based + 1,
                "source_col_letter": col_letter,
                "source_cell_a1": cell_a1,
                "raw_cell_value": raw_cell_value,
                "normalized_candidate": normalized_candidate,
                "resolved_sheet_name": resolved_sheet_name,
                "matched_by": matched_by,
                "source_code": {
                    "file": "Multi_agen/packages/agents/row_walker.py",
                    "line_start": 106,
                    "line_end": 153,
                },
            },
        }

    def _attach_task_provenance(self, state: PipelineState, task_provenance: List[Dict[str, Any]]) -> None:
        try:
            setattr(state, "task_provenance", task_provenance)
        except Exception:
            logger.exception("RowWalker:failed attaching task_provenance to state")

    def _get_workbook_path(self, state: PipelineState) -> Optional[str]:
        try:
            return getattr(getattr(state, "input", None), "workbook_path", None)
        except Exception:
            return None

    def _column_index_to_letter(self, col_idx: int) -> str:
        if col_idx < 0:
            raise ValueError("col_idx must be >= 0")

        n = col_idx + 1
        letters: List[str] = []

        while n > 0:
            n, rem = divmod(n - 1, 26)
            letters.append(chr(ord("A") + rem))

        return "".join(reversed(letters))

    def _should_skip_sheet_name(self, sheet_name: str) -> bool:
        """
        Skip obvious presentation/control sheets in workbook-sheet fallback mode.
        """
        lower = sheet_name.lower()

        skip_terms = [
            "summary",
            "output",
            "result",
            "results",
            "report",
            "reports",
            "bod",
            "slides",
            "dashboard",
            "actual vs budget",
        ]

        for term in skip_terms:
            if term in lower:
                return True

        return False