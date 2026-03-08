from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional
import logging
import unicodedata

from Multi_agen.packages.core import (
    MainSheetSchema,
    MissingValueSentinel,
    OutputColumns,
    PipelineState,
    RowTask,
)

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

    Important improvement:
      - Candidate sheet names taken from cells are resolved against the
        real workbook sheet names before RowTask objects are created.
      - This prevents MCP 400 failures caused by near-matches such as:
            "Sheet A" vs "Sheet A "
            Hebrew punctuation variants
            hidden RTL/LTR Unicode marks
    """

    def __init__(self, config: RowWalkerConfig | None = None):
        self.config = config or RowWalkerConfig()

    def build_tasks(self, state: PipelineState, tools) -> List[RowTask]:
        if state.main_sheet is None:
            raise RuntimeError("MainSheetSchema missing")

        schema: MainSheetSchema = state.main_sheet
        all_sheets = tools.excel_list_sheets()
        filename_col = schema.columns.get(OutputColumns.FILENAME.value)

        # Preferred mode: explicit Filename column on the main sheet
        if filename_col is not None:
            logger.info(
                "RowWalker:using filename column mode main_sheet=%s header_row=%d filename_col=%d",
                schema.name,
                schema.header_row_index,
                filename_col,
            )
            tasks = self._build_from_filename_column(schema, filename_col, all_sheets, tools)
            if tasks:
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
        tasks = self._build_from_workbook_sheets(schema, all_sheets)

        if not tasks:
            raise RuntimeError(
                "Failed to build row tasks: no Filename column in main sheet schema and no usable workbook sheets found"
            )

        logger.info("RowWalker:built tasks from workbook-sheet fallback count=%d", len(tasks))
        return tasks

    def _build_from_filename_column(
        self,
        schema: MainSheetSchema,
        filename_col: int,
        all_sheets: List[str],
        tools,
    ) -> List[RowTask]:
        """
        Build tasks from the explicit Filename column on the main sheet.

        Improvement:
        - resolve each raw cell value to an exact workbook sheet name
          before creating a RowTask.
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
        for i, row in enumerate(grid):
            cell = row[0] if row else None
            raw_name = self._normalize_sheet_name_cell(cell)

            if not raw_name:
                # Common summary layout: stop on first empty row
                break

            resolved_name = self._resolve_sheet_name(raw_name, all_sheets)
            if resolved_name is None:
                logger.warning(
                    "RowWalker:unresolved sheet reference raw=%r row_index=%d available_count=%d",
                    raw_name,
                    row0 + i,
                    len(all_sheets),
                )
                continue

            if resolved_name == schema.name:
                logger.info(
                    "RowWalker:skipping self-reference raw=%r resolved=%r row_index=%d",
                    raw_name,
                    resolved_name,
                    row0 + i,
                )
                continue

            tasks.append(RowTask(row_index=row0 + i, sheet_name=resolved_name))

        return tasks

    def _build_from_workbook_sheets(
        self,
        schema: MainSheetSchema,
        all_sheets: List[str],
    ) -> List[RowTask]:
        """
        Build tasks directly from workbook sheet names when no explicit
        Filename column exists in the detected main-sheet schema.
        """
        main_sheet_name = (schema.name or "").strip()

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
            synthetic_row_index += 1

        return tasks

    def _normalize_sheet_name_cell(self, cell) -> str:
        """
        Normalize raw cell content into a candidate sheet-name string.
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

    def _resolve_sheet_name(self, raw_name: str, actual_sheet_names: List[str]) -> Optional[str]:
        """
        Resolve a candidate name from a cell to an exact workbook sheet name.

        Matching strategy:
        1) exact match
        2) trimmed exact match
        3) canonical Unicode/punctuation match

        Returns:
            exact workbook sheet name, or None if no match found
        """
        raw = raw_name or ""
        trimmed = raw.strip()

        # 1) exact
        if raw in actual_sheet_names:
            return raw

        # 2) trimmed exact
        if trimmed in actual_sheet_names:
            logger.info(
                "RowWalker:resolved sheet by trimmed exact match raw=%r actual=%r",
                raw_name,
                trimmed,
            )
            return trimmed

        if not self.config.allow_sheet_name_resolution:
            return None

        # 3) canonicalized
        target = self._canonicalize_sheet_name(trimmed)
        for actual in actual_sheet_names:
            if self._canonicalize_sheet_name(actual) == target:
                logger.info(
                    "RowWalker:resolved sheet by canonical match raw=%r actual=%r",
                    raw_name,
                    actual,
                )
                return actual

        return None

    def _canonicalize_sheet_name(self, name: str) -> str:
        """
        Normalize Unicode/punctuation differences that often cause
        visually-equal sheet names to fail exact matching.

        Handles:
        - Hebrew gershayim / geresh variants
        - non-breaking spaces
        - hidden RTL/LTR marks
        - repeated internal whitespace
        """
        text = unicodedata.normalize("NFKC", (name or "").strip())

        replacements = {
            "״": '"',      # Hebrew gershayim -> double quote
            "׳": "'",      # Hebrew geresh -> apostrophe
            "\u00A0": " ", # non-breaking space
            "\u200f": "",  # RTL mark
            "\u200e": "",  # LTR mark
            "\u202a": "",
            "\u202b": "",
            "\u202c": "",
            "\u202d": "",
            "\u202e": "",
        }

        for src, dst in replacements.items():
            text = text.replace(src, dst)

        text = " ".join(text.split())
        return text.casefold()

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