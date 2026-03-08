from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
import logging

from Multi_agen.packages.core import (
    MainSheetSchema,
    OPTIONAL_OUTPUT_COLUMNS,
    OUTPUT_HEADER_SYNONYMS,
    OutputColumns,
    normalize_text,
)

logger = logging.getLogger("multi_agen.agents.schema_detector")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)


@dataclass(frozen=True, slots=True)
class SchemaDetectorConfig:
    """
    Detect the main Summary / presentation sheet and its header schema.

    Strategy:
      1) Prefer workbook_structure.main_sheet_name if available
      2) Inspect sheets for an explicit summary/output schema using known output header synonyms
      3) If no explicit schema is found, fall back to heuristic sheet ranking
      4) Infer header row conservatively from the best candidate sheet
    """
    scan_rows: int = 30
    scan_cols: int = 30
    header_search_rows: int = 20
    max_preview_cols: int = 12
    min_required_hits: int = 4
    required_headers: Tuple[str, ...] = (
        OutputColumns.FILENAME.value,
        OutputColumns.BS.value,
        OutputColumns.PL.value,
        OutputColumns.MAIN_COMPANY_DOLLAR.value,
        OutputColumns.SUB_COMPANY.value,
    )


class MainSheetSchemaDetector:
    """
    Detects MainSheetSchema.

    Supports:
      - workbook_structure hints from upstream ORC stage
      - prompt_profile argument for ORC compatibility
      - rich summary/output header matching
      - heuristic fallback when no explicit summary schema is found
    """

    def __init__(self, config: Optional[SchemaDetectorConfig] = None) -> None:
        self.config = config or SchemaDetectorConfig()

    def detect(
        self,
        state: Any,
        tools: Any,
        prompt_profile: Any = None,
    ) -> MainSheetSchema:
        logger.info("SchemaDetector:start run_id=%s", getattr(state, "run_id", None))

        workbook_structure = getattr(state, "workbook_structure", None)
        preferred_sheet = getattr(workbook_structure, "main_sheet_name", None)

        sheets: List[str] = tools.excel_list_sheets()
        if not sheets:
            raise RuntimeError("Failed to detect main sheet schema: no sheets found.")

        # 1) If workbook_structure suggests a preferred sheet, inspect it first.
        if preferred_sheet and preferred_sheet in sheets:
            logger.info("SchemaDetector:preferred sheet from workbook_structure=%s", preferred_sheet)

            grid = self._safe_read_grid(preferred_sheet, tools, self.config.scan_rows, self.config.scan_cols)
            if grid:
                schema, score = self._try_sheet(preferred_sheet, grid)
                if schema is not None:
                    logger.info(
                        "SchemaDetector:preferred sheet matched explicit schema sheet=%s score=%d",
                        preferred_sheet,
                        score,
                    )
                    return schema

            # If the preferred sheet is not a summary schema, still use it as a strong hint.
            header_row_index = self._infer_header_row(preferred_sheet, tools)
            logger.info(
                "SchemaDetector:preferred sheet used via heuristic fallback sheet=%s header_row=%d",
                preferred_sheet,
                header_row_index,
            )
            return self._build_fallback_schema(
                sheet_name=preferred_sheet,
                header_row_index=header_row_index,
            )

        # 2) Try explicit summary/output schema detection across all sheets.
        best: Optional[MainSheetSchema] = None
        best_score: int = -1

        for sheet_name in sheets:
            grid = self._safe_read_grid(sheet_name, tools, self.config.scan_rows, self.config.scan_cols)
            if not grid:
                continue

            schema, score = self._try_sheet(sheet_name, grid)
            if schema is None:
                continue

            logger.info(
                "SchemaDetector:explicit schema candidate sheet=%s score=%d header_row=%d",
                sheet_name,
                score,
                schema.header_row_index,
            )

            if score > best_score:
                best = schema
                best_score = score

        if best is not None:
            logger.info(
                "SchemaDetector:selected explicit schema sheet=%s score=%d",
                best.name,
                best_score,
            )
            return best

        # 3) Heuristic fallback for non-summary financial workbooks.
        ranked = self._rank_sheet_candidates(sheets)
        main_sheet_name = ranked[0]
        header_row_index = self._infer_header_row(main_sheet_name, tools)

        logger.info(
            "SchemaDetector:heuristic fallback selected sheet=%s header_row=%d",
            main_sheet_name,
            header_row_index,
        )

        return self._build_fallback_schema(
            sheet_name=main_sheet_name,
            header_row_index=header_row_index,
        )

    def _build_fallback_schema(self, sheet_name: str, header_row_index: int) -> MainSheetSchema:
        """
        Build a conservative schema object for heuristic fallback cases where
        we know the likely main sheet and header row, but have not identified
        a formal output-column mapping.
        """
        optional_present = {h: False for h in OPTIONAL_OUTPUT_COLUMNS}

        return MainSheetSchema(
            name=sheet_name,
            header_row_index=header_row_index,
            columns={},
            optional_columns_present=optional_present,
        )

    def _safe_read_grid(
        self,
        sheet_name: str,
        tools: Any,
        nrows: int,
        ncols: int,
    ) -> List[List[Any]]:
        try:
            return tools.excel_read_sheet_range(
                sheet_name=sheet_name,
                row0=0,
                col0=0,
                nrows=nrows,
                ncols=ncols,
            )
        except Exception:
            logger.exception("SchemaDetector:failed reading grid sheet=%s", sheet_name)
            return []

    def _try_sheet(self, sheet_name: str, grid: List[List[Any]]) -> Tuple[Optional[MainSheetSchema], int]:
        """
        Returns (schema, score).

        Score = number of matched known headers on the chosen header row.
        """
        syn_to_canonical: Dict[str, str] = {}
        for canonical, syns in OUTPUT_HEADER_SYNONYMS.items():
            for s in syns:
                syn_to_canonical[normalize_text(s)] = canonical
            syn_to_canonical[normalize_text(canonical)] = canonical

        best_row_schema: Optional[MainSheetSchema] = None
        best_row_score: int = -1

        known_headers = {oc.value for oc in OutputColumns}

        for r, row in enumerate(grid):
            if not isinstance(row, list):
                continue

            cols: Dict[str, int] = {}

            for c, cell in enumerate(row):
                if cell is None:
                    continue

                txt = normalize_text(str(cell))
                if not txt:
                    continue

                canonical = syn_to_canonical.get(txt)
                if canonical is not None:
                    cols[canonical] = c
                    continue

                hits: List[str] = []
                for syn_norm, canon in syn_to_canonical.items():
                    if syn_norm and syn_norm in txt:
                        hits.append(canon)

                hits = list(dict.fromkeys(hits))
                if len(hits) == 1:
                    cols[hits[0]] = c

            required_hits = sum(1 for h in self.config.required_headers if h in cols)
            if required_hits < len(self.config.required_headers):
                continue

            total_hits = sum(1 for h in cols.keys() if h in known_headers)
            if total_hits < self.config.min_required_hits:
                continue

            if total_hits > best_row_score:
                optional_present = {h: (h in cols) for h in OPTIONAL_OUTPUT_COLUMNS}
                best_row_schema = MainSheetSchema(
                    name=sheet_name,
                    header_row_index=r,
                    columns=cols,
                    optional_columns_present=optional_present,
                )
                best_row_score = total_hits

        return best_row_schema, best_row_score

    def _rank_sheet_candidates(self, sheets: List[str]) -> List[str]:
        preferred_terms = [
            "summary",
            "output",
            "result",
            "results",
            "fs",
            "financial",
            "statement",
            "statements",
            "bs",
            "balance",
            "p&l",
            "pnl",
            "income",
            "profit",
            "loss",
            "consolidated",
        ]

        penalty_terms = [
            "sap",
            "gl",
            "ledger",
            "trial",
            "tb",
            "dump",
            "raw",
            "data",
            "hidden",
            "tmp",
            "_",
        ]

        def score(name: str) -> int:
            s = 0
            lower = name.lower()

            for term in preferred_terms:
                if term in lower:
                    s += 10

            for term in penalty_terms:
                if term in lower:
                    s -= 6

            return s

        ranked = sorted(sheets, key=score, reverse=True)
        logger.info("SchemaDetector:ranked heuristic candidates=%s", ranked[:10])
        return ranked

    def _infer_header_row(self, sheet_name: str, tools: Any) -> int:
        grid = self._safe_read_grid(
            sheet_name=sheet_name,
            tools=tools,
            nrows=self.config.header_search_rows,
            ncols=self.config.max_preview_cols,
        )
        if not grid:
            return 0

        keywords = {
            "account",
            "description",
            "balance",
            "assets",
            "liabilities",
            "equity",
            "revenue",
            "expense",
            "profit",
            "loss",
            "nis",
            "usd",
            "consolidated",
            "aje",
            "filename",
            "file name",
            "main company",
            "sub company",
        }

        best_row = 0
        best_score = -1

        for r_idx, row in enumerate(grid):
            if not isinstance(row, list):
                continue

            score = 0
            non_empty = 0

            for cell in row:
                text = "" if cell is None else str(cell).strip().lower()
                if not text:
                    continue

                non_empty += 1

                if text in keywords:
                    score += 3

                for kw in keywords:
                    if kw in text:
                        score += 1

            if 2 <= non_empty <= self.config.max_preview_cols:
                score += 1

            if score > best_score:
                best_score = score
                best_row = r_idx

        return best_row if best_score >= 0 else 0