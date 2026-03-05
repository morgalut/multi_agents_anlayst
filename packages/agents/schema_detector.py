from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from Multi_agen.packages.core import (
    MainSheetSchema,
    OPTIONAL_OUTPUT_COLUMNS,
    OUTPUT_HEADER_SYNONYMS,
    OutputColumns,
    normalize_text,
)


@dataclass(frozen=True, slots=True)
class SchemaDetectorConfig:
    """
    Detect the main Summary sheet and its header schema.

    Approach:
      1) List sheets
      2) For each sheet, read a small top window
      3) Find a header row that matches OutputColumns (via synonyms + normalization)
      4) Choose the best scoring sheet/row
    """
    scan_rows: int = 30
    scan_cols: int = 30
    min_required_hits: int = 4  # must match at least these headers to accept a sheet
    required_headers: Tuple[str, ...] = (
        OutputColumns.FILENAME.value,
        OutputColumns.BS.value,
        OutputColumns.PL.value,
        OutputColumns.MAIN_COMPANY_DOLLAR.value,
        OutputColumns.SUB_COMPANY.value,
    )


class MainSheetSchemaDetector:
    """
    Detects MainSheetSchema { name, header_row_index, columns, optional_columns_present }.

    Tool contract assumed:
      - excel.list_sheets() -> list[str]
      - excel.read_sheet_range(sheet_name, row0, col0, nrows, ncols) -> grid (list[list[Any]])
    """

    def __init__(self, config: Optional[SchemaDetectorConfig] = None):
        self.config = config or SchemaDetectorConfig()

    def detect(self, state, tools) -> MainSheetSchema:
        sheets: List[str] = tools.excel_list_sheets()

        best: Optional[MainSheetSchema] = None
        best_score: int = -1

        for sheet_name in sheets:
            grid = tools.excel_read_sheet_range(
                sheet_name=sheet_name,
                row0=0,
                col0=0,
                nrows=self.config.scan_rows,
                ncols=self.config.scan_cols,
            )

            schema, score = self._try_sheet(sheet_name, grid)
            if schema is None:
                continue

            if score > best_score:
                best = schema
                best_score = score

        if best is None:
            raise RuntimeError(
                "Failed to detect main Summary sheet schema: no sheet matched required headers."
            )
        return best

    def _try_sheet(self, sheet_name: str, grid: List[List[object]]) -> Tuple[Optional[MainSheetSchema], int]:
        """
        Returns (schema, score). Score is number of matched headers in the chosen header row.
        """
        # Precompute normalized synonyms -> canonical header
        syn_to_canonical: Dict[str, str] = {}
        for canonical, syns in OUTPUT_HEADER_SYNONYMS.items():
            for s in syns:
                syn_to_canonical[normalize_text(s)] = canonical
            # Also allow exact canonical text to match itself
            syn_to_canonical[normalize_text(canonical)] = canonical

        best_row_schema: Optional[MainSheetSchema] = None
        best_row_score: int = -1

        for r, row in enumerate(grid):
            # Build map of canonical header -> col index for this row
            cols: Dict[str, int] = {}

            for c, cell in enumerate(row):
                if cell is None:
                    continue
                txt = normalize_text(str(cell))
                if not txt:
                    continue

                # Direct synonym match
                canonical = syn_to_canonical.get(txt)
                if canonical is not None:
                    cols[canonical] = c
                    continue

                # Soft match: if a synonym is contained inside cell text (merged-ish headers)
                # (Conservative: only accept if one canonical wins unambiguously)
                hits: List[str] = []
                for syn_norm, canon in syn_to_canonical.items():
                    if syn_norm and syn_norm in txt:
                        hits.append(canon)
                hits = list(dict.fromkeys(hits))  # preserve order unique
                if len(hits) == 1:
                    cols[hits[0]] = c

            # Check required headers
            required_hits = sum(1 for h in self.config.required_headers if h in cols)
            if required_hits < len(self.config.required_headers):
                continue

            # Score: total canonical matches, but only for known OutputColumns
            known_headers = {oc.value for oc in OutputColumns}
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