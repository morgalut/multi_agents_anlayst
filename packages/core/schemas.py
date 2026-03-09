"""
Single Source of Truth (SSOT) for workbook-structure extraction.

This schema is designed for financial workbook mapping, not summary-sheet writeback.

Goals:
- represent workbook-level analysis
- represent per-sheet financial structure
- represent per-column mapping in the exact role vocabulary required by the spec
- support deterministic final rendering into the EXAMPLE format

All agents MUST import from here and MUST NOT redefine these schemas elsewhere.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Literal


# ----------------------------------------------------------------------
# Constants / enums
# ----------------------------------------------------------------------

MissingValueSentinel: str = "no"

StatementType = Literal["BS", "PL", "BS+PL"]
ColumnRole = Literal[
    "coa_name",
    "entity_value",
    "debit",
    "credit",
    "aje",
    "consolidated_aje",
    "consolidated",
    "budget",
    "prior_period",
    "other",
]
AJEType = Literal["per-entity", "consolidated", "both"]


ALLOWED_COLUMN_ROLES: tuple[str, ...] = (
    "coa_name",
    "entity_value",
    "debit",
    "credit",
    "aje",
    "consolidated_aje",
    "consolidated",
    "budget",
    "prior_period",
    "other",
)


# ----------------------------------------------------------------------
# Workbook / sheet selection
# ----------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class SheetCandidate:
    """
    Candidate worksheet classified at workbook level.

    kind:
      - presentation : likely top-level financial presentation sheet
      - source       : likely SAP / GL / TB / ledger source sheet
      - support      : helper / mapping / control sheet
      - unknown      : not yet clear
    """
    name: str
    kind: str = "unknown"
    confidence: float = 0.0
    evidence: List[str] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class WorkbookEntity:
    name: str
    currency: Optional[str] = None
    confidence: float = 0.0
    evidence: List[str] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class WorkbookStructure:
    """
    Workbook-level structure summary.

    This is the upstream object used to decide:
    - likely presentation sheets
    - likely entities
    - whether consolidation/AJE likely exists
    """
    main_sheet_names: List[str] = field(default_factory=list)
    contains: List[str] = field(default_factory=list)  # BS / PL
    entities: List[WorkbookEntity] = field(default_factory=list)
    has_consolidated: bool = False
    consolidated_formula_pattern: str = ""
    has_aje: bool = False
    aje_types: List[str] = field(default_factory=list)
    likely_units: Optional[str] = None
    likely_current_period: Optional[str] = None
    sheet_candidates: List[SheetCandidate] = field(default_factory=list)
    quality_flags: List[str] = field(default_factory=list)
    confidence: float = 0.0
    raw_llm_text: str = ""


# ----------------------------------------------------------------------
# Extraction tasking
# ----------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class SheetTask:
    """
    Unit of work for the pipeline.

    One task == one worksheet to structurally analyze.
    """
    sheet_name: str
    is_main_sheet: bool = False
    parent_sheet_name: Optional[str] = None


# ----------------------------------------------------------------------
# Column mapping
# ----------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class ColumnMapping:
    """
    Final resolved mapping for one physical worksheet column.

    This matches the required final render fields.
    """
    col_idx: int
    col_letter: str
    role: ColumnRole
    entity: str = ""
    currency: str = ""
    period: str = ""
    header_text: str = ""
    formula_pattern: str = ""
    row_start: Optional[int] = None
    row_end: Optional[int] = None
    sheet_name: str = ""
    confidence: float = 0.0
    evidence: List[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        if self.role not in ALLOWED_COLUMN_ROLES:
            raise ValueError(f"Invalid column role: {self.role}")

        if self.col_idx < 0:
            raise ValueError("col_idx must be >= 0")

        if self.row_start is not None and self.row_start < 1:
            raise ValueError("row_start must be >= 1 when provided")

        if self.row_end is not None and self.row_end < 1:
            raise ValueError("row_end must be >= 1 when provided")

        if self.row_start is not None and self.row_end is not None and self.row_end < self.row_start:
            raise ValueError("row_end must be >= row_start")

        # Enforce the spec rule:
        # entity should only be set for entity-bearing roles.
        if self.role not in {"entity_value", "aje", "consolidated_aje", "consolidated"} and self.entity:
            raise ValueError(f"Entity must be empty for role={self.role}")


@dataclass(frozen=True, slots=True)
class SheetExtractionResult:
    """
    Structural extraction result for one worksheet section.

    One physical sheet may yield:
    - one result with contains="BS+PL"
    - one result with contains="BS"
    - one result with contains="PL"
    """
    sheet_name: str
    contains: StatementType
    unit: str = ""
    data_row_start: Optional[int] = None
    data_row_end: Optional[int] = None
    columns: List[ColumnMapping] = field(default_factory=list)
    quality_flags: List[str] = field(default_factory=list)
    confidence: float = 0.0

    def sorted_columns(self) -> List[ColumnMapping]:
        return sorted(self.columns, key=lambda c: c.col_idx)


# ----------------------------------------------------------------------
# Workbook-level final extraction result
# ----------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class WorkbookExtractionResult:
    """
    Final structured result before rendering to the EXAMPLE text format.
    """
    sheets: List[SheetExtractionResult] = field(default_factory=list)
    entities: List[str] = field(default_factory=list)
    has_consolidated: bool = False
    consolidated_formula_pattern: str = ""
    has_aje: bool = False
    aje_types: List[str] = field(default_factory=list)
    has_nis: bool = False
    quality_flags: List[str] = field(default_factory=list)

    def sorted_sheets(self) -> List[SheetExtractionResult]:
        return list(self.sheets)


# ----------------------------------------------------------------------
# Rendered output container
# ----------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class FinalRenderOutput:
    """
    Final deterministic text payload matching the required EXAMPLE format.
    """
    text: str
    sheets_count: int = 0
    columns_count: int = 0
    entities_count: int = 0


# ----------------------------------------------------------------------
# Legacy compatibility notes
# ----------------------------------------------------------------------
# The following old concepts are intentionally removed from the SSOT:
# - OutputColumns
# - MainSheetSchema (summary-sheet schema)
# - RowTask
# - RoleCandidate
# - RowResolvedOutput
# - RowResult
#
# They belonged to the old summary-sheet writeback workflow and should be
# replaced across agents by:
# - WorkbookStructure
# - SheetTask
# - ColumnMapping
# - SheetExtractionResult
# - WorkbookExtractionResult
# - FinalRenderOutput