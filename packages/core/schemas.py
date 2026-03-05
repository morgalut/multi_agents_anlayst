"""
Single Source of Truth (SSOT) for shared types and rules.

All agents MUST import from here and MUST NOT redefine these schemas elsewhere.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional


# Task 1.1 — Lock the output schema
class OutputColumns(str, Enum):
    FILENAME = "Filename"
    BS = "BS"
    PL = "P&L"
    MAIN_COMPANY_IL = "Main Company IL"
    MAIN_COMPANY_DOLLAR = "Main Company Dollar"
    AJE = "AJE"
    CONSOLIDATED = "Consolidated"
    SUB_COMPANY = "Sub Company"


MissingValueSentinel: str = "no"


# Task 1.2 — Main-sheet schema detection object
@dataclass(frozen=True, slots=True)
class MainSheetSchema:
    """
    Schema detection result for the main Summary sheet.

    - header_row_index: 0-based row index in the sheet grid.
    - columns: mapping from OutputColumns value (string header) -> 0-based column index.
    - optional_columns_present: mapping from optional OutputColumns value -> bool.
    """
    name: str
    header_row_index: int
    columns: Dict[str, int]
    optional_columns_present: Dict[str, bool] = field(default_factory=dict)


# Task 1.3 — Row task + row result objects
@dataclass(frozen=True, slots=True)
class RowTask:
    row_index: int
    sheet_name: str


@dataclass(frozen=True, slots=True)
class RoleCandidate:
    col_idx: int
    confidence: float
    evidence: List[str] = field(default_factory=list)


# RoleMap keys:
#   main_company_dollar, main_company_il (opt), sub_company, aje (opt), consolidated (opt)
RoleKey = str


@dataclass(frozen=True, slots=True)
class RowResolvedOutput:
    """
    Each field is an Excel column letter "A"..."Z" (or beyond) OR MissingValueSentinel ("no").
    Store exactly what the Summary expects to be written.

    IMPORTANT: Use MissingValueSentinel for missing/uncertain values. Never guess.
    """
    filename: str = MissingValueSentinel
    bs: str = MissingValueSentinel
    pl: str = MissingValueSentinel
    main_company_il: str = MissingValueSentinel
    main_company_dollar: str = MissingValueSentinel
    aje: str = MissingValueSentinel
    consolidated: str = MissingValueSentinel
    sub_company: str = MissingValueSentinel


@dataclass(frozen=True, slots=True)
class RowResult:
    """
    End-to-end per-row result.
    - classification: agent-defined object (dict/dataclass) with confidence + evidence.
    - role_map: role -> RoleCandidate (or None if absent)
    - resolved: final output values (letters or MissingValueSentinel)
    - quality_flags: list of strings for auditing (corruption, needs_review, etc.)
    """
    row_index: int
    sheet_name: str
    classification: object
    role_map: Dict[RoleKey, Optional[RoleCandidate]]
    resolved: RowResolvedOutput
    quality_flags: List[str] = field(default_factory=list)


# Canonical RoleMap keys (locked)
ROLE_MAIN_COMPANY_DOLLAR: RoleKey = "main_company_dollar"
ROLE_MAIN_COMPANY_IL: RoleKey = "main_company_il"          # optional
ROLE_SUB_COMPANY: RoleKey = "sub_company"
ROLE_AJE: RoleKey = "aje"                                  # optional
ROLE_CONSOLIDATED: RoleKey = "consolidated"                # optional


OPTIONAL_OUTPUT_COLUMNS: set[str] = {
    OutputColumns.MAIN_COMPANY_IL.value,
    OutputColumns.AJE.value,
    OutputColumns.CONSOLIDATED.value,
}