# Multi_agen\packages\core\schemas.py
from __future__ import annotations

import dataclasses
from dataclasses import dataclass, field
from typing import Any, ClassVar, Dict, FrozenSet, List, Literal, Optional, Tuple, Union, get_args


# ---------------------------------------------------------------------------
# Constants / type aliases
# ---------------------------------------------------------------------------

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

# Derived directly from ColumnRole — single source of truth, no duplication.
ALLOWED_COLUMN_ROLES: Tuple[str, ...] = get_args(ColumnRole)

_ENTITY_BEARING_ROLES: FrozenSet[str] = frozenset(
    {"entity_value", "aje", "consolidated_aje", "consolidated"}
)

# Confidence threshold for "exact" match in comparison block.
EXACT_MATCH_THRESHOLD: float = 0.65

# Quality-flag substrings that indicate structural incompleteness.
BLOCKING_FLAG_PATTERNS: Tuple[str, ...] = (
    "missing_entity_column",
    "consolidated_without_entities",
    "aje_without_entity_columns",
    "no_entity_hits_found",
    "no_columns_mapped",
)

# company field in sheet-export JSON can arrive as str, bool, or None.
# Using a union keeps mypy happy while matching real-world data.
CompanyField = Optional[Union[str, bool]]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _check_confidence(value: float, owner: str) -> None:
    """Shared guard — raises ValueError when confidence is outside [0, 1]."""
    if not (0.0 <= value <= 1.0):
        raise ValueError(f"{owner}: confidence must be in [0, 1], got {value}")


# ---------------------------------------------------------------------------
# Workbook / sheet candidate helpers
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class SheetCandidate:
    """Lightweight candidate record produced during workbook scanning."""

    name: str
    kind: str = "unknown"
    confidence: float = 0.0
    evidence: List[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("SheetCandidate.name must not be empty.")
        _check_confidence(self.confidence, "SheetCandidate")


@dataclass(frozen=True, slots=True)
class WorkbookEntity:
    """Represents a single legal / consolidation entity discovered in the workbook."""

    name: str
    currency: Optional[str] = None
    confidence: float = 0.0
    evidence: List[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("WorkbookEntity.name must not be empty.")
        _check_confidence(self.confidence, "WorkbookEntity")

    @property
    def display(self) -> str:
        """Human-readable label including currency when available."""
        return f"{self.name} ({self.currency})" if self.currency else self.name


# ---------------------------------------------------------------------------
# Workbook-level structure
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class WorkbookStructure:
    """
    Top-level structural fingerprint of a workbook.

    Produced by WorkbookStructureAgent and passed downstream to sheet-level
    agents and OutputRenderer.
    """

    main_sheet_names: List[str] = field(default_factory=list)
    contains: List[str] = field(default_factory=list)
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

    @property
    def entity_names(self) -> List[str]:
        """Plain names of all discovered entities."""
        return [e.name for e in self.entities]

    @property
    def has_blocking_flags(self) -> bool:
        """True when any quality flag matches a structural blocker pattern."""
        return any(
            any(pat in flag for pat in BLOCKING_FLAG_PATTERNS)
            for flag in self.quality_flags
        )


# ---------------------------------------------------------------------------
# Extraction tasking
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class SheetTask:
    """Minimal task descriptor dispatched to a per-sheet extraction agent."""

    sheet_name: str
    is_main_sheet: bool = False
    parent_sheet_name: Optional[str] = None

    def __post_init__(self) -> None:
        if not self.sheet_name:
            raise ValueError("SheetTask.sheet_name must not be empty.")


# ---------------------------------------------------------------------------
# Sheet-level entity / currency extraction
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class SheetEntityHit:
    """A single entity detected in a sheet column header."""

    entity: str
    sheet_name: str
    header_text: str
    col_idx: int
    col_letter: str
    row_idx: Optional[int]
    confidence: float
    evidence: List[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        if self.col_idx < 0:
            raise ValueError(f"col_idx must be >= 0, got {self.col_idx}")
        _check_confidence(self.confidence, "SheetEntityHit")


@dataclass(frozen=True, slots=True)
class SheetCompanyExtraction:
    """Aggregated entity-detection result for a single sheet."""

    entities: List[SheetEntityHit] = field(default_factory=list)
    quality_flags: List[str] = field(default_factory=list)
    confidence: float = 0.0

    @property
    def entity_names(self) -> List[str]:
        return [hit.entity for hit in self.entities]


@dataclass(frozen=True, slots=True)
class SheetCurrencyHit:
    """A single currency detected in a sheet column header."""

    currency: str
    sheet_name: str
    header_text: str
    col_idx: int
    col_letter: str
    row_idx: Optional[int]
    entity: str
    confidence: float
    evidence: List[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        if self.col_idx < 0:
            raise ValueError(f"col_idx must be >= 0, got {self.col_idx}")
        _check_confidence(self.confidence, "SheetCurrencyHit")


@dataclass(frozen=True, slots=True)
class SheetCurrencyExtraction:
    """Aggregated currency-detection result for a single sheet."""

    currencies: List[SheetCurrencyHit] = field(default_factory=list)
    quality_flags: List[str] = field(default_factory=list)
    confidence: float = 0.0

    @property
    def currency_codes(self) -> List[str]:
        """Deduplicated set of currency codes found across all hits."""
        return list({hit.currency for hit in self.currencies})


# ---------------------------------------------------------------------------
# Column mapping
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class ColumnMapping:
    """
    Fully resolved mapping for a single spreadsheet column.

    Invariants enforced in ``__post_init__``:
    - ``role`` must be one of ``ALLOWED_COLUMN_ROLES``
    - ``col_idx`` must be >= 0
    - ``row_start`` / ``row_end`` must be >= 1 when provided
    - ``row_end`` must be >= ``row_start`` when both are set
    - ``entity`` must be empty for non-entity-bearing roles
    """

    ENTITY_BEARING_ROLES: ClassVar[FrozenSet[str]] = _ENTITY_BEARING_ROLES

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

    @property
    def one_based_index(self) -> int:
        """Excel-style 1-based column index."""
        return self.col_idx + 1

    @property
    def is_entity_bearing(self) -> bool:
        """True when this role is expected to carry an entity name."""
        return self.role in _ENTITY_BEARING_ROLES

    @property
    def row_range(self) -> Optional[Tuple[int, int]]:
        """Returns (row_start, row_end) tuple, or None when not fully defined."""
        if self.row_start is not None and self.row_end is not None:
            return (self.row_start, self.row_end)
        return None

    def __post_init__(self) -> None:
        if self.role not in ALLOWED_COLUMN_ROLES:
            raise ValueError(
                f"Invalid column role {self.role!r}. Allowed: {ALLOWED_COLUMN_ROLES}"
            )
        if self.col_idx < 0:
            raise ValueError(f"col_idx must be >= 0, got {self.col_idx}")
        if self.row_start is not None and self.row_start < 1:
            raise ValueError(f"row_start must be >= 1 when provided, got {self.row_start}")
        if self.row_end is not None and self.row_end < 1:
            raise ValueError(f"row_end must be >= 1 when provided, got {self.row_end}")
        if (
            self.row_start is not None
            and self.row_end is not None
            and self.row_end < self.row_start
        ):
            raise ValueError(
                f"row_end ({self.row_end}) must be >= row_start ({self.row_start})"
            )
        if self.role not in _ENTITY_BEARING_ROLES and self.entity:
            raise ValueError(
                f"entity must be empty for role={self.role!r}. "
                f"Entity-bearing roles: {sorted(_ENTITY_BEARING_ROLES)}"
            )
        _check_confidence(self.confidence, "ColumnMapping")


# ---------------------------------------------------------------------------
# Per-sheet extraction result
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class SheetExtractionResult:
    """
    Complete extraction output for one worksheet.

    Produced by individual sheet-extraction agents; aggregated by
    ``ORCAgent._aggregate_workbook_result()`` into a ``WorkbookExtractionResult``.
    """

    sheet_name: str
    contains: StatementType = "BS+PL"
    unit: str = ""
    data_row_start: Optional[int] = None
    data_row_end: Optional[int] = None
    columns: List[ColumnMapping] = field(default_factory=list)
    quality_flags: List[str] = field(default_factory=list)
    confidence: float = 0.0

    def __post_init__(self) -> None:
        if not self.sheet_name:
            raise ValueError("SheetExtractionResult.sheet_name must not be empty.")

    def sorted_columns(self) -> List[ColumnMapping]:
        """Returns columns ordered by their spreadsheet position (col_idx)."""
        return sorted(self.columns, key=lambda c: c.col_idx)

    @property
    def has_blocking_flags(self) -> bool:
        """True when any quality flag matches a structural blocker pattern."""
        return any(
            any(pat in flag for pat in BLOCKING_FLAG_PATTERNS)
            for flag in self.quality_flags
        )

    @property
    def column_roles(self) -> List[str]:
        """Flat list of role values for quick membership checks."""
        return [c.role for c in self.columns]


# ---------------------------------------------------------------------------
# Entity–currency pair
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class EntityCurrencyPair:
    """Resolved (entity, currency) pairing anchored to a specific column."""

    entity: str
    currency: str
    sheet_name: str
    header: str
    column: str
    confidence: float
    source: str = ""

    def __post_init__(self) -> None:
        if not self.entity:
            raise ValueError("EntityCurrencyPair.entity must not be empty.")
        _check_confidence(self.confidence, "EntityCurrencyPair")


# ---------------------------------------------------------------------------
# Comparison / reconciliation dataclasses
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class ExpectedColumn:
    """
    Declarative specification of a column that *should* exist.

    Built from ``WorkbookStructure.entities`` and related flags by
    ``OutputRenderer._build_expected_columns()``.
    """

    role: str
    entity: str = ""
    currency: str = ""
    source: str = ""  # e.g. "workbook_structure.entities[LTD]"

    def __post_init__(self) -> None:
        if self.role not in ALLOWED_COLUMN_ROLES:
            raise ValueError(
                f"Invalid role {self.role!r} for ExpectedColumn. "
                f"Allowed: {ALLOWED_COLUMN_ROLES}"
            )

    @property
    def label(self) -> str:
        """Short human-readable label for logging and reports."""
        parts = [self.role]
        if self.entity:
            parts.append(self.entity)
        if self.currency:
            parts.append(self.currency)
        return "/".join(parts)


@dataclass(frozen=True, slots=True)
class ColumnComparisonHit:
    """Single expected → actual reconciliation record."""

    expected: ExpectedColumn
    match_type: Literal["exact", "partial", "missing"]
    actual_column: Optional[Dict[str, Any]] = None
    notes: str = ""

    @property
    def is_matched(self) -> bool:
        return self.match_type in ("exact", "partial")


@dataclass(frozen=True, slots=True)
class ComparisonBlock:
    """
    Full reconciliation: expected vs actual columns.

    Built by ``OutputRenderer._build_comparison()`` and embedded in
    ``FinalRenderOutput``.

    Counts (``exact_count``, ``partial_count``, ``missing_count``,
    ``expected_count``) are **computed properties** derived from the
    lists — there is no way for them to fall out of sync.
    """

    exact: List[ColumnComparisonHit] = field(default_factory=list)
    partial: List[ColumnComparisonHit] = field(default_factory=list)
    missing: List[ColumnComparisonHit] = field(default_factory=list)

    # ------------------------------------------------------------------
    # Computed counts — always consistent with the lists above
    # ------------------------------------------------------------------

    @property
    def exact_count(self) -> int:
        return len(self.exact)

    @property
    def partial_count(self) -> int:
        return len(self.partial)

    @property
    def missing_count(self) -> int:
        return len(self.missing)

    @property
    def expected_count(self) -> int:
        return self.exact_count + self.partial_count + self.missing_count

    # ------------------------------------------------------------------
    # Derived metrics
    # ------------------------------------------------------------------

    @property
    def completeness_pct(self) -> float:
        """Percentage of expected columns that were found (exact + partial)."""
        if self.expected_count == 0:
            return 0.0
        return round(
            (self.exact_count + self.partial_count) / self.expected_count * 100, 1
        )

    @property
    def is_complete(self) -> bool:
        """True when every expected column was matched exactly."""
        return (
            self.expected_count > 0
            and self.missing_count == 0
            and self.partial_count == 0
        )

    def summary_line(self) -> str:
        """Single-line status string suitable for logging."""
        return (
            f"{self.exact_count} exact / {self.partial_count} partial / "
            f"{self.missing_count} missing  ({self.completeness_pct}% complete)"
        )


@dataclass(frozen=True, slots=True)
class ExtractionSummary:
    """
    High-level outcome record for the full workbook extraction run.

    Embedded in ``FinalRenderOutput.summary``; also used for downstream
    quality-gating logic.

    ``has_nis`` — True when New Israeli Shekel (NIS / ILS) columns were found.
    """

    status: Literal["complete", "partial", "minimal", "failed"]
    main_sheet: str
    sheets_processed: int
    expected_key_columns: int
    actual_key_columns: int
    exact_matches: int
    partial_matches: int
    missing_columns: int
    entities_found: int
    has_consolidated: bool
    has_aje: bool
    has_nis: bool
    quality_flag_count: int
    blocking_flags: List[str] = field(default_factory=list)

    @property
    def is_usable(self) -> bool:
        """False when status is 'failed' or any blocking flag is present."""
        return self.status != "failed" and len(self.blocking_flags) == 0

    @property
    def coverage_pct(self) -> float:
        """Fraction of expected key columns that were actually found."""
        if self.expected_key_columns == 0:
            return 0.0
        return round(self.actual_key_columns / self.expected_key_columns * 100, 1)


# ---------------------------------------------------------------------------
# Workbook-level aggregated extraction result
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class WorkbookExtractionResult:
    """
    Final aggregated result across all processed sheets.

    Produced by ``ORCAgent._aggregate_workbook_result()``.
    Consumed exclusively by ``OutputRenderer.render()``.
    """

    sheets: List[SheetExtractionResult] = field(default_factory=list)
    entities: List[str] = field(default_factory=list)
    has_consolidated: bool = False
    consolidated_formula_pattern: str = ""
    has_aje: bool = False
    aje_types: List[str] = field(default_factory=list)
    has_nis: bool = False
    quality_flags: List[str] = field(default_factory=list)

    # Enriched fields from focused agents
    main_sheet_name: str = ""
    entity_currency_pairs: List[EntityCurrencyPair] = field(default_factory=list)
    companies_table: List[Dict[str, Any]] = field(default_factory=list)
    currencies_table: List[Dict[str, Any]] = field(default_factory=list)

    # Forwarded from WorkbookStructureAgent for renderer comparison logic
    workbook_structure_entities: List[WorkbookEntity] = field(default_factory=list)

    def sorted_sheets(self) -> List[SheetExtractionResult]:
        """Returns sheets in stable insertion order.

        Named ``sorted_sheets`` for API compatibility with OutputRenderer,
        which calls this in 5 places.  Order matches original insertion order
        because WorkbookExtractionResult.sheets is built sequentially by
        ORCAgent and there is no meaningful sort key.
        """
        return list(self.sheets)

    @property
    def main_sheet(self) -> Optional[SheetExtractionResult]:
        """Returns the SheetExtractionResult whose name matches main_sheet_name."""
        for sheet in self.sheets:
            if sheet.sheet_name == self.main_sheet_name:
                return sheet
        return None

    @property
    def all_columns(self) -> List[ColumnMapping]:
        """Flat list of every ColumnMapping across all sheets."""
        return [col for sheet in self.sheets for col in sheet.columns]

    @property
    def sheets_by_name(self) -> Dict[str, SheetExtractionResult]:
        """Index of sheets keyed by sheet_name for O(1) lookup."""
        return {s.sheet_name: s for s in self.sheets}

    @property
    def has_blocking_flags(self) -> bool:
        return any(
            any(pat in flag for pat in BLOCKING_FLAG_PATTERNS)
            for flag in self.quality_flags
        )


# ---------------------------------------------------------------------------
# Final render output
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class FinalRenderOutput:
    """
    Terminal output record returned by ``OutputRenderer.render()``.

    Contains both human-readable text and structured tables ready for
    downstream consumption (API responses, UI rendering, export).
    """

    text: str = ""
    sheets_count: int = 0
    columns_count: int = 0
    entities_count: int = 0

    key_columns_table: List[Dict[str, Any]] = field(default_factory=list)
    all_columns_table: List[Dict[str, Any]] = field(default_factory=list)

    companies_table: List[Dict[str, Any]] = field(default_factory=list)
    currencies_table: List[Dict[str, Any]] = field(default_factory=list)

    summary: Optional[ExtractionSummary] = None
    comparison: Optional[ComparisonBlock] = None
    normalized_output: List[Dict[str, Any]] = field(default_factory=list)

    @property
    def is_empty(self) -> bool:
        """True when no columns or entities were extracted."""
        return self.columns_count == 0 and self.entities_count == 0

    @property
    def status(self) -> str:
        """Delegates to ExtractionSummary.status when available."""
        return self.summary.status if self.summary else "unknown"


# ---------------------------------------------------------------------------
# Sheet-profile export schema
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class SheetProfileColumn:
    """
    Per-column profile in the sheet-export format.

    Mirrors the shape of ``exmplete.json`` column objects exactly.
    ``column_type`` is a human-readable label (e.g. ``"Debit $"``,
    ``"Account name"``).

    Note: ``company`` is typed as ``Optional[Union[str, bool]]`` because
    real-world workbook data sometimes delivers ``false`` (bool) rather
    than ``null`` for columns that have no associated entity.
    """

    column_letter: str
    column_type: str
    company: CompanyField = None
    currency: Optional[str] = None
    is_tb: Optional[bool] = None
    is_aje_debit: Optional[bool] = None
    is_aje_credit: Optional[bool] = None
    is_consolidated: Optional[bool] = None
    is_final: Optional[bool] = None
    is_account_number: Optional[bool] = None
    is_account_description: Optional[bool] = None

    def __post_init__(self) -> None:
        if not self.column_letter:
            raise ValueError("SheetProfileColumn.column_letter must not be empty.")
        if not self.column_type:
            raise ValueError("SheetProfileColumn.column_type must not be empty.")

    def to_dict(self) -> Dict[str, Any]:
        """Serialises to the canonical JSON column-object shape."""
        return dataclasses.asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SheetProfileColumn":
        """Deserialises from a JSON column-object (e.g. one entry in exmplete.json)."""
        return cls(
            column_letter=data["column_letter"],
            column_type=data["column_type"],
            company=data.get("company"),
            currency=data.get("currency"),
            is_tb=data.get("is_tb"),
            is_aje_debit=data.get("is_aje_debit"),
            is_aje_credit=data.get("is_aje_credit"),
            is_consolidated=data.get("is_consolidated"),
            is_final=data.get("is_final"),
            is_account_number=data.get("is_account_number"),
            is_account_description=data.get("is_account_description"),
        )


@dataclass(frozen=True, slots=True)
class SheetProfileResult:
    """
    Full per-sheet profile in the sheet-export format.

    Keyed by ``sheet_name`` in ``WorkbookSheetProfilesResult``.
    """

    sheet_name: str
    is_main_sheet: bool = False
    is_card_sheet: bool = False
    is_aje_card_sheet: bool = False
    is_gl_sheet: bool = False
    confidence: float = 0.0
    columns: List[SheetProfileColumn] = field(default_factory=list)
    additional_info_on_sheet: str = ""

    def __post_init__(self) -> None:
        if not self.sheet_name:
            raise ValueError("SheetProfileResult.sheet_name must not be empty.")
        _check_confidence(self.confidence, "SheetProfileResult")

    @property
    def sheet_kind(self) -> str:
        """
        Returns the most specific kind label for this sheet.

        Priority: main > aje_card > card > gl > unknown.
        A sheet flagged as main takes precedence over all sub-types.
        """
        if self.is_main_sheet:
            return "main"
        if self.is_aje_card_sheet:
            return "aje_card"
        if self.is_card_sheet:
            return "card"
        if self.is_gl_sheet:
            return "gl"
        return "unknown"

    def to_dict(self) -> Dict[str, Any]:
        """Serialises to the shape expected by WorkbookSheetProfilesResult.to_dict()."""
        return {
            "is_main_sheet":            self.is_main_sheet,
            "is_card_sheet":            self.is_card_sheet,
            "is_aje_card_sheet":        self.is_aje_card_sheet,
            "is_gl_sheet":              self.is_gl_sheet,
            "confidence":               self.confidence,
            "columns":                  [col.to_dict() for col in self.columns],
            "additional_info_on_sheet": self.additional_info_on_sheet,
        }

    @classmethod
    def from_dict(cls, sheet_name: str, data: Dict[str, Any]) -> "SheetProfileResult":
        """Deserialises from a single sheet entry in the workbook-profiles JSON."""
        return cls(
            sheet_name=sheet_name,
            is_main_sheet=data.get("is_main_sheet", False),
            is_card_sheet=data.get("is_card_sheet", False),
            is_aje_card_sheet=data.get("is_aje_card_sheet", False),
            is_gl_sheet=data.get("is_gl_sheet", False),
            confidence=data.get("confidence", 0.0),
            columns=[
                SheetProfileColumn.from_dict(c) for c in data.get("columns", [])
            ],
            additional_info_on_sheet=data.get("additional_info_on_sheet", ""),
        )


@dataclass(frozen=True, slots=True)
class WorkbookSheetProfilesResult:
    """
    Workbook-wide collection of per-sheet profiles.

    Serialises to the shape::

        {
            "AE": { <SheetProfileResult fields> },
            "FS": { ... },
            ...
        }
    """

    profiles: Dict[str, SheetProfileResult] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Returns the full workbook profile as a plain JSON-serialisable dict."""
        return {name: profile.to_dict() for name, profile in self.profiles.items()}

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "WorkbookSheetProfilesResult":
        """Deserialises from the raw workbook-profiles JSON (e.g. exmplete.json)."""
        return cls(
            profiles={
                sheet_name: SheetProfileResult.from_dict(sheet_name, sheet_data)
                for sheet_name, sheet_data in data.items()
            }
        )

    @property
    def main_sheet_name(self) -> Optional[str]:
        """Returns the name of the sheet flagged as ``is_main_sheet``, if any."""
        for name, profile in self.profiles.items():
            if profile.is_main_sheet:
                return name
        return None

    @property
    def sheet_names(self) -> List[str]:
        """Ordered list of all sheet names in this result."""
        return list(self.profiles.keys())

    def get(self, sheet_name: str) -> Optional[SheetProfileResult]:
        """Safe lookup — returns None when the sheet is not present."""
        return self.profiles.get(sheet_name)


# ---------------------------------------------------------------------------
# Public surface
# ---------------------------------------------------------------------------

__all__ = [
    # Constants
    "MissingValueSentinel",
    "StatementType",
    "ColumnRole",
    "AJEType",
    "ALLOWED_COLUMN_ROLES",
    "EXACT_MATCH_THRESHOLD",
    "BLOCKING_FLAG_PATTERNS",
    "CompanyField",
    # Dataclasses
    "SheetCandidate",
    "WorkbookEntity",
    "WorkbookStructure",
    "SheetTask",
    "SheetEntityHit",
    "SheetCompanyExtraction",
    "SheetCurrencyHit",
    "SheetCurrencyExtraction",
    "ColumnMapping",
    "SheetExtractionResult",
    "EntityCurrencyPair",
    "ExpectedColumn",
    "ColumnComparisonHit",
    "ComparisonBlock",
    "ExtractionSummary",
    "WorkbookExtractionResult",
    "FinalRenderOutput",
    "SheetProfileColumn",
    "SheetProfileResult",
    "WorkbookSheetProfilesResult",
]