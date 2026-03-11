# Multi_agen\packages\agents\output_renderer.py
from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional

from Multi_agen.packages.core.schemas import (
    BLOCKING_FLAG_PATTERNS,
    EXACT_MATCH_THRESHOLD,
    ColumnComparisonHit,
    ColumnMapping,
    ComparisonBlock,
    ExpectedColumn,
    ExtractionSummary,
    FinalRenderOutput,
    SheetExtractionResult,
    WorkbookExtractionResult,
    WorkbookEntity,
)
from Multi_agen.packages.core.search_config import ColumnSearchConfig


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class OutputRendererConfig:
    line_break: str = "\n"
    indent: int = 2
    ensure_ascii: bool = False


# ---------------------------------------------------------------------------
# Renderer
# ---------------------------------------------------------------------------

class OutputRenderer:

    def __init__(
        self,
        config: Optional[OutputRendererConfig] = None,
        search_config: Optional[ColumnSearchConfig] = None,
    ) -> None:
        self.config = config or OutputRendererConfig()
        self.search_config = search_config or ColumnSearchConfig()

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def render(self, workbook_result: WorkbookExtractionResult) -> FinalRenderOutput:
        sorted_sheets = workbook_result.sorted_sheets()
        total_columns = sum(len(s.columns) for s in sorted_sheets)

        # ── Build comparison block ─────────────────────────────────────
        expected_cols = self._derive_expected_columns(workbook_result)
        comparison = self._build_comparison(expected_cols, workbook_result)

        # ── Build summary ──────────────────────────────────────────────
        summary = self._build_summary(workbook_result, comparison)

        # ── Build structured tables ────────────────────────────────────
        key_columns_table, all_columns_table = self._build_tables(workbook_result)
        companies_table = self._build_company_table(workbook_result)
        currencies_table = self._build_currency_table(workbook_result)
        normalized_output = self._build_normalized_output(
            workbook_result, comparison, companies_table, currencies_table
        )

        # ── Build human-readable text ──────────────────────────────────
        lines: List[str] = []
        lines.extend(self._render_summary_block(summary))
        lines.append("")
        lines.extend(self._render_comparison_block(comparison))
        lines.append("")
        for idx, sheet in enumerate(sorted_sheets):
            if idx > 0:
                lines.append("")
            lines.extend(self._render_sheet(sheet))
        if lines:
            lines.append("")
        lines.extend(self._render_workbook_footer(workbook_result))
        if companies_table:
            lines.append("")
            lines.extend(self._render_companies_section(companies_table))
        if currencies_table:
            lines.append("")
            lines.extend(self._render_currencies_section(currencies_table))

        text = self.config.line_break.join(lines).rstrip()

        return FinalRenderOutput(
            text=text,
            sheets_count=len(sorted_sheets),
            columns_count=total_columns,
            entities_count=len(workbook_result.entities),
            key_columns_table=key_columns_table,
            all_columns_table=all_columns_table,
            companies_table=companies_table,
            currencies_table=currencies_table,
            summary=summary,
            comparison=comparison,
            normalized_output=normalized_output,
        )

    # ------------------------------------------------------------------
    # Expected columns derivation
    # ------------------------------------------------------------------

    def _derive_expected_columns(
        self, workbook_result: WorkbookExtractionResult
    ) -> List[ExpectedColumn]:
        expected: List[ExpectedColumn] = []

        expected.append(ExpectedColumn(
            role="coa_name",
            entity="",
            currency="",
            source="always_expected",
        ))

        ws_entities: List[WorkbookEntity] = (
            workbook_result.workbook_structure_entities or []
        )
        if not ws_entities:
            for ent_name in workbook_result.entities:
                ws_entities.append(WorkbookEntity(name=str(ent_name).strip()))

        for ent in ws_entities:
            name = str(getattr(ent, "name", "") or "").strip()
            currency = str(getattr(ent, "currency", "") or "").strip()
            if not name:
                continue
            expected.append(ExpectedColumn(
                role="entity_value",
                entity=name,
                currency=currency,
                source=f"workbook_structure.entities[{name}]",
            ))

        if workbook_result.has_aje:
            expected.append(ExpectedColumn(
                role="aje",
                entity="",
                currency="",
                source="workbook_structure.has_aje",
            ))

        if workbook_result.has_consolidated:
            expected.append(ExpectedColumn(
                role="consolidated",
                entity="",
                currency="",
                source="workbook_structure.has_consolidated",
            ))

        return expected

    # ------------------------------------------------------------------
    # Comparison block builder
    # ------------------------------------------------------------------

    def _build_comparison(
        self,
        expected_cols: List[ExpectedColumn],
        workbook_result: WorkbookExtractionResult,
    ) -> ComparisonBlock:
        # Flatten all actual columns
        all_actual: List[ColumnMapping] = []
        for sheet in workbook_result.sorted_sheets():
            all_actual.extend(sheet.sorted_columns())

        exact_hits: List[ColumnComparisonHit] = []
        partial_hits: List[ColumnComparisonHit] = []
        missing_hits: List[ColumnComparisonHit] = []

        for exp in expected_cols:
            role_matches = [c for c in all_actual if c.role == exp.role]

            if not role_matches:
                missing_hits.append(ColumnComparisonHit(
                    expected=exp,
                    match_type="missing",
                    actual_column=None,
                    notes=f"No column with role='{exp.role}' found in any sheet",
                ))
                continue

            if exp.entity:
                entity_matches = [
                    c for c in role_matches
                    if c.entity.upper() == exp.entity.upper()
                ]
            else:
                entity_matches = role_matches

            if entity_matches:
                best = max(entity_matches, key=lambda c: c.confidence)
                actual_dict = self._col_to_dict(best)

                if best.confidence >= EXACT_MATCH_THRESHOLD:
                    notes = ""
                    if exp.currency and best.currency.upper() != exp.currency.upper():
                        notes = (
                            f"currency mismatch: expected={exp.currency} "
                            f"actual={best.currency}"
                        )
                    exact_hits.append(ColumnComparisonHit(
                        expected=exp,
                        match_type="exact",
                        actual_column=actual_dict,
                        notes=notes,
                    ))
                else:
                    partial_hits.append(ColumnComparisonHit(
                        expected=exp,
                        match_type="partial",
                        actual_column=actual_dict,
                        notes=f"confidence too low: {best.confidence:.2f} < {EXACT_MATCH_THRESHOLD}",
                    ))
            else:
                best = max(role_matches, key=lambda c: c.confidence)
                actual_dict = self._col_to_dict(best)
                partial_hits.append(ColumnComparisonHit(
                    expected=exp,
                    match_type="partial",
                    actual_column=actual_dict,
                    notes=(
                        f"entity mismatch: expected='{exp.entity}' "
                        f"actual='{best.entity}'"
                    ),
                ))

        # FIX: ComparisonBlock counts are computed properties derived from
        # the lists — do NOT pass expected_count/exact_count/etc. as kwargs.
        return ComparisonBlock(
            exact=exact_hits,
            partial=partial_hits,
            missing=missing_hits,
        )

    # ------------------------------------------------------------------
    # Summary builder
    # ------------------------------------------------------------------

    def _build_summary(
        self,
        workbook_result: WorkbookExtractionResult,
        comparison: ComparisonBlock,
    ) -> ExtractionSummary:
        all_flags = workbook_result.quality_flags or []
        blocking = [
            f for f in all_flags
            if any(pat in f for pat in BLOCKING_FLAG_PATTERNS)
        ]

        key_roles = set(self.search_config.key_roles)
        actual_key_count = sum(
            1
            for sheet in workbook_result.sorted_sheets()
            for col in sheet.columns
            if col.role in key_roles
        )

        # comparison.exact_count / partial_count / missing_count / expected_count
        # are all computed properties — safe to read here.
        matched = comparison.exact_count + comparison.partial_count
        total_exp = comparison.expected_count

        if total_exp == 0:
            status = "failed"
        elif comparison.exact_count == total_exp:
            status = "complete"
        elif matched >= total_exp / 2:
            status = "partial"
        elif matched > 0:
            status = "minimal"
        else:
            status = "failed"

        return ExtractionSummary(
            status=status,
            main_sheet=workbook_result.main_sheet_name or "",
            sheets_processed=len(workbook_result.sheets),
            expected_key_columns=total_exp,
            actual_key_columns=actual_key_count,
            exact_matches=comparison.exact_count,
            partial_matches=comparison.partial_count,
            missing_columns=comparison.missing_count,
            entities_found=len(workbook_result.entities),
            has_consolidated=workbook_result.has_consolidated,
            has_aje=workbook_result.has_aje,
            has_nis=workbook_result.has_nis,
            quality_flag_count=len(all_flags),
            blocking_flags=blocking,
        )

    # ------------------------------------------------------------------
    # Normalized output builder
    # ------------------------------------------------------------------

    def _build_normalized_output(
        self,
        workbook_result: WorkbookExtractionResult,
        comparison: ComparisonBlock,
        companies_table: List[Dict[str, Any]],
        currencies_table: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        entity_currency: Dict[str, str] = {}
        for row in currencies_table:
            ent = str(row.get("entity", "") or "").strip()
            cur = str(row.get("currency", "") or "").strip().upper()
            if ent and cur:
                entity_currency[ent.upper()] = cur

        rows: List[Dict[str, Any]] = []

        all_hits: List[ColumnComparisonHit] = (
            comparison.exact + comparison.partial + comparison.missing
        )

        for hit in all_hits:
            exp = hit.expected
            resolved_currency = (
                exp.currency
                or entity_currency.get(exp.entity.upper(), "")
            )

            row: Dict[str, Any] = {
                "expected_role":     exp.role,
                "expected_entity":   exp.entity,
                "expected_currency": resolved_currency,
                "match_type":        hit.match_type,
                "notes":             hit.notes,
                "source":            exp.source,
            }

            if hit.actual_column:
                ac = hit.actual_column
                row["column"]         = ac.get("column", "")
                row["sheet"]          = ac.get("sheet", "")
                row["actual_role"]    = ac.get("role", "")
                row["actual_entity"]  = ac.get("entity", "")
                row["actual_currency"]= ac.get("currency", "")
                row["header"]         = ac.get("header", "")
                row["confidence"]     = ac.get("confidence", 0.0)
                row["row_start"]      = ac.get("row_start")
                row["row_end"]        = ac.get("row_end")
            else:
                row["column"]         = ""
                row["sheet"]          = ""
                row["actual_role"]    = ""
                row["actual_entity"]  = ""
                row["actual_currency"]= ""
                row["header"]         = ""
                row["confidence"]     = 0.0
                row["row_start"]      = None
                row["row_end"]        = None

            rows.append(row)

        order = {"exact": 0, "partial": 1, "missing": 2}
        rows.sort(key=lambda r: (order.get(r["match_type"], 3), r["expected_role"]))
        return rows

    # ------------------------------------------------------------------
    # Text rendering — summary block
    # ------------------------------------------------------------------

    def _render_summary_block(self, summary: ExtractionSummary) -> List[str]:
        lines: List[str] = []
        status_marker = {
            "complete": "✓ COMPLETE",
            "partial":  "~ PARTIAL",
            "minimal":  "! MINIMAL",
            "failed":   "✗ FAILED",
        }.get(summary.status, summary.status.upper())

        lines.append(f"EXTRACTION STATUS: {status_marker}")
        lines.append(f"  Main sheet     : {summary.main_sheet or '(unknown)'}")
        lines.append(f"  Sheets         : {summary.sheets_processed}")
        lines.append(
            f"  Key columns    : {summary.actual_key_columns} found"
            f" / {summary.expected_key_columns} expected"
        )
        lines.append(
            f"  Matches        : {summary.exact_matches} exact"
            f" | {summary.partial_matches} partial"
            f" | {summary.missing_columns} missing"
        )
        lines.append(
            f"  Entities found : {summary.entities_found}"
            f"  |  Consolidated: {'yes' if summary.has_consolidated else 'no'}"
            f"  |  AJE: {'yes' if summary.has_aje else 'no'}"
            f"  |  NIS: {'yes' if summary.has_nis else 'no'}"
        )
        if summary.blocking_flags:
            lines.append(
                f"  Blocking flags : {', '.join(summary.blocking_flags)}"
            )
        return lines

    # ------------------------------------------------------------------
    # Text rendering — comparison block
    # ------------------------------------------------------------------

    def _render_comparison_block(self, comparison: ComparisonBlock) -> List[str]:
        lines: List[str] = [
            f"COMPARISON: {comparison.expected_count} expected"
            f" | {comparison.exact_count} exact"
            f" | {comparison.partial_count} partial"
            f" | {comparison.missing_count} missing"
            f" | completeness {comparison.completeness_pct}%"
        ]

        if comparison.exact:
            lines.append("  EXACT:")
            for hit in comparison.exact:
                exp = hit.expected
                ac = hit.actual_column or {}
                suffix = f" [{hit.notes}]" if hit.notes else ""
                lines.append(
                    f"    ✓ {exp.role:20s} entity={exp.entity or '-':10s}"
                    f"  → col {ac.get('column', '?')}"
                    f"  conf={ac.get('confidence', 0):.2f}{suffix}"
                )

        if comparison.partial:
            lines.append("  PARTIAL:")
            for hit in comparison.partial:
                exp = hit.expected
                ac = hit.actual_column or {}
                lines.append(
                    f"    ~ {exp.role:20s} entity={exp.entity or '-':10s}"
                    f"  → col {ac.get('column', '?')}"
                    f"  [{hit.notes}]"
                )

        if comparison.missing:
            lines.append("  MISSING:")
            for hit in comparison.missing:
                exp = hit.expected
                lines.append(
                    f"    ✗ {exp.role:20s} entity={exp.entity or '-':10s}"
                    f"  — not found"
                )

        return lines

    # ------------------------------------------------------------------
    # Text rendering — sheets
    # ------------------------------------------------------------------

    def _render_sheet(self, sheet: SheetExtractionResult) -> List[str]:
        lines: List[str] = []
        unit = sheet.unit or "units"
        row_start = self._fmt_int(sheet.data_row_start)
        row_end = self._fmt_int(sheet.data_row_end)
        lines.append(
            f"SHEET: {sheet.sheet_name}"
            f" | Contains: {sheet.contains}"
            f" | Unit: {unit}"
            f" | Data rows: {row_start}-{row_end}"
        )
        lines.append("")
        for col in sheet.sorted_columns():
            lines.append(self._render_column(col))
        return lines

    def _render_column(self, col: ColumnMapping) -> str:
        return (
            f"COLUMN: {col.col_letter}({col.one_based_index})"
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

    # ------------------------------------------------------------------
    # Text rendering — footer
    # ------------------------------------------------------------------

    def _render_workbook_footer(self, workbook_result: WorkbookExtractionResult) -> List[str]:
        entities = [str(x).strip() for x in workbook_result.entities if str(x).strip()]
        main_sheet = str(workbook_result.main_sheet_name or "").strip()

        return [
            f"MAIN_SHEET: {main_sheet or '(unknown)'}",
            (
                f"ENTITIES: {len(entities)} entities: {', '.join(entities)}"
                if entities else "ENTITIES: 0 entities:"
            ),
            (
                f"CONSOLIDATED: {'yes' if workbook_result.has_consolidated else 'no'}"
                f" - formula pattern: {workbook_result.consolidated_formula_pattern}"
            ),
            (
                f"AJE: {'yes' if workbook_result.has_aje else 'no'}"
                f" - types found: {', '.join(workbook_result.aje_types)}"
            ),
            f"NIS: {'yes' if workbook_result.has_nis else 'no'}",
        ]

    # ------------------------------------------------------------------
    # Text rendering — companies / currencies
    # ------------------------------------------------------------------

    def _render_companies_section(self, rows: List[Dict[str, Any]]) -> List[str]:
        lines = ["COMPANIES:"]
        for r in rows:
            lines.append(
                f"  ENTITY: {self._s(r.get('entity'))}"
                f" | Sheet: {self._s(r.get('sheet'))}"
                f" | Column: {self._s(r.get('column'))}"
                f" | Header: {self._s(r.get('header'))}"
                f" | Source: {self._s(r.get('source'))}"
                f" | Confidence: {r.get('confidence', 0.0):.3f}"
            )
        return lines

    def _render_currencies_section(self, rows: List[Dict[str, Any]]) -> List[str]:
        lines = ["CURRENCIES:"]
        for r in rows:
            lines.append(
                f"  ENTITY: {self._s(r.get('entity'))}"
                f" | Currency: {self._s(r.get('currency'))}"
                f" | Sheet: {self._s(r.get('sheet'))}"
                f" | Column: {self._s(r.get('column'))}"
                f" | Source: {self._s(r.get('source'))}"
                f" | Confidence: {r.get('confidence', 0.0):.3f}"
            )
        return lines

    # ------------------------------------------------------------------
    # Structured table builders — columns
    # ------------------------------------------------------------------

    def _build_tables(
        self, workbook_result: WorkbookExtractionResult
    ) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        key_rows: List[Dict[str, Any]] = []
        all_rows: List[Dict[str, Any]] = []
        key_roles = set(self.search_config.key_roles)

        for sheet in workbook_result.sorted_sheets():
            for col in sheet.sorted_columns():
                all_rows.append(self._to_all_row(col))
                if col.role in key_roles:
                    key_rows.append(self._to_key_row(col))

        if self.search_config.sort_by_index:
            all_rows.sort(key=lambda r: (r["sheet"], r["index"]))
            key_rows.sort(key=lambda r: (r["sheet"], self._letter_sort_key(r["column"])))

        return key_rows, all_rows

    def _to_key_row(self, col: ColumnMapping) -> Dict[str, Any]:
        return {
            "column":     self._s(col.col_letter),
            "sheet":      self._s(col.sheet_name),
            "role":       self._s(col.role),
            "entity":     self._s(col.entity),
            "currency":   self._s(col.currency),
            "row_start":  col.row_start,
            "row_end":    col.row_end,
            "header":     self._s(col.header_text),
            "formula":    self._s(col.formula_pattern),
            "confidence": round(float(col.confidence), 3),
        }

    def _to_all_row(self, col: ColumnMapping) -> Dict[str, Any]:
        return {
            "column":     self._s(col.col_letter),
            "index":      col.one_based_index,
            "sheet":      self._s(col.sheet_name),
            "role":       self._s(col.role),
            "entity":     self._s(col.entity),
            "currency":   self._s(col.currency),
            "period":     self._s(col.period),
            "row_start":  col.row_start,
            "row_end":    col.row_end,
            "header":     self._s(col.header_text),
            "formula":    self._s(col.formula_pattern),
            "confidence": round(float(col.confidence), 3),
        }

    # ------------------------------------------------------------------
    # Structured table builders — companies / currencies
    # ------------------------------------------------------------------

    def _build_company_table(
        self, workbook_result: WorkbookExtractionResult
    ) -> List[Dict[str, Any]]:
        pre_built = getattr(workbook_result, "companies_table", None) or []
        if pre_built:
            return list(pre_built)

        rows: List[Dict[str, Any]] = []
        seen: set[str] = set()

        for sheet in workbook_result.sorted_sheets():
            for col in sheet.sorted_columns():
                if col.role != "entity_value":
                    continue
                entity = self._s(col.entity)
                if not entity or entity in seen:
                    continue
                seen.add(entity)
                rows.append({
                    "entity":     entity,
                    "sheet":      self._s(col.sheet_name),
                    "source":     "column_mapping",
                    "header":     self._s(col.header_text),
                    "column":     self._s(col.col_letter),
                    "confidence": round(float(col.confidence), 3),
                })

        for name in workbook_result.entities:
            entity = str(name).strip()
            if entity and entity not in seen:
                seen.add(entity)
                rows.append({
                    "entity":     entity,
                    "sheet":      "",
                    "source":     "workbook_structure",
                    "header":     "",
                    "column":     "",
                    "confidence": 0.0,
                })

        rows.sort(key=lambda r: r["entity"])
        return rows

    def _build_currency_table(
        self, workbook_result: WorkbookExtractionResult
    ) -> List[Dict[str, Any]]:
        pre_built = getattr(workbook_result, "currencies_table", None) or []
        if pre_built:
            return list(pre_built)

        rows: List[Dict[str, Any]] = []
        seen: set[tuple[str, str]] = set()

        for sheet in workbook_result.sorted_sheets():
            for col in sheet.sorted_columns():
                entity = self._s(col.entity)
                currency = self._s(col.currency).upper()
                if not currency:
                    continue
                key = (entity, currency)
                if key in seen:
                    continue
                seen.add(key)
                rows.append({
                    "entity":     entity,
                    "currency":   currency,
                    "sheet":      self._s(col.sheet_name),
                    "header":     self._s(col.header_text),
                    "column":     self._s(col.col_letter),
                    "confidence": round(float(col.confidence), 3),
                    "source":     "column_mapping",
                })

        rows.sort(key=lambda r: (r["entity"], r["currency"]))
        return rows

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _col_to_dict(self, col: ColumnMapping) -> Dict[str, Any]:
        return {
            "column":     self._s(col.col_letter),
            "index":      col.one_based_index,
            "sheet":      self._s(col.sheet_name),
            "role":       self._s(col.role),
            "entity":     self._s(col.entity),
            "currency":   self._s(col.currency),
            "period":     self._s(col.period),
            "row_start":  col.row_start,
            "row_end":    col.row_end,
            "header":     self._s(col.header_text),
            "formula":    self._s(col.formula_pattern),
            "confidence": round(float(col.confidence), 3),
        }

    @staticmethod
    def _s(value: Any) -> str:
        if value is None:
            return ""
        return str(value).strip()

    @staticmethod
    def _fmt_int(value: Optional[int]) -> str:
        return "" if value is None else str(int(value))

    @staticmethod
    def _letter_sort_key(col_letter: str) -> tuple[int, str]:
        return (len(col_letter), col_letter)