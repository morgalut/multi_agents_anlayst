from __future__ import annotations

import json
import logging
import re
import time
from typing import Any, Dict, List, Optional

from Multi_agen.packages.llm import LLMClient, LLMMessage
from Multi_agen.packages.llm.stage_prompts import StagePromptProfile
from Multi_agen.packages.core.schemas import (
    SheetCompanyExtraction,
    SheetCurrencyExtraction,
    SheetCurrencyHit,
    SheetEntityHit,
    SheetTask,
    WorkbookStructure,
)

logger = logging.getLogger("multi_agen.agents.sheet_currency_agent")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)


# ---------------------------------------------------------------------------
# Hard safety ceilings
# ---------------------------------------------------------------------------
_HARD_MAX_PREVIEW_ROWS: int = 30
_HARD_MAX_PREVIEW_COLS: int = 60
_HARD_MAX_MERGED: int = 80

# ---------------------------------------------------------------------------
# Known currency vocabulary
# ---------------------------------------------------------------------------
_CURRENCY_CODES: frozenset[str] = frozenset({
    "USD", "NIS", "ILS", "EUR", "GBP", "JPY", "CHF", "CAD", "AUD",
    "CNY", "HKD", "SGD", "MXN", "BRL", "INR", "KRW", "TRY", "RUB",
})

_CURRENCY_SYMBOLS: Dict[str, str] = {
    "$": "USD",
    "₪": "NIS",
    "€": "EUR",
    "£": "GBP",
    "¥": "JPY",
}

# Regex: matches USD, NIS, ILS, $ … inside a cell value
_CURRENCY_PATTERN = re.compile(
    r"\b(USD|NIS|ILS|EUR|GBP|JPY|CHF|CAD|AUD|CNY|HKD|SGD)\b"
    r"|[$₪€£¥]",
    re.IGNORECASE,
)


class SheetCurrencyAgent:
    """
    Narrow-purpose agent: detect currency markers in one sheet and align
    them to the physical columns (and entities) identified by SheetCompanyAgent.

    Runs ONLY on main sheets (task.is_main_sheet == True).

    Strategy
    --------
    1. Heuristic pass
       a. Scan header rows for explicit currency text (USD, NIS, $, ₪ …).
       b. Align each hit to a nearby entity column from company_extraction.
       c. Fall back to workbook-structure entity→currency priors when no
          in-sheet evidence exists for a known entity.

    2. LLM refinement pass (optional)
       Ask the LLM to confirm and extend heuristic hits.

    3. Merge: LLM wins per (currency, col_idx) key; heuristic fills gaps.

    Output is SheetCurrencyExtraction consumed by RoleMapperAgent to set
    currency= on entity_value / aje / consolidated columns.
    """

    def __init__(
        self,
        llm: Optional[LLMClient] = None,
        llm_enabled: bool = True,
        max_preview_rows: int = 20,
        max_preview_cols: int = 40,
        max_merged: int = 60,
    ) -> None:
        self.llm = llm
        self.llm_enabled = llm_enabled
        self.max_preview_rows: int = min(max_preview_rows, _HARD_MAX_PREVIEW_ROWS)
        self.max_preview_cols: int = min(max_preview_cols, _HARD_MAX_PREVIEW_COLS)
        self.max_merged: int = min(max_merged, _HARD_MAX_MERGED)

    def set_llm(self, llm: LLMClient) -> None:
        self.llm = llm

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def extract(
        self,
        task: SheetTask,
        state: Any,
        analysis: Any,
        tools: Any,
        company_extraction: Optional[SheetCompanyExtraction] = None,
        prompt_profile: Optional[StagePromptProfile] = None,
    ) -> SheetCurrencyExtraction:
        """
        Detect currency markers on task.sheet_name.

        company_extraction is the output of SheetCompanyAgent; when provided
        currency markers are aligned to the nearest entity column.
        """
        if not task.is_main_sheet:
            return SheetCurrencyExtraction(
                quality_flags=["skipped_non_main_sheet"],
                confidence=0.0,
            )

        t0 = time.perf_counter()
        logger.info("SheetCurrencyAgent:start sheet=%s", task.sheet_name)

        workbook_structure: Optional[WorkbookStructure] = getattr(
            state, "workbook_structure", None
        )

        # ------------------------------------------------------------------
        # Step 1: collect sheet evidence
        # ------------------------------------------------------------------
        grid, merged, formulas = self._collect_evidence(task.sheet_name, tools)

        # ------------------------------------------------------------------
        # Step 2: heuristic pass
        # ------------------------------------------------------------------
        heuristic_hits = self._heuristic_currency_scan(
            sheet_name=task.sheet_name,
            grid=grid,
            merged=merged,
            formulas=formulas,
            company_extraction=company_extraction,
            workbook_structure=workbook_structure,
            tools=tools,
        )

        # ------------------------------------------------------------------
        # Step 3: LLM refinement pass
        # ------------------------------------------------------------------
        if not self.llm_enabled or self.llm is None:
            elapsed = (time.perf_counter() - t0) * 1000
            logger.info(
                "SheetCurrencyAgent:done (no_llm) sheet=%s hits=%d elapsed_ms=%.1f",
                task.sheet_name, len(heuristic_hits), elapsed,
            )
            return self._build_extraction(heuristic_hits, extra_flags=["llm_disabled"])

        try:
            llm_hits = self._llm_currency_scan(
                task=task,
                grid=grid,
                merged=merged,
                formulas=formulas,
                company_extraction=company_extraction,
                workbook_structure=workbook_structure,
                prompt_profile=prompt_profile,
            )
        except Exception as exc:
            logger.exception(
                "SheetCurrencyAgent:llm_failed sheet=%s err=%s",
                task.sheet_name, type(exc).__name__,
            )
            return self._build_extraction(
                heuristic_hits,
                extra_flags=[f"llm_error:{type(exc).__name__}"],
            )

        merged_hits = self._merge_hits(heuristic_hits, llm_hits)

        elapsed = (time.perf_counter() - t0) * 1000
        logger.info(
            "SheetCurrencyAgent:done sheet=%s hits=%d elapsed_ms=%.1f",
            task.sheet_name, len(merged_hits), elapsed,
        )
        return self._build_extraction(merged_hits)

    # ------------------------------------------------------------------
    # Evidence collection
    # ------------------------------------------------------------------

    def _collect_evidence(
        self, sheet_name: str, tools: Any
    ) -> tuple[List[List[Any]], List[Dict[str, Any]], List[List[Optional[str]]]]:
        grid: List[List[Any]] = []
        merged: List[Dict[str, Any]] = []
        formulas: List[List[Optional[str]]] = []

        try:
            if hasattr(tools, "excel_read_sheet_range"):
                grid = tools.excel_read_sheet_range(
                    sheet_name=sheet_name, row0=0, col0=0,
                    nrows=self.max_preview_rows, ncols=self.max_preview_cols,
                )
        except Exception:
            logger.exception("SheetCurrencyAgent:grid_read_failed sheet=%s", sheet_name)

        try:
            if hasattr(tools, "excel_detect_merged_cells"):
                merged = (tools.excel_detect_merged_cells(sheet_name=sheet_name) or [])[: self.max_merged]
        except Exception:
            logger.exception("SheetCurrencyAgent:merged_read_failed sheet=%s", sheet_name)

        try:
            if hasattr(tools, "excel_get_formulas"):
                formulas = tools.excel_get_formulas(
                    sheet_name=sheet_name, row0=0, col0=0,
                    nrows=min(self.max_preview_rows, 15), ncols=self.max_preview_cols,
                )
        except Exception:
            logger.exception("SheetCurrencyAgent:formula_read_failed sheet=%s", sheet_name)

        return grid, merged, formulas

    # ------------------------------------------------------------------
    # Heuristic currency scan
    # ------------------------------------------------------------------

    def _heuristic_currency_scan(
        self,
        sheet_name: str,
        grid: List[List[Any]],
        merged: List[Dict[str, Any]],
        formulas: List[List[Optional[str]]],
        company_extraction: Optional[SheetCompanyExtraction],
        workbook_structure: Optional[WorkbookStructure],
        tools: Any,
    ) -> List[SheetCurrencyHit]:
        hits: List[SheetCurrencyHit] = []

        # Build entity→currency prior from workbook structure
        entity_currency_prior: Dict[str, str] = {}
        if workbook_structure is not None:
            for ent in (workbook_structure.entities or []):
                name = str(getattr(ent, "name", "") or "").strip()
                currency = str(getattr(ent, "currency", "") or "").strip()
                if name and currency:
                    entity_currency_prior[name.upper()] = currency.upper()

        # Build col_idx → entity map from company extraction
        col_to_entity: Dict[int, str] = {}
        entity_hits: List[SheetEntityHit] = []
        if company_extraction is not None:
            entity_hits = company_extraction.entities or []
            for eh in entity_hits:
                col_to_entity[eh.col_idx] = eh.entity

        # --- Pass 1: scan header rows for explicit currency markers ---
        hits.extend(
            self._scan_grid_for_currencies(
                sheet_name=sheet_name,
                grid=grid,
                col_to_entity=col_to_entity,
                entity_hits=entity_hits,
                tools=tools,
            )
        )

        # --- Pass 2: scan merged cells ---
        hits.extend(
            self._scan_merged_for_currencies(
                sheet_name=sheet_name,
                merged=merged,
                col_to_entity=col_to_entity,
                entity_hits=entity_hits,
                tools=tools,
            )
        )

        # --- Pass 3: fill missing currencies from workbook structure priors ---
        hits.extend(
            self._fill_from_priors(
                sheet_name=sheet_name,
                existing_hits=hits,
                entity_hits=entity_hits,
                entity_currency_prior=entity_currency_prior,
                tools=tools,
            )
        )

        return self._dedupe_hits(hits)

    def _scan_grid_for_currencies(
        self,
        sheet_name: str,
        grid: List[List[Any]],
        col_to_entity: Dict[int, str],
        entity_hits: List[SheetEntityHit],
        tools: Any,
    ) -> List[SheetCurrencyHit]:
        hits: List[SheetCurrencyHit] = []
        if not isinstance(grid, list):
            return hits

        header_rows = min(len(grid), 10)
        for row_idx in range(header_rows):
            row = grid[row_idx] if isinstance(grid[row_idx], list) else []
            for col_idx, cell_val in enumerate(row):
                cell_str = str(cell_val).strip() if cell_val is not None else ""
                if not cell_str:
                    continue

                currency = self._extract_currency_from_text(cell_str)
                if not currency:
                    continue

                col_letter = self._col_letter(col_idx, tools)
                entity = col_to_entity.get(col_idx, "")

                # If not directly on an entity column, find closest entity col
                if not entity:
                    entity = self._find_nearest_entity(col_idx, entity_hits)

                hits.append(SheetCurrencyHit(
                    currency=currency,
                    sheet_name=sheet_name,
                    header_text=cell_str,
                    col_idx=col_idx,
                    col_letter=col_letter,
                    row_idx=row_idx,
                    entity=entity,
                    confidence=0.85,
                    evidence=[f"grid_header_row{row_idx}_col{col_idx}"],
                ))

        return hits

    def _scan_merged_for_currencies(
        self,
        sheet_name: str,
        merged: List[Dict[str, Any]],
        col_to_entity: Dict[int, str],
        entity_hits: List[SheetEntityHit],
        tools: Any,
    ) -> List[SheetCurrencyHit]:
        hits: List[SheetCurrencyHit] = []
        if not isinstance(merged, list):
            return hits

        for cell_info in merged:
            if not isinstance(cell_info, dict):
                continue
            text = str(cell_info.get("value", "") or cell_info.get("text", "") or "").strip()
            if not text:
                continue

            currency = self._extract_currency_from_text(text)
            if not currency:
                continue

            col_idx = int(cell_info.get("min_col", cell_info.get("col", 0)) or 0)
            row_idx = int(cell_info.get("min_row", cell_info.get("row", 0)) or 0)
            col_letter = self._col_letter(col_idx, tools)
            entity = col_to_entity.get(col_idx, "")
            if not entity:
                entity = self._find_nearest_entity(col_idx, entity_hits)

            hits.append(SheetCurrencyHit(
                currency=currency,
                sheet_name=sheet_name,
                header_text=text,
                col_idx=col_idx,
                col_letter=col_letter,
                row_idx=row_idx,
                entity=entity,
                confidence=0.75,
                evidence=["merged_cell_label"],
            ))

        return hits

    def _fill_from_priors(
        self,
        sheet_name: str,
        existing_hits: List[SheetCurrencyHit],
        entity_hits: List[SheetEntityHit],
        entity_currency_prior: Dict[str, str],
        tools: Any,
    ) -> List[SheetCurrencyHit]:
        """
        For entity columns where no in-sheet currency evidence was found,
        use workbook-structure entity→currency priors (lower confidence).
        """
        hits: List[SheetCurrencyHit] = []
        covered_entities: set[str] = {h.entity.upper() for h in existing_hits if h.entity}

        for eh in entity_hits:
            entity_upper = eh.entity.upper()
            if entity_upper in covered_entities:
                continue
            prior_currency = entity_currency_prior.get(entity_upper)
            if not prior_currency:
                continue

            hits.append(SheetCurrencyHit(
                currency=prior_currency,
                sheet_name=sheet_name,
                header_text=f"(prior: {eh.entity} → {prior_currency})",
                col_idx=eh.col_idx,
                col_letter=eh.col_letter,
                row_idx=eh.row_idx,
                entity=eh.entity,
                confidence=0.60,
                evidence=["workbook_structure_prior"],
            ))
            covered_entities.add(entity_upper)

        return hits

    # ------------------------------------------------------------------
    # LLM currency scan
    # ------------------------------------------------------------------

    def _llm_currency_scan(
        self,
        task: SheetTask,
        grid: List[List[Any]],
        merged: List[Dict[str, Any]],
        formulas: List[List[Optional[str]]],
        company_extraction: Optional[SheetCompanyExtraction],
        workbook_structure: Optional[WorkbookStructure],
        prompt_profile: Optional[StagePromptProfile],
    ) -> List[SheetCurrencyHit]:
        entity_currency_prior: Dict[str, str] = {}
        if workbook_structure is not None:
            for ent in (workbook_structure.entities or []):
                name = str(getattr(ent, "name", "") or "").strip()
                currency = str(getattr(ent, "currency", "") or "").strip()
                if name and currency:
                    entity_currency_prior[name] = currency

        known_entity_cols: List[Dict[str, Any]] = []
        if company_extraction is not None:
            for eh in (company_extraction.entities or []):
                known_entity_cols.append({
                    "entity": eh.entity,
                    "col_idx": eh.col_idx,
                    "col_letter": eh.col_letter,
                    "header_text": eh.header_text,
                })

        grid_text = _grid_to_text(grid, max_rows=self.max_preview_rows, max_cols=self.max_preview_cols)

        context_block = json.dumps({
            "sheet_name": task.sheet_name,
            "entity_currency_prior": entity_currency_prior,
            "known_entity_columns": known_entity_cols,
            "grid_preview": grid_text,
            "merged_preview": merged[: self.max_merged],
        }, ensure_ascii=False, indent=2)

        if prompt_profile is not None:
            system_prompt = prompt_profile.compose_system_prompt()
            user_prompt = prompt_profile.compose_user_prompt(context_block)
        else:
            system_prompt = _DEFAULT_SYSTEM_PROMPT
            user_prompt = _DEFAULT_USER_PROMPT.format(context_block=context_block)

        result = self.llm.chat([
            LLMMessage(role="system", content=system_prompt),
            LLMMessage(role="user", content=user_prompt),
        ])

        raw_text = (getattr(result, "text", "") or "").strip()
        data = _parse_json_loose(raw_text)
        if not isinstance(data, dict):
            logger.warning("SheetCurrencyAgent:invalid_llm_json sheet=%s", task.sheet_name)
            return []

        return self._coerce_llm_hits(task.sheet_name, data)

    def _coerce_llm_hits(
        self, sheet_name: str, data: Dict[str, Any]
    ) -> List[SheetCurrencyHit]:
        hits: List[SheetCurrencyHit] = []
        raw = data.get("currencies", [])
        if not isinstance(raw, list):
            return hits

        for item in raw:
            if not isinstance(item, dict):
                continue
            currency = str(item.get("currency", "") or "").strip().upper()
            if not currency:
                continue

            col_idx_raw = item.get("col_idx")
            try:
                col_idx = int(col_idx_raw) if col_idx_raw is not None else 0
            except Exception:
                col_idx = 0

            col_letter = str(item.get("col_letter", "") or "").strip()
            row_idx_raw = item.get("row_idx")
            try:
                row_idx: Optional[int] = int(row_idx_raw) if row_idx_raw is not None else None
            except Exception:
                row_idx = None

            header_text = str(item.get("header_text", "") or "").strip()
            entity = str(item.get("entity", "") or "").strip()

            try:
                confidence = max(0.0, min(1.0, float(item.get("confidence", 0.8))))
            except Exception:
                confidence = 0.8

            evidence = item.get("evidence", [])
            if not isinstance(evidence, list):
                evidence = []

            hits.append(SheetCurrencyHit(
                currency=currency,
                sheet_name=sheet_name,
                header_text=header_text,
                col_idx=col_idx,
                col_letter=col_letter,
                row_idx=row_idx,
                entity=entity,
                confidence=confidence,
                evidence=[str(e) for e in evidence][:8],
            ))

        return hits

    # ------------------------------------------------------------------
    # Merge
    # ------------------------------------------------------------------

    def _merge_hits(
        self,
        heuristic: List[SheetCurrencyHit],
        llm: List[SheetCurrencyHit],
    ) -> List[SheetCurrencyHit]:
        llm_keys: set[tuple[str, int]] = {(h.currency.upper(), h.col_idx) for h in llm}
        merged = list(llm)
        for h in heuristic:
            if (h.currency.upper(), h.col_idx) not in llm_keys:
                merged.append(h)
        return self._dedupe_hits(merged)

    # ------------------------------------------------------------------
    # Build final extraction object
    # ------------------------------------------------------------------

    def _build_extraction(
        self,
        hits: List[SheetCurrencyHit],
        extra_flags: Optional[List[str]] = None,
    ) -> SheetCurrencyExtraction:
        flags: List[str] = list(extra_flags or [])
        if not hits:
            flags.append("no_currency_hits_found")

        confidence = 0.0
        if hits:
            confidence = max(h.confidence for h in hits)

        return SheetCurrencyExtraction(
            currencies=hits,
            quality_flags=flags,
            confidence=confidence,
        )

    # ------------------------------------------------------------------
    # Deduplication
    # ------------------------------------------------------------------

    @staticmethod
    def _dedupe_hits(hits: List[SheetCurrencyHit]) -> List[SheetCurrencyHit]:
        best: Dict[tuple[str, int], SheetCurrencyHit] = {}
        for h in hits:
            key = (h.currency.upper(), h.col_idx)
            if key not in best or h.confidence > best[key].confidence:
                best[key] = h
        return sorted(best.values(), key=lambda h: h.col_idx)

    # ------------------------------------------------------------------
    # Currency extraction from text
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_currency_from_text(text: str) -> Optional[str]:
        """Return the first recognised currency code from *text*, or None."""
        text_stripped = text.strip()

        # Check symbols first (single char, fast)
        for symbol, code in _CURRENCY_SYMBOLS.items():
            if symbol in text_stripped:
                return code

        # Check codes (case-insensitive word boundary)
        match = _CURRENCY_PATTERN.search(text_stripped.upper())
        if match:
            raw = match.group(0).upper()
            # Normalise ILS → NIS
            if raw == "ILS":
                return "NIS"
            if raw in _CURRENCY_CODES:
                return raw

        return None

    # ------------------------------------------------------------------
    # Nearest entity column lookup
    # ------------------------------------------------------------------

    @staticmethod
    def _find_nearest_entity(col_idx: int, entity_hits: List[SheetEntityHit]) -> str:
        """
        Return the entity whose column is closest (by distance) to col_idx.
        Returns "" when no entity hits are available.
        """
        if not entity_hits:
            return ""
        closest = min(entity_hits, key=lambda eh: abs(eh.col_idx - col_idx))
        # Only assign if within 3 columns — beyond that it is too speculative
        if abs(closest.col_idx - col_idx) <= 3:
            return closest.entity
        return ""

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _col_letter(col_idx: int, tools: Any) -> str:
        try:
            if hasattr(tools, "excel_column_index_to_letter"):
                return str(tools.excel_column_index_to_letter(col_idx) or "")
        except Exception:
            pass
        result = ""
        idx = col_idx
        while True:
            result = chr(ord("A") + idx % 26) + result
            idx = idx // 26 - 1
            if idx < 0:
                break
        return result


# ---------------------------------------------------------------------------
# Module-level grid renderer (shared / standalone)
# ---------------------------------------------------------------------------

def _grid_to_text(grid: List[List[Any]], max_rows: int, max_cols: int) -> str:
    if not isinstance(grid, list) or not grid:
        return ""
    lines: List[str] = []
    for r_idx, row in enumerate(grid[:max_rows]):
        row = row if isinstance(row, list) else []
        cells = []
        for val in row[:max_cols]:
            s = "" if val is None else str(val).replace("\n", " ").strip()
            if len(s) > 40:
                s = s[:40] + "…"
            cells.append(s)
        lines.append(f"{r_idx:02d} | " + " | ".join(cells))
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# JSON parsing helper
# ---------------------------------------------------------------------------

def _parse_json_loose(text: str) -> Optional[Dict[str, Any]]:
    if not text:
        return None
    fenced = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, flags=re.DOTALL | re.IGNORECASE)
    if fenced:
        try:
            return json.loads(fenced.group(1).strip())
        except Exception:
            return None
    if text.startswith("{") and text.endswith("}"):
        try:
            return json.loads(text)
        except Exception:
            pass
    candidate = re.search(r"(\{.*\})", text, flags=re.DOTALL)
    if candidate:
        try:
            return json.loads(candidate.group(1).strip())
        except Exception:
            return None
    return None


# ---------------------------------------------------------------------------
# Default prompts
# ---------------------------------------------------------------------------

_DEFAULT_SYSTEM_PROMPT = """
You are a financial Excel sheet currency detection agent.

Rules:
- Identify only currency markers: codes (USD, NIS, ILS, EUR …) or symbols ($, ₪, €, £ …).
- Align each currency to the nearest entity/company column when possible.
- Use workbook-level entity→currency priors if no in-sheet evidence exists.
- Return valid JSON only.
""".strip()

_DEFAULT_USER_PROMPT = """
Detect currency markers in the sheet.

Context:
{context_block}

Return JSON exactly:
{{
  "currencies": [
    {{
      "currency": "",
      "col_idx": null,
      "col_letter": "",
      "row_idx": null,
      "header_text": "",
      "entity": "",
      "confidence": 0.0,
      "evidence": []
    }}
  ],
  "quality_flags": []
}}
""".strip()
