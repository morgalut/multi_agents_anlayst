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
    SheetEntityHit,
    SheetTask,
    WorkbookStructure,
)

logger = logging.getLogger("multi_agen.agents.sheet_company_agent")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)


# ---------------------------------------------------------------------------
# Hard safety ceilings
# ---------------------------------------------------------------------------
_HARD_MAX_PREVIEW_ROWS: int = 30   # header rows only; we are not reading data rows
_HARD_MAX_PREVIEW_COLS: int = 60
_HARD_MAX_MERGED: int = 80


class SheetCompanyAgent:
    """
    Narrow-purpose agent: detect entity/company labels in one sheet.

    This agent runs ONLY on main sheets (task.is_main_sheet == True).
    It uses workbook-level entity priors from WorkbookStructureAgent to
    guide its search, then confirms or extends those findings by inspecting:
      - grid header rows
      - merged cell ranges
      - formula source references (e.g. GL_LTD, WP_INC)

    Output is a SheetCompanyExtraction which the RoleMapperAgent uses to
    assign entity= values on entity_value / aje / consolidated columns.

    This agent is intentionally narrow:
      - it does NOT detect currencies (that is SheetCurrencyAgent)
      - it does NOT produce ColumnMapping objects (that is RoleMapperAgent)
      - it does NOT run on non-main sheets
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
        prompt_profile: Optional[StagePromptProfile] = None,
    ) -> SheetCompanyExtraction:
        """
        Detect company/entity labels on task.sheet_name.

        Returns a minimal SheetCompanyExtraction (no entities, confidence=0)
        when called on a non-main sheet — the agent is optimized for the
        main presentation sheet only.
        """
        if not task.is_main_sheet:
            return SheetCompanyExtraction(
                quality_flags=["skipped_non_main_sheet"],
                confidence=0.0,
            )

        t0 = time.perf_counter()
        logger.info("SheetCompanyAgent:start sheet=%s", task.sheet_name)

        workbook_structure: Optional[WorkbookStructure] = getattr(
            state, "workbook_structure", None
        )

        # ------------------------------------------------------------------
        # Step 1: collect sheet evidence (grid preview + merged + formulas)
        # ------------------------------------------------------------------
        grid, merged, formulas = self._collect_evidence(task.sheet_name, tools)

        # ------------------------------------------------------------------
        # Step 2: heuristic pass (always runs — no LLM dependency)
        # ------------------------------------------------------------------
        heuristic_hits = self._heuristic_entity_scan(
            sheet_name=task.sheet_name,
            grid=grid,
            merged=merged,
            formulas=formulas,
            workbook_structure=workbook_structure,
            tools=tools,
        )

        # ------------------------------------------------------------------
        # Step 3: LLM refinement pass (optional)
        # ------------------------------------------------------------------
        if not self.llm_enabled or self.llm is None:
            elapsed = (time.perf_counter() - t0) * 1000
            logger.info(
                "SheetCompanyAgent:done (no_llm) sheet=%s hits=%d elapsed_ms=%.1f",
                task.sheet_name, len(heuristic_hits), elapsed,
            )
            return self._build_extraction(heuristic_hits, extra_flags=["llm_disabled"])

        try:
            llm_hits = self._llm_entity_scan(
                task=task,
                grid=grid,
                merged=merged,
                formulas=formulas,
                workbook_structure=workbook_structure,
                prompt_profile=prompt_profile,
            )
        except Exception as exc:
            logger.exception(
                "SheetCompanyAgent:llm_failed sheet=%s err=%s",
                task.sheet_name, type(exc).__name__,
            )
            return self._build_extraction(
                heuristic_hits,
                extra_flags=[f"llm_error:{type(exc).__name__}"],
            )

        # ------------------------------------------------------------------
        # Step 4: merge heuristic + LLM hits (LLM takes precedence per col)
        # ------------------------------------------------------------------
        merged_hits = self._merge_hits(heuristic_hits, llm_hits)

        elapsed = (time.perf_counter() - t0) * 1000
        logger.info(
            "SheetCompanyAgent:done sheet=%s hits=%d elapsed_ms=%.1f",
            task.sheet_name, len(merged_hits), elapsed,
        )
        return self._build_extraction(merged_hits)

    # ------------------------------------------------------------------
    # Evidence collection
    # ------------------------------------------------------------------

    def _collect_evidence(
        self,
        sheet_name: str,
        tools: Any,
    ) -> tuple[List[List[Any]], List[Dict[str, Any]], List[List[Optional[str]]]]:
        grid: List[List[Any]] = []
        merged: List[Dict[str, Any]] = []
        formulas: List[List[Optional[str]]] = []

        try:
            if hasattr(tools, "excel_read_sheet_range"):
                grid = tools.excel_read_sheet_range(
                    sheet_name=sheet_name,
                    row0=0,
                    col0=0,
                    nrows=self.max_preview_rows,
                    ncols=self.max_preview_cols,
                )
        except Exception:
            logger.exception("SheetCompanyAgent:grid_read_failed sheet=%s", sheet_name)

        try:
            if hasattr(tools, "excel_detect_merged_cells"):
                merged = tools.excel_detect_merged_cells(sheet_name=sheet_name) or []
                merged = merged[: self.max_merged]
        except Exception:
            logger.exception("SheetCompanyAgent:merged_read_failed sheet=%s", sheet_name)

        try:
            if hasattr(tools, "excel_get_formulas"):
                formulas = tools.excel_get_formulas(
                    sheet_name=sheet_name,
                    row0=0,
                    col0=0,
                    nrows=min(self.max_preview_rows, 15),
                    ncols=self.max_preview_cols,
                )
        except Exception:
            logger.exception("SheetCompanyAgent:formula_read_failed sheet=%s", sheet_name)

        return grid, merged, formulas

    # ------------------------------------------------------------------
    # Heuristic entity scan
    # ------------------------------------------------------------------

    def _heuristic_entity_scan(
        self,
        sheet_name: str,
        grid: List[List[Any]],
        merged: List[Dict[str, Any]],
        formulas: List[List[Optional[str]]],
        workbook_structure: Optional[WorkbookStructure],
        tools: Any,
    ) -> List[SheetEntityHit]:
        """
        Rule-based entity detection.

        Priority order:
        1. Workbook-known entity names found in header cells
        2. Formula references that encode entity names (GL_LTD, WP_INC …)
        3. Merged cell text that looks like company/entity names
        """
        hits: List[SheetEntityHit] = []

        # Collect known entity names from workbook structure
        known_entities: List[str] = []
        if workbook_structure is not None:
            for ent in (workbook_structure.entities or []):
                name = str(getattr(ent, "name", "") or "").strip()
                if name:
                    known_entities.append(name)

        # --- Pass 1: scan header rows for known entity names ---
        hits.extend(
            self._scan_grid_for_entities(
                sheet_name=sheet_name,
                grid=grid,
                known_entities=known_entities,
                tools=tools,
            )
        )

        # --- Pass 2: scan formulas for entity references ---
        hits.extend(
            self._scan_formulas_for_entities(
                sheet_name=sheet_name,
                formulas=formulas,
                known_entities=known_entities,
                tools=tools,
            )
        )

        # --- Pass 3: scan merged cells for entity-like labels ---
        hits.extend(
            self._scan_merged_for_entities(
                sheet_name=sheet_name,
                merged=merged,
                known_entities=known_entities,
                tools=tools,
            )
        )

        return self._dedupe_hits(hits)

    def _scan_grid_for_entities(
        self,
        sheet_name: str,
        grid: List[List[Any]],
        known_entities: List[str],
        tools: Any,
    ) -> List[SheetEntityHit]:
        hits: List[SheetEntityHit] = []
        if not isinstance(grid, list):
            return hits

        # Examine top rows (headers are usually within first 10 rows)
        header_rows = min(len(grid), 10)
        for row_idx in range(header_rows):
            row = grid[row_idx] if isinstance(grid[row_idx], list) else []
            for col_idx, cell_val in enumerate(row):
                cell_str = str(cell_val).strip() if cell_val is not None else ""
                if not cell_str:
                    continue

                cell_upper = cell_str.upper()

                # Check against known workbook entities
                for entity_name in known_entities:
                    if entity_name.upper() in cell_upper:
                        col_letter = self._col_letter(col_idx, tools)
                        hits.append(SheetEntityHit(
                            entity=entity_name,
                            sheet_name=sheet_name,
                            header_text=cell_str,
                            col_idx=col_idx,
                            col_letter=col_letter,
                            row_idx=row_idx,
                            confidence=0.85,
                            evidence=[f"grid_header_row{row_idx}_col{col_idx}"],
                        ))
                        break  # one entity per cell

        return hits

    def _scan_formulas_for_entities(
        self,
        sheet_name: str,
        formulas: List[List[Optional[str]]],
        known_entities: List[str],
        tools: Any,
    ) -> List[SheetEntityHit]:
        hits: List[SheetEntityHit] = []
        if not isinstance(formulas, list):
            return hits

        for row_idx, row in enumerate(formulas):
            if not isinstance(row, list):
                continue
            for col_idx, formula in enumerate(row):
                if not formula:
                    continue
                formula_upper = str(formula).upper()

                for entity_name in known_entities:
                    entity_upper = entity_name.upper()
                    # Match patterns like: GL_LTD, WP_LTD, LTD!, '[LTD]
                    patterns = [
                        f"_{entity_upper}",
                        f"{entity_upper}_",
                        f"{entity_upper}!",
                        f"[{entity_upper}]",
                        f"'{entity_upper}",
                    ]
                    if any(p in formula_upper for p in patterns):
                        col_letter = self._col_letter(col_idx, tools)
                        hits.append(SheetEntityHit(
                            entity=entity_name,
                            sheet_name=sheet_name,
                            header_text=formula[:80],
                            col_idx=col_idx,
                            col_letter=col_letter,
                            row_idx=row_idx,
                            confidence=0.75,
                            evidence=[f"formula_ref_row{row_idx}_col{col_idx}"],
                        ))
                        break

        return hits

    def _scan_merged_for_entities(
        self,
        sheet_name: str,
        merged: List[Dict[str, Any]],
        known_entities: List[str],
        tools: Any,
    ) -> List[SheetEntityHit]:
        hits: List[SheetEntityHit] = []
        if not isinstance(merged, list):
            return hits

        for cell_info in merged:
            if not isinstance(cell_info, dict):
                continue
            text = str(cell_info.get("value", "") or cell_info.get("text", "") or "").strip()
            if not text:
                continue
            text_upper = text.upper()

            for entity_name in known_entities:
                if entity_name.upper() in text_upper:
                    # Use the left/top column of the merged range
                    col_idx = int(cell_info.get("min_col", cell_info.get("col", 0)) or 0)
                    row_idx = int(cell_info.get("min_row", cell_info.get("row", 0)) or 0)
                    col_letter = self._col_letter(col_idx, tools)
                    hits.append(SheetEntityHit(
                        entity=entity_name,
                        sheet_name=sheet_name,
                        header_text=text,
                        col_idx=col_idx,
                        col_letter=col_letter,
                        row_idx=row_idx,
                        confidence=0.70,
                        evidence=["merged_cell_label"],
                    ))
                    break

        return hits

    # ------------------------------------------------------------------
    # LLM entity scan
    # ------------------------------------------------------------------

    def _llm_entity_scan(
        self,
        task: SheetTask,
        grid: List[List[Any]],
        merged: List[Dict[str, Any]],
        formulas: List[List[Optional[str]]],
        workbook_structure: Optional[WorkbookStructure],
        prompt_profile: Optional[StagePromptProfile],
    ) -> List[SheetEntityHit]:
        known_entities: List[str] = []
        entity_currencies: Dict[str, str] = {}
        if workbook_structure is not None:
            for ent in (workbook_structure.entities or []):
                name = str(getattr(ent, "name", "") or "").strip()
                currency = str(getattr(ent, "currency", "") or "").strip()
                if name:
                    known_entities.append(name)
                    if currency:
                        entity_currencies[name] = currency

        grid_text = self._grid_to_text(grid, max_rows=self.max_preview_rows, max_cols=self.max_preview_cols)
        formula_text = self._formulas_to_text(formulas, max_rows=10, max_cols=self.max_preview_cols)

        context_block = json.dumps({
            "sheet_name": task.sheet_name,
            "known_entities": known_entities,
            "entity_currencies": entity_currencies,
            "grid_preview": grid_text,
            "merged_preview": merged[: self.max_merged],
            "formula_preview": formula_text,
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
            logger.warning("SheetCompanyAgent:invalid_llm_json sheet=%s", task.sheet_name)
            return []

        return self._coerce_llm_hits(task.sheet_name, data)

    def _coerce_llm_hits(
        self, sheet_name: str, data: Dict[str, Any]
    ) -> List[SheetEntityHit]:
        hits: List[SheetEntityHit] = []
        raw_entities = data.get("entities", [])
        if not isinstance(raw_entities, list):
            return hits

        for item in raw_entities:
            if not isinstance(item, dict):
                continue
            entity = str(item.get("entity", "") or "").strip()
            if not entity:
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
            try:
                confidence = max(0.0, min(1.0, float(item.get("confidence", 0.8))))
            except Exception:
                confidence = 0.8

            evidence = item.get("evidence", [])
            if not isinstance(evidence, list):
                evidence = []

            hits.append(SheetEntityHit(
                entity=entity,
                sheet_name=sheet_name,
                header_text=header_text,
                col_idx=col_idx,
                col_letter=col_letter,
                row_idx=row_idx,
                confidence=confidence,
                evidence=[str(e) for e in evidence][:8],
            ))

        return hits

    # ------------------------------------------------------------------
    # Merge heuristic + LLM hits
    # ------------------------------------------------------------------

    def _merge_hits(
        self,
        heuristic: List[SheetEntityHit],
        llm: List[SheetEntityHit],
    ) -> List[SheetEntityHit]:
        """
        LLM hits take precedence per (entity, col_idx) key.
        Heuristic hits fill gaps not covered by the LLM.
        """
        llm_keys: set[tuple[str, int]] = {(h.entity.upper(), h.col_idx) for h in llm}
        merged = list(llm)
        for h in heuristic:
            if (h.entity.upper(), h.col_idx) not in llm_keys:
                merged.append(h)
        return self._dedupe_hits(merged)

    # ------------------------------------------------------------------
    # Build final extraction object
    # ------------------------------------------------------------------

    def _build_extraction(
        self,
        hits: List[SheetEntityHit],
        extra_flags: Optional[List[str]] = None,
    ) -> SheetCompanyExtraction:
        flags: List[str] = list(extra_flags or [])
        if not hits:
            flags.append("no_entity_hits_found")

        confidence = 0.0
        if hits:
            confidence = max(h.confidence for h in hits)

        return SheetCompanyExtraction(
            entities=hits,
            quality_flags=flags,
            confidence=confidence,
        )

    # ------------------------------------------------------------------
    # Deduplication
    # ------------------------------------------------------------------

    @staticmethod
    def _dedupe_hits(hits: List[SheetEntityHit]) -> List[SheetEntityHit]:
        """
        Keep the highest-confidence hit per (entity_upper, col_idx) key.
        """
        best: Dict[tuple[str, int], SheetEntityHit] = {}
        for h in hits:
            key = (h.entity.upper(), h.col_idx)
            if key not in best or h.confidence > best[key].confidence:
                best[key] = h
        return sorted(best.values(), key=lambda h: h.col_idx)

    # ------------------------------------------------------------------
    # Renderers / helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _col_letter(col_idx: int, tools: Any) -> str:
        try:
            if hasattr(tools, "excel_column_index_to_letter"):
                return str(tools.excel_column_index_to_letter(col_idx) or "")
        except Exception:
            pass
        # Fallback: simple A-Z, AA-ZZ computation
        result = ""
        idx = col_idx
        while True:
            result = chr(ord("A") + idx % 26) + result
            idx = idx // 26 - 1
            if idx < 0:
                break
        return result

    @staticmethod
    def _grid_to_text(
        grid: List[List[Any]], max_rows: int, max_cols: int
    ) -> str:
        if not isinstance(grid, list) or not grid:
            return ""
        lines: List[str] = []
        for r_idx, row in enumerate(grid[:max_rows]):
            row = row if isinstance(row, list) else []
            cells = []
            for c_idx, val in enumerate(row[:max_cols]):
                s = "" if val is None else str(val).replace("\n", " ").strip()
                if len(s) > 40:
                    s = s[:40] + "…"
                cells.append(s)
            lines.append(f"{r_idx:02d} | " + " | ".join(cells))
        return "\n".join(lines)

    @staticmethod
    def _formulas_to_text(
        formulas: List[List[Optional[str]]], max_rows: int, max_cols: int
    ) -> str:
        if not isinstance(formulas, list) or not formulas:
            return ""
        lines: List[str] = []
        for r_idx, row in enumerate(formulas[:max_rows]):
            row = row if isinstance(row, list) else []
            cells = []
            for val in row[:max_cols]:
                s = "" if val is None else str(val).replace("\n", " ").strip()
                if len(s) > 50:
                    s = s[:50] + "…"
                cells.append(s)
            lines.append(f"{r_idx:02d} | " + " | ".join(cells))
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Default prompts (used when no StagePromptProfile is supplied)
# ---------------------------------------------------------------------------

_DEFAULT_SYSTEM_PROMPT = """
You are a financial Excel sheet company/entity detection agent.

Rules:
- Identify only company or legal entity labels — not generic column headers.
- Prefer workbook-known entity names (provided in context) over guesses.
- Detect entities from: header cells, merged range labels, formula references.
- Return valid JSON only.
""".strip()

_DEFAULT_USER_PROMPT = """
Detect company/entity labels in the sheet.

Context:
{context_block}

Return JSON exactly:
{{
  "entities": [
    {{
      "entity": "",
      "col_idx": null,
      "col_letter": "",
      "row_idx": null,
      "header_text": "",
      "confidence": 0.0,
      "evidence": []
    }}
  ],
  "quality_flags": []
}}
""".strip()


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