
# Multi_agen\packages\agents\react_sheet_analyzer.py
from __future__ import annotations

import json
import logging
import re
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from Multi_agen.packages.core import ALLOWED_COLUMN_ROLES, normalize_text
from Multi_agen.packages.llm import LLMClient, LLMMessage
from Multi_agen.packages.llm.prompts import build_sheet_analysis_messages
from Multi_agen.packages.llm.stage_prompts import StagePromptProfile
from Multi_agen.packages.core.search_config import ColumnSearchConfig

logger = logging.getLogger("multi_agen.agents.react_sheet_analyzer")


@dataclass(frozen=True, slots=True)
class SheetClassification:
    """types: ["BS"], ["PL"], or ["BS", "PL"]"""
    types: List[str] = field(default_factory=list)
    confidence: float = 0.0
    evidence: List[str] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class SheetAnalysis:
    """
    classification:  high-level statement-type classification
    observations:    raw tool outputs used by this stage
    signals:         normalized structured payload for downstream stages
    """
    classification: SheetClassification
    observations: Dict[str, Any] = field(default_factory=dict)
    signals: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ReActAnalyzerConfig:
    # Grid read dimensions (always read; cheap)
    top_rows: int = 200
    top_cols: int = 30

    # Formula probe dimensions (conditional; narrow to avoid 60s timeouts)
    formula_probe_rows: int = 20
    formula_probe_cols: int = 14

    # Whether to attempt merged-cell reads at all
    attempt_merged_reads: bool = False   # disabled by default (60s timeout risk)

    llm_enabled: bool = True
    store_llm_raw_chars: int = 2500

    log_grid_preview_rows: int = 5
    log_grid_preview_cols: int = 8
    header_hint_scan_rows: int = 20

    heuristic_min_data_rows: int = 5
    heuristic_min_numeric_col_hits: int = 4
    heuristic_add_when_no_entity_columns: bool = True


class ReActSheetAnalyzer:
    """
    Observe / Think / Act / Reflect analyzer.

    Call order (cheap-first):
      1. _observe_grid()               — always; fast
      2. _infer_header_hint_rows()     — pure CPU; free
      3. _observe_merged() [gated]     — skipped if grid signals strong enough
      4. _decide_formula_probe_window() — derives row/col window from grid inference
      5. _observe_formulas_focused()   — narrow window only; skip if previous timeout cached
      6. LLM call
      7. Heuristic supplement / post-validation

    Failure caching:
      Tool failures are recorded in state.memory["tool_failures"][sheet_name]
      to prevent re-burning 60-second timeouts on the same sheet.
    """

    _DATE_RE = re.compile(r"\b(20\d{2})\b")
    _SHEET_REF_RE = re.compile(
        r"(?:^|[=+\-*/,( ])(?:'([^']+)'|([A-Za-z_][A-Za-z0-9_ .&-]*))!"
    )

    def __init__(
        self,
        llm: Optional[LLMClient] = None,
        config: Optional[ReActAnalyzerConfig] = None,
        search_config: Optional[ColumnSearchConfig] = None
    ) -> None:
        self.config = config or ReActAnalyzerConfig()
        self.llm = llm
        self.search_config = search_config or ColumnSearchConfig()

    def set_llm(self, llm: LLMClient) -> None:
        self.llm = llm

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------

    def analyze(
        self,
        task: Any,
        state: Any,
        tools: Any,
        prompt_profile: Optional[StagePromptProfile] = None,
    ) -> SheetAnalysis:
        t0 = time.perf_counter()
        sheet_name = getattr(task, "sheet_name", "?")
        logger.info("ReAct:start sheet=%s", sheet_name)

        workbook_context = self._extract_workbook_context(state)

        # ----------------------------------------------------------------
        # STEP 1: Always read the grid (cheap, fast, always works)
        # ----------------------------------------------------------------
        grid = self._observe_grid(sheet_name=sheet_name, tools=tools)
        if not grid:
            classification = SheetClassification(
                types=[],
                confidence=0.0,
                evidence=["observe_failed:grid_unavailable"],
            )
            return SheetAnalysis(
                classification=classification,
                observations={},
                signals={
                    "columns": [],
                    "normalized_columns": [],
                    "unit": None,
                    "quality_flags": ["observe_failed:grid_unavailable"],
                },
            )

        # ----------------------------------------------------------------
        # STEP 2: Infer header / data band from grid (free, CPU only)
        # ----------------------------------------------------------------
        header_hint_rows = self._infer_header_hint_rows(grid)
        header_end = header_hint_rows[1] if header_hint_rows is not None else min(6, len(grid) - 1)

        # ----------------------------------------------------------------
        # STEP 3: Merged cells (conditional, risky — skip by default)
        # ----------------------------------------------------------------
        merged: List[Dict[str, Any]] = []
        if self.config.attempt_merged_reads and not self._tool_failed_previously(state, sheet_name, "merged"):
            merged = self._observe_merged(sheet_name=sheet_name, tools=tools, state=state)

        # ----------------------------------------------------------------
        # STEP 4: Formulas (narrow window; skip if previously timed out)
        # ----------------------------------------------------------------
        formulas: List[List[Optional[str]]] = []
        if not self._tool_failed_previously(state, sheet_name, "formulas"):
            probe_rows, probe_cols = self._decide_formula_probe_window(
                grid=grid,
                header_end=header_end,
            )
            formulas = self._observe_formulas_focused(
                sheet_name=sheet_name,
                tools=tools,
                state=state,
                nrows=probe_rows,
                ncols=probe_cols,
            )

        observations = {
            "top_grid": grid,
            "merged_cells": merged,
            "formulas": formulas,
            "header_hint_rows": header_hint_rows,
        }
        self._log_grid_preview(sheet_name, grid)

        # ----------------------------------------------------------------
        # STEP 5: Heuristic extraction (always runs; backstop)
        # ----------------------------------------------------------------
        heuristic_classification = self._heuristic_classification(grid)
        heuristic_unit = self._heuristic_unit(grid, workbook_context=workbook_context)
        heuristic_columns = self._heuristic_columns(
            grid=grid,
            formulas=formulas,
            sheet_name=sheet_name,
            header_hint_rows=header_hint_rows,
            workbook_context=workbook_context,
        )

        # ----------------------------------------------------------------
        # NO LLM FALLBACK
        # ----------------------------------------------------------------
        if not self.config.llm_enabled or self.llm is None:
            logger.info("ReAct:llm_disabled_or_missing sheet=%s", sheet_name)
            return SheetAnalysis(
                classification=heuristic_classification,
                observations=observations,
                signals={
                    "columns": heuristic_columns,
                    "normalized_columns": heuristic_columns,
                    "unit": heuristic_unit,
                    "quality_flags": ["llm_disabled_or_missing"],
                    "workbook_context_used": True,
                },
            )

        # ----------------------------------------------------------------
        # STEP 6: LLM call
        # ----------------------------------------------------------------
        system_prompt, user_prompt = build_sheet_analysis_messages(
            sheet_name=sheet_name,
            grid=grid,
            merged_ranges=merged,
            prompt_profile=prompt_profile,
            formulas=formulas,
            header_hint_rows=header_hint_rows,
        )

        logger.info("ReAct:llm_call sheet=%s", sheet_name)
        llm_t0 = time.perf_counter()

        try:
            result = self.llm.chat(
                [
                    LLMMessage(role="system", content=system_prompt),
                    LLMMessage(role="user", content=user_prompt),
                ]
            )
        except Exception as exc:
            logger.exception("ReAct:llm_failed sheet=%s err=%s", sheet_name, type(exc).__name__)
            return SheetAnalysis(
                classification=heuristic_classification,
                observations=observations,
                signals={
                    "columns": heuristic_columns,
                    "normalized_columns": heuristic_columns,
                    "unit": heuristic_unit,
                    "quality_flags": [f"llm_error:{type(exc).__name__}"],
                    "workbook_context_used": True,
                },
            )

        logger.info(
            "ReAct:llm_ok sheet=%s model=%s elapsed_ms=%.2f",
            sheet_name,
            getattr(result, "model", "unknown"),
            (time.perf_counter() - llm_t0) * 1000,
        )

        raw_text = (getattr(result, "text", "") or "").strip()
        data = self._parse_json_loose(raw_text)

        if data is None or not isinstance(data, dict):
            logger.warning("ReAct:llm_invalid_json sheet=%s", sheet_name)
            return SheetAnalysis(
                classification=heuristic_classification,
                observations=observations,
                signals={
                    "columns": heuristic_columns,
                    "normalized_columns": heuristic_columns,
                    "unit": heuristic_unit,
                    "quality_flags": ["llm_invalid_json"],
                    "llm_model": getattr(result, "model", "unknown"),
                    "llm_raw": raw_text[: self.config.store_llm_raw_chars],
                    "workbook_context_used": True,
                },
            )

        # ----------------------------------------------------------------
        # STEP 7: Validate LLM output + heuristic supplement
        # ----------------------------------------------------------------
        llm_classification = self._extract_classification(data)
        classification = self._combine_classifications(
            primary=llm_classification,
            fallback=heuristic_classification,
        )

        raw_columns = self._extract_columns(data, default_sheet_name=sheet_name)
        normalized_columns, validation_flags = self._validate_and_normalize_columns(
            raw_columns=raw_columns,
            default_sheet_name=sheet_name,
            workbook_context=workbook_context,
        )

        # Supplement with heuristic columns for any roles the LLM missed
        normalized_columns = self._supplement_columns(
            current=normalized_columns,
            heuristic=heuristic_columns,
        )

        unit = self._extract_unit(data, grid, workbook_context=workbook_context)
        quality_flags = self._extract_quality_flags(data)
        quality_flags.extend(validation_flags)

        if not normalized_columns:
            quality_flags.append("no_columns_after_validation")

        if (
            self.config.heuristic_add_when_no_entity_columns
            and not self._has_role(normalized_columns, "entity_value")
            and self._has_role(heuristic_columns, "entity_value")
        ):
            quality_flags.append("heuristic_entity_columns_supplemented")

        elapsed_ms = (time.perf_counter() - t0) * 1000
        logger.info(
            "ReAct:done sheet=%s types=%s conf=%.3f columns=%d elapsed_ms=%.2f",
            sheet_name,
            classification.types,
            classification.confidence,
            len(normalized_columns),
            elapsed_ms,
        )

        return SheetAnalysis(
            classification=classification,
            observations=observations,
            signals={
                "columns": normalized_columns,
                "normalized_columns": normalized_columns,
                "unit": unit,
                "quality_flags": self._dedupe_strings(quality_flags),
                "llm_model": getattr(result, "model", "unknown"),
                "llm_usage": getattr(result, "usage", {}),
                "llm_raw": raw_text[: self.config.store_llm_raw_chars],
                "workbook_context_used": True,
            },
        )

    # ------------------------------------------------------------------
    # Observation steps (independently fault-tolerant)
    # ------------------------------------------------------------------

    def _observe_grid(self, *, sheet_name: str, tools: Any) -> List[List[Any]]:
        try:
            return tools.excel_read_sheet_range(
                sheet_name=sheet_name,
                row0=0,
                col0=0,
                nrows=self.config.top_rows,
                ncols=self.config.top_cols,
            )
        except Exception:
            logger.exception("ReAct:grid_read_failed sheet=%s", sheet_name)
            return []

    def _observe_merged(
        self,
        *,
        sheet_name: str,
        tools: Any,
        state: Any,
    ) -> List[Dict[str, Any]]:
        try:
            result = tools.excel_detect_merged_cells(sheet_name=sheet_name)
            return result or []
        except Exception:
            logger.exception("ReAct:merged_read_failed sheet=%s", sheet_name)
            self._record_tool_failure(state, sheet_name, "merged")
            return []

    def _decide_formula_probe_window(
        self,
        *,
        grid: List[List[Any]],
        header_end: int,
    ) -> Tuple[int, int]:
        """
        Derive a narrow formula read window from the inferred header/data band.
        Read: header zone + first N data rows, capped at config limits.
        """
        probe_rows = min(
            header_end + 1 + self.config.formula_probe_rows,
            self.config.formula_probe_rows,
        )
        probe_cols = self.config.formula_probe_cols
        return probe_rows, probe_cols

    def _observe_formulas_focused(
        self,
        *,
        sheet_name: str,
        tools: Any,
        state: Any,
        nrows: int,
        ncols: int,
    ) -> List[List[Optional[str]]]:
        if not hasattr(tools, "excel_get_formulas"):
            return []

        try:
            result = tools.excel_get_formulas(
                sheet_name=sheet_name,
                row0=0,
                col0=0,
                nrows=nrows,
                ncols=ncols,
            )
            return result or []
        except Exception:
            logger.exception("ReAct:formula_read_failed sheet=%s", sheet_name)
            self._record_tool_failure(state, sheet_name, "formulas")
            return []

    # ------------------------------------------------------------------
    # Failure cache (per-sheet, stored in state.memory)
    # ------------------------------------------------------------------

    def _record_tool_failure(self, state: Any, sheet_name: str, tool_key: str) -> None:
        try:
            memory = getattr(state, "memory", None)
            if not isinstance(memory, dict):
                return
            tool_failures = memory.setdefault("tool_failures", {})
            sheet_failures = tool_failures.setdefault(sheet_name, {})
            sheet_failures[f"{tool_key}_timeout"] = True
        except Exception:
            pass

    def _tool_failed_previously(self, state: Any, sheet_name: str, tool_key: str) -> bool:
        try:
            memory = getattr(state, "memory", None)
            if not isinstance(memory, dict):
                return False
            return bool(
                memory
                .get("tool_failures", {})
                .get(sheet_name, {})
                .get(f"{tool_key}_timeout", False)
            )
        except Exception:
            return False

    # ------------------------------------------------------------------
    # Workbook context extraction
    # ------------------------------------------------------------------

    def _extract_workbook_context(self, state: Any) -> Dict[str, Any]:
        ws = getattr(state, "workbook_structure", None)

        entities: List[Dict[str, str]] = []
        entity_names: List[str] = []
        entity_currency_map: Dict[str, str] = {}

        if ws is not None:
            for ent in getattr(ws, "entities", []) or []:
                name = str(getattr(ent, "name", "") or "").strip()
                currency = str(getattr(ent, "currency", "") or "").strip()
                if not name:
                    continue
                entities.append({"name": name, "currency": currency})
                entity_names.append(name)
                if currency:
                    entity_currency_map[name.upper()] = currency

        return {
            "main_sheet_names": list(getattr(ws, "main_sheet_names", []) or []) if ws is not None else [],
            "contains": list(getattr(ws, "contains", []) or []) if ws is not None else [],
            "likely_units": getattr(ws, "likely_units", None) if ws is not None else None,
            "likely_current_period": getattr(ws, "likely_current_period", None) if ws is not None else None,
            "entities": entities,
            "entity_names": entity_names,
            "entity_currency_map": entity_currency_map,
        }

    # ------------------------------------------------------------------
    # LLM output extraction
    # ------------------------------------------------------------------

    def _extract_classification(self, data: Dict[str, Any]) -> SheetClassification:
        c = data.get("classification", {})
        if not isinstance(c, dict):
            c = {}

        raw_types = c.get("types", [])
        if not isinstance(raw_types, list):
            raw_types = []

        valid_types: List[str] = []
        for item in raw_types:
            s = str(item)
            if s in ("BS", "PL") and s not in valid_types:
                valid_types.append(s)

        conf_val = c.get("confidence", 0.0)
        try:
            conf = float(conf_val)
        except Exception:
            conf = 0.0

        raw_evidence = c.get("evidence", [])
        if not isinstance(raw_evidence, list):
            raw_evidence = []

        return SheetClassification(
            types=valid_types,
            confidence=max(0.0, min(1.0, conf)),
            evidence=[str(x) for x in raw_evidence][:10],
        )

    def _combine_classifications(
        self,
        *,
        primary: SheetClassification,
        fallback: SheetClassification,
    ) -> SheetClassification:
        if not primary.types:
            return fallback

        types = list(primary.types)
        evidence = list(primary.evidence)

        if primary.confidence < 0.70:
            for t in fallback.types:
                if t not in types:
                    types.append(t)
            if fallback.types and fallback.evidence:
                evidence.append("heuristic_support")
                evidence.extend(fallback.evidence[:2])

        return SheetClassification(
            types=types,
            confidence=max(primary.confidence, fallback.confidence if primary.types else primary.confidence),
            evidence=self._dedupe_strings(evidence)[:10],
        )

    def _extract_columns(
        self, data: Dict[str, Any], default_sheet_name: str
    ) -> List[Dict[str, Any]]:
        raw_columns = data.get("columns", [])
        if not isinstance(raw_columns, list):
            raw_columns = []

        out: List[Dict[str, Any]] = []
        for item in raw_columns:
            if not isinstance(item, dict):
                continue

            normalized = dict(item)

            # Tolerate alternate field names
            if "header_text" not in normalized and "header" in normalized:
                normalized["header_text"] = normalized.pop("header", "")
            if "formula_pattern" not in normalized and "formula" in normalized:
                normalized["formula_pattern"] = normalized.pop("formula", "")

            normalized.setdefault("entity", "")
            normalized.setdefault("currency", "")
            normalized.setdefault("period", "")
            normalized.setdefault("confidence", 0.0)
            normalized.setdefault("evidence", [])

            sheet_name = str(normalized.get("sheet_name", "") or "").strip()
            normalized["sheet_name"] = sheet_name or default_sheet_name

            out.append(normalized)

        return out

    def _validate_and_normalize_columns(
        self,
        *,
        raw_columns: List[Dict[str, Any]],
        default_sheet_name: str,
        workbook_context: Dict[str, Any],
    ) -> Tuple[List[Dict[str, Any]], List[str]]:
        out: List[Dict[str, Any]] = []
        flags: List[str] = []

        for item in raw_columns:
            role = str(item.get("role", "") or "").strip()
            if role not in ALLOWED_COLUMN_ROLES:
                flags.append(f"invalid_role:{role or 'missing'}")
                continue

            try:
                col_idx = int(item.get("col_idx"))
            except Exception:
                flags.append("invalid_col_idx")
                continue

            if col_idx < 0:
                flags.append("negative_col_idx")
                continue

            row_start = self._to_optional_positive_int(item.get("row_start"))
            row_end = self._to_optional_positive_int(item.get("row_end"))
            if row_start is not None and row_end is not None and row_end < row_start:
                flags.append("invalid_row_bounds")
                continue

            confidence = self._clamp_confidence(item.get("confidence", 0.0))
            header_text = str(item.get("header_text", "") or "").strip()
            formula_pattern = str(item.get("formula_pattern", "") or "").strip()
            entity = str(item.get("entity", "") or "").strip()
            currency = str(item.get("currency", "") or "").strip()
            period = str(item.get("period", "") or "").strip()

            sheet_name = str(item.get("sheet_name", "") or "").strip()
            if not sheet_name or sheet_name != default_sheet_name:
                sheet_name = default_sheet_name

            # Entity semantics enforcement
            if role not in {"entity_value", "aje", "consolidated_aje", "consolidated"}:
                entity = ""

            if role in {"consolidated", "consolidated_aje"} and not entity:
                entity = "Consolidated"

            if role in {"entity_value", "aje"} and not entity:
                entity = self._infer_entity_from_text_or_formula(
                    header_text=header_text,
                    formula_pattern=formula_pattern,
                    workbook_context=workbook_context,
                ) or ""

            if not currency:
                currency = self._derive_currency(
                    header_text=header_text,
                    entity=entity,
                    workbook_context=workbook_context,
                )

            if not period:
                period = self._extract_period_from_text(
                    text=f"{header_text} {formula_pattern}",
                    workbook_context=workbook_context,
                )

            evidence = item.get("evidence", [])
            if not isinstance(evidence, list):
                evidence = []

            out.append(
                {
                    "col_idx": col_idx,
                    "role": role,
                    "entity": entity,
                    "currency": currency,
                    "period": period,
                    "header_text": header_text,
                    "formula_pattern": formula_pattern,
                    "row_start": row_start,
                    "row_end": row_end,
                    "sheet_name": sheet_name,
                    "confidence": confidence,
                    "evidence": [str(x) for x in evidence][:10],
                }
            )

        out.sort(key=lambda x: x["col_idx"])
        return out, self._dedupe_strings(flags)

    def _extract_unit(
        self,
        data: Dict[str, Any],
        grid: List[List[Any]],
        *,
        workbook_context: Dict[str, Any],
    ) -> Optional[str]:
        for key in ("unit", "units", "likely_unit"):
            value = data.get(key)
            if value not in (None, ""):
                return str(value).strip()

        return self._heuristic_unit(grid, workbook_context=workbook_context)

    def _extract_quality_flags(self, data: Dict[str, Any]) -> List[str]:
        qf = data.get("quality_flags", [])
        if not isinstance(qf, list):
            return []
        return [str(x) for x in qf][:20]

    # ------------------------------------------------------------------
    # Heuristic classification and unit
    # ------------------------------------------------------------------

    def _heuristic_classification(self, grid: List[List[Any]]) -> SheetClassification:
        top_blob = self._grid_text_blob(grid[: min(10, len(grid))])
        full_blob = self._grid_text_blob(grid)
        combined = top_blob + " | " + full_blob

        bs_hits = self._count_terms(combined, [
            "balance sheet", "statement of financial position", "assets",
            "liabilities", "equity", "מאזן", "נכסים", "התחייבויות", "הון",
        ])
        pl_hits = self._count_terms(combined, [
            "profit and loss", "p&l", "income statement", "revenue", "expenses",
            "gross profit", "operating profit", "רווח והפסד", "הכנסות", "הוצאות",
        ])

        types: List[str] = []
        evidence: List[str] = []

        if bs_hits > 0:
            types.append("BS")
            evidence.append(f"heuristic_bs_hits:{bs_hits}")
        if pl_hits > 0:
            types.append("PL")
            evidence.append(f"heuristic_pl_hits:{pl_hits}")

        if not types:
            return SheetClassification(types=[], confidence=0.0, evidence=["heuristic_no_hits"])

        confidence = 0.50 if len(types) == 2 else 0.45
        return SheetClassification(types=types, confidence=confidence, evidence=evidence)

    def _heuristic_unit(
        self, grid: List[List[Any]], *, workbook_context: Dict[str, Any]
    ) -> Optional[str]:
        blob = self._grid_text_blob(grid).lower()

        if "thousand" in blob or "thousands" in blob or "k$" in blob or "$000" in blob:
            if "nis" in blob or "₪" in blob or 'ש"ח' in blob:
                return "NIS thousands"
            if "usd" in blob or "$" in blob:
                return "USD thousands"
            return "thousands"

        if "million" in blob or "millions" in blob:
            if "nis" in blob or "₪" in blob:
                return "NIS millions"
            if "usd" in blob or "$" in blob:
                return "USD millions"
            return "millions"

        if "nis" in blob or "₪" in blob or 'ש"ח' in blob:
            return "NIS"
        if "usd" in blob or "$" in blob or "u.s. dollars" in blob or "us dollars" in blob:
            return "USD"

        likely_units = workbook_context.get("likely_units")
        if likely_units not in (None, ""):
            return str(likely_units).strip()

        return None

    # ------------------------------------------------------------------
    # Heuristic column extraction (grid-based backstop)
    # ------------------------------------------------------------------

    def _heuristic_columns(
        self,
        *,
        grid: List[List[Any]],
        formulas: List[List[Optional[str]]],
        sheet_name: str,
        header_hint_rows: Optional[Tuple[int, int]],
        workbook_context: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        if not grid:
            return []

        row_count = len(grid)
        col_count = max((len(r) for r in grid if isinstance(r, list)), default=0)
        if row_count == 0 or col_count == 0:
            return []

        header_end = header_hint_rows[1] if header_hint_rows is not None else min(6, row_count - 1)
        data_start, data_end = self._infer_data_band(
            grid=grid,
            formulas=formulas,
            start_row=max(header_end + 1, 0),
        )
        if data_start is None or data_end is None:
            return []

        if (data_end - data_start + 1) < self.config.heuristic_min_data_rows:
            return []

        header_texts = [
            self._collect_header_text(
                grid=grid,
                col_idx=c,
                row0=0,
                row1=min(header_end + 2, row_count - 1),
            )
            for c in range(col_count)
        ]
        formula_patterns = [
            self._first_formula_pattern(
                formulas=formulas,
                col_idx=c,
                row0=data_start,
                row1=data_end,
            )
            for c in range(col_count)
        ]
        stats = [
            self._column_stats(
                grid=grid,
                formulas=formulas,
                col_idx=c,
                row0=data_start,
                row1=data_end,
            )
            for c in range(col_count)
        ]

        out: List[Dict[str, Any]] = []

        # COA column
        coa_idx = self._pick_coa_column(
            header_texts=header_texts,
            stats=stats,
            row0=data_start,
            row1=data_end,
        )
        if coa_idx is not None:
            out.append(
                {
                    "col_idx": coa_idx,
                    "role": "coa_name",
                    "entity": "",
                    "currency": "",
                    "period": "",
                    "header_text": header_texts[coa_idx],
                    "formula_pattern": "",
                    "row_start": data_start + 1,
                    "row_end": data_end + 1,
                    "sheet_name": sheet_name,
                    "confidence": 0.72,
                    "evidence": ["heuristic_coa_text_density"],
                }
            )

        # Value / structural columns
        for c in range(col_count):
            if c == coa_idx:
                continue

            stat = stats[c]
            header_text = header_texts[c]
            formula_pattern = formula_patterns[c]

            if not self._is_value_like_column(stat) and not self._looks_interesting_non_value_header(header_text):
                continue

            role, conf, evidence = self._classify_heuristic_column(
                col_idx=c,
                header_text=header_text,
                formula_pattern=formula_pattern,
                workbook_context=workbook_context,
                row_start=data_start + 1,
                row_end=data_end + 1,
                stats=stat,
            )
            if not role:
                continue

            entity = ""
            if role in {"entity_value", "aje"}:
                entity = self._infer_entity_from_text_or_formula(
                    header_text=header_text,
                    formula_pattern=formula_pattern,
                    workbook_context=workbook_context,
                ) or ""
            elif role in {"consolidated", "consolidated_aje"}:
                entity = "Consolidated"

            currency = self._derive_currency(
                header_text=header_text,
                entity=entity,
                workbook_context=workbook_context,
            )
            period = self._extract_period_from_text(
                text=f"{header_text} {formula_pattern}",
                workbook_context=workbook_context,
            )

            out.append(
                {
                    "col_idx": c,
                    "role": role,
                    "entity": entity,
                    "currency": currency,
                    "period": period,
                    "header_text": header_text,
                    "formula_pattern": formula_pattern,
                    "row_start": data_start + 1,
                    "row_end": data_end + 1,
                    "sheet_name": sheet_name,
                    "confidence": conf,
                    "evidence": evidence,
                }
            )

        out.sort(key=lambda x: x["col_idx"])
        return out

    # ------------------------------------------------------------------
    # Column supplementation
    # ------------------------------------------------------------------

    def _supplement_columns(
        self,
        *,
        current: List[Dict[str, Any]],
        heuristic: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        if not heuristic:
            return current

        out = list(current)

        def add_if_missing(target_role: str, allow_multiple: bool = False) -> None:
            if not allow_multiple and self._has_role(out, target_role):
                return
            for col in heuristic:
                if col.get("role") != target_role:
                    continue
                if any(
                    e.get("col_idx") == col.get("col_idx") and e.get("role") == col.get("role")
                    for e in out
                ):
                    continue
                out.append(col)

        add_if_missing("coa_name")
        add_if_missing("consolidated")
        add_if_missing("consolidated_aje")
        add_if_missing("aje", allow_multiple=True)
        add_if_missing("entity_value", allow_multiple=True)
        add_if_missing("prior_period", allow_multiple=True)
        add_if_missing("budget", allow_multiple=True)

        out.sort(key=lambda x: int(x.get("col_idx", 0)))
        return out

    # ------------------------------------------------------------------
    # Data-band and column stat helpers
    # ------------------------------------------------------------------

    def _infer_data_band(
        self,
        *,
        grid: List[List[Any]],
        formulas: List[List[Optional[str]]],
        start_row: int,
    ) -> Tuple[Optional[int], Optional[int]]:
        row_count = len(grid)
        if start_row >= row_count:
            return None, None

        candidate_rows: List[int] = []

        for r in range(start_row, row_count):
            row = grid[r] if r < len(grid) and isinstance(grid[r], list) else []
            formula_row = formulas[r] if r < len(formulas) and isinstance(formulas[r], list) else []

            numeric_count = 0
            formula_count = 0
            non_empty_count = 0

            width = max(len(row), len(formula_row))
            for c in range(width):
                value = row[c] if c < len(row) else None
                f = formula_row[c] if c < len(formula_row) else None

                if f not in (None, ""):
                    formula_count += 1
                    non_empty_count += 1
                    continue

                if value not in (None, ""):
                    non_empty_count += 1
                    if self._is_numericish(value):
                        numeric_count += 1

            if formula_count >= 1 or numeric_count >= 2 or (numeric_count >= 1 and non_empty_count >= 3):
                candidate_rows.append(r)

        if not candidate_rows:
            return None, None

        start = candidate_rows[0]
        end = candidate_rows[0]
        best_span = (start, end)
        prev = candidate_rows[0]

        for r in candidate_rows[1:]:
            if r - prev <= 1:
                end = r
            else:
                if (end - start) > (best_span[1] - best_span[0]):
                    best_span = (start, end)
                start = r
                end = r
            prev = r

        if (end - start) > (best_span[1] - best_span[0]):
            best_span = (start, end)

        return best_span

    def _collect_header_text(
        self, *, grid: List[List[Any]], col_idx: int, row0: int, row1: int
    ) -> str:
        parts: List[str] = []
        for r in range(row0, row1 + 1):
            row = grid[r] if r < len(grid) and isinstance(grid[r], list) else []
            if col_idx >= len(row):
                continue
            value = row[col_idx]
            if value in (None, ""):
                continue
            s = str(value).strip()
            if s:
                parts.append(s)
        return " | ".join(parts)

    def _first_formula_pattern(
        self,
        *,
        formulas: List[List[Optional[str]]],
        col_idx: int,
        row0: int,
        row1: int,
    ) -> str:
        for r in range(row0, row1 + 1):
            row = formulas[r] if r < len(formulas) and isinstance(formulas[r], list) else []
            if col_idx >= len(row):
                continue
            value = row[col_idx]
            if value not in (None, ""):
                return str(value).strip()
        return ""

    def _column_stats(
        self,
        *,
        grid: List[List[Any]],
        formulas: List[List[Optional[str]]],
        col_idx: int,
        row0: int,
        row1: int,
    ) -> Dict[str, int]:
        text_count = 0
        numeric_count = 0
        formula_count = 0
        non_empty_count = 0

        for r in range(row0, row1 + 1):
            row = grid[r] if r < len(grid) and isinstance(grid[r], list) else []
            formula_row = formulas[r] if r < len(formulas) and isinstance(formulas[r], list) else []

            value = row[col_idx] if col_idx < len(row) else None
            f = formula_row[col_idx] if col_idx < len(formula_row) else None

            if f not in (None, ""):
                formula_count += 1
                non_empty_count += 1
                continue

            if value in (None, ""):
                continue

            non_empty_count += 1
            if self._is_numericish(value):
                numeric_count += 1
            else:
                text_count += 1

        return {
            "text_count": text_count,
            "numeric_count": numeric_count,
            "formula_count": formula_count,
            "non_empty_count": non_empty_count,
        }

    def _pick_coa_column(
        self,
        *,
        header_texts: List[str],
        stats: List[Dict[str, int]],
        row0: int,
        row1: int,
    ) -> Optional[int]:
        best_idx: Optional[int] = None
        best_score = -1.0

        coverage = max(row1 - row0 + 1, 1)

        for idx, stat in enumerate(stats):
            text_count = stat["text_count"]
            numeric_count = stat["numeric_count"]
            formula_count = stat["formula_count"]

            header = normalize_text(header_texts[idx])
            header_bonus = 0
            if any(t in header for t in ("account", "description", "coa", "name", "תיאור", "חשבון")):
                header_bonus += 3

            text_ratio = text_count / coverage
            score = (text_count * 2.0) + header_bonus + (text_ratio * 5.0)
            score -= (numeric_count * 2.0) + (formula_count * 2.0)
            # mild left-side bias
            score += max(0.0, 2.0 - (idx * 0.15))

            if text_count >= 4 and score > best_score:
                best_score = score
                best_idx = idx

        return best_idx

    def _is_value_like_column(self, stat: Dict[str, int]) -> bool:
        return (stat["numeric_count"] + stat["formula_count"]) >= self.config.heuristic_min_numeric_col_hits

    def _looks_interesting_non_value_header(self, header_text: str) -> bool:
        h = normalize_text(header_text)
        return any(t in h for t in (
            "aje", "adjust", "consolidated", "group", "budget", "forecast",
            "prior", "previous", "fx", "rate", "nis", "usd", "₪", "$",
        ))

    def _classify_heuristic_column(
        self,
        *,
        col_idx: int,
        header_text: str,
        formula_pattern: str,
        workbook_context: Dict[str, Any],
        row_start: int,
        row_end: int,
        stats: Dict[str, int],
    ) -> Tuple[Optional[str], float, List[str]]:
        h = normalize_text(header_text)
        formula_refs = self._extract_formula_ref_sheets_from_formula(formula_pattern)

        if "fx" in h or "rate" in h:
            return "other", 0.60, ["heuristic_fx_helper"]

        if "budget" in h or "forecast" in h or "plan" in h:
            return "budget", 0.68, ["heuristic_budget_header"]

        if "prior" in h or "previous" in h or "comparative" in h:
            return "prior_period", 0.68, ["heuristic_prior_header"]

        current_period = str(workbook_context.get("likely_current_period", "") or "")
        current_year = None
        m = self._DATE_RE.search(current_period)
        if m:
            try:
                current_year = int(m.group(1))
            except Exception:
                pass

        year_in_header = self._extract_year(header_text)
        if current_year is not None and year_in_header is not None and year_in_header < current_year:
            return "prior_period", 0.66, ["heuristic_prior_year_header"]

        if "debit" in h:
            return "debit", 0.62, ["heuristic_debit_header"]
        if "credit" in h:
            return "credit", 0.62, ["heuristic_credit_header"]

        if "aje" in h or "adjust" in h or "adjustment" in h:
            if "consolidated" in h or "group" in h or self._formula_looks_arithmetic(formula_pattern):
                return "consolidated_aje", 0.72, ["heuristic_consolidated_aje_header"]
            return "aje", 0.70, ["heuristic_aje_header"]

        explicit_entity = self._infer_entity_from_text_or_formula(
            header_text=header_text,
            formula_pattern=formula_pattern,
            workbook_context=workbook_context,
        )
        if explicit_entity:
            return "entity_value", 0.74, ["heuristic_entity_match"]

        if (
            "consolidated" in h
            or "group" in h
            or ("balance" in h and "account" not in h)
            or self._formula_looks_arithmetic(formula_pattern)
        ):
            return "consolidated", 0.73, ["heuristic_consolidated_header_or_formula"]

        return None, 0.0, []

    # ------------------------------------------------------------------
    # Small text / parsing helpers
    # ------------------------------------------------------------------

    def _infer_header_hint_rows(self, grid: List[List[Any]]) -> Optional[Tuple[int, int]]:
        if not grid:
            return None

        max_rows = min(len(grid), self.config.header_hint_scan_rows)
        best_row = None
        best_score = -1

        header_terms = [
            "account", "description", "account name", "account description",
            "coa", "balance", "assets", "liabilities", "equity", "revenue",
            "expense", "profit", "loss", "usd", "nis", "consolidated", "aje",
        ]

        for r in range(max_rows):
            row = grid[r] if isinstance(grid[r], list) else []
            score = sum(
                1
                for cell in row
                if cell is not None and any(t in str(cell).strip().lower() for t in header_terms)
            )
            if score > best_score:
                best_score = score
                best_row = r

        if best_row is None:
            return None

        return (max(0, best_row - 1), min(best_row + 1, max_rows - 1))

    def _infer_entity_from_text_or_formula(
        self,
        *,
        header_text: str,
        formula_pattern: str,
        workbook_context: Dict[str, Any],
    ) -> Optional[str]:
        text = normalize_text(f"{header_text} {formula_pattern}")
        for name in workbook_context.get("entity_names", []) or []:
            norm_name = normalize_text(name)
            if norm_name and norm_name in text:
                return name
        return None

    def _derive_currency(
        self, *, header_text: str, entity: str, workbook_context: Dict[str, Any]
    ) -> str:
        h = normalize_text(header_text)
        if "nis" in h or "₪" in header_text or 'ש"ח' in header_text:
            return "NIS"
        if "usd" in h or "$" in header_text or "u.s. dollars" in h or "us dollars" in h:
            return "USD"
        if entity:
            return str(workbook_context.get("entity_currency_map", {}).get(entity.upper(), "") or "").strip()
        return ""

    def _extract_period_from_text(self, *, text: str, workbook_context: Dict[str, Any]) -> str:
        year = self._extract_year(str(text or ""))
        if year is not None:
            return str(year)
        current_period = workbook_context.get("likely_current_period")
        if current_period not in (None, ""):
            return str(current_period)
        return ""

    def _extract_year(self, text: str) -> Optional[int]:
        m = self._DATE_RE.search(str(text or ""))
        if not m:
            return None
        try:
            return int(m.group(1))
        except Exception:
            return None

    def _extract_formula_ref_sheets_from_formula(self, formula: str) -> List[str]:
        refs: List[str] = []
        for match in self._SHEET_REF_RE.finditer(str(formula or "")):
            name = (match.group(1) or match.group(2) or "").strip()
            if name:
                refs.append(name)
        return refs

    def _formula_looks_arithmetic(self, formula: str) -> bool:
        s = str(formula or "").upper()
        if not s:
            return False
        if "+" in s or "-" in s:
            return True
        if "SUM(" in s and "SUMIF(" not in s:
            return True
        return False

    def _to_optional_positive_int(self, value: Any) -> Optional[int]:
        if value in (None, "", "null"):
            return None
        try:
            iv = int(value)
        except Exception:
            return None
        return iv if iv >= 1 else None

    def _clamp_confidence(self, value: Any) -> float:
        try:
            conf = float(value)
        except Exception:
            conf = 0.0
        return max(0.0, min(1.0, conf))

    def _is_numericish(self, value: Any) -> bool:
        if value is None:
            return False
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return True
        s = str(value).strip().replace(",", "").replace("$", "").replace("₪", "")
        s = s.replace("%", "").replace("(", "-").replace(")", "")
        try:
            float(s)
            return True
        except Exception:
            return False

    def _grid_text_blob(self, grid: List[List[Any]]) -> str:
        parts: List[str] = []
        for row in grid:
            if not isinstance(row, list):
                continue
            for cell in row:
                if cell is None:
                    continue
                s = str(cell).strip()
                if s:
                    parts.append(s.lower())
        return " | ".join(parts)

    def _count_terms(self, haystack: str, needles: List[str]) -> int:
        h = haystack.lower()
        return sum(1 for n in needles if n.strip().lower() in h)

    def _has_role(self, columns: List[Dict[str, Any]], role: str) -> bool:
        return any(str(col.get("role", "") or "") == role for col in columns)

    def _dedupe_strings(self, items: List[str]) -> List[str]:
        out: List[str] = []
        seen: set = set()
        for item in items:
            s = str(item).strip()
            if s and s not in seen:
                seen.add(s)
                out.append(s)
        return out

    # ------------------------------------------------------------------
    # Logging and JSON parsing
    # ------------------------------------------------------------------

    def _log_grid_preview(self, sheet_name: str, grid: List[List[Any]]) -> None:
        try:
            rmax = min(len(grid), self.config.log_grid_preview_rows)
            cmax = min(
                max((len(r) for r in grid[:rmax] if isinstance(r, list)), default=0),
                self.config.log_grid_preview_cols,
            )
            preview = []
            for r in range(rmax):
                row = grid[r] if r < len(grid) and isinstance(grid[r], list) else []
                cells = []
                for c in range(cmax):
                    v = row[c] if c < len(row) else ""
                    s = "" if v is None else str(v).replace("\n", " ").replace("\r", " ").strip()
                    cells.append(s[:30] + "…" if len(s) > 30 else s)
                preview.append(cells)
            logger.debug("ReAct:grid_preview sheet=%s preview=%s", sheet_name, preview)
        except Exception:
            pass

    def _parse_json_loose(self, text: str) -> Optional[Dict[str, Any]]:
        if not text:
            return None

        # Try fenced code block first
        fenced = re.search(
            r"```(?:json)?\s*(\{.*?\})\s*```", text, flags=re.DOTALL | re.IGNORECASE
        )
        if fenced:
            try:
                return json.loads(fenced.group(1).strip())
            except Exception:
                pass

        # Try bare JSON
        stripped = text.strip()
        if stripped.startswith("{") and stripped.endswith("}"):
            try:
                return json.loads(stripped)
            except Exception:
                pass

        # Balanced-brace extraction (handles leading prose)
        balanced = self._extract_balanced_json_object(text)
        if balanced:
            try:
                return json.loads(balanced)
            except Exception:
                return None

        return None

    def _extract_balanced_json_object(self, text: str) -> Optional[str]:
        start = text.find("{")
        if start < 0:
            return None

        depth = 0
        in_string = False
        escape = False

        for idx in range(start, len(text)):
            ch = text[idx]

            if in_string:
                if escape:
                    escape = False
                elif ch == "\\":
                    escape = True
                elif ch == '"':
                    in_string = False
                continue

            if ch == '"':
                in_string = True
            elif ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    return text[start: idx + 1]

        return None