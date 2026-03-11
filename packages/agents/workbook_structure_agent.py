
# Multi_agen\packages\agents\workbook_structure_agent.py
from __future__ import annotations

import json
import logging
import re
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from Multi_agen.packages.llm import LLMClient, LLMMessage
from Multi_agen.packages.llm.stage_prompts import StagePromptProfile
from Multi_agen.packages.core import (
    SheetCandidate,
    WorkbookEntity,
    WorkbookStructure,
)

logger = logging.getLogger("multi_agen.agents.workbook_structure_agent")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)


# ---------------------------------------------------------------------------
# Hard safety ceilings — absolute upper bounds regardless of config values.
# These prevent runaway reads if config is set too aggressively.
# ---------------------------------------------------------------------------
_HARD_MAX_ROWS: int = 400
_HARD_MAX_COLS: int = 60
_HARD_MAX_FORMULA_ROWS: int = 120
_HARD_MAX_FORMULA_COLS: int = 40
_HARD_MAX_MERGED: int = 120
_HARD_MAX_SHEETS_COUNT: int = 30
_HARD_MAX_SAMPLE_SHEETS: int = 15
_HARD_MAX_STORE_CHARS: int = 20_000
_HARD_MAX_TOTAL_PREVIEW_CHARS: int = 120_000   # LLM context guard across ALL sheet previews


@dataclass(frozen=True, slots=True)
class WorkbookStructureAgentConfig:
    """
    All tunable knobs for WorkbookStructureAgent.

    Design rules
    ------------
    - Every magic number that controls read scope lives here — nothing hardcoded in methods.
    - Each field is capped at runtime by a module-level _HARD_MAX_* ceiling so callers
      cannot accidentally create runaway reads or overflow the LLM context window.
    - Raise values freely; the hard ceilings act as the last-resort safety net.
    """

    llm_enabled: bool = True

    # ------------------------------------------------------------------
    # Grid (cell value) preview
    # ------------------------------------------------------------------
    max_sheet_preview_rows: int = 150
    """
    Rows read per sheet for cell-value preview.
    35 was far too low for financial statements (Income Statement alone can
    span 80–150 rows; Balance Sheet 60–120 rows).
    """

    max_sheet_preview_cols: int = 35
    """
    Columns read per sheet for cell-value preview.
    18 missed multi-entity consolidation layouts and wide trial-balance dumps.
    """

    # ------------------------------------------------------------------
    # Formula preview (separate read, usually cheaper/smaller than grid)
    # ------------------------------------------------------------------
    max_formula_preview_rows: int = 80
    """
    Rows read per sheet for formula preview.
    Previously hardcoded to 20 (read) and 12 (render) — both far too small
    to detect SUM/SUMIF patterns across multi-section P&L blocks.
    """

    max_formula_preview_cols: int = 30
    """
    Columns read per sheet for formula preview.
    Previously hardcoded to 16 (read) and 10 (render).
    """

    # ------------------------------------------------------------------
    # Merged-cell preview
    # ------------------------------------------------------------------
    max_merged_preview: int = 60
    """
    Merged-cell records kept per sheet.
    Previously hardcoded to 20 — missed header bands in tall statements.
    """

    # ------------------------------------------------------------------
    # Sheet scope
    # ------------------------------------------------------------------
    max_sheet_count: int = 20
    """
    Candidate sheets passed to the LLM for name-level awareness.
    Was 12 — increased to surface edge-case multi-entity workbooks.
    """

    preview_sample_sheets: int = 10
    """
    Sheets that receive a full content preview (grid + formulas + merged).
    Was 6 — sheets ranked 7-12 got zero content inspection even though
    the heuristic ranker can mis-rank non-obviously-named sheets.
    """

    # ------------------------------------------------------------------
    # LLM / output
    # ------------------------------------------------------------------
    store_llm_raw_chars: int = 8_000
    """
    Characters of raw LLM response stored for diagnostics.
    Was 2500 — silently truncated quality flags and error traces from
    the tail of longer LLM responses.
    """

    max_total_preview_chars: int = 80_000
    """
    Soft total-character budget across all sheet previews combined
    (grid_preview + formula_preview text).  When this is breached the
    loop stops adding more sheets — this is the primary infinite-loop /
    context-overflow guard.  Hard ceiling: _HARD_MAX_TOTAL_PREVIEW_CHARS.
    """


class WorkbookStructureAgent:
    """
    Workbook-level structural analyzer.

    Responsibilities:
      - inspect workbook sheet inventory
      - read previews from candidate sheets
      - infer workbook-level structure conservatively
      - identify likely presentation sheets, entities, consolidation, AJE, units, period

    Important:
      - this is workbook-centric, not row-centric
      - weak evidence must remain weak
      - output uses the new WorkbookStructure SSOT
    """

    def __init__(
        self,
        llm: Optional[LLMClient] = None,
        config: Optional[WorkbookStructureAgentConfig] = None,
    ) -> None:
        self.llm = llm
        self.config = config or WorkbookStructureAgentConfig()
        self._effective = self._resolve_effective_config(self.config)

    def set_llm(self, llm: LLMClient) -> None:
        self.llm = llm

    # ------------------------------------------------------------------
    # Internal: resolve effective (hard-capped) config once at init time
    # ------------------------------------------------------------------
    @staticmethod
    def _resolve_effective_config(cfg: WorkbookStructureAgentConfig) -> Dict[str, Any]:
        """
        Apply module-level hard ceilings to every config field.
        Returns a plain dict so callers use self._effective["key"] and
        the capping logic is centralised in one place.
        """
        return {
            "llm_enabled": cfg.llm_enabled,
            "max_sheet_preview_rows": min(cfg.max_sheet_preview_rows, _HARD_MAX_ROWS),
            "max_sheet_preview_cols": min(cfg.max_sheet_preview_cols, _HARD_MAX_COLS),
            "max_formula_preview_rows": min(cfg.max_formula_preview_rows, _HARD_MAX_FORMULA_ROWS),
            "max_formula_preview_cols": min(cfg.max_formula_preview_cols, _HARD_MAX_FORMULA_COLS),
            "max_merged_preview": min(cfg.max_merged_preview, _HARD_MAX_MERGED),
            "max_sheet_count": min(cfg.max_sheet_count, _HARD_MAX_SHEETS_COUNT),
            "preview_sample_sheets": min(cfg.preview_sample_sheets, _HARD_MAX_SAMPLE_SHEETS),
            "store_llm_raw_chars": min(cfg.store_llm_raw_chars, _HARD_MAX_STORE_CHARS),
            "max_total_preview_chars": min(cfg.max_total_preview_chars, _HARD_MAX_TOTAL_PREVIEW_CHARS),
        }

    # ------------------------------------------------------------------
    # Public entry-point
    # ------------------------------------------------------------------
    def analyze_workbook(
        self,
        state: Any,
        tools: Any,
        prompt_profile: Optional[StagePromptProfile] = None,
    ) -> WorkbookStructure:
        t0 = time.perf_counter()
        logger.info("WorkbookStructure:start run_id=%s", getattr(state, "run_id", None))

        try:
            sheet_names = self._list_sheets(tools)
        except Exception:
            logger.exception("WorkbookStructure:list_sheets_failed")
            return WorkbookStructure(
                quality_flags=["list_sheets_failed"],
                confidence=0.0,
            )

        if not sheet_names:
            return WorkbookStructure(
                quality_flags=["no_sheets_found"],
                confidence=0.0,
            )

        candidate_names = self._rank_candidate_sheets(sheet_names)[
            : self._effective["max_sheet_count"]
        ]
        previews = self._collect_sheet_previews(candidate_names, tools)

        if not self._effective["llm_enabled"] or self.llm is None:
            logger.info("WorkbookStructure:llm_disabled_or_missing")
            structure = self._fallback_structure(candidate_names, previews)
            self._attach_provenance(state, structure, matched_by="fallback_no_llm")
            return structure

        system_prompt, user_prompt = self._build_messages(
            sheet_names=sheet_names,
            previews=previews,
            prompt_profile=prompt_profile,
        )

        try:
            result = self.llm.chat(
                [
                    LLMMessage(role="system", content=system_prompt),
                    LLMMessage(role="user", content=user_prompt),
                ]
            )
        except Exception as exc:
            logger.exception("WorkbookStructure:llm_failed err=%s", type(exc).__name__)
            fallback = self._fallback_structure(candidate_names, previews)
            structure = WorkbookStructure(
                main_sheet_names=fallback.main_sheet_names,
                contains=fallback.contains,
                entities=fallback.entities,
                has_consolidated=fallback.has_consolidated,
                consolidated_formula_pattern=fallback.consolidated_formula_pattern,
                has_aje=fallback.has_aje,
                aje_types=fallback.aje_types,
                likely_units=fallback.likely_units,
                likely_current_period=fallback.likely_current_period,
                sheet_candidates=fallback.sheet_candidates,
                quality_flags=fallback.quality_flags + [f"llm_error:{type(exc).__name__}"],
                confidence=0.0,
            )
            self._attach_provenance(state, structure, matched_by="fallback_llm_error")
            return structure

        raw_text = (getattr(result, "text", "") or "").strip()
        data = self._parse_json_loose(raw_text)

        if not isinstance(data, dict):
            logger.warning("WorkbookStructure:invalid_json")
            fallback = self._fallback_structure(candidate_names, previews)
            structure = WorkbookStructure(
                main_sheet_names=fallback.main_sheet_names,
                contains=fallback.contains,
                entities=fallback.entities,
                has_consolidated=fallback.has_consolidated,
                consolidated_formula_pattern=fallback.consolidated_formula_pattern,
                has_aje=fallback.has_aje,
                aje_types=fallback.aje_types,
                likely_units=fallback.likely_units,
                likely_current_period=fallback.likely_current_period,
                sheet_candidates=fallback.sheet_candidates,
                quality_flags=fallback.quality_flags + ["llm_invalid_json"],
                confidence=0.0,
                raw_llm_text=raw_text[: self._effective["store_llm_raw_chars"]],
            )
            self._attach_provenance(state, structure, matched_by="fallback_invalid_json")
            return structure

        structure = self._coerce_structure(data)
        structure = WorkbookStructure(
            main_sheet_names=structure.main_sheet_names,
            contains=structure.contains,
            entities=structure.entities,
            has_consolidated=structure.has_consolidated,
            consolidated_formula_pattern=structure.consolidated_formula_pattern,
            has_aje=structure.has_aje,
            aje_types=structure.aje_types,
            likely_units=structure.likely_units,
            likely_current_period=structure.likely_current_period,
            sheet_candidates=structure.sheet_candidates,
            quality_flags=structure.quality_flags,
            confidence=structure.confidence,
            raw_llm_text=raw_text[: self._effective["store_llm_raw_chars"]],
        )

        self._attach_provenance(state, structure, matched_by="llm_workbook_structure")

        elapsed_ms = (time.perf_counter() - t0) * 1000
        logger.info(
            "WorkbookStructure:done main_sheets=%s conf=%.3f elapsed_ms=%.2f",
            structure.main_sheet_names,
            structure.confidence,
            elapsed_ms,
        )
        return structure

    # ------------------------------------------------------------------
    # Provenance
    # ------------------------------------------------------------------
    def _attach_provenance(
        self,
        state: Any,
        structure: WorkbookStructure,
        *,
        matched_by: str,
    ) -> None:
        try:
            setattr(
                state,
                "workbook_structure_provenance",
                {
                    "main_sheet_names": list(structure.main_sheet_names),
                    "contains": list(structure.contains),
                    "matched_by": matched_by,
                    "source_code": {
                        "file": "Multi_agen/packages/agents/workbook_structure_agent.py",
                        "line_start": 1,
                        "line_end": 999,
                    },
                },
            )
        except Exception:
            logger.exception("WorkbookStructure:failed attaching provenance")

    # ------------------------------------------------------------------
    # Sheet listing
    # ------------------------------------------------------------------
    def _list_sheets(self, tools: Any) -> List[str]:
        raw = None

        if hasattr(tools, "excel_list_sheets"):
            raw = tools.excel_list_sheets()
        elif hasattr(tools, "list_sheets"):
            raw = tools.list_sheets()
        else:
            raise RuntimeError("Missing sheet listing tool")

        if isinstance(raw, list):
            return [str(x) for x in raw]

        if isinstance(raw, dict):
            if isinstance(raw.get("sheets"), list):
                return [str(x) for x in raw["sheets"]]
            if isinstance(raw.get("sheet_names"), list):
                return [str(x) for x in raw["sheet_names"]]

        return []

    # ------------------------------------------------------------------
    # Candidate ranking
    # ------------------------------------------------------------------
    def _rank_candidate_sheets(self, sheet_names: List[str]) -> List[str]:
        """
        Heuristic ranking only.

        Important:
        - presentation sheets should be preferred
        - obvious source sheets should be penalized
        """
        preferred_terms = [
            "fs",
            "financial",
            "statements",
            "statement",
            "bs",
            "balance",
            "p&l",
            "pl",
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
            "mapping",
            "tmp",
        ]

        def score(name: str) -> int:
            lower = name.lower()
            s = 0
            for term in preferred_terms:
                if term in lower:
                    s += 10
            for term in penalty_terms:
                if term in lower:
                    s -= 8
            if lower.startswith("_"):
                s -= 10
            return s

        return sorted(sheet_names, key=score, reverse=True)

    # ------------------------------------------------------------------
    # Preview collection — all scopes now config-driven; budget guard added
    # ------------------------------------------------------------------
    def _collect_sheet_previews(self, sheet_names: List[str], tools: Any) -> List[Dict[str, Any]]:
        """
        Collect cell-value, formula, and merged-cell previews for candidate sheets.

        Context-overflow guard
        ----------------------
        A running `total_chars` counter accumulates the rendered text length of
        every preview.  Once it exceeds `max_total_preview_chars` the loop stops
        — no more sheets are read.  This is the single mechanism that prevents
        excessive LLM context usage without using arbitrary hard row/col limits
        scattered throughout the code.

        Scope is controlled exclusively by config (WorkbookStructureAgentConfig)
        and the module-level _HARD_MAX_* ceilings applied in _resolve_effective_config.
        No magic numbers appear below.
        """
        eff = self._effective  # resolved, hard-capped config dict

        max_rows: int = eff["max_sheet_preview_rows"]
        max_cols: int = eff["max_sheet_preview_cols"]
        max_frows: int = eff["max_formula_preview_rows"]
        max_fcols: int = eff["max_formula_preview_cols"]
        max_merged: int = eff["max_merged_preview"]
        sample_limit: int = eff["preview_sample_sheets"]
        char_budget: int = eff["max_total_preview_chars"]

        previews: List[Dict[str, Any]] = []
        total_chars: int = 0

        for sheet_name in sheet_names[:sample_limit]:
            # ── safety check: stop adding sheets if budget already consumed ──
            if total_chars >= char_budget:
                logger.info(
                    "WorkbookStructure:preview_budget_reached "
                    "sheets_previewed=%d total_chars=%d budget=%d",
                    len(previews),
                    total_chars,
                    char_budget,
                )
                break

            grid: List[List[Any]] = []
            merged: List[Dict[str, Any]] = []
            formulas: List[List[Optional[str]]] = []

            try:
                if hasattr(tools, "excel_read_sheet_range"):
                    grid = tools.excel_read_sheet_range(
                        sheet_name=sheet_name,
                        row0=0,
                        col0=0,
                        nrows=max_rows,        # ← config-driven (was config, still config)
                        ncols=max_cols,        # ← config-driven (was config, still config)
                    )
                if hasattr(tools, "excel_detect_merged_cells"):
                    merged = tools.excel_detect_merged_cells(sheet_name=sheet_name)
                if hasattr(tools, "excel_get_formulas"):
                    formulas = tools.excel_get_formulas(
                        sheet_name=sheet_name,
                        row0=0,
                        col0=0,
                        nrows=max_frows,       # ← was: min(20, max_sheet_preview_rows) FIXED
                        ncols=max_fcols,       # ← was: min(16, max_sheet_preview_cols) FIXED
                    )
            except Exception:
                logger.exception("WorkbookStructure:preview_failed sheet=%s", sheet_name)

            grid_text = self._grid_to_text(grid, max_rows=max_rows, max_cols=max_cols)
            formula_text = self._formulas_to_text(
                formulas,
                max_rows=max_frows,   # ← was hardcoded 12  FIXED
                max_cols=max_fcols,   # ← was hardcoded 10  FIXED
            )

            preview_entry = {
                "sheet_name": sheet_name,
                "grid_preview": grid_text,
                "merged_preview": (
                    merged[:max_merged] if isinstance(merged, list) else []  # ← was hardcoded 20  FIXED
                ),
                "formula_preview": formula_text,
            }
            previews.append(preview_entry)

            # Accumulate character count for budget tracking
            total_chars += len(grid_text) + len(formula_text)

        return previews

    # ------------------------------------------------------------------
    # Prompt building
    # ------------------------------------------------------------------
    def _build_messages(
        self,
        sheet_names: List[str],
        previews: List[Dict[str, Any]],
        prompt_profile: Optional[StagePromptProfile] = None,
    ) -> tuple[str, str]:
        if prompt_profile is None:
            system_prompt = """
You are a senior Excel workbook structure analysis agent.

Your task is to analyze workbook structure conservatively.

Rules:
- Never guess.
- Prefer direct sheet, header, formula, and layout evidence over assumptions.
- Identify likely presentation sheets, entities, and workbook-level structure.
- Prefer top-level presentation sheets over SAP/GL/TB source sheets.
- If evidence is weak, lower confidence and return quality flags.
- Return valid JSON only.
""".strip()

            user_prompt = f"""
Analyze the workbook structure.

Available sheet names:
{sheet_names}

Sheet previews:
{json.dumps(previews, ensure_ascii=False, indent=2)}

Return JSON exactly:
{{
  "main_sheet_names": [],
  "contains": [],
  "entities": [
    {{
      "name": "",
      "currency": null,
      "confidence": 0.0,
      "evidence": []
    }}
  ],
  "has_consolidated": false,
  "consolidated_formula_pattern": "",
  "has_aje": false,
  "aje_types": [],
  "likely_units": null,
  "likely_current_period": null,
  "sheet_candidates": [
    {{
      "name": "",
      "kind": "unknown",
      "confidence": 0.0,
      "evidence": []
    }}
  ],
  "quality_flags": [],
  "confidence": 0.0
}}
""".strip()
            return system_prompt, user_prompt

        context_block = f"""
Available sheet names:
{sheet_names}

Sheet previews:
{json.dumps(previews, ensure_ascii=False, indent=2)}
""".strip()

        system_prompt = prompt_profile.compose_system_prompt()
        user_prompt = prompt_profile.compose_user_prompt(context_block)
        return system_prompt, user_prompt

    # ------------------------------------------------------------------
    # Fallback (no LLM)
    # ------------------------------------------------------------------
    def _fallback_structure(
        self,
        candidate_names: List[str],
        previews: List[Dict[str, Any]],
    ) -> WorkbookStructure:
        inferred_main = candidate_names[:2] if candidate_names else []

        sheet_candidates = [
            SheetCandidate(
                name=item["sheet_name"],
                kind=self._infer_sheet_kind(item["sheet_name"]),
                confidence=0.35 if i == 0 else 0.20,
                evidence=["heuristic_name_ranking"],
            )
            for i, item in enumerate(previews)
        ]

        flags = ["workbook_structure_fallback_used"]
        if not inferred_main:
            flags.append("main_sheets_unknown")

        return WorkbookStructure(
            main_sheet_names=inferred_main,
            contains=[],
            entities=[],
            has_consolidated=False,
            consolidated_formula_pattern="",
            has_aje=False,
            aje_types=[],
            likely_units=None,
            likely_current_period=None,
            sheet_candidates=sheet_candidates,
            quality_flags=flags,
            confidence=0.20 if inferred_main else 0.0,
        )

    def _infer_sheet_kind(self, sheet_name: str) -> str:
        s = sheet_name.lower()
        if any(x in s for x in ["fs", "financial", "balance", "p&l", "pnl", "income", "profit", "loss"]):
            return "presentation"
        if any(x in s for x in ["sap", "gl", "ledger", "tb", "trial"]):
            return "source"
        return "unknown"

    # ------------------------------------------------------------------
    # LLM response coercion
    # ------------------------------------------------------------------
    def _coerce_structure(self, data: Dict[str, Any]) -> WorkbookStructure:
        main_sheet_names_raw = data.get("main_sheet_names", [])
        if not isinstance(main_sheet_names_raw, list):
            single = data.get("main_sheet_name")
            if single not in (None, ""):
                main_sheet_names_raw = [single]
            else:
                main_sheet_names_raw = []

        main_sheet_names: List[str] = []
        seen_names = set()
        for item in main_sheet_names_raw:
            name = str(item).strip()
            if not name or name in seen_names:
                continue
            seen_names.add(name)
            main_sheet_names.append(name)

        contains = data.get("contains", [])
        if not isinstance(contains, list):
            contains = []
        contains = [str(x) for x in contains if str(x) in ("BS", "PL")]

        entities_raw = data.get("entities", [])
        entities: List[WorkbookEntity] = []
        if isinstance(entities_raw, list):
            for item in entities_raw:
                if not isinstance(item, dict):
                    continue
                name = str(item.get("name", "")).strip()
                if not name:
                    continue
                currency = item.get("currency")
                currency = None if currency in ("", None) else str(currency)
                try:
                    conf = float(item.get("confidence", 0.0))
                except Exception:
                    conf = 0.0
                evidence = item.get("evidence", [])
                if not isinstance(evidence, list):
                    evidence = []
                entities.append(
                    WorkbookEntity(
                        name=name,
                        currency=currency,
                        confidence=max(0.0, min(1.0, conf)),
                        evidence=[str(x) for x in evidence][:10],
                    )
                )

        has_consolidated = bool(data.get("has_consolidated", False))
        consolidated_formula_pattern = str(data.get("consolidated_formula_pattern", "") or "")
        has_aje = bool(data.get("has_aje", False))

        aje_types = data.get("aje_types", [])
        if not isinstance(aje_types, list):
            aje_types = []
        aje_types = [str(x) for x in aje_types if str(x).strip()]

        likely_units = data.get("likely_units")
        likely_units = None if likely_units in ("", None) else str(likely_units)

        likely_current_period = data.get("likely_current_period")
        likely_current_period = None if likely_current_period in ("", None) else str(likely_current_period)

        candidates_raw = data.get("sheet_candidates", [])
        candidates: List[SheetCandidate] = []
        if isinstance(candidates_raw, list):
            for item in candidates_raw:
                if not isinstance(item, dict):
                    continue
                name = str(item.get("name", "")).strip()
                if not name:
                    continue
                kind = str(item.get("kind", "unknown") or "unknown")
                try:
                    conf = float(item.get("confidence", 0.0))
                except Exception:
                    conf = 0.0
                evidence = item.get("evidence", [])
                if not isinstance(evidence, list):
                    evidence = []
                candidates.append(
                    SheetCandidate(
                        name=name,
                        kind=kind,
                        confidence=max(0.0, min(1.0, conf)),
                        evidence=[str(x) for x in evidence][:10],
                    )
                )

        quality_flags = data.get("quality_flags", [])
        if not isinstance(quality_flags, list):
            quality_flags = []
        quality_flags = [str(x) for x in quality_flags][:20]

        try:
            confidence = float(data.get("confidence", 0.0))
        except Exception:
            confidence = 0.0
        confidence = max(0.0, min(1.0, confidence))

        return WorkbookStructure(
            main_sheet_names=main_sheet_names,
            contains=contains,
            entities=entities,
            has_consolidated=has_consolidated,
            consolidated_formula_pattern=consolidated_formula_pattern,
            has_aje=has_aje,
            aje_types=aje_types,
            likely_units=likely_units,
            likely_current_period=likely_current_period,
            sheet_candidates=candidates,
            quality_flags=quality_flags,
            confidence=confidence,
        )

    # ------------------------------------------------------------------
    # Grid / formula renderers
    # ------------------------------------------------------------------
    def _grid_to_text(self, grid: List[List[Any]], max_rows: int, max_cols: int) -> str:
        if not isinstance(grid, list) or not grid:
            return ""

        lines: List[str] = []
        row_count = min(len(grid), max_rows)
        width = 0
        for r in range(row_count):
            row = grid[r] if isinstance(grid[r], list) else []
            width = max(width, len(row))
        col_count = min(width, max_cols)

        for r in range(row_count):
            row = grid[r] if r < len(grid) and isinstance(grid[r], list) else []
            cells: List[str] = []
            for c in range(col_count):
                value = row[c] if c < len(row) else ""
                s = "" if value is None else str(value)
                s = s.replace("\n", " ").replace("\r", " ").strip()
                if len(s) > 40:
                    s = s[:40] + "…"
                cells.append(s)
            lines.append(f"{r:02d} | " + " | ".join(cells))

        return "\n".join(lines)

    def _formulas_to_text(
        self,
        formulas: List[List[Optional[str]]],
        max_rows: int,
        max_cols: int,
    ) -> str:
        if not isinstance(formulas, list) or not formulas:
            return ""

        lines: List[str] = []
        row_count = min(len(formulas), max_rows)
        width = 0
        for r in range(row_count):
            row = formulas[r] if isinstance(formulas[r], list) else []
            width = max(width, len(row))
        col_count = min(width, max_cols)

        for r in range(row_count):
            row = formulas[r] if r < len(formulas) and isinstance(formulas[r], list) else []
            cells: List[str] = []
            for c in range(col_count):
                value = row[c] if c < len(row) else ""
                s = "" if value is None else str(value)
                s = s.replace("\n", " ").replace("\r", " ").strip()
                if len(s) > 50:
                    s = s[:50] + "…"
                cells.append(s)
            lines.append(f"{r:02d} | " + " | ".join(cells))

        return "\n".join(lines)

    # ------------------------------------------------------------------
    # JSON parsing
    # ------------------------------------------------------------------
    def _parse_json_loose(self, text: str) -> Optional[Dict[str, Any]]:
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