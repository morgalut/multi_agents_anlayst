# Multi_agen\packages\agents\workbook_structure_agent.py
from __future__ import annotations

import json
import logging
import re
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

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
# Token estimation  (conservative: 3.5 chars/token, rounded up)
# ---------------------------------------------------------------------------
_CHARS_PER_TOKEN: float = 3.5

def _est(text: str) -> int:
    return max(1, int(len(text) / _CHARS_PER_TOKEN) + 1)


# ---------------------------------------------------------------------------
# Hard safety ceilings
# ---------------------------------------------------------------------------
_HARD_MAX_ROWS: int             = 400
_HARD_MAX_COLS: int             = 60
_HARD_MAX_FORMULA_ROWS: int     = 120
_HARD_MAX_FORMULA_COLS: int     = 40
_HARD_MAX_MERGED: int           = 120
_HARD_MAX_SHEETS_COUNT: int     = 30
_HARD_MAX_SAMPLE_SHEETS: int    = 15
_HARD_MAX_STORE_CHARS: int      = 20_000
_HARD_MAX_CHUNK_TOKENS: int     = 6_000   # hard cap per LLM call
_HARD_MAX_TOTAL_TOKENS: int     = 40_000  # hard cap across entire analysis


@dataclass(frozen=True, slots=True)
class WorkbookStructureAgentConfig:
    """
    Chunked two-phase analysis config.

    Phase 1 — Per-sheet analysis
    ─────────────────────────────
    Each sheet preview is sent as an independent LLM call.
    Token budget per call: `chunk_token_limit` (default 3 000).
    This keeps every call well under TPM quota.

    Phase 2 — Merge
    ────────────────
    Compact summaries from Phase 1 (~100 tokens/sheet) are merged
    in a single tiny call to determine overall workbook structure.
    """
    llm_enabled: bool = True

    # Grid preview
    max_sheet_preview_rows: int = 60
    max_sheet_preview_cols: int = 18

    # Formula preview
    max_formula_preview_rows: int = 30
    max_formula_preview_cols: int = 12

    # Merged cells
    max_merged_preview: int = 30

    # Sheet scope
    max_sheet_count: int = 20
    preview_sample_sheets: int = 8

    # Cell truncation
    max_cell_chars: int = 28
    max_formula_chars: int = 35

    # ── PRIMARY QUOTA GUARD ──────────────────────────────────────────────────
    chunk_token_limit: int = 3_000
    """
    Maximum tokens per individual LLM call (Phase 1 per-sheet calls).
    Keep this at 3 000–4 000 to stay under Azure TPM quota.
    If a single sheet preview exceeds this, it is trimmed before sending.
    """

    inter_call_delay: float = 2.0
    """
    Seconds to sleep between Phase 1 calls to avoid burst TPM exhaustion.
    Increase to 5.0 if 429s still occur.
    """

    store_llm_raw_chars: int = 6_000


class WorkbookStructureAgent:
    """
    Two-phase chunked workbook analyser.

    Phase 1:  One LLM call per sheet  → compact SheetSummary dict
    Phase 2:  One merge call           → final WorkbookStructure JSON

    Why chunking works
    ──────────────────
    A single 21k-token call consumes the entire TPM budget in one shot.
    Eight 3k-token calls spread the load across time; with a small sleep
    between calls the TPM window (usually 60s) refills between requests.
    """

    # ── Phase 1 system prompt (reused for every sheet call) ──────────────────
    _SHEET_SYSTEM = (
        "You are an Excel sheet structure analyser. "
        "Analyse the single sheet preview provided and return ONLY compact JSON. "
        "Be concise — every token counts. Never guess. "
        "Return valid JSON only, no prose."
    )

    # ── Phase 2 system prompt ─────────────────────────────────────────────────
    _MERGE_SYSTEM = (
        "You are a senior Excel workbook structure agent. "
        "You receive compact per-sheet summaries and must determine the "
        "overall workbook structure. Return valid JSON only, no prose."
    )

    def __init__(
        self,
        llm: Optional[LLMClient] = None,
        config: Optional[WorkbookStructureAgentConfig] = None,
    ) -> None:
        self.llm    = llm
        self.config = config or WorkbookStructureAgentConfig()
        self._eff   = self._resolve_effective_config(self.config)

    def set_llm(self, llm: LLMClient) -> None:
        self.llm = llm

    # ------------------------------------------------------------------
    # Config resolution
    # ------------------------------------------------------------------
    @staticmethod
    def _resolve_effective_config(cfg: WorkbookStructureAgentConfig) -> Dict[str, Any]:
        return {
            "llm_enabled":              cfg.llm_enabled,
            "max_sheet_preview_rows":   min(cfg.max_sheet_preview_rows,   _HARD_MAX_ROWS),
            "max_sheet_preview_cols":   min(cfg.max_sheet_preview_cols,   _HARD_MAX_COLS),
            "max_formula_preview_rows": min(cfg.max_formula_preview_rows, _HARD_MAX_FORMULA_ROWS),
            "max_formula_preview_cols": min(cfg.max_formula_preview_cols, _HARD_MAX_FORMULA_COLS),
            "max_merged_preview":       min(cfg.max_merged_preview,       _HARD_MAX_MERGED),
            "max_sheet_count":          min(cfg.max_sheet_count,          _HARD_MAX_SHEETS_COUNT),
            "preview_sample_sheets":    min(cfg.preview_sample_sheets,    _HARD_MAX_SAMPLE_SHEETS),
            "store_llm_raw_chars":      min(cfg.store_llm_raw_chars,      _HARD_MAX_STORE_CHARS),
            "chunk_token_limit":        min(cfg.chunk_token_limit,        _HARD_MAX_CHUNK_TOKENS),
            "max_cell_chars":           max(10, cfg.max_cell_chars),
            "max_formula_chars":        max(10, cfg.max_formula_chars),
            "inter_call_delay":         max(0.0, cfg.inter_call_delay),
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

        # ── List sheets ───────────────────────────────────────────────────────
        try:
            sheet_names = self._list_sheets(tools)
        except Exception:
            logger.exception("WorkbookStructure:list_sheets_failed")
            return WorkbookStructure(quality_flags=["list_sheets_failed"], confidence=0.0)

        if not sheet_names:
            return WorkbookStructure(quality_flags=["no_sheets_found"], confidence=0.0)

        candidate_names = self._rank_candidate_sheets(sheet_names)[: self._eff["max_sheet_count"]]

        # ── No LLM path ───────────────────────────────────────────────────────
        if not self._eff["llm_enabled"] or self.llm is None:
            logger.info("WorkbookStructure:llm_disabled_or_missing")
            previews = self._collect_sheet_previews(candidate_names, tools)
            structure = self._fallback_structure(candidate_names, previews)
            self._attach_provenance(state, structure, matched_by="fallback_no_llm")
            return structure

        # ── Phase 1: per-sheet analysis ───────────────────────────────────────
        sheet_summaries = self._phase1_analyze_sheets(
            candidate_names=candidate_names,
            tools=tools,
            prompt_profile=prompt_profile,
        )

        # ── Phase 2: merge ────────────────────────────────────────────────────
        try:
            structure = self._phase2_merge(
                sheet_names=sheet_names,
                sheet_summaries=sheet_summaries,
                prompt_profile=prompt_profile,
            )
        except Exception as exc:
            logger.exception("WorkbookStructure:merge_failed err=%s", type(exc).__name__)
            structure = self._fallback_from_summaries(candidate_names, sheet_summaries)
            structure = WorkbookStructure(
                **{**structure.__dict__,
                   "quality_flags": structure.quality_flags + [f"merge_error:{type(exc).__name__}"],
                   "confidence": 0.0}
            )

        self._attach_provenance(state, structure, matched_by="llm_chunked_workbook_structure")

        elapsed_ms = (time.perf_counter() - t0) * 1000
        logger.info(
            "WorkbookStructure:done main_sheets=%s conf=%.3f elapsed_ms=%.0f",
            structure.main_sheet_names, structure.confidence, elapsed_ms,
        )
        return structure

    # ------------------------------------------------------------------
    # Phase 1 — one LLM call per sheet
    # ------------------------------------------------------------------
    def _phase1_analyze_sheets(
        self,
        candidate_names: List[str],
        tools: Any,
        prompt_profile: Optional[StagePromptProfile],
    ) -> List[Dict[str, Any]]:
        """
        For each candidate sheet:
          1. Collect a token-trimmed preview
          2. Call LLM with a focused single-sheet prompt
          3. Parse response into a compact SheetSummary dict

        Inter-call delay prevents burst TPM exhaustion.
        If a sheet call fails, a heuristic fallback summary is used —
        the pipeline never aborts due to a single sheet failure.
        """
        eff           = self._eff
        sample_limit  = eff["preview_sample_sheets"]
        chunk_limit   = eff["chunk_token_limit"]
        delay         = eff["inter_call_delay"]
        summaries: List[Dict[str, Any]] = []

        for i, sheet_name in enumerate(candidate_names[:sample_limit]):
            t_sheet = time.perf_counter()

            # ── Collect and trim preview to chunk_limit tokens ────────────────
            preview = self._collect_one_sheet_preview(sheet_name, tools)
            grid_text    = preview["grid_preview"]
            formula_text = preview["formula_preview"]
            merged       = preview["merged_preview"]

            # Trim texts if combined exceeds chunk budget
            grid_text, formula_text = self._trim_to_token_budget(
                grid_text, formula_text, chunk_limit
            )

            preview_tokens = _est(grid_text) + _est(formula_text)
            logger.info(
                "WorkbookStructure:phase1  sheet=%r  tokens~=%d  budget=%d",
                sheet_name, preview_tokens, chunk_limit,
            )

            # ── Build single-sheet prompt ─────────────────────────────────────
            user_prompt = self._build_sheet_prompt(
                sheet_name=sheet_name,
                grid_text=grid_text,
                formula_text=formula_text,
                merged=merged,
            )

            # ── LLM call with fallback ────────────────────────────────────────
            summary = self._call_sheet_llm(sheet_name, user_prompt)
            summary["sheet_name"]     = sheet_name
            summary["_tokens_used"]   = preview_tokens
            summaries.append(summary)

            elapsed = (time.perf_counter() - t_sheet) * 1000
            logger.info(
                "WorkbookStructure:phase1_done  sheet=%r  kind=%s  elapsed_ms=%.0f",
                sheet_name, summary.get("kind", "?"), elapsed,
            )

            # Throttle between calls — except after the last one
            if i < sample_limit - 1 and i < len(candidate_names) - 1:
                time.sleep(delay)

        total_tokens = sum(s.get("_tokens_used", 0) for s in summaries)
        logger.info(
            "WorkbookStructure:phase1_complete  sheets=%d  total_tokens~=%d",
            len(summaries), total_tokens,
        )
        return summaries

    def _call_sheet_llm(self, sheet_name: str, user_prompt: str) -> Dict[str, Any]:
        """Call LLM for a single sheet. Returns parsed dict or heuristic fallback."""
        try:
            result = self.llm.chat([
                LLMMessage(role="system", content=self._SHEET_SYSTEM),
                LLMMessage(role="user",   content=user_prompt),
            ])
            raw  = (getattr(result, "text", "") or "").strip()
            data = self._parse_json_loose(raw)
            if isinstance(data, dict):
                return data
            logger.warning(
                "WorkbookStructure:phase1_bad_json  sheet=%r  raw=%r",
                sheet_name, raw[:120],
            )
        except Exception:
            logger.exception(
                "WorkbookStructure:phase1_llm_error  sheet=%r", sheet_name
            )

        # Heuristic fallback for this sheet
        return {
            "kind":       self._infer_sheet_kind(sheet_name),
            "confidence": 0.15,
            "evidence":   ["heuristic_fallback"],
            "has_bs":     False,
            "has_pl":     False,
            "entities":   [],
            "currency":   None,
            "period":     None,
            "units":      None,
            "has_aje":    False,
            "is_consolidated": False,
        }

    def _build_sheet_prompt(
        self,
        sheet_name: str,
        grid_text: str,
        formula_text: str,
        merged: List[Any],
    ) -> str:
        merged_str = json.dumps(merged[:10], ensure_ascii=False) if merged else "[]"
        return (
            f'Sheet: "{sheet_name}"\n\n'
            f"Grid (row|col values):\n{grid_text or '(empty)'}\n\n"
            f"Formulas:\n{formula_text or '(none)'}\n\n"
            f"Merged cells (sample): {merged_str}\n\n"
            f"Return compact JSON:\n"
            f'{{"kind":"presentation|source|calc|unknown",'
            f'"confidence":0.0,'
            f'"has_bs":false,"has_pl":false,'
            f'"entities":[],'
            f'"currency":null,"period":null,"units":null,'
            f'"has_aje":false,"is_consolidated":false,'
            f'"evidence":[]}}'
        )

    # ------------------------------------------------------------------
    # Phase 2 — merge summaries into final WorkbookStructure
    # ------------------------------------------------------------------
    def _phase2_merge(
        self,
        sheet_names: List[str],
        sheet_summaries: List[Dict[str, Any]],
        prompt_profile: Optional[StagePromptProfile],
    ) -> WorkbookStructure:
        """
        Send compact per-sheet summaries (~100 tokens/sheet) to the LLM
        and ask it to determine the overall workbook structure.

        Typical merge prompt size: 8 sheets × 100 tokens = ~800 tokens.
        This is ~25× smaller than the original single-shot approach.
        """
        # Strip internal _tokens_used before sending to LLM
        clean = [{k: v for k, v in s.items() if not k.startswith("_")}
                 for s in sheet_summaries]

        merge_tokens = _est(json.dumps(clean))
        logger.info(
            "WorkbookStructure:phase2_merge  sheets=%d  merge_tokens~=%d",
            len(clean), merge_tokens,
        )

        user_prompt = (
            f"All sheet names in workbook: {sheet_names}\n\n"
            f"Per-sheet analysis results:\n"
            f"{json.dumps(clean, ensure_ascii=False, indent=2)}\n\n"
            f"Determine overall workbook structure. Return JSON exactly:\n"
            f'{{\n'
            f'  "main_sheet_names": [],\n'
            f'  "contains": [],\n'
            f'  "entities": [{{"name":"","currency":null,"confidence":0.0,"evidence":[]}}],\n'
            f'  "has_consolidated": false,\n'
            f'  "consolidated_formula_pattern": "",\n'
            f'  "has_aje": false,\n'
            f'  "aje_types": [],\n'
            f'  "likely_units": null,\n'
            f'  "likely_current_period": null,\n'
            f'  "sheet_candidates": [{{"name":"","kind":"unknown","confidence":0.0,"evidence":[]}}],\n'
            f'  "quality_flags": [],\n'
            f'  "confidence": 0.0\n'
            f'}}'
        )

        result = self.llm.chat([
            LLMMessage(role="system", content=self._MERGE_SYSTEM),
            LLMMessage(role="user",   content=user_prompt),
        ])

        raw  = (getattr(result, "text", "") or "").strip()
        data = self._parse_json_loose(raw)

        if not isinstance(data, dict):
            logger.warning("WorkbookStructure:phase2_bad_json  raw=%r", raw[:200])
            return self._fallback_from_summaries(
                [s["sheet_name"] for s in sheet_summaries], sheet_summaries
            )

        structure = self._coerce_structure(data)
        return WorkbookStructure(
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
            raw_llm_text=raw[: self._eff["store_llm_raw_chars"]],
        )

    # ------------------------------------------------------------------
    # Token-aware trimming
    # ------------------------------------------------------------------
    def _trim_to_token_budget(
        self,
        grid_text: str,
        formula_text: str,
        budget: int,
    ) -> Tuple[str, str]:
        """
        Trim grid and formula texts so their combined token estimate
        stays within `budget`.  Grid is prioritised; formulas are trimmed
        or dropped first if budget is tight.

        Strategy:
          1. Give formulas at most 25% of budget
          2. Give grid the rest (up to 75%)
          3. If grid alone exceeds 75%, trim by dropping tail rows
        """
        formula_budget = int(budget * 0.25)
        grid_budget    = budget - formula_budget

        formula_text = self._trim_text_to_tokens(formula_text, formula_budget)
        grid_text    = self._trim_text_to_tokens(grid_text,    grid_budget)

        actual = _est(grid_text) + _est(formula_text)
        if actual > budget:
            # Final safety trim — drop more grid rows
            grid_text = self._trim_text_to_tokens(grid_text, budget - _est(formula_text))

        logger.debug(
            "WorkbookStructure:trim  grid_tokens=%d  formula_tokens=%d  budget=%d",
            _est(grid_text), _est(formula_text), budget,
        )
        return grid_text, formula_text

    @staticmethod
    def _trim_text_to_tokens(text: str, token_limit: int) -> str:
        """Trim text by dropping tail lines until it fits within token_limit."""
        if not text or _est(text) <= token_limit:
            return text
        char_limit = int(token_limit * _CHARS_PER_TOKEN)
        # Trim at a line boundary to avoid cutting mid-row
        trimmed = text[:char_limit]
        last_nl = trimmed.rfind("\n")
        if last_nl > 0:
            trimmed = trimmed[:last_nl]
        return trimmed + "\n…(trimmed)"

    # ------------------------------------------------------------------
    # Single-sheet preview collector
    # ------------------------------------------------------------------
    def _collect_one_sheet_preview(
        self, sheet_name: str, tools: Any
    ) -> Dict[str, Any]:
        eff = self._eff
        grid: List[List[Any]]           = []
        merged: List[Dict[str, Any]]     = []
        formulas: List[List[Optional[str]]] = []

        try:
            if hasattr(tools, "excel_read_sheet_range"):
                grid = tools.excel_read_sheet_range(
                    sheet_name=sheet_name, row0=0, col0=0,
                    nrows=eff["max_sheet_preview_rows"],
                    ncols=eff["max_sheet_preview_cols"],
                )
            if hasattr(tools, "excel_detect_merged_cells"):
                merged = tools.excel_detect_merged_cells(sheet_name=sheet_name)
            if hasattr(tools, "excel_get_formulas"):
                formulas = tools.excel_get_formulas(
                    sheet_name=sheet_name, row0=0, col0=0,
                    nrows=eff["max_formula_preview_rows"],
                    ncols=eff["max_formula_preview_cols"],
                )
        except Exception:
            logger.exception("WorkbookStructure:preview_failed sheet=%s", sheet_name)

        grid_text    = self._grid_to_text(
            grid,
            max_rows=eff["max_sheet_preview_rows"],
            max_cols=eff["max_sheet_preview_cols"],
            max_cell=eff["max_cell_chars"],
        )
        formula_text = self._formulas_to_text(
            formulas,
            max_rows=eff["max_formula_preview_rows"],
            max_cols=eff["max_formula_preview_cols"],
            max_cell=eff["max_formula_chars"],
        )

        return {
            "sheet_name":      sheet_name,
            "grid_preview":    grid_text,
            "merged_preview":  merged[: eff["max_merged_preview"]] if isinstance(merged, list) else [],
            "formula_preview": formula_text,
        }

    # Legacy bulk collector — kept for fallback path only
    def _collect_sheet_previews(
        self, sheet_names: List[str], tools: Any
    ) -> List[Dict[str, Any]]:
        return [
            self._collect_one_sheet_preview(n, tools)
            for n in sheet_names[: self._eff["preview_sample_sheets"]]
        ]

    # ------------------------------------------------------------------
    # Provenance
    # ------------------------------------------------------------------
    def _attach_provenance(self, state: Any, structure: WorkbookStructure, *, matched_by: str) -> None:
        try:
            setattr(state, "workbook_structure_provenance", {
                "main_sheet_names": list(structure.main_sheet_names),
                "contains":         list(structure.contains),
                "matched_by":       matched_by,
                "source_code": {
                    "file": "Multi_agen/packages/agents/workbook_structure_agent.py",
                },
            })
        except Exception:
            logger.exception("WorkbookStructure:failed attaching provenance")

    # ------------------------------------------------------------------
    # Sheet listing & ranking
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
            for key in ("sheets", "sheet_names"):
                if isinstance(raw.get(key), list):
                    return [str(x) for x in raw[key]]
        return []

    def _rank_candidate_sheets(self, sheet_names: List[str]) -> List[str]:
        preferred = ["fs","financial","statements","statement","bs","balance",
                     "p&l","pl","pnl","income","profit","loss","consolidated"]
        penalty   = ["sap","gl","ledger","trial","tb","dump","raw","data","mapping","tmp"]

        def score(name: str) -> int:
            lo = name.lower()
            return (sum(10 for t in preferred if t in lo)
                    - sum(8  for t in penalty  if t in lo)
                    - (10 if lo.startswith("_") else 0))

        return sorted(sheet_names, key=score, reverse=True)

    # ------------------------------------------------------------------
    # Fallbacks
    # ------------------------------------------------------------------
    def _fallback_structure(
        self, candidate_names: List[str], previews: List[Dict[str, Any]]
    ) -> WorkbookStructure:
        inferred_main = candidate_names[:2] if candidate_names else []
        sheet_candidates = [
            SheetCandidate(
                name=p["sheet_name"],
                kind=self._infer_sheet_kind(p["sheet_name"]),
                confidence=0.35 if i == 0 else 0.20,
                evidence=["heuristic_name_ranking"],
            )
            for i, p in enumerate(previews)
        ]
        flags = ["workbook_structure_fallback_used"]
        if not inferred_main:
            flags.append("main_sheets_unknown")
        return WorkbookStructure(
            main_sheet_names=inferred_main, contains=[], entities=[],
            has_consolidated=False, consolidated_formula_pattern="",
            has_aje=False, aje_types=[], likely_units=None, likely_current_period=None,
            sheet_candidates=sheet_candidates, quality_flags=flags,
            confidence=0.20 if inferred_main else 0.0,
        )

    def _fallback_from_summaries(
        self,
        candidate_names: List[str],
        summaries: List[Dict[str, Any]],
    ) -> WorkbookStructure:
        """Build a best-effort WorkbookStructure from Phase 1 summaries alone."""
        main = [
            s["sheet_name"] for s in summaries
            if s.get("kind") == "presentation"
        ][:3]
        if not main:
            main = candidate_names[:2]

        contains = []
        if any(s.get("has_bs") for s in summaries):
            contains.append("BS")
        if any(s.get("has_pl") for s in summaries):
            contains.append("PL")

        sheet_candidates = [
            SheetCandidate(
                name=s["sheet_name"],
                kind=s.get("kind", "unknown"),
                confidence=float(s.get("confidence", 0.20)),
                evidence=s.get("evidence", ["phase1_llm"]),
            )
            for s in summaries
        ]

        return WorkbookStructure(
            main_sheet_names=main,
            contains=contains,
            entities=[],
            has_consolidated=any(s.get("is_consolidated") for s in summaries),
            consolidated_formula_pattern="",
            has_aje=any(s.get("has_aje") for s in summaries),
            aje_types=[],
            likely_units=next((s.get("units") for s in summaries if s.get("units")), None),
            likely_current_period=next((s.get("period") for s in summaries if s.get("period")), None),
            sheet_candidates=sheet_candidates,
            quality_flags=["phase2_merge_skipped", "workbook_structure_fallback_used"],
            confidence=0.30 if main else 0.0,
        )

    def _infer_sheet_kind(self, name: str) -> str:
        s = name.lower()
        if any(x in s for x in ["fs","financial","balance","p&l","pnl","income","profit","loss"]):
            return "presentation"
        if any(x in s for x in ["sap","gl","ledger","tb","trial"]):
            return "source"
        return "unknown"

    # ------------------------------------------------------------------
    # Grid / formula renderers
    # ------------------------------------------------------------------
    def _grid_to_text(
        self, grid: List[List[Any]],
        max_rows: int, max_cols: int, max_cell: int = 28,
    ) -> str:
        if not isinstance(grid, list) or not grid:
            return ""
        row_count = min(len(grid), max_rows)
        width     = max(
            (len(grid[r]) if isinstance(grid[r], list) else 0 for r in range(row_count)),
            default=0,
        )
        col_count = min(width, max_cols)
        lines: List[str] = []
        for r in range(row_count):
            row = grid[r] if r < len(grid) and isinstance(grid[r], list) else []
            cells = []
            for c in range(col_count):
                v = row[c] if c < len(row) else ""
                s = "" if v is None else str(v)
                s = s.replace("\n", " ").replace("\r", " ").strip()
                cells.append((s[:max_cell] + "…") if len(s) > max_cell else s)
            lines.append(f"{r:02d}|" + "|".join(cells))
        return "\n".join(lines)

    def _formulas_to_text(
        self, formulas: List[List[Optional[str]]],
        max_rows: int, max_cols: int, max_cell: int = 35,
    ) -> str:
        if not isinstance(formulas, list) or not formulas:
            return ""
        row_count = min(len(formulas), max_rows)
        width     = max(
            (len(formulas[r]) if isinstance(formulas[r], list) else 0 for r in range(row_count)),
            default=0,
        )
        col_count = min(width, max_cols)
        lines: List[str] = []
        for r in range(row_count):
            row = formulas[r] if r < len(formulas) and isinstance(formulas[r], list) else []
            cells = []
            for c in range(col_count):
                v = row[c] if c < len(row) else ""
                s = "" if v is None else str(v)
                s = s.replace("\n", " ").replace("\r", " ").strip()
                cells.append((s[:max_cell] + "…") if len(s) > max_cell else s)
            if any(cells):   # skip entirely empty formula rows
                lines.append(f"{r:02d}|" + "|".join(cells))
        return "\n".join(lines)

    # ------------------------------------------------------------------
    # JSON parsing
    # ------------------------------------------------------------------
    def _parse_json_loose(self, text: str) -> Optional[Dict[str, Any]]:
        if not text:
            return None
        fenced = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, flags=re.DOTALL | re.IGNORECASE)
        if fenced:
            try: return json.loads(fenced.group(1).strip())
            except: return None
        if text.startswith("{") and text.endswith("}"):
            try: return json.loads(text)
            except: pass
        candidate = re.search(r"(\{.*\})", text, flags=re.DOTALL)
        if candidate:
            try: return json.loads(candidate.group(1).strip())
            except: return None
        return None

    # ------------------------------------------------------------------
    # LLM response coercion (unchanged)
    # ------------------------------------------------------------------
    def _coerce_structure(self, data: Dict[str, Any]) -> WorkbookStructure:
        raw = data.get("main_sheet_names", [])
        if not isinstance(raw, list):
            single = data.get("main_sheet_name")
            raw = [single] if single not in (None, "") else []
        seen: set = set()
        main_sheet_names: List[str] = []
        for item in raw:
            n = str(item).strip()
            if n and n not in seen:
                seen.add(n); main_sheet_names.append(n)

        contains_raw = data.get("contains", [])
        contains = [str(x) for x in (contains_raw if isinstance(contains_raw, list) else [])
                    if str(x) in ("BS", "PL")]

        entities: List[WorkbookEntity] = []
        for item in (data.get("entities", []) or []):
            if not isinstance(item, dict): continue
            name = str(item.get("name", "")).strip()
            if not name: continue
            currency = item.get("currency")
            currency = None if currency in ("", None) else str(currency)
            try:    conf = float(item.get("confidence", 0.0))
            except: conf = 0.0
            ev = item.get("evidence", [])
            entities.append(WorkbookEntity(
                name=name, currency=currency,
                confidence=max(0.0, min(1.0, conf)),
                evidence=[str(x) for x in (ev if isinstance(ev, list) else [])][:10],
            ))

        aje_types_raw = data.get("aje_types", [])
        aje_types = [str(x) for x in (aje_types_raw if isinstance(aje_types_raw, list) else [])
                     if str(x).strip()]

        candidates: List[SheetCandidate] = []
        for item in (data.get("sheet_candidates", []) or []):
            if not isinstance(item, dict): continue
            name = str(item.get("name", "")).strip()
            if not name: continue
            try:    conf = float(item.get("confidence", 0.0))
            except: conf = 0.0
            ev = item.get("evidence", [])
            candidates.append(SheetCandidate(
                name=name,
                kind=str(item.get("kind", "unknown") or "unknown"),
                confidence=max(0.0, min(1.0, conf)),
                evidence=[str(x) for x in (ev if isinstance(ev, list) else [])][:10],
            ))

        quality_flags = [str(x) for x in (data.get("quality_flags", []) or [])][:20]
        try:    confidence = float(data.get("confidence", 0.0))
        except: confidence = 0.0

        likely_units          = data.get("likely_units")
        likely_units          = None if likely_units in ("", None) else str(likely_units)
        likely_current_period = data.get("likely_current_period")
        likely_current_period = None if likely_current_period in ("", None) else str(likely_current_period)

        return WorkbookStructure(
            main_sheet_names=main_sheet_names,
            contains=contains, entities=entities,
            has_consolidated=bool(data.get("has_consolidated", False)),
            consolidated_formula_pattern=str(data.get("consolidated_formula_pattern", "") or ""),
            has_aje=bool(data.get("has_aje", False)),
            aje_types=aje_types,
            likely_units=likely_units, likely_current_period=likely_current_period,
            sheet_candidates=candidates,
            quality_flags=quality_flags,
            confidence=max(0.0, min(1.0, confidence)),
        )
