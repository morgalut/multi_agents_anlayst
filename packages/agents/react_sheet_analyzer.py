from __future__ import annotations

import json
import logging
import re
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from Multi_agen.packages.llm import LLMClient, LLMMessage
from Multi_agen.packages.llm.prompts import SYSTEM_PROMPT, build_sheet_analysis_prompt


logger = logging.getLogger("multi_agen.agents.react_sheet_analyzer")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)


# -----------------------------
# Data objects
# -----------------------------

@dataclass(frozen=True, slots=True)
class SheetClassification:
    """
    types: ["BS"], ["PL"], or ["BS","PL"]
    """
    types: List[str] = field(default_factory=list)
    confidence: float = 0.0
    evidence: List[str] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class SheetAnalysis:
    classification: SheetClassification
    observations: Dict[str, Any] = field(default_factory=dict)
    signals: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ReActAnalyzerConfig:
    # top window to read from Excel sheet
    top_rows: int = 200
    top_cols: int = 30

    # gate for turning LLM on/off
    llm_enabled: bool = True

    # safety: don't keep huge raw text
    store_llm_raw_chars: int = 800

    # debug-only preview (avoid giant logs)
    log_grid_preview_rows: int = 5
    log_grid_preview_cols: int = 8


# -----------------------------
# Analyzer
# -----------------------------

class ReActSheetAnalyzer:
    """
    Observe/Think/Act/Reflect.

    - OBSERVE: Read top grid + merged cells (Excel MCP tools)
    - THINK: Use LLM to infer classification + role candidates (JSON contract)
    - SAFE: If LLM fails or output invalid -> confidence 0 -> downstream outputs "no"
    """

    def __init__(self, llm: Optional[LLMClient] = None, config: Optional[ReActAnalyzerConfig] = None):
        self.config = config or ReActAnalyzerConfig()
        self.llm = llm

    # Optional injection pattern (ORC can call this)
    def set_llm(self, llm: LLMClient) -> None:
        self.llm = llm

    def analyze(self, task, state, tools) -> SheetAnalysis:
        t0 = time.perf_counter()
        logger.info("ReAct:start sheet=%s row_index=%s", task.sheet_name, task.row_index)

        # -----------------------------
        # OBSERVE
        # -----------------------------
        try:
            grid = tools.excel_read_sheet_range(
                sheet_name=task.sheet_name,
                row0=0,
                col0=0,
                nrows=self.config.top_rows,
                ncols=self.config.top_cols,
            )
            merged = tools.excel_detect_merged_cells(sheet_name=task.sheet_name)
        except Exception:
            logger.exception("ReAct:observe_failed sheet=%s", task.sheet_name)
            classification = SheetClassification(types=[], confidence=0.0, evidence=["observe_failed"])
            return SheetAnalysis(classification=classification, observations={}, signals={})

        observations = {"top_grid": grid, "merged_cells": merged}
        self._log_grid_preview(task.sheet_name, grid)

        # -----------------------------
        # LLM gate
        # -----------------------------
        if not self.config.llm_enabled or self.llm is None:
            logger.info("ReAct:llm_disabled_or_missing sheet=%s", task.sheet_name)
            classification = SheetClassification(types=[], confidence=0.0, evidence=["llm_disabled_or_missing"])
            return SheetAnalysis(classification=classification, observations=observations, signals={})

        # -----------------------------
        # THINK (LLM)
        # -----------------------------
        prompt = build_sheet_analysis_prompt(task.sheet_name, grid, merged)

        logger.info("ReAct:llm_call sheet=%s", task.sheet_name)
        llm_t0 = time.perf_counter()

        try:
            result = self.llm.chat(
                [
                    LLMMessage(role="system", content=SYSTEM_PROMPT),
                    LLMMessage(role="user", content=prompt),
                ]
            )
        except Exception as e:
            logger.exception("ReAct:llm_failed sheet=%s err=%s", task.sheet_name, type(e).__name__)
            classification = SheetClassification(types=[], confidence=0.0, evidence=[f"llm_error:{type(e).__name__}"])
            return SheetAnalysis(classification=classification, observations=observations, signals={})

        logger.info(
            "ReAct:llm_ok sheet=%s model=%s elapsed_ms=%.2f",
            task.sheet_name,
            getattr(result, "model", "unknown"),
            (time.perf_counter() - llm_t0) * 1000,
        )

        # -----------------------------
        # Parse JSON (robust)
        # -----------------------------
        raw_text = (getattr(result, "text", "") or "").strip()
        data = self._parse_json_loose(raw_text)

        if data is None or not isinstance(data, dict):
            logger.warning("ReAct:llm_invalid_json sheet=%s", task.sheet_name)
            classification = SheetClassification(types=[], confidence=0.0, evidence=["llm_invalid_json"])
            return SheetAnalysis(
                classification=classification,
                observations=observations,
                signals={
                    "llm_model": getattr(result, "model", "unknown"),
                    "llm_raw": raw_text[: self.config.store_llm_raw_chars],
                },
            )

        # -----------------------------
        # Extract classification safely
        # -----------------------------
        c = data.get("classification", {})
        if not isinstance(c, dict):
            c = {}

        types = c.get("types", [])
        if not isinstance(types, list):
            types = []

        conf_val = c.get("confidence", 0.0)
        try:
            conf = float(conf_val)
        except Exception:
            conf = 0.0

        ev = c.get("evidence", [])
        if not isinstance(ev, list):
            ev = []

        classification = SheetClassification(
            types=[t for t in types if t in ("BS", "PL")],
            confidence=max(0.0, min(1.0, conf)),
            evidence=[str(x) for x in ev][:10],
        )

        roles = data.get("roles", {})
        if not isinstance(roles, dict):
            roles = {}

        qf = data.get("quality_flags", [])
        if not isinstance(qf, list):
            qf = []

        elapsed_ms = (time.perf_counter() - t0) * 1000
        logger.info(
            "ReAct:done sheet=%s types=%s conf=%.3f elapsed_ms=%.2f",
            task.sheet_name,
            classification.types,
            classification.confidence,
            elapsed_ms,
        )

        return SheetAnalysis(
            classification=classification,
            observations=observations,
            signals={
                "llm_model": getattr(result, "model", "unknown"),
                "llm_usage": getattr(result, "usage", {}),
                "llm_roles": roles,
                "quality_flags": qf,
            },
        )

    # -----------------------------
    # Helpers
    # -----------------------------

    def _log_grid_preview(self, sheet_name: str, grid: List[List[Any]]) -> None:
        # debug-only preview, never fail analysis because of logging
        try:
            rmax = min(len(grid), self.config.log_grid_preview_rows)
            cmax = min(len(grid[0]) if grid else 0, self.config.log_grid_preview_cols)

            preview: List[List[str]] = []
            for r in range(rmax):
                row = grid[r] if r < len(grid) else []
                cells: List[str] = []
                for c in range(cmax):
                    v = row[c] if c < len(row) else ""
                    s = "" if v is None else str(v)
                    s = s.replace("\n", " ").replace("\r", " ").strip()
                    if len(s) > 30:
                        s = s[:30] + "…"
                    cells.append(s)
                preview.append(cells)

            logger.debug("ReAct:grid_preview sheet=%s preview=%s", sheet_name, preview)
        except Exception:
            pass

    def _parse_json_loose(self, text: str) -> Optional[Dict[str, Any]]:
        """
        Robust parsing:
          - supports fenced blocks: ```json { ... } ```
          - supports raw JSON: { ... }
          - extracts first {...} if extra text exists
        """
        if not text:
            return None

        # fenced JSON
        m = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, flags=re.DOTALL | re.IGNORECASE)
        if m:
            try:
                return json.loads(m.group(1).strip())
            except Exception:
                return None

        # raw JSON object
        if text.startswith("{") and text.endswith("}"):
            try:
                return json.loads(text)
            except Exception:
                pass

        # best-effort: first {...}
        m2 = re.search(r"(\{.*\})", text, flags=re.DOTALL)
        if m2:
            try:
                return json.loads(m2.group(1).strip())
            except Exception:
                return None

        return None