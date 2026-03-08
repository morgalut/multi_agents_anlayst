from __future__ import annotations

import json
import logging
import re
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from Multi_agen.packages.llm import LLMClient, LLMMessage
from Multi_agen.packages.llm.prompts import build_sheet_analysis_messages
from Multi_agen.packages.llm.stage_prompts import StagePromptProfile


logger = logging.getLogger("multi_agen.agents.react_sheet_analyzer")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)


@dataclass(frozen=True, slots=True)
class SheetClassification:
    """
    types: ["BS"], ["PL"], or ["BS", "PL"]
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
    top_rows: int = 200
    top_cols: int = 30
    llm_enabled: bool = True
    store_llm_raw_chars: int = 1200
    log_grid_preview_rows: int = 5
    log_grid_preview_cols: int = 8


class ReActSheetAnalyzer:
    """
    Observe / Think / Act / Reflect analyzer.

    - OBSERVE: read top grid + merged cells
    - THINK: use stage-specific prompt contract
    - SAFE: on failure, return low-confidence empty classification
    """

    def __init__(
        self,
        llm: Optional[LLMClient] = None,
        config: Optional[ReActAnalyzerConfig] = None,
    ) -> None:
        self.config = config or ReActAnalyzerConfig()
        self.llm = llm

    def set_llm(self, llm: LLMClient) -> None:
        self.llm = llm

    def analyze(
        self,
        task: Any,
        state: Any,
        tools: Any,
        prompt_profile: Optional[StagePromptProfile] = None,
    ) -> SheetAnalysis:
        t0 = time.perf_counter()
        logger.info("ReAct:start sheet=%s row_index=%s", task.sheet_name, task.row_index)

        # OBSERVE
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

        observations = {
            "top_grid": grid,
            "merged_cells": merged,
        }
        self._log_grid_preview(task.sheet_name, grid)

        if not self.config.llm_enabled or self.llm is None:
            logger.info("ReAct:llm_disabled_or_missing sheet=%s", task.sheet_name)
            classification = SheetClassification(types=[], confidence=0.0, evidence=["llm_disabled_or_missing"])
            return SheetAnalysis(classification=classification, observations=observations, signals={})

        system_prompt, user_prompt = build_sheet_analysis_messages(
            sheet_name=task.sheet_name,
            grid=grid,
            merged_ranges=merged,
            prompt_profile=prompt_profile,
        )

        logger.info("ReAct:llm_call sheet=%s", task.sheet_name)
        llm_t0 = time.perf_counter()

        try:
            result = self.llm.chat(
                [
                    LLMMessage(role="system", content=system_prompt),
                    LLMMessage(role="user", content=user_prompt),
                ]
            )
        except Exception as exc:
            logger.exception("ReAct:llm_failed sheet=%s err=%s", task.sheet_name, type(exc).__name__)
            classification = SheetClassification(
                types=[],
                confidence=0.0,
                evidence=[f"llm_error:{type(exc).__name__}"],
            )
            return SheetAnalysis(classification=classification, observations=observations, signals={})

        logger.info(
            "ReAct:llm_ok sheet=%s model=%s elapsed_ms=%.2f",
            task.sheet_name,
            getattr(result, "model", "unknown"),
            (time.perf_counter() - llm_t0) * 1000,
        )

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

        classification = self._extract_classification(data)
        roles = data.get("roles", {})
        if not isinstance(roles, dict):
            roles = {}

        quality_flags = data.get("quality_flags", [])
        if not isinstance(quality_flags, list):
            quality_flags = []

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
                "quality_flags": quality_flags,
                "llm_raw": raw_text[: self.config.store_llm_raw_chars],
            },
        )

    def _extract_classification(self, data: Dict[str, Any]) -> SheetClassification:
        c = data.get("classification", {})
        if not isinstance(c, dict):
            c = {}

        raw_types = c.get("types", [])
        if not isinstance(raw_types, list):
            raw_types = []

        valid_types = [t for t in raw_types if t in ("BS", "PL")]

        conf_val = c.get("confidence", 0.0)
        try:
            conf = float(conf_val)
        except Exception:
            conf = 0.0

        raw_evidence = c.get("evidence", [])
        if not isinstance(raw_evidence, list):
            raw_evidence = []

        evidence = [str(x) for x in raw_evidence][:10]

        return SheetClassification(
            types=valid_types,
            confidence=max(0.0, min(1.0, conf)),
            evidence=evidence,
        )

    def _log_grid_preview(self, sheet_name: str, grid: List[List[Any]]) -> None:
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