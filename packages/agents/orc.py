from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional, List
import inspect
import logging
import time

from Multi_agen.packages.core import (
    FinalRenderOutput,
    PipelineState,
    SheetExtractionResult,
    SheetTask,
    WorkbookExtractionResult,
)

logger = logging.getLogger("multi_agen.agents.orc")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)


@dataclass(frozen=True, slots=True)
class OrcConfig:
    """
    ORC runtime configuration for workbook/sheet structural extraction.
    """
    require_capabilities: tuple[str, ...] = (
        "excel.list_sheets",
        "excel.read_sheet_range",
        "excel.find_text",
        "excel.column_index_to_letter",
        "excel.detect_merged_cells",
        "excel.get_formulas",
    )
    continue_on_sheet_error: bool = False
    enable_workbook_structure_pass: bool = True
    enable_quality_audit: bool = True
    enable_expert_arbitration: bool = True


@dataclass(frozen=True, slots=True)
class OrcPromptPolicy:
    """
    Stage-level prompt routing policy owned by the ORC.
    """
    workbook_structure: str = "workbook_structure"
    schema_detection: str = "schema_detection"
    sheet_analysis: str = "sheet_analysis"
    role_mapping: str = "role_mapping"
    quality_audit: str = "quality_audit"
    expert_arbitration: str = "expert_arbitration"
    final_render: str = "final_render"


class ORCAgent:
    """
    Workbook/sheet-centric orchestrator.

    New flow:
      1. validate tool capabilities
      2. analyze workbook structure
      3. detect presentation-sheet tasks
      4. analyze each sheet structurally
      5. map resolved columns
      6. optionally audit / arbitrate
      7. accumulate SheetExtractionResult objects
      8. aggregate WorkbookExtractionResult
      9. render deterministic final text output
    """

    def __init__(
        self,
        tools: Any,
        schema_detector: Any,
        sheet_analyzer: Any,
        role_mapper: Any,
        output_renderer: Any,
        workbook_structure_agent: Optional[Any] = None,
        expert_panel: Optional[Any] = None,
        quality_auditor: Optional[Any] = None,
        prompt_registry: Optional[Any] = None,
        prompt_policy: Optional[OrcPromptPolicy] = None,
        config: Optional[OrcConfig] = None,
    ) -> None:
        logger.info("ORC initializing")

        self.tools = tools
        self.schema_detector = schema_detector
        self.sheet_analyzer = sheet_analyzer
        self.role_mapper = role_mapper
        self.output_renderer = output_renderer
        self.workbook_structure_agent = workbook_structure_agent
        self.expert_panel = expert_panel
        self.quality_auditor = quality_auditor
        self.prompt_registry = prompt_registry
        self.prompt_policy = prompt_policy or OrcPromptPolicy()
        self.config = config or OrcConfig()

        logger.info("ORC initialized successfully")

    def _check_capabilities(self, state: PipelineState) -> None:
        logger.info("ORC checking MCP capabilities")

        caps = set(getattr(state.tooling, "available_capabilities", []) or [])
        missing = [c for c in self.config.require_capabilities if c not in caps]

        if missing:
            logger.error("Missing MCP capabilities: %s", missing)
            raise RuntimeError(f"Missing required MCP capabilities: {missing}")

        logger.info("All required MCP capabilities available")

    def _get_prompt_profile(self, stage_name: str) -> Any:
        if self.prompt_registry is None:
            return None

        try:
            return self.prompt_registry.get(stage_name)
        except Exception:
            logger.exception("Failed to resolve prompt profile for stage=%s", stage_name)
            return None

    def _invoke_stage_method(
        self,
        method: Any,
        *args: Any,
        prompt_profile: Any = None,
        **kwargs: Any,
    ) -> Any:
        """
        Backward-compatible stage invocation.

        Retry without prompt_profile only when the failure is actually caused
        by an unexpected prompt_profile keyword.
        """
        try:
            sig = inspect.signature(method)
            if "prompt_profile" in sig.parameters:
                return method(*args, prompt_profile=prompt_profile, **kwargs)
            return method(*args, **kwargs)
        except TypeError as exc:
            msg = str(exc)
            should_retry_without_prompt = (
                "prompt_profile" in msg and
                (
                    "unexpected keyword argument" in msg
                    or "got an unexpected keyword argument" in msg
                )
            )

            if should_retry_without_prompt:
                logger.exception("Stage method rejected prompt_profile; retrying without it")
                return method(*args, **kwargs)

            raise

    def run(self, state: PipelineState) -> PipelineState:
        start_time = time.perf_counter()

        logger.info("ORC pipeline started run_id=%s", state.run_id)
        self._check_capabilities(state)

        # ------------------------------------------------------------
        # Stage 1: workbook structure
        # ------------------------------------------------------------
        if self.config.enable_workbook_structure_pass and self.workbook_structure_agent is not None:
            logger.info("Analyzing workbook structure")
            try:
                workbook_structure = self._invoke_stage_method(
                    self.workbook_structure_agent.analyze_workbook,
                    state,
                    self.tools,
                    prompt_profile=self._get_prompt_profile(self.prompt_policy.workbook_structure),
                )
                state.workbook_structure = workbook_structure

                logger.info(
                    "Workbook structure analyzed main_sheets=%s confidence=%s",
                    getattr(workbook_structure, "main_sheet_names", None),
                    getattr(workbook_structure, "confidence", None),
                )
            except Exception:
                logger.exception("Workbook structure analysis failed")
                if not self.config.continue_on_sheet_error:
                    raise

        # ------------------------------------------------------------
        # Stage 2: detect sheet tasks
        # ------------------------------------------------------------
        logger.info("Detecting presentation-sheet tasks")
        sheet_tasks: List[SheetTask] = self._invoke_stage_method(
            self.schema_detector.detect,
            state,
            self.tools,
            prompt_profile=self._get_prompt_profile(self.prompt_policy.schema_detection),
        )

        state.sheet_tasks = sheet_tasks

        logger.info(
            "Presentation-sheet tasks detected count=%d names=%s",
            len(sheet_tasks),
            [t.sheet_name for t in sheet_tasks],
        )

        # ------------------------------------------------------------
        # Stage 3: process each sheet task
        # ------------------------------------------------------------
        for i, task in enumerate(sheet_tasks):
            logger.info(
                "Processing sheet %d/%d sheet=%s is_main=%s",
                i + 1,
                len(sheet_tasks),
                task.sheet_name,
                task.is_main_sheet,
            )

            try:
                result = self._process_one_sheet(task, state)
            except Exception as exc:
                logger.exception(
                    "Sheet processing failed sheet=%s err=%s",
                    task.sheet_name,
                    type(exc).__name__,
                )

                if not self.config.continue_on_sheet_error:
                    raise

                result = SheetExtractionResult(
                    sheet_name=task.sheet_name,
                    contains="BS+PL",
                    unit="",
                    data_row_start=None,
                    data_row_end=None,
                    columns=[],
                    quality_flags=[f"sheet_processing_error:{type(exc).__name__}"],
                    confidence=0.0,
                )

            state.add_sheet_result(result)

            logger.info(
                "Sheet processed sheet=%s contains=%s columns=%d",
                result.sheet_name,
                result.contains,
                len(result.columns),
            )

        # ------------------------------------------------------------
        # Stage 4: aggregate workbook result
        # ------------------------------------------------------------
        logger.info("Aggregating workbook result")
        workbook_result = self._aggregate_workbook_result(state)
        state.set_workbook_result(workbook_result)

        logger.info(
            "Workbook result aggregated sheets=%d entities=%d consolidated=%s aje=%s nis=%s",
            len(workbook_result.sheets),
            len(workbook_result.entities),
            workbook_result.has_consolidated,
            workbook_result.has_aje,
            workbook_result.has_nis,
        )

        # ------------------------------------------------------------
        # Stage 5: final render
        # ------------------------------------------------------------
        logger.info("Rendering final workbook output")
        final_render: FinalRenderOutput = self._invoke_stage_method(
            self.output_renderer.render,
            workbook_result,
            prompt_profile=self._get_prompt_profile(self.prompt_policy.final_render),
        )
        state.set_final_render(final_render)

        elapsed = (time.perf_counter() - start_time) * 1000
        logger.info(
            "ORC pipeline finished sheets=%d elapsed_ms=%.2f",
            len(state.sheet_results),
            elapsed,
        )

        return state

    def _process_one_sheet(
        self,
        task: SheetTask,
        state: PipelineState,
    ) -> SheetExtractionResult:
        logger.info("Analyzing sheet=%s", task.sheet_name)

        # ------------------------------------------------------------
        # Analyze sheet
        # ------------------------------------------------------------
        analysis = self._invoke_stage_method(
            self.sheet_analyzer.analyze,
            task,
            state,
            self.tools,
            prompt_profile=self._get_prompt_profile(self.prompt_policy.sheet_analysis),
        )

        logger.info(
            "Sheet analysis complete sheet=%s classification=%s confidence=%s",
            task.sheet_name,
            getattr(getattr(analysis, "classification", None), "types", None),
            getattr(getattr(analysis, "classification", None), "confidence", None),
        )

        # ------------------------------------------------------------
        # Quality audit
        # ------------------------------------------------------------
        quality_flags: List[str] = []
        if self.config.enable_quality_audit and self.quality_auditor is not None:
            logger.info("Running quality audit sheet=%s", task.sheet_name)
            quality_flags = self._invoke_stage_method(
                self.quality_auditor.audit,
                task,
                state,
                analysis,
                self.tools,
                prompt_profile=self._get_prompt_profile(self.prompt_policy.quality_audit),
            ) or []

            if not isinstance(quality_flags, list):
                quality_flags = [str(quality_flags)]

            logger.info("Quality audit flags=%s", quality_flags)

        # ------------------------------------------------------------
        # Role mapping -> ColumnMapping list
        # ------------------------------------------------------------
        logger.info("Mapping structural columns sheet=%s", task.sheet_name)
        columns = self._invoke_stage_method(
            self.role_mapper.map_roles,
            task,
            state,
            analysis,
            prompt_profile=self._get_prompt_profile(self.prompt_policy.role_mapping),
        ) or []

        # ------------------------------------------------------------
        # Expert arbitration
        # ------------------------------------------------------------
        if self.config.enable_expert_arbitration and self.expert_panel is not None:
            logger.info("Running expert arbitration sheet=%s", task.sheet_name)
            columns = self._invoke_stage_method(
                self.expert_panel.maybe_arbitrate,
                task,
                state,
                analysis,
                columns,
                prompt_profile=self._get_prompt_profile(self.prompt_policy.expert_arbitration),
            ) or columns

        # ------------------------------------------------------------
        # Build SheetExtractionResult
        # ------------------------------------------------------------
        contains = self._resolve_contains(analysis)
        unit = self._resolve_unit(analysis)
        data_row_start, data_row_end = self._resolve_data_row_bounds(columns)
        confidence = self._resolve_confidence(analysis, columns)

        return SheetExtractionResult(
            sheet_name=task.sheet_name,
            contains=contains,
            unit=unit,
            data_row_start=data_row_start,
            data_row_end=data_row_end,
            columns=list(columns),
            quality_flags=list(quality_flags),
            confidence=confidence,
        )

    def _resolve_contains(self, analysis: Any) -> str:
        raw_types = list(getattr(getattr(analysis, "classification", None), "types", []) or [])

        has_bs = "BS" in raw_types
        has_pl = "PL" in raw_types

        if has_bs and has_pl:
            return "BS+PL"
        if has_bs:
            return "BS"
        if has_pl:
            return "PL"

        # Conservative fallback when classifier is weak/empty.
        return "BS+PL"

    def _resolve_unit(self, analysis: Any) -> str:
        signals = getattr(analysis, "signals", None) or {}
        if not isinstance(signals, dict):
            return "units"

        for key in ("unit", "likely_unit", "units"):
            value = signals.get(key)
            if value is None:
                continue
            text = str(value).strip()
            if text:
                return text

        llm_raw = str(signals.get("llm_raw", "") or "").lower()
        if "thousand" in llm_raw or "thousands" in llm_raw:
            return "thousands"
        if "million" in llm_raw or "millions" in llm_raw:
            return "millions"

        return "units"

    def _resolve_data_row_bounds(self, columns: List[Any]) -> tuple[Optional[int], Optional[int]]:
        starts: List[int] = []
        ends: List[int] = []

        for col in columns:
            rs = getattr(col, "row_start", None)
            re = getattr(col, "row_end", None)

            if isinstance(rs, int) and rs >= 1:
                starts.append(rs)
            if isinstance(re, int) and re >= 1:
                ends.append(re)

        row_start = min(starts) if starts else None
        row_end = max(ends) if ends else None
        return row_start, row_end

    def _resolve_confidence(self, analysis: Any, columns: List[Any]) -> float:
        cls_conf = getattr(getattr(analysis, "classification", None), "confidence", 0.0)
        try:
            cls_conf_f = float(cls_conf)
        except Exception:
            cls_conf_f = 0.0

        col_confs: List[float] = []
        for col in columns:
            try:
                col_confs.append(float(getattr(col, "confidence", 0.0) or 0.0))
            except Exception:
                continue

        if not col_confs:
            return max(0.0, min(1.0, cls_conf_f))

        avg_col_conf = sum(col_confs) / len(col_confs)
        combined = (cls_conf_f * 0.4) + (avg_col_conf * 0.6)
        return max(0.0, min(1.0, combined))

    def _aggregate_workbook_result(self, state: PipelineState) -> WorkbookExtractionResult:
        sheet_results = list(getattr(state, "sheet_results", []) or [])

        entities_seen = set()
        entities: List[str] = []

        has_consolidated = False
        consolidated_patterns: List[str] = []

        has_aje = False
        has_per_entity_aje = False
        has_consolidated_aje = False

        has_nis = False
        quality_flags: List[str] = []

        for sheet in sheet_results:
            quality_flags.extend(list(getattr(sheet, "quality_flags", []) or []))

            for col in getattr(sheet, "columns", []) or []:
                role = str(getattr(col, "role", "") or "")
                entity = str(getattr(col, "entity", "") or "").strip()
                currency = str(getattr(col, "currency", "") or "").strip().upper()
                formula_pattern = str(getattr(col, "formula_pattern", "") or "").strip()

                if role == "entity_value" and entity and entity not in entities_seen:
                    entities_seen.add(entity)
                    entities.append(entity)

                if role in {"consolidated", "consolidated_aje"}:
                    has_consolidated = True
                    if formula_pattern:
                        consolidated_patterns.append(formula_pattern)

                if role == "aje":
                    has_aje = True
                    has_per_entity_aje = True

                if role == "consolidated_aje":
                    has_aje = True
                    has_consolidated_aje = True

                if currency == "NIS":
                    has_nis = True

                header_text = str(getattr(col, "header_text", "") or "").upper()
                if "NIS" in header_text or "₪" in header_text:
                    has_nis = True

        consolidated_formula_pattern = consolidated_patterns[0] if consolidated_patterns else ""

        aje_types: List[str] = []
        if has_per_entity_aje and has_consolidated_aje:
            aje_types = ["both"]
        elif has_per_entity_aje:
            aje_types = ["per-entity"]
        elif has_consolidated_aje:
            aje_types = ["consolidated"]

        # Workbook structure stage may contain extra global signals; use them conservatively.
        ws = getattr(state, "workbook_structure", None)
        if ws is not None:
            if not entities:
                for ent in getattr(ws, "entities", []) or []:
                    name = str(getattr(ent, "name", "") or "").strip()
                    if name and name not in entities_seen:
                        entities_seen.add(name)
                        entities.append(name)

            if not has_consolidated:
                has_consolidated = bool(getattr(ws, "has_consolidated", False))

            if not consolidated_formula_pattern:
                consolidated_formula_pattern = str(
                    getattr(ws, "consolidated_formula_pattern", "") or ""
                ).strip()

            if not has_aje:
                has_aje = bool(getattr(ws, "has_aje", False))

            if not aje_types:
                ws_aje_types = [str(x).strip() for x in (getattr(ws, "aje_types", []) or []) if str(x).strip()]
                if ws_aje_types:
                    aje_types = ws_aje_types

            quality_flags.extend(list(getattr(ws, "quality_flags", []) or []))

        # de-duplicate quality flags while preserving order
        qf_out: List[str] = []
        seen_qf = set()
        for flag in quality_flags:
            s = str(flag).strip()
            if not s or s in seen_qf:
                continue
            seen_qf.add(s)
            qf_out.append(s)

        return WorkbookExtractionResult(
            sheets=sheet_results,
            entities=entities,
            has_consolidated=has_consolidated,
            consolidated_formula_pattern=consolidated_formula_pattern,
            has_aje=has_aje,
            aje_types=aje_types,
            has_nis=has_nis,
            quality_flags=qf_out,
        )