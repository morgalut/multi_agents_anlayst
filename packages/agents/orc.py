from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional
import inspect
import logging
import time

from Multi_agen.packages.core import (
    MainSheetSchema,
    PipelineState,
    RowResult,
    RowTask,
)

logger = logging.getLogger("multi_agen.agents.orc")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)


@dataclass(frozen=True, slots=True)
class OrcConfig:
    """
    ORC runtime configuration.
    """
    require_capabilities: tuple[str, ...] = (
        "excel.list_sheets",
        "excel.read_sheet_range",
        "excel.find_text",
        "excel.column_index_to_letter",
        "excel.write_cells",
    )
    continue_on_row_error: bool = False
    write_summary_per_row: bool = True
    enable_workbook_structure_pass: bool = True


@dataclass(frozen=True, slots=True)
class OrcPromptPolicy:
    """
    Stage-level prompt routing policy owned by the ORC.
    """
    workbook_structure: str = "workbook_structure"
    schema_detection: str = "schema_detection"
    sheet_analysis: str = "sheet_analysis"
    role_mapping: str = "role_mapping"
    entity_resolution: str = "entity_resolution"
    quality_audit: str = "quality_audit"
    expert_arbitration: str = "expert_arbitration"
    final_render: str = "final_render"


class ORCAgent:
    """
    Orchestrator / Router.
    """

    def __init__(
        self,
        tools: Any,
        schema_detector: Any,
        row_walker: Any,
        sheet_analyzer: Any,
        role_mapper: Any,
        entity_resolver: Any,
        output_renderer: Any,
        summary_writer: Any,
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
        self.row_walker = row_walker
        self.sheet_analyzer = sheet_analyzer
        self.role_mapper = role_mapper
        self.entity_resolver = entity_resolver
        self.output_renderer = output_renderer
        self.summary_writer = summary_writer
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

    def _store_workbook_structure_on_state(self, state: PipelineState, structure: Any) -> None:
        try:
            setattr(state, "workbook_structure", structure)
        except Exception:
            logger.exception("Failed to attach workbook_structure to state")

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
        by an unexpected prompt_profile keyword. Do not swallow unrelated
        TypeErrors raised inside the callee.
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

        if self.config.enable_workbook_structure_pass and self.workbook_structure_agent is not None:
            logger.info("Analyzing workbook structure")
            try:
                workbook_structure = self._invoke_stage_method(
                    self.workbook_structure_agent.analyze_workbook,
                    state,
                    self.tools,
                    prompt_profile=self._get_prompt_profile(self.prompt_policy.workbook_structure),
                )
                self._store_workbook_structure_on_state(state, workbook_structure)

                logger.info(
                    "Workbook structure analyzed main_sheet=%s confidence=%s",
                    getattr(workbook_structure, "main_sheet_name", None),
                    getattr(workbook_structure, "confidence", None),
                )
            except Exception:
                logger.exception("Workbook structure analysis failed")

        logger.info("Detecting main sheet schema")
        main_schema: MainSheetSchema = self._invoke_stage_method(
            self.schema_detector.detect,
            state,
            self.tools,
            prompt_profile=self._get_prompt_profile(self.prompt_policy.schema_detection),
        )

        state.main_sheet = main_schema

        logger.info(
            "Main sheet detected name=%s header_row=%s",
            getattr(main_schema, "name", None),
            getattr(main_schema, "header_row_index", None),
        )

        logger.info("Building row tasks")
        tasks: list[RowTask] = self.row_walker.build_tasks(state, self.tools)
        state.row_tasks = tasks

        logger.info("Row tasks created count=%d", len(tasks))

        for i, task in enumerate(tasks):
            logger.info(
                "Processing row %d/%d row_index=%s sheet=%s",
                i + 1,
                len(tasks),
                task.row_index,
                task.sheet_name,
            )

            try:
                rr = self._process_one(task, state)
            except Exception as exc:
                logger.exception(
                    "Row processing failed row_index=%s sheet=%s err=%s",
                    task.row_index,
                    task.sheet_name,
                    type(exc).__name__,
                )

                if not self.config.continue_on_row_error:
                    raise

                rr = RowResult(
                    row_index=task.row_index,
                    sheet_name=task.sheet_name,
                    classification=None,
                    role_map={},
                    resolved={},
                    quality_flags=[f"row_processing_error:{type(exc).__name__}"],
                )

            state.add_result(rr)

            logger.info(
                "Row processed row_index=%s sheet=%s",
                rr.row_index,
                rr.sheet_name,
            )

            if self.config.write_summary_per_row:
                logger.info("Writing row result to summary")
                self.summary_writer.write_row(state, rr, self.tools)

        elapsed = (time.perf_counter() - start_time) * 1000

        logger.info(
            "ORC pipeline finished rows=%d elapsed_ms=%.2f",
            len(tasks),
            elapsed,
        )

        return state

    def _process_one(self, task: RowTask, state: PipelineState) -> RowResult:
        logger.info(
            "Analyzing sheet row_index=%s sheet=%s",
            task.row_index,
            task.sheet_name,
        )

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

        quality_flags = []
        if self.quality_auditor is not None:
            logger.info("Running quality audit")
            quality_flags = self._invoke_stage_method(
                self.quality_auditor.audit,
                task,
                state,
                analysis,
                self.tools,
                prompt_profile=self._get_prompt_profile(self.prompt_policy.quality_audit),
            )
            logger.info("Quality audit flags=%s", quality_flags)

        logger.info("Mapping column roles")
        role_map = self._invoke_stage_method(
            self.role_mapper.map_roles,
            task,
            state,
            analysis,
            prompt_profile=self._get_prompt_profile(self.prompt_policy.role_mapping),
        )
        logger.info("Role map created")

        if self.expert_panel is not None:
            logger.info("Running expert arbitration")
            role_map = self._invoke_stage_method(
                self.expert_panel.maybe_arbitrate,
                task,
                state,
                analysis,
                role_map,
                prompt_profile=self._get_prompt_profile(self.prompt_policy.expert_arbitration),
            )

        logger.info("Resolving column indices to Excel letters")
        resolved = self._invoke_stage_method(
            self.entity_resolver.resolve,
            task,
            state,
            role_map,
            self.tools,
            prompt_profile=self._get_prompt_profile(self.prompt_policy.entity_resolution),
        )
        logger.info("Entity resolution complete")

        logger.info("Rendering final output row")
        resolved_out = self._invoke_stage_method(
            self.output_renderer.render,
            task,
            state,
            resolved,
            prompt_profile=self._get_prompt_profile(self.prompt_policy.final_render),
        )
        logger.info("Row render complete")

        return RowResult(
            row_index=task.row_index,
            sheet_name=task.sheet_name,
            classification=getattr(analysis, "classification", None),
            role_map=role_map,
            resolved=resolved_out,
            quality_flags=quality_flags,
        )