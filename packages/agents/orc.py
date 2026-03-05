from __future__ import annotations

from dataclasses import dataclass
from typing import Optional
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
    # safety: if any required capability missing, stop early
    require_capabilities: tuple[str, ...] = (
        "excel.list_sheets",
        "excel.read_sheet_range",
        "excel.find_text",
        "excel.column_index_to_letter",
        "excel.write_cells",
    )


class ORCAgent:
    """
    Orchestrator / Router.

    Responsibilities:
      - Detect main sheet schema
      - Create row tasks
      - For each row: analyze sheet -> map roles -> resolve letters -> render -> write
      - Never guess: missing/low confidence => "no"
    """

    def __init__(
        self,
        tools,
        schema_detector,
        row_walker,
        sheet_analyzer,
        role_mapper,
        entity_resolver,
        output_renderer,
        summary_writer,
        expert_panel=None,
        quality_auditor=None,
        config: Optional[OrcConfig] = None,
    ):
        logger.info("ORC initializing")

        self.tools = tools
        self.schema_detector = schema_detector
        self.row_walker = row_walker
        self.sheet_analyzer = sheet_analyzer
        self.role_mapper = role_mapper
        self.entity_resolver = entity_resolver
        self.output_renderer = output_renderer
        self.summary_writer = summary_writer
        self.expert_panel = expert_panel
        self.quality_auditor = quality_auditor
        self.config = config or OrcConfig()

        logger.info("ORC initialized successfully")

    def _check_capabilities(self, state: PipelineState) -> None:

        logger.info("ORC checking MCP capabilities")

        caps = set(state.tooling.available_capabilities or [])
        missing = [c for c in self.config.require_capabilities if c not in caps]

        if missing:
            logger.error("Missing MCP capabilities: %s", missing)
            raise RuntimeError(f"Missing required MCP capabilities: {missing}")

        logger.info("All required MCP capabilities available")

    def run(self, state: PipelineState) -> PipelineState:

        start_time = time.perf_counter()

        logger.info("ORC pipeline started run_id=%s", state.run_id)

        self._check_capabilities(state)

        # 1) detect main sheet schema
        logger.info("Detecting main sheet schema")

        main_schema: MainSheetSchema = self.schema_detector.detect(state, self.tools)

        state.main_sheet = main_schema

        logger.info(
            "Main sheet detected name=%s header_row=%s",
            main_schema.name,
            main_schema.header_row_index,
        )

        # 2) build row tasks
        logger.info("Building row tasks")

        tasks: list[RowTask] = self.row_walker.build_tasks(state, self.tools)

        state.row_tasks = tasks

        logger.info("Row tasks created count=%d", len(tasks))

        # 3) per-row processing
        for i, task in enumerate(tasks):

            logger.info(
                "Processing row %d/%d row_index=%s sheet=%s",
                i + 1,
                len(tasks),
                task.row_index,
                task.sheet_name,
            )

            rr = self._process_one(task, state)

            state.add_result(rr)

            logger.info(
                "Row processed row_index=%s sheet=%s",
                rr.row_index,
                rr.sheet_name,
            )

            # write row to Summary sheet
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

        # ReAct analysis
        analysis = self.sheet_analyzer.analyze(task, state, self.tools)

        logger.info(
            "Sheet analysis complete sheet=%s classification=%s confidence=%s",
            task.sheet_name,
            getattr(analysis.classification, "types", None),
            getattr(analysis.classification, "confidence", None),
        )

        # Optional quality audit
        quality_flags = []

        if self.quality_auditor is not None:

            logger.info("Running quality audit")

            quality_flags = self.quality_auditor.audit(
                task,
                state,
                analysis,
                self.tools,
            )

            logger.info("Quality audit flags=%s", quality_flags)

        # Map roles
        logger.info("Mapping column roles")

        role_map = self.role_mapper.map_roles(task, state, analysis)

        logger.info("Role map created")

        # Expert arbitration
        if self.expert_panel is not None:

            logger.info("Running expert arbitration")

            role_map = self.expert_panel.maybe_arbitrate(
                task,
                state,
                analysis,
                role_map,
            )

        # Resolve entities to column letters
        logger.info("Resolving column indices to Excel letters")

        resolved = self.entity_resolver.resolve(task, state, role_map, self.tools)

        logger.info("Entity resolution complete")

        # Render final output
        logger.info("Rendering final output row")

        resolved_out = self.output_renderer.render(task, state, resolved)

        logger.info("Row render complete")

        return RowResult(
            row_index=task.row_index,
            sheet_name=task.sheet_name,
            classification=analysis.classification,
            role_map=role_map,
            resolved=resolved_out,
            quality_flags=quality_flags,
        )