from __future__ import annotations

from dataclasses import asdict
from typing import Any, Dict, List, Optional
import time
import logging

from fastapi import APIRouter, FastAPI
from pydantic import BaseModel, Field

from Multi_agen.packages.core import PipelineState, RunInput, ToolingState
from Multi_agen.packages.agents import (
    ORCAgent,
    RowWalkerAgent,
    ReActSheetAnalyzer,
    RoleMapperAgent,
    EntityColumnLetterResolver,
    OutputRenderer,
    SummaryRowWriterAgent,
    ExpertPanelAgent,
    DataQualityAuditor,
)
from Multi_agen.packages.agents.schema_detector import MainSheetSchemaDetector
from Multi_agen.packages.llm import LLMClient

from .tool_router import ToolRouter, ToolRouterConfig


# -----------------------------
# Logger
# -----------------------------

logger = logging.getLogger("multi_agen.router.api")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)


# -----------------------------
# Request/response models
# -----------------------------

class RunRequest(BaseModel):
    run_id: str = Field(default="run-1")
    workbook_path: str
    workbook_out_path: Optional[str] = None


class RunResponse(BaseModel):
    run_id: str
    main_sheet: Optional[Dict[str, Any]] = None
    row_tasks: List[Dict[str, Any]] = Field(default_factory=list)
    row_results: List[Dict[str, Any]] = Field(default_factory=list)


# -----------------------------
# FastAPI application builder
# -----------------------------

def build_app(
    *,
    server_base_urls: Dict[str, str],
    available_capabilities: List[str],
    llm_config: Optional[Dict[str, Any]] = None,
) -> FastAPI:
    """
    llm_config expected shape (from YAML):
      llm:
        enabled: true
    If llm_config is missing -> LLM disabled by default (safe).
    """

    logger.info("Router API init servers=%s", list(server_base_urls.keys()))

    app = FastAPI(title="Multi-Agent Excel Router", version="0.1.0")
    router = APIRouter()

    # -----------------------------
    # Root endpoint
    # -----------------------------
    @router.get("/")
    def root() -> Dict[str, Any]:
        logger.info("Root requested")
        return {"service": "multi-agent-router", "ok": True}

    # -----------------------------
    # Health endpoint
    # -----------------------------
    @router.get("/health")
    def health() -> Dict[str, Any]:
        logger.info("Health requested")
        return {"ok": True}

    # -----------------------------
    # Capabilities endpoint
    # -----------------------------
    @router.get("/capabilities")
    def capabilities() -> Dict[str, Any]:
        logger.info("Capabilities requested")
        return {
            "available_capabilities": available_capabilities,
            "mcp_servers": list(server_base_urls.keys()),
        }

    # -----------------------------
    # Run pipeline endpoint
    # -----------------------------
    @router.post("/run", response_model=RunResponse)
    def run(req: RunRequest) -> RunResponse:
        start_time = time.perf_counter()
        logger.info("RUN start run_id=%s workbook=%s", req.run_id, req.workbook_path)

        try:
            # -----------------------------
            # Initialize MCP transport
            # -----------------------------
            logger.info("RUN init MCP transport")
            from .transport_http import HttpMCPTransport
            transport = HttpMCPTransport(server_base_urls=server_base_urls)

            # -----------------------------
            # Initialize ToolRouter
            # -----------------------------
            logger.info("RUN init ToolRouter")
            tools = ToolRouter(
                transport=transport,
                config=ToolRouterConfig(workbook_path=req.workbook_path),
            )
            logger.info("RUN ToolRouter ready")

            # -----------------------------
            # Initialize LLM (Azure .env)
            # -----------------------------
            enabled = False
            if isinstance(llm_config, dict):
                # default True only if llm section exists, but we still respect explicit enabled flag
                enabled = bool(llm_config.get("enabled", True))
            else:
                # if no llm section in config -> disabled (safe default)
                enabled = False

            llm = None
            if enabled:
                logger.info("RUN init LLM client (Azure env)")
                llm = LLMClient()  # reads AZURE_OPENAI_* from .env
                logger.info("RUN LLM enabled")
            else:
                logger.info("RUN LLM disabled")

            # -----------------------------
            # Build agents
            # -----------------------------
            logger.info("RUN init agents")

            schema_detector = MainSheetSchemaDetector()
            row_walker = RowWalkerAgent()
            sheet_analyzer = ReActSheetAnalyzer(llm=llm)
            role_mapper = RoleMapperAgent()
            entity_resolver = EntityColumnLetterResolver()
            output_renderer = OutputRenderer()
            summary_writer = SummaryRowWriterAgent()
            expert_panel = ExpertPanelAgent()
            quality_auditor = DataQualityAuditor()

            logger.info("RUN agents ready")

            # -----------------------------
            # Create ORC orchestrator
            # -----------------------------
            logger.info("RUN init ORC orchestrator")

            orc = ORCAgent(
                tools=tools,
                schema_detector=schema_detector,
                row_walker=row_walker,
                sheet_analyzer=sheet_analyzer,
                role_mapper=role_mapper,
                entity_resolver=entity_resolver,
                output_renderer=output_renderer,
                summary_writer=summary_writer,
                expert_panel=expert_panel,
                quality_auditor=quality_auditor,
            )
            logger.info("RUN ORC ready")

            # -----------------------------
            # Build pipeline state
            # -----------------------------
            logger.info("RUN create PipelineState")

            state = PipelineState(
                run_id=req.run_id,
                input=RunInput(workbook_path=req.workbook_path),
                tooling=ToolingState(
                    mcp_registry=list(server_base_urls.keys()),
                    available_capabilities=available_capabilities,
                ),
            )

            # -----------------------------
            # Execute pipeline
            # -----------------------------
            logger.info("RUN ORC run() start")
            state = orc.run(state)
            logger.info(
                "RUN ORC run() done row_tasks=%d row_results=%d",
                len(state.row_tasks),
                len(state.row_results),
            )

            # -----------------------------
            # Serialize results
            # -----------------------------
            logger.info("RUN serialize response")

            main_sheet = asdict(state.main_sheet) if state.main_sheet else None
            row_tasks = [asdict(t) for t in state.row_tasks]

            row_results: List[Dict[str, Any]] = []
            for rr in state.row_results:
                d = asdict(rr)
                d["classification"] = rr.classification  # may not be dataclass
                row_results.append(d)

            elapsed_ms = (time.perf_counter() - start_time) * 1000
            logger.info("RUN complete run_id=%s elapsed_ms=%.2f", req.run_id, elapsed_ms)

            return RunResponse(
                run_id=state.run_id,
                main_sheet=main_sheet,
                row_tasks=row_tasks,
                row_results=row_results,
            )

        except Exception:
            logger.exception("RUN failed run_id=%s workbook=%s", req.run_id, req.workbook_path)
            raise

    app.include_router(router)
    logger.info("Router API ready")
    return app