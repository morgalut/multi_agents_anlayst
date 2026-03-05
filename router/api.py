# Multi_agen/router/api.py
from __future__ import annotations

from dataclasses import asdict
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional
import time
import logging

from fastapi import APIRouter, FastAPI, Request
from fastapi.responses import JSONResponse
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

# NEW: MCP process manager (auto-start servers)
from Multi_agen.router.mcp_manager import MCPManager

try:
    # Python urllib error for the exact failure you're seeing
    from urllib.error import URLError
except Exception:  # pragma: no cover
    URLError = Exception  # type: ignore


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
    # NEW: MCP auto-start parameters (wired from main.py)
    mcp_servers_cfg: Optional[Dict[str, dict]] = None,
    mcp_auto_start: bool = False,
    mcp_startup_timeout_seconds: float = 20.0,
    mcp_stop_on_shutdown: bool = True,
) -> FastAPI:
    """
    llm_config expected shape (from YAML):
      llm:
        enabled: true
    If llm_config is missing -> LLM disabled by default (safe).

    Improvements:
    - Optional MCP subprocess auto-start on app startup + stop on shutdown
    - Converts MCP connection failures into a clean 503 JSON response (instead of raw 500 stack dump)
    - Adds richer /health output
    """

    logger.info("Router API init servers=%s", list(server_base_urls.keys()))

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.mcp_manager = None

        if mcp_auto_start and mcp_servers_cfg:
            logger.info(
                "MCP auto-start enabled. Spawning MCP servers: %s",
                list(mcp_servers_cfg.keys()),
            )
            mgr = MCPManager(
                servers_cfg=mcp_servers_cfg,
                startup_timeout_seconds=float(mcp_startup_timeout_seconds),
            )
            mgr.start_all()
            app.state.mcp_manager = mgr
            logger.info("MCP servers ready")

        try:
            yield
        finally:
            mgr = getattr(app.state, "mcp_manager", None)
            if mgr and mcp_stop_on_shutdown:
                logger.info("Stopping MCP servers on shutdown")
                mgr.stop_all()

    app = FastAPI(
        title="Multi-Agent Excel Router",
        version="0.2.0",
        lifespan=lifespan,
    )
    router = APIRouter()

    # -----------------------------
    # Exception handlers (return 503 when MCP is down)
    # -----------------------------
    @app.exception_handler(URLError)
    async def urlerror_handler(request: Request, exc: Exception):
        return JSONResponse(
            status_code=503,
            content={
                "error": "MCP dependency unavailable",
                "detail": str(exc),
                "hint": "An MCP server is unreachable. Verify it is running and base_url host/port are correct.",
            },
        )

    @app.exception_handler(ConnectionRefusedError)
    async def connrefused_handler(request: Request, exc: Exception):
        return JSONResponse(
            status_code=503,
            content={
                "error": "MCP connection refused",
                "detail": str(exc),
                "hint": "Nothing is listening on the configured MCP port. Start the MCP server or fix base_url/port.",
            },
        )

    # -----------------------------
    # Root endpoint
    # -----------------------------
    @router.get("/")
    def root() -> Dict[str, Any]:
        logger.info("Root requested")
        return {"service": "multi-agent-router", "ok": True}

    # -----------------------------
    # Health endpoint (improved)
    # -----------------------------
    @router.get("/health")
    def health() -> Dict[str, Any]:
        logger.info("Health requested")
        llm_enabled = bool((llm_config or {}).get("enabled", False)) if isinstance(llm_config, dict) else False
        return {
            "ok": True,
            "mcp_servers": list(server_base_urls.keys()),
            "capabilities_count": len(available_capabilities),
            "llm_enabled": llm_enabled,
            "mcp_auto_start": bool(mcp_auto_start),
        }

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
                enabled = bool(llm_config.get("enabled", True))
            else:
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
                # rr.classification may not be a dataclass field
                d["classification"] = getattr(rr, "classification", None)
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
            # The exception handlers above will turn common MCP failures into 503.
            # Any remaining exceptions are logged and re-raised.
            logger.exception("RUN failed run_id=%s workbook=%s", req.run_id, req.workbook_path)
            raise

    app.include_router(router)
    logger.info("Router API ready")
    return app