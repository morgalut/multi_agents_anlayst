# Multi_agen/router/api.py
from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import asdict, is_dataclass
from typing import Any, Dict, List, Optional
import logging
import time

from fastapi import APIRouter, FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from Multi_agen.packages.core import PipelineState, RunInput, ToolingState
from Multi_agen.packages.agents import (
    ORCAgent,
    ReActSheetAnalyzer,
    RoleMapperAgent,
    OutputRenderer,
    ExpertPanelAgent,
)
from Multi_agen.packages.agents.orc import OrcConfig, OrcPromptPolicy
from Multi_agen.packages.agents.schema_detector import MainSheetSchemaDetector
from Multi_agen.packages.agents.workbook_structure_agent import WorkbookStructureAgent
from Multi_agen.packages.agents.quality_auditor import QualityAuditorAgent
from Multi_agen.packages.llm import LLMClient
from Multi_agen.packages.llm.stage_prompts import PromptRegistry
from Multi_agen.packages.mcp_clients.excel_client import ExcelClientError

from .tool_router import ToolRouter, ToolRouterConfig
from Multi_agen.router.mcp_manager import MCPManager

try:
    from urllib.error import URLError
except Exception:  # pragma: no cover
    URLError = Exception  # type: ignore


APP_VERSION = "0.4.0"

logger = logging.getLogger("multi_agen.router.api")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)


# ============================================================
# Request / Response models
# ============================================================

class RunRequest(BaseModel):
    run_id: str = Field(default="run-1")
    workbook_path: str
    workbook_out_path: Optional[str] = None


class RunResponse(BaseModel):
    run_id: str
    workbook_structure: Optional[Dict[str, Any]] = None
    workbook_structure_provenance: Optional[Dict[str, Any]] = None
    task_provenance: List[Dict[str, Any]] = Field(default_factory=list)
    sheet_tasks: List[Dict[str, Any]] = Field(default_factory=list)
    sheet_results: List[Dict[str, Any]] = Field(default_factory=list)
    workbook_result: Optional[Dict[str, Any]] = None
    final_render: Optional[Dict[str, Any]] = None


# ============================================================
# Helpers
# ============================================================

def _llm_enabled(llm_config: Optional[Dict[str, Any]]) -> bool:
    return isinstance(llm_config, dict) and bool(llm_config.get("enabled", False))


def _safe_asdict(value: Any) -> Any:
    """
    Convert dataclasses recursively when possible.
    Fall back to the raw value for non-dataclasses.
    """
    if value is None:
        return None
    if is_dataclass(value):
        return asdict(value)
    return value


def _serialize_sheet_result(value: Any) -> Dict[str, Any]:
    if value is None:
        return {}

    if is_dataclass(value):
        return asdict(value)
    if isinstance(value, dict):
        return dict(value)

    return {"value": value}


def _attach_workbook_out_path(state: PipelineState, workbook_out_path: Optional[str]) -> None:
    if not workbook_out_path:
        return

    try:
        setattr(state.input, "workbook_out_path", workbook_out_path)
    except Exception:
        logger.exception("RUN failed to attach workbook_out_path to state.input")


def _build_orc(*, tools: Any, llm: Optional[LLMClient]) -> ORCAgent:
    """
    Build the workbook/sheet-centric ORC dependency graph.
    """
    prompt_registry = PromptRegistry()

    schema_detector = MainSheetSchemaDetector()
    workbook_structure_agent = WorkbookStructureAgent(llm=llm)
    sheet_analyzer = ReActSheetAnalyzer(llm=llm)
    role_mapper = RoleMapperAgent()
    output_renderer = OutputRenderer()
    expert_panel = ExpertPanelAgent()
    quality_auditor = QualityAuditorAgent()

    prompt_policy = OrcPromptPolicy(
        workbook_structure="workbook_structure",
        schema_detection="schema_detection",
        sheet_analysis="sheet_analysis",
        role_mapping="role_mapping",
        quality_audit="quality_audit",
        expert_arbitration="expert_arbitration",
        final_render="final_render",
    )

    config = OrcConfig(
        continue_on_sheet_error=False,
        enable_workbook_structure_pass=True,
        enable_quality_audit=True,
        enable_expert_arbitration=True,
    )

    return ORCAgent(
        tools=tools,
        schema_detector=schema_detector,
        sheet_analyzer=sheet_analyzer,
        role_mapper=role_mapper,
        output_renderer=output_renderer,
        workbook_structure_agent=workbook_structure_agent,
        expert_panel=expert_panel,
        quality_auditor=quality_auditor,
        prompt_registry=prompt_registry,
        prompt_policy=prompt_policy,
        config=config,
    )


def _build_run_response(state: PipelineState) -> RunResponse:
    """
    Centralized API response construction for workbook/sheet extraction.
    """
    return RunResponse(
        run_id=state.run_id,
        workbook_structure=_safe_asdict(getattr(state, "workbook_structure", None)),
        workbook_structure_provenance=_safe_asdict(
            getattr(state, "workbook_structure_provenance", None)
        ),
        task_provenance=list(getattr(state, "task_provenance", []) or []),
        sheet_tasks=[_safe_asdict(t) for t in (getattr(state, "sheet_tasks", []) or [])],
        sheet_results=[_serialize_sheet_result(x) for x in (getattr(state, "sheet_results", []) or [])],
        workbook_result=_safe_asdict(getattr(state, "workbook_result", None)),
        final_render=_safe_asdict(getattr(state, "final_render", None)),
    )


# ============================================================
# FastAPI application builder
# ============================================================

def build_app(
    *,
    server_base_urls: Dict[str, str],
    available_capabilities: List[str],
    llm_config: Optional[Dict[str, Any]] = None,
    mcp_servers_cfg: Optional[Dict[str, dict]] = None,
    mcp_auto_start: bool = False,
    mcp_startup_timeout_seconds: float = 20.0,
    mcp_stop_on_shutdown: bool = True,
) -> FastAPI:
    """
    Build the FastAPI app for the multi-agent Excel pipeline.

    Features:
    - optional MCP subprocess auto-start on app startup
    - graceful MCP shutdown
    - centralized ORC dependency construction
    - structured Excel MCP error handling
    - response serialization for workbook/sheet-centric extraction
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
                allow_partial_start=True,
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
        version=APP_VERSION,
        lifespan=lifespan,
    )
    router = APIRouter()

    # ============================================================
    # Exception handlers
    # ============================================================

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

    @app.exception_handler(ExcelClientError)
    async def excel_client_error_handler(request: Request, exc: ExcelClientError):
        status_code = exc.status_code if isinstance(exc.status_code, int) and 400 <= exc.status_code <= 599 else 503
        return JSONResponse(
            status_code=status_code,
            content={
                "error": "Excel MCP tool failure",
                "detail": str(exc),
                "server_id": exc.server_id,
                "tool_name": exc.tool_name,
                "tool_args": exc.args_payload,
                "response_body": exc.response_body,
            },
        )

    @app.exception_handler(HTTPException)
    async def http_exception_handler(request: Request, exc: HTTPException):
        return JSONResponse(
            status_code=exc.status_code,
            content={
                "error": "HTTP exception",
                "detail": exc.detail,
            },
        )

    # ============================================================
    # Routes
    # ============================================================

    @router.get("/")
    def root() -> Dict[str, Any]:
        logger.info("Root requested")
        return {
            "service": "multi-agent-router",
            "ok": True,
            "version": APP_VERSION,
        }

    @router.get("/health")
    def health() -> Dict[str, Any]:
        logger.info("Health requested")
        return {
            "ok": True,
            "router_version": APP_VERSION,
            "mcp_servers": list(server_base_urls.keys()),
            "capabilities_count": len(available_capabilities),
            "llm_enabled": _llm_enabled(llm_config),
            "mcp_auto_start": bool(mcp_auto_start),
            "workbook_structure_stage_enabled": True,
            "prompt_registry_enabled": True,
            "mode": "workbook_sheet_structural_extraction",
        }

    @router.get("/capabilities")
    def capabilities() -> Dict[str, Any]:
        logger.info("Capabilities requested")
        return {
            "available_capabilities": available_capabilities,
            "mcp_servers": list(server_base_urls.keys()),
        }

    @router.post("/run", response_model=RunResponse)
    def run(req: RunRequest) -> RunResponse:
        start_time = time.perf_counter()
        logger.info("RUN start run_id=%s workbook=%s", req.run_id, req.workbook_path)

        try:
            # ------------------------------------------------------------
            # MCP transport
            # ------------------------------------------------------------
            logger.info("RUN init MCP transport")
            from .transport_http import HttpMCPTransport
            transport = HttpMCPTransport(server_base_urls=server_base_urls)

            # ------------------------------------------------------------
            # Tool router
            # ------------------------------------------------------------
            logger.info("RUN init ToolRouter")
            tools = ToolRouter(
                transport=transport,
                config=ToolRouterConfig(workbook_path=req.workbook_path),
            )
            logger.info("RUN ToolRouter ready")

            # ------------------------------------------------------------
            # LLM
            # ------------------------------------------------------------
            enabled = _llm_enabled(llm_config)
            llm: Optional[LLMClient] = None

            if enabled:
                logger.info("RUN init LLM client (Azure env)")
                llm = LLMClient()
                logger.info("RUN LLM enabled")
            else:
                logger.info("RUN LLM disabled")

            # ------------------------------------------------------------
            # ORC
            # ------------------------------------------------------------
            logger.info("RUN init ORC orchestrator and agents")
            orc = _build_orc(tools=tools, llm=llm)
            logger.info("RUN ORC ready")

            # ------------------------------------------------------------
            # Pipeline state
            # ------------------------------------------------------------
            logger.info("RUN create PipelineState")
            state = PipelineState(
                run_id=req.run_id,
                input=RunInput(workbook_path=req.workbook_path),
                tooling=ToolingState(
                    mcp_registry=list(server_base_urls.keys()),
                    available_capabilities=available_capabilities,
                ),
            )
            _attach_workbook_out_path(state, req.workbook_out_path)

            # ------------------------------------------------------------
            # Execute pipeline
            # ------------------------------------------------------------
            logger.info("RUN ORC run() start")
            state = orc.run(state)
            logger.info(
                "RUN ORC run() done sheet_tasks=%d sheet_results=%d",
                len(getattr(state, "sheet_tasks", []) or []),
                len(getattr(state, "sheet_results", []) or []),
            )

            # ------------------------------------------------------------
            # Serialize response
            # ------------------------------------------------------------
            logger.info("RUN serialize response")
            response = _build_run_response(state)

            elapsed_ms = (time.perf_counter() - start_time) * 1000
            logger.info("RUN complete run_id=%s elapsed_ms=%.2f", req.run_id, elapsed_ms)
            return response

        except ExcelClientError:
            logger.exception("RUN excel MCP failure run_id=%s workbook=%s", req.run_id, req.workbook_path)
            raise
        except HTTPException:
            logger.exception("RUN http exception run_id=%s workbook=%s", req.run_id, req.workbook_path)
            raise
        except Exception:
            logger.exception("RUN failed run_id=%s workbook=%s", req.run_id, req.workbook_path)
            raise

    app.include_router(router)
    logger.info("Router API ready")
    return app