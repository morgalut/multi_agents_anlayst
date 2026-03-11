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
from Multi_agen.packages.agents.sheet_company_agent import SheetCompanyAgent
from Multi_agen.packages.agents.sheet_currency_agent import SheetCurrencyAgent
from Multi_agen.packages.agents.quality_auditor import QualityAuditorAgent
from Multi_agen.packages.llm import LLMClient
from Multi_agen.packages.llm.stage_prompts import PromptRegistry
from Multi_agen.packages.mcp_clients.excel_client import ExcelClientError
from Multi_agen.packages.core.search_config import ColumnSearchConfig

from .tool_router import ToolRouter, ToolRouterConfig
from Multi_agen.router.mcp_manager import MCPManager

try:
    from urllib.error import URLError
except Exception:  # pragma: no cover
    URLError = Exception  # type: ignore


APP_VERSION = "0.6.0"

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
# Serialization helpers
# ============================================================

def _llm_enabled(llm_config: Optional[Dict[str, Any]]) -> bool:
    return isinstance(llm_config, dict) and bool(llm_config.get("enabled", False))


def _safe_asdict(value: Any) -> Any:
    """
    Recursively convert dataclasses to dicts.

    Handles nested dataclasses (ExtractionSummary, ComparisonBlock,
    ColumnComparisonHit, ExpectedColumn, EntityCurrencyPair …) that are
    new in v0.6.0 / FinalRenderOutput.
    Falls back to raw value for non-dataclasses.
    """
    if value is None:
        return None
    if is_dataclass(value) and not isinstance(value, type):
        return asdict(value)
    if isinstance(value, list):
        return [_safe_asdict(item) for item in value]
    if isinstance(value, dict):
        return {k: _safe_asdict(v) for k, v in value.items()}
    return value


def _serialize_sheet_result(value: Any) -> Dict[str, Any]:
    if value is None:
        return {}
    if is_dataclass(value) and not isinstance(value, type):
        return asdict(value)
    if isinstance(value, dict):
        return dict(value)
    return {"value": value}


def _attach_workbook_out_path(state: PipelineState, path: Optional[str]) -> None:
    if not path:
        return
    try:
        setattr(state.input, "workbook_out_path", path)
    except Exception:
        logger.exception("Failed to attach workbook_out_path")


# ============================================================
# ORC factory
# ============================================================

def _build_orc(*, tools: Any, llm: Optional[LLMClient]) -> ORCAgent:
    """
    Build the full ORC dependency graph.

    v0.6.0: wires SheetCompanyAgent + SheetCurrencyAgent and
    enables the comparison-first FinalRenderOutput.
    """
    prompt_registry = PromptRegistry()
    search_config = ColumnSearchConfig()

    schema_detector = MainSheetSchemaDetector()
    workbook_structure_agent = WorkbookStructureAgent(llm=llm)
    sheet_analyzer = ReActSheetAnalyzer(llm=llm)
    role_mapper = RoleMapperAgent()
    output_renderer = OutputRenderer(search_config=search_config)
    expert_panel = ExpertPanelAgent()
    quality_auditor = QualityAuditorAgent()
    company_agent = SheetCompanyAgent(llm=llm, llm_enabled=(llm is not None))
    currency_agent = SheetCurrencyAgent(llm=llm, llm_enabled=(llm is not None))

    prompt_policy = OrcPromptPolicy(
        workbook_structure="workbook_structure",
        schema_detection="schema_detection",
        sheet_analysis="sheet_analysis",
        sheet_company="sheet_company",
        sheet_currency="sheet_currency",
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
        enable_company_extraction=True,
        enable_currency_extraction=True,
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
        company_agent=company_agent,
        currency_agent=currency_agent,
        prompt_registry=prompt_registry,
        prompt_policy=prompt_policy,
        config=config,
    )


# ============================================================
# Response builder
# ============================================================

def _build_run_response(state: PipelineState) -> RunResponse:
    """
    Build the API response from pipeline state.

    final_render is now a richer object containing:
      summary, comparison, normalized_output (new in v0.6.0)
    _safe_asdict handles recursive dataclass serialization for all nested types.
    """
    return RunResponse(
        run_id=state.run_id,
        workbook_structure=_safe_asdict(getattr(state, "workbook_structure", None)),
        workbook_structure_provenance=_safe_asdict(
            getattr(state, "workbook_structure_provenance", None)
        ),
        task_provenance=list(getattr(state, "task_provenance", []) or []),
        sheet_tasks=[_safe_asdict(t) for t in (getattr(state, "sheet_tasks", []) or [])],
        sheet_results=[
            _serialize_sheet_result(x)
            for x in (getattr(state, "sheet_results", []) or [])
        ],
        workbook_result=_safe_asdict(getattr(state, "workbook_result", None)),
        final_render=_safe_asdict(getattr(state, "final_render", None)),
    )


# ============================================================
# FastAPI app builder
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

    v0.6.0 additions:
    - Comparison-first FinalRenderOutput (summary + comparison + normalized_output)
    - _safe_asdict upgraded to handle nested dataclasses recursively
    - SheetCompanyAgent + SheetCurrencyAgent wired in ORC
    - workbook_structure_entities forwarded from ORC → renderer for comparison derivation
    """
    logger.info("API init servers=%s", list(server_base_urls.keys()))

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.mcp_manager = None
        if mcp_auto_start and mcp_servers_cfg:
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
                mgr.stop_all()

    app = FastAPI(
        title="Multi-Agent Excel Router",
        version=APP_VERSION,
        lifespan=lifespan,
    )
    router = APIRouter()

    # ── Exception handlers ───────────────────────────────────────────

    @app.exception_handler(URLError)
    async def urlerror_handler(request: Request, exc: Exception):
        return JSONResponse(status_code=503, content={
            "error": "MCP dependency unavailable", "detail": str(exc),
            "hint": "An MCP server is unreachable.",
        })

    @app.exception_handler(ConnectionRefusedError)
    async def connrefused_handler(request: Request, exc: Exception):
        return JSONResponse(status_code=503, content={
            "error": "MCP connection refused", "detail": str(exc),
        })

    @app.exception_handler(ExcelClientError)
    async def excel_error_handler(request: Request, exc: ExcelClientError):
        sc = exc.status_code if isinstance(exc.status_code, int) and 400 <= exc.status_code <= 599 else 503
        return JSONResponse(status_code=sc, content={
            "error": "Excel MCP tool failure", "detail": str(exc),
            "server_id": exc.server_id, "tool_name": exc.tool_name,
        })

    @app.exception_handler(HTTPException)
    async def http_handler(request: Request, exc: HTTPException):
        return JSONResponse(status_code=exc.status_code, content={
            "error": "HTTP exception", "detail": exc.detail,
        })

    # ── Routes ───────────────────────────────────────────────────────

    @router.get("/")
    def root() -> Dict[str, Any]:
        return {"service": "multi-agent-router", "ok": True, "version": APP_VERSION}

    @router.get("/health")
    def health() -> Dict[str, Any]:
        return {
            "ok": True,
            "router_version": APP_VERSION,
            "mcp_servers": list(server_base_urls.keys()),
            "capabilities_count": len(available_capabilities),
            "llm_enabled": _llm_enabled(llm_config),
            "mcp_auto_start": bool(mcp_auto_start),
            "workbook_structure_stage_enabled": True,
            "sheet_company_agent_enabled": True,
            "sheet_currency_agent_enabled": True,
            "comparison_first_output_enabled": True,   # new in v0.6.0
            "mode": "workbook_sheet_structural_extraction",
        }

    @router.get("/capabilities")
    def capabilities() -> Dict[str, Any]:
        return {
            "available_capabilities": available_capabilities,
            "mcp_servers": list(server_base_urls.keys()),
        }

    @router.post("/run", response_model=RunResponse)
    def run(req: RunRequest) -> RunResponse:
        t0 = time.perf_counter()
        logger.info("RUN start run_id=%s workbook=%s", req.run_id, req.workbook_path)

        try:
            from .transport_http import HttpMCPTransport
            transport = HttpMCPTransport(server_base_urls=server_base_urls)
            tools = ToolRouter(
                transport=transport,
                config=ToolRouterConfig(workbook_path=req.workbook_path),
            )

            llm: Optional[LLMClient] = None
            if _llm_enabled(llm_config):
                llm = LLMClient()
                logger.info("RUN LLM enabled")

            orc = _build_orc(tools=tools, llm=llm)

            state = PipelineState(
                run_id=req.run_id,
                input=RunInput(workbook_path=req.workbook_path),
                tooling=ToolingState(
                    mcp_registry=list(server_base_urls.keys()),
                    available_capabilities=available_capabilities,
                ),
            )
            _attach_workbook_out_path(state, req.workbook_out_path)

            state = orc.run(state)
            response = _build_run_response(state)

            logger.info(
                "RUN complete run_id=%s elapsed_ms=%.1f",
                req.run_id, (time.perf_counter() - t0) * 1000,
            )
            return response

        except (ExcelClientError, HTTPException):
            logger.exception("RUN known error run_id=%s", req.run_id)
            raise
        except Exception:
            logger.exception("RUN failed run_id=%s", req.run_id)
            raise

    app.include_router(router)
    logger.info("API ready version=%s", APP_VERSION)
    return app