from __future__ import annotations

from typing import Any, Dict

from fastapi import FastAPI
from pydantic import BaseModel

from .server import ExcelMCPServer


# TODO: replace with your real workbook provider (openpyxl loader etc.)
class WorkbookProvider:
    def open(self, workbook_path: str):
        raise NotImplementedError


workbook_provider = WorkbookProvider()
excel_server = ExcelMCPServer(workbook_provider=workbook_provider)

app = FastAPI(title="excel-mcp", version="0.1.0")


# ---- Router transport contract (transport_http.py) ----
class CallRequest(BaseModel):
    tool_name: str
    args: Dict[str, Any] = {}
    ctx: Dict[str, Any] = {}


# ---- Legacy shape (kept for compatibility) ----
class CallToolRequest(BaseModel):
    tool: str
    args: Dict[str, Any] = {}
    ctx: Dict[str, Any] = {}


@app.get("/health")
def health():
    return {"ok": True}


@app.get("/list_tools")
def list_tools():
    return {"tools": excel_server.list_tools()}


# ✅ Required by HttpMCPTransport: POST /call
@app.post("/call")
def call(req: CallRequest):
    return excel_server.dispatch(req.tool_name, req.args, req.ctx)


# (Optional) keep legacy endpoint
@app.post("/call_tool")
def call_tool(req: CallToolRequest):
    return excel_server.dispatch(req.tool, req.args, req.ctx)