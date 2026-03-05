from __future__ import annotations

from typing import Any, Dict

from fastapi import FastAPI
from pydantic import BaseModel

from .server import ExcelMCPServer


# TODO: replace with your real workbook provider (openpyxl loader etc.)
class WorkbookProvider:
    def open(self, workbook_path: str):
        # Example placeholder:
        # import openpyxl
        # return openpyxl.load_workbook(workbook_path)
        raise NotImplementedError


workbook_provider = WorkbookProvider()
excel_server = ExcelMCPServer(workbook_provider=workbook_provider)

app = FastAPI(title="excel-mcp", version="0.1.0")


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


@app.post("/call_tool")
def call_tool(req: CallToolRequest):
    return excel_server.dispatch(req.tool, req.args, req.ctx)