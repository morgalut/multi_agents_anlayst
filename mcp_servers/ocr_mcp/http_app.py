from __future__ import annotations

import os
from typing import Any, Dict

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from .server import OCRMCPServer

# ---- OCR backend selection ----
# You need to implement/provide an OCR backend object that has:
#   - render_sheet_image(workbook_path, sheet_name) -> image_path (str)
#   - extract_text(image_path) -> dict (or text)
#
# If you already have one elsewhere, import and use it here.
class DummyOCRBackend:
    def render_sheet_image(self, workbook_path: str, sheet_name: str) -> str:
        raise NotImplementedError("Configure a real OCR backend")

    def extract_text(self, image_path: str) -> Dict[str, Any]:
        raise NotImplementedError("Configure a real OCR backend")


def build_server() -> OCRMCPServer:
    # TODO: replace DummyOCRBackend with your real backend
    backend = DummyOCRBackend()
    return OCRMCPServer(backend)


app = FastAPI(title="ocr-mcp", version="0.1.0")
_server = build_server()


class InvokeRequest(BaseModel):
    tool_name: str
    args: Dict[str, Any] = {}
    ctx: Dict[str, Any] = {}


@app.get("/health")
def health() -> Dict[str, Any]:
    return {"ok": True, "service": "ocr-mcp"}


@app.get("/tools")
def tools() -> Dict[str, Any]:
    return {"tools": _server.list_tools()}


@app.post("/invoke")
def invoke(req: InvokeRequest) -> Dict[str, Any]:
    try:
        return _server.dispatch(tool_name=req.tool_name, args=req.args, ctx=req.ctx)
    except KeyError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        # Keep it readable for debugging
        raise HTTPException(status_code=500, detail=f"Tool failed: {e}")