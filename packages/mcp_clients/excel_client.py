from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple
import json

try:
    from urllib.error import HTTPError, URLError
except Exception:  # pragma: no cover
    HTTPError = Exception  # type: ignore
    URLError = Exception  # type: ignore


class MCPTransport:
    """
    Minimal transport abstraction (stdio/http/etc).

    Implement:
        call_tool(server_id, tool_name, args, ctx=None) -> dict
    """
    def call_tool(
        self,
        server_id: str,
        tool_name: str,
        args: Dict[str, Any],
        ctx: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        raise NotImplementedError


@dataclass(frozen=True, slots=True)
class ExcelClientConfig:
    server_id: str = "excel-mcp"
    max_error_body_chars: int = 4000


class ExcelClientError(RuntimeError):
    """
    Domain-specific client error that carries MCP/tool details.
    """

    def __init__(
        self,
        message: str,
        *,
        server_id: str,
        tool_name: str,
        args: Optional[Dict[str, Any]] = None,
        status_code: Optional[int] = None,
        response_body: Optional[str] = None,
        cause: Optional[BaseException] = None,
    ) -> None:
        super().__init__(message)
        self.server_id = server_id
        self.tool_name = tool_name
        self.args_payload = args or {}
        self.status_code = status_code
        self.response_body = response_body
        self.cause = cause

    def to_dict(self) -> Dict[str, Any]:
        return {
            "message": str(self),
            "server_id": self.server_id,
            "tool_name": self.tool_name,
            "args": self.args_payload,
            "status_code": self.status_code,
            "response_body": self.response_body,
            "cause": type(self.cause).__name__ if self.cause else None,
        }


class ExcelMCPClient:
    """
    Thin wrapper around MCP Excel tools.

    Contract:
    - tool args go in `args`
    - workbook identity goes in `ctx["workbook_path"]`
    """

    def __init__(
        self,
        transport: MCPTransport,
        config: Optional[ExcelClientConfig] = None,
        workbook_path: str = "",
    ) -> None:
        self._t = transport
        self._cfg = config or ExcelClientConfig()
        self._workbook_path = workbook_path

    # ------------------------------------------------------------------
    # Public Excel MCP tools
    # ------------------------------------------------------------------

    def list_sheets(self) -> List[str]:
        resp = self._call_tool("excel.list_sheets", {})
        sheets = self._extract_list(resp, keys=("sheets", "sheet_names"))
        return [str(x) for x in sheets]

    def read_sheet_range(
        self,
        sheet_name: str,
        row0: int,
        col0: int,
        nrows: int,
        ncols: int,
    ) -> List[List[Any]]:
        args = {
            "sheet_name": sheet_name,
            "row0": row0,
            "col0": col0,
            "nrows": nrows,
            "ncols": ncols,
        }
        resp = self._call_tool("excel.read_sheet_range", args)
        grid = self._extract_list(resp, keys=("grid", "values", "cells"))
        return [list(row) if isinstance(row, (list, tuple)) else [row] for row in grid]

    def write_cells(self, sheet_name: str, cells: Sequence[Tuple[int, int, Any]]) -> Dict[str, Any]:
        args = {
            "sheet_name": sheet_name,
            "cells": [{"row": r, "col": c, "value": v} for (r, c, v) in cells],
        }
        return self._call_tool("excel.write_cells", args)

    def detect_merged_cells(self, sheet_name: str) -> List[Dict[str, Any]]:
        resp = self._call_tool("excel.detect_merged_cells", {"sheet_name": sheet_name})
        merged = self._extract_list(resp, keys=("merged_ranges", "ranges", "merged"))
        return [item for item in merged if isinstance(item, dict)]

    def get_formulas(
        self,
        sheet_name: str,
        row0: int,
        col0: int,
        nrows: int,
        ncols: int,
    ) -> List[List[Optional[str]]]:
        args = {
            "sheet_name": sheet_name,
            "row0": row0,
            "col0": col0,
            "nrows": nrows,
            "ncols": ncols,
        }
        resp = self._call_tool("excel.get_formulas", args)
        formulas = self._extract_list(resp, keys=("formulas",))
        normalized: List[List[Optional[str]]] = []
        for row in formulas:
            if isinstance(row, (list, tuple)):
                normalized.append([None if v is None else str(v) for v in row])
            else:
                normalized.append([None if row is None else str(row)])
        return normalized

    def find_text(self, sheet_name: str, query: str, max_hits: int = 50) -> List[Dict[str, Any]]:
        args = {
            "sheet_name": sheet_name,
            "query": query,
            "max_hits": max_hits,
        }
        resp = self._call_tool("excel.find_text", args)
        hits = self._extract_list(resp, keys=("hits", "matches", "results"))
        return [item for item in hits if isinstance(item, dict)]

    # ------------------------------------------------------------------
    # Core transport wrapper
    # ------------------------------------------------------------------

    def _call_tool(self, tool_name: str, args: Dict[str, Any]) -> Dict[str, Any]:
        ctx = self._build_ctx()

        try:
            resp = self._t.call_tool(self._cfg.server_id, tool_name, args, ctx=ctx)
        except HTTPError as exc:
            body = self._read_http_error_body(exc)
            raise ExcelClientError(
                f"MCP tool failed with HTTP {getattr(exc, 'code', None)}: {tool_name}",
                server_id=self._cfg.server_id,
                tool_name=tool_name,
                args=args,
                status_code=getattr(exc, "code", None),
                response_body=body,
                cause=exc,
            ) from exc
        except URLError as exc:
            raise ExcelClientError(
                f"MCP transport error calling tool: {tool_name}",
                server_id=self._cfg.server_id,
                tool_name=tool_name,
                args=args,
                response_body=str(exc),
                cause=exc,
            ) from exc
        except Exception as exc:
            raise ExcelClientError(
                f"Unexpected MCP error calling tool: {tool_name}",
                server_id=self._cfg.server_id,
                tool_name=tool_name,
                args=args,
                cause=exc,
            ) from exc

        if not isinstance(resp, dict):
            raise ExcelClientError(
                f"Invalid MCP response type for tool: {tool_name}",
                server_id=self._cfg.server_id,
                tool_name=tool_name,
                args=args,
                response_body=repr(resp),
            )

        if "error" in resp and resp["error"]:
            raise ExcelClientError(
                f"MCP tool returned logical error: {tool_name}",
                server_id=self._cfg.server_id,
                tool_name=tool_name,
                args=args,
                response_body=self._stringify(resp["error"]),
            )

        return resp

    def _build_ctx(self) -> Dict[str, Any]:
        workbook_path = (self._workbook_path or "").strip()
        if not workbook_path:
            raise ExcelClientError(
                "Workbook path is empty; cannot call Excel MCP tool",
                server_id=self._cfg.server_id,
                tool_name="(context-build)",
                args={},
            )
        return {"workbook_path": workbook_path}

    # ------------------------------------------------------------------
    # Response extraction helpers
    # ------------------------------------------------------------------

    def _extract_list(self, resp: Dict[str, Any], keys: Tuple[str, ...]) -> List[Any]:
        for key in keys:
            if isinstance(resp.get(key), list):
                return list(resp[key])

        nested_result = resp.get("result")
        if isinstance(nested_result, dict):
            for key in keys:
                if isinstance(nested_result.get(key), list):
                    return list(nested_result[key])

        nested_data = resp.get("data")
        if isinstance(nested_data, dict):
            for key in keys:
                if isinstance(nested_data.get(key), list):
                    return list(nested_data[key])

        return []

    def _read_http_error_body(self, exc: BaseException) -> Optional[str]:
        cached = getattr(exc, "_cached_body", None)
        if cached is not None:
            return cached

        fp = getattr(exc, "fp", None)
        if fp is None:
            return None

        try:
            raw = fp.read()
            if raw is None:
                return None
            if isinstance(raw, bytes):
                text = raw.decode("utf-8", errors="replace")
            else:
                text = str(raw)
            text = text.strip()
            if len(text) > self._cfg.max_error_body_chars:
                text = text[: self._cfg.max_error_body_chars] + "...[truncated]"
            return text or None
        except Exception:
            return None

    def _stringify(self, value: Any) -> str:
        try:
            return json.dumps(value, ensure_ascii=False)
        except Exception:
            return repr(value)

    # ------------------------------------------------------------------
    # Client-side helper (no MCP call)
    # ------------------------------------------------------------------

    @staticmethod
    def column_index_to_letter(col_idx: int) -> str:
        if col_idx < 0:
            raise ValueError("col_idx must be >= 0")

        n = col_idx + 1
        letters: List[str] = []

        while n > 0:
            n, rem = divmod(n - 1, 26)
            letters.append(chr(ord("A") + rem))

        return "".join(reversed(letters))