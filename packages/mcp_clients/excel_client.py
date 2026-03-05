from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple


class MCPTransport:
    """
    Minimal transport abstraction (stdio/http/etc).
    Implement call_tool(tool_name, args) -> dict.
    """
    def call_tool(self, server_id: str, tool_name: str, args: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError


@dataclass(frozen=True, slots=True)
class ExcelClientConfig:
    server_id: str = "excel-mcp"


class ExcelMCPClient:
    """
    Thin wrapper around MCP Excel tools.
    """

    def __init__(self, transport: MCPTransport, config: Optional[ExcelClientConfig] = None):
        self._t = transport
        self._cfg = config or ExcelClientConfig()

    # ---- Excel MCP tools (names match capabilities) ----

    def list_sheets(self) -> List[str]:
        resp = self._t.call_tool(self._cfg.server_id, "excel.list_sheets", {})
        return list(resp.get("sheets", []))

    def read_sheet_range(self, sheet_name: str, row0: int, col0: int, nrows: int, ncols: int) -> List[List[Any]]:
        args = {"sheet_name": sheet_name, "row0": row0, "col0": col0, "nrows": nrows, "ncols": ncols}
        resp = self._t.call_tool(self._cfg.server_id, "excel.read_sheet_range", args)
        return list(resp.get("grid", []))

    def write_cells(self, sheet_name: str, cells: Sequence[Tuple[int, int, Any]]) -> Dict[str, Any]:
        # cells: [(row_idx, col_idx, value), ...]
        args = {"sheet_name": sheet_name, "cells": [{"row": r, "col": c, "value": v} for (r, c, v) in cells]}
        resp = self._t.call_tool(self._cfg.server_id, "excel.write_cells", args)
        return resp

    def detect_merged_cells(self, sheet_name: str) -> List[Dict[str, Any]]:
        resp = self._t.call_tool(self._cfg.server_id, "excel.detect_merged_cells", {"sheet_name": sheet_name})
        return list(resp.get("merged_ranges", []))

    def get_formulas(self, sheet_name: str, row0: int, col0: int, nrows: int, ncols: int) -> List[List[Optional[str]]]:
        args = {"sheet_name": sheet_name, "row0": row0, "col0": col0, "nrows": nrows, "ncols": ncols}
        resp = self._t.call_tool(self._cfg.server_id, "excel.get_formulas", args)
        return list(resp.get("formulas", []))

    def find_text(self, sheet_name: str, query: str, max_hits: int = 50) -> List[Dict[str, Any]]:
        args = {"sheet_name": sheet_name, "query": query, "max_hits": max_hits}
        resp = self._t.call_tool(self._cfg.server_id, "excel.find_text", args)
        return list(resp.get("hits", []))

    # ---- client-side helper (no MCP call) ----

    @staticmethod
    def column_index_to_letter(col_idx: int) -> str:
        """
        0-based column index -> Excel letters (A, B, ..., Z, AA, AB, ...).
        """
        if col_idx < 0:
            raise ValueError("col_idx must be >= 0")
        n = col_idx + 1
        letters = []
        while n > 0:
            n, rem = divmod(n - 1, 26)
            letters.append(chr(ord("A") + rem))
        return "".join(reversed(letters))