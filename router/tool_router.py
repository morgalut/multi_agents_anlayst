from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

import logging
import time

# Prefer your central logger if it exists
try:
    from Multi_agen.router.logger import logger as _logger  # type: ignore
except Exception:  # pragma: no cover
    _logger = logging.getLogger("multi_agen.router.tool_router")
    if not _logger.handlers:
        logging.basicConfig(level=logging.INFO)

# Import your MCP clients (from earlier skeleton)
from Multi_agen.packages.mcp_clients.excel_client import ExcelMCPClient, ExcelClientConfig, MCPTransport
from Multi_agen.packages.mcp_clients.ocr_client import OCRMCPClient, OcrClientConfig
from Multi_agen.packages.mcp_clients.mem0_client import Mem0MCPClient, Mem0ClientConfig


@dataclass(frozen=True, slots=True)
class ToolRouterConfig:
    workbook_path: str
    excel_server_id: str = "excel-mcp"
    ocr_server_id: str = "ocr-mcp"
    mem0_server_id: str = "mem0-mcp"


class ToolRouter:
    """
    Object passed into agents as `tools`.

    Exposes endpoint-like methods agents call (excel_read_sheet_range, etc.)
    and routes them to MCP clients, injecting ctx={"workbook_path": ...} for Excel tools.
    """

    def __init__(self, transport: MCPTransport, config: ToolRouterConfig):
        self._cfg = config

        _logger.info(
            "ToolRouter:init workbook_path=%s excel_server=%s ocr_server=%s mem0_server=%s",
            self._cfg.workbook_path,
            self._cfg.excel_server_id,
            self._cfg.ocr_server_id,
            self._cfg.mem0_server_id,
        )

        t0 = time.perf_counter()
        try:
            self._excel = ExcelMCPClient(
                transport=transport,
                config=ExcelClientConfig(server_id=config.excel_server_id),
            )
            self._ocr = OCRMCPClient(
                transport=transport,
                config=OcrClientConfig(server_id=config.ocr_server_id),
            )
            self._mem0 = Mem0MCPClient(
                transport=transport,
                config=Mem0ClientConfig(server_id=config.mem0_server_id),
            )
        except Exception:
            _logger.exception("ToolRouter:init FAILED")
            raise
        finally:
            _logger.info("ToolRouter:init done elapsed_ms=%.2f", (time.perf_counter() - t0) * 1000)

    # -----------------------------
    # Excel endpoints (agents use these)
    # -----------------------------

    def excel_list_sheets(self) -> List[str]:
        _logger.info("ToolRouter:excel_list_sheets start")
        t0 = time.perf_counter()
        try:
            sheets = self._excel.list_sheets()
            _logger.info(
                "ToolRouter:excel_list_sheets ok count=%d elapsed_ms=%.2f",
                len(sheets),
                (time.perf_counter() - t0) * 1000,
            )
            _logger.debug("ToolRouter:excel_list_sheets sheets=%s", sheets)
            return sheets
        except Exception:
            _logger.exception("ToolRouter:excel_list_sheets FAILED")
            raise

    def excel_read_sheet_range(
        self, sheet_name: str, row0: int, col0: int, nrows: int, ncols: int
    ) -> List[List[Any]]:
        _logger.info(
            "ToolRouter:excel_read_sheet_range start sheet=%s row0=%d col0=%d nrows=%d ncols=%d",
            sheet_name, row0, col0, nrows, ncols
        )
        t0 = time.perf_counter()
        try:
            resp = self._excel._t.call_tool(  # type: ignore[attr-defined]
                self._excel._cfg.server_id,  # type: ignore[attr-defined]
                "excel.read_sheet_range",
                {"sheet_name": sheet_name, "row0": row0, "col0": col0, "nrows": nrows, "ncols": ncols},
                ctx={"workbook_path": self._cfg.workbook_path},
            )
            grid = resp.get("grid", [])
            rows = len(grid)
            cols = len(grid[0]) if rows > 0 and isinstance(grid[0], list) else 0
            _logger.info(
                "ToolRouter:excel_read_sheet_range ok sheet=%s grid=%dx%d elapsed_ms=%.2f",
                sheet_name, rows, cols, (time.perf_counter() - t0) * 1000
            )
            return grid
        except Exception:
            _logger.exception(
                "ToolRouter:excel_read_sheet_range FAILED sheet=%s row0=%d col0=%d nrows=%d ncols=%d",
                sheet_name, row0, col0, nrows, ncols
            )
            raise

    def excel_write_cells(self, sheet_name: str, cells: Sequence[Tuple[int, int, Any]]) -> Dict[str, Any]:
        _logger.info("ToolRouter:excel_write_cells start sheet=%s count=%d", sheet_name, len(cells))
        t0 = time.perf_counter()
        try:
            resp = self._excel._t.call_tool(  # type: ignore[attr-defined]
                self._excel._cfg.server_id,  # type: ignore[attr-defined]
                "excel.write_cells",
                {"sheet_name": sheet_name, "cells": [{"row": r, "col": c, "value": v} for (r, c, v) in cells]},
                ctx={"workbook_path": self._cfg.workbook_path},
            )
            written = resp.get("written")
            _logger.info(
                "ToolRouter:excel_write_cells ok sheet=%s written=%s elapsed_ms=%.2f",
                sheet_name, str(written), (time.perf_counter() - t0) * 1000
            )
            return resp
        except Exception:
            _logger.exception("ToolRouter:excel_write_cells FAILED sheet=%s count=%d", sheet_name, len(cells))
            raise

    def excel_detect_merged_cells(self, sheet_name: str) -> List[Dict[str, Any]]:
        _logger.info("ToolRouter:excel_detect_merged_cells start sheet=%s", sheet_name)
        t0 = time.perf_counter()
        try:
            resp = self._excel._t.call_tool(  # type: ignore[attr-defined]
                self._excel._cfg.server_id,  # type: ignore[attr-defined]
                "excel.detect_merged_cells",
                {"sheet_name": sheet_name},
                ctx={"workbook_path": self._cfg.workbook_path},
            )
            merged = resp.get("merged_ranges", [])
            _logger.info(
                "ToolRouter:excel_detect_merged_cells ok sheet=%s ranges=%d elapsed_ms=%.2f",
                sheet_name, len(merged), (time.perf_counter() - t0) * 1000
            )
            return merged
        except Exception:
            _logger.exception("ToolRouter:excel_detect_merged_cells FAILED sheet=%s", sheet_name)
            raise

    def excel_get_formulas(
        self, sheet_name: str, row0: int, col0: int, nrows: int, ncols: int
    ) -> List[List[Optional[str]]]:
        _logger.info(
            "ToolRouter:excel_get_formulas start sheet=%s row0=%d col0=%d nrows=%d ncols=%d",
            sheet_name, row0, col0, nrows, ncols
        )
        t0 = time.perf_counter()
        try:
            resp = self._excel._t.call_tool(  # type: ignore[attr-defined]
                self._excel._cfg.server_id,  # type: ignore[attr-defined]
                "excel.get_formulas",
                {"sheet_name": sheet_name, "row0": row0, "col0": col0, "nrows": nrows, "ncols": ncols},
                ctx={"workbook_path": self._cfg.workbook_path},
            )
            formulas = resp.get("formulas", [])
            rows = len(formulas)
            cols = len(formulas[0]) if rows > 0 and isinstance(formulas[0], list) else 0

            # count how many formula strings exist
            non_null = 0
            for r in formulas:
                if isinstance(r, list):
                    for v in r:
                        if v:
                            non_null += 1

            _logger.info(
                "ToolRouter:excel_get_formulas ok sheet=%s grid=%dx%d formulas=%d elapsed_ms=%.2f",
                sheet_name, rows, cols, non_null, (time.perf_counter() - t0) * 1000
            )
            return formulas
        except Exception:
            _logger.exception("ToolRouter:excel_get_formulas FAILED sheet=%s", sheet_name)
            raise

    def excel_find_text(self, sheet_name: str, query: str, max_hits: int = 50) -> List[Dict[str, Any]]:
        _logger.info(
            "ToolRouter:excel_find_text start sheet=%s query=%r max_hits=%d",
            sheet_name, query, max_hits
        )
        t0 = time.perf_counter()
        try:
            resp = self._excel._t.call_tool(  # type: ignore[attr-defined]
                self._excel._cfg.server_id,  # type: ignore[attr-defined]
                "excel.find_text",
                {"sheet_name": sheet_name, "query": query, "max_hits": max_hits},
                ctx={"workbook_path": self._cfg.workbook_path},
            )
            hits = resp.get("hits", [])
            _logger.info(
                "ToolRouter:excel_find_text ok sheet=%s hits=%d elapsed_ms=%.2f",
                sheet_name, len(hits), (time.perf_counter() - t0) * 1000
            )
            # Keep debug only (may include cell contents)
            _logger.debug("ToolRouter:excel_find_text hits=%s", hits[:10])
            return hits
        except Exception:
            _logger.exception("ToolRouter:excel_find_text FAILED sheet=%s query=%r", sheet_name, query)
            raise

    def excel_column_index_to_letter(self, col_idx: int) -> str:
        _logger.info("ToolRouter:excel_column_index_to_letter start col_idx=%d", col_idx)
        t0 = time.perf_counter()
        try:
            letter = self._excel.column_index_to_letter(col_idx)
            _logger.info(
                "ToolRouter:excel_column_index_to_letter ok col_idx=%d letter=%s elapsed_ms=%.2f",
                col_idx, letter, (time.perf_counter() - t0) * 1000
            )
            return letter
        except Exception:
            _logger.exception("ToolRouter:excel_column_index_to_letter FAILED col_idx=%d", col_idx)
            raise

    # -----------------------------
    # OCR endpoints (optional fallback)
    # -----------------------------

    def ocr_render_sheet_image(self, sheet_name: str) -> Dict[str, Any]:
        _logger.info("ToolRouter:ocr_render_sheet_image start sheet=%s", sheet_name)
        t0 = time.perf_counter()
        try:
            resp = self._ocr.render_sheet_image(workbook_path=self._cfg.workbook_path, sheet_name=sheet_name)
            _logger.info(
                "ToolRouter:ocr_render_sheet_image ok sheet=%s image_path=%s elapsed_ms=%.2f",
                sheet_name, resp.get("image_path"), (time.perf_counter() - t0) * 1000
            )
            return resp
        except Exception:
            _logger.exception("ToolRouter:ocr_render_sheet_image FAILED sheet=%s", sheet_name)
            raise

    def ocr_extract_text(self, image_path: str) -> Dict[str, Any]:
        _logger.info("ToolRouter:ocr_extract_text start image_path=%s", image_path)
        t0 = time.perf_counter()
        try:
            resp = self._ocr.extract_text(image_path=image_path)
            text_len = len(resp.get("text", "") or "")
            blocks = resp.get("blocks", [])
            _logger.info(
                "ToolRouter:ocr_extract_text ok text_len=%d blocks=%d elapsed_ms=%.2f",
                text_len, len(blocks) if isinstance(blocks, list) else 0, (time.perf_counter() - t0) * 1000
            )
            return resp
        except Exception:
            _logger.exception("ToolRouter:ocr_extract_text FAILED image_path=%s", image_path)
            raise

    # -----------------------------
    # Memory endpoints (optional)
    # -----------------------------

    def memory_get(self, key: str) -> Dict[str, Any]:
        _logger.info("ToolRouter:memory_get start key=%s", key)
        t0 = time.perf_counter()
        try:
            resp = self._mem0.get(key)
            _logger.info(
                "ToolRouter:memory_get ok key=%s elapsed_ms=%.2f",
                key, (time.perf_counter() - t0) * 1000
            )
            _logger.debug("ToolRouter:memory_get resp=%s", resp)
            return resp
        except Exception:
            _logger.exception("ToolRouter:memory_get FAILED key=%s", key)
            raise

    def memory_put(self, key: str, value: Any) -> Dict[str, Any]:
        # avoid logging full values (can be big); log type/size only
        v_type = type(value).__name__
        v_size = None
        try:
            v_size = len(value)  # type: ignore[arg-type]
        except Exception:
            v_size = None

        _logger.info("ToolRouter:memory_put start key=%s value_type=%s value_len=%s", key, v_type, str(v_size))
        t0 = time.perf_counter()
        try:
            resp = self._mem0.put(key, value)
            _logger.info(
                "ToolRouter:memory_put ok key=%s elapsed_ms=%.2f",
                key, (time.perf_counter() - t0) * 1000
            )
            _logger.debug("ToolRouter:memory_put resp=%s", resp)
            return resp
        except Exception:
            _logger.exception("ToolRouter:memory_put FAILED key=%s", key)
            raise