from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

from Multi_agen.packages.mcp_clients.excel_client import (
    ExcelClientConfig,
    ExcelMCPClient,
    MCPTransport,
)
from .logger import logger


@dataclass(frozen=True, slots=True)
class ToolRouterConfig:
    workbook_path: str = ""
    excel_server_id: str = "excel-mcp"
    max_error_body_chars: int = 4000
    timeout_seconds: Optional[float] = None


class ToolRouter:
    """
    Thin routing layer between agents and MCP tool clients.

    Contract:
    - ToolRouter owns workbook_path
    - ExcelMCPClient sends workbook_path via ctx
    """

    def __init__(
        self,
        transport: MCPTransport,
        config: Optional[ToolRouterConfig] = None,
    ) -> None:
        self._cfg = config or ToolRouterConfig()

        workbook_path = (self._cfg.workbook_path or "").strip()
        if not workbook_path:
            raise ValueError("ToolRouterConfig.workbook_path is required")

        self._excel = ExcelMCPClient(
            transport=transport,
            config=ExcelClientConfig(
                server_id=self._cfg.excel_server_id,
                max_error_body_chars=self._cfg.max_error_body_chars,
                timeout_seconds=self._cfg.timeout_seconds,
            ),
            workbook_path=workbook_path,
        )

        logger.info(
            "ToolRouter ready workbook=%s server=%s",
            workbook_path,
            self._cfg.excel_server_id,
        )

    # ------------------------------------------------------------------
    # Excel tool wrappers
    # ------------------------------------------------------------------

    def excel_list_sheets(self) -> List[str]:
        logger.info("ToolRouter:excel_list_sheets")
        try:
            sheets = self._excel.list_sheets()
            logger.info("ToolRouter:excel_list_sheets ok count=%d", len(sheets))
            return sheets
        except Exception:
            logger.exception("ToolRouter:excel_list_sheets FAILED")
            raise

    def excel_read_sheet_range(
        self,
        sheet_name: str,
        row0: int,
        col0: int,
        nrows: int,
        ncols: int,
    ) -> List[List[Any]]:
        logger.info(
            "ToolRouter:excel_read_sheet_range sheet=%s row0=%d col0=%d nrows=%d ncols=%d",
            sheet_name, row0, col0, nrows, ncols,
        )
        try:
            result = self._excel.read_sheet_range(sheet_name, row0, col0, nrows, ncols)
            logger.info("ToolRouter:excel_read_sheet_range ok rows=%d", len(result))
            return result
        except Exception:
            logger.exception("ToolRouter:excel_read_sheet_range FAILED sheet=%s", sheet_name)
            raise

    def excel_write_cells(
        self,
        sheet_name: str,
        cells: Sequence[Tuple[int, int, Any]],
    ) -> Dict[str, Any]:
        cells_list = list(cells)
        logger.info("ToolRouter:excel_write_cells sheet=%s cells=%d", sheet_name, len(cells_list))
        try:
            result = self._excel.write_cells(sheet_name, cells_list)
            logger.info("ToolRouter:excel_write_cells ok")
            return result
        except Exception:
            logger.exception("ToolRouter:excel_write_cells FAILED sheet=%s", sheet_name)
            raise

    def excel_detect_merged_cells(self, sheet_name: str) -> List[Dict[str, Any]]:
        logger.info("ToolRouter:excel_detect_merged_cells sheet=%s", sheet_name)
        try:
            result = self._excel.detect_merged_cells(sheet_name)
            logger.info("ToolRouter:excel_detect_merged_cells ok count=%d", len(result))
            return result
        except Exception:
            logger.exception("ToolRouter:excel_detect_merged_cells FAILED sheet=%s", sheet_name)
            raise

    def excel_get_formulas(
        self,
        sheet_name: str,
        row0: int,
        col0: int,
        nrows: int,
        ncols: int,
    ) -> List[List[Optional[str]]]:
        logger.info(
            "ToolRouter:excel_get_formulas sheet=%s row0=%d col0=%d nrows=%d ncols=%d",
            sheet_name, row0, col0, nrows, ncols,
        )

        try:
            result = self._excel.get_formulas(sheet_name, row0, col0, nrows, ncols)
            logger.info("ToolRouter:excel_get_formulas ok rows=%d", len(result))
            return result

        except Exception as first_exc:
            logger.exception(
                "ToolRouter:excel_get_formulas FAILED first_attempt sheet=%s",
                sheet_name,
            )

            # Retry once with a smaller preview window
            small_nrows = max(1, min(nrows, 5))
            small_ncols = max(1, min(ncols, 8))

            if small_nrows == nrows and small_ncols == ncols:
                raise first_exc

            try:
                logger.info(
                    "ToolRouter:excel_get_formulas retry_small sheet=%s nrows=%d ncols=%d",
                    sheet_name, small_nrows, small_ncols,
                )
                result = self._excel.get_formulas(
                    sheet_name=sheet_name,
                    row0=row0,
                    col0=col0,
                    nrows=small_nrows,
                    ncols=small_ncols,
                )
                logger.info(
                    "ToolRouter:excel_get_formulas retry_small ok rows=%d",
                    len(result),
                )
                return result

            except Exception as retry_exc:
                logger.exception(
                    "ToolRouter:excel_get_formulas FAILED retry_small sheet=%s",
                    sheet_name,
                )
                raise retry_exc from first_exc

    def excel_get_formulas_safe(
        self,
        sheet_name: str,
        row0: int,
        col0: int,
        nrows: int,
        ncols: int,
    ) -> List[List[Optional[str]]]:
        try:
            return self.excel_get_formulas(
                sheet_name=sheet_name,
                row0=row0,
                col0=col0,
                nrows=nrows,
                ncols=ncols,
            )
        except Exception:
            logger.exception("ToolRouter:excel_get_formulas_safe FAILED sheet=%s", sheet_name)
            return []

    def excel_find_text(
        self,
        sheet_name: str,
        query: str,
        max_hits: int = 50,
    ) -> List[Dict[str, Any]]:
        logger.info("ToolRouter:excel_find_text sheet=%s query=%r", sheet_name, query)
        try:
            result = self._excel.find_text(sheet_name, query, max_hits)
            logger.info("ToolRouter:excel_find_text ok hits=%d", len(result))
            return result
        except Exception:
            logger.exception("ToolRouter:excel_find_text FAILED sheet=%s", sheet_name)
            raise

    def excel_column_index_to_letter(self, col_idx: int) -> str:
        return ExcelMCPClient.column_index_to_letter(col_idx)