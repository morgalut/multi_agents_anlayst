
#Multi_agen\packages\agents\role_mapper.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional
import logging

from Multi_agen.packages.core import (
    ALLOWED_COLUMN_ROLES,
    ColumnMapping,
)

logger = logging.getLogger("multi_agen.agents.role_mapper")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)


@dataclass(frozen=True, slots=True)
class RoleMapperConfig:
    """
    Maps resolved column-analysis payloads into validated ColumnMapping objects.

    This agent no longer maps a tiny 5-role summary-sheet schema.
    It converts rich column payloads into the structural SSOT required for
    workbook/sheet extraction.
    """
    max_evidence: int = 10
    clamp_confidence_min: float = 0.0
    clamp_confidence_max: float = 1.0


class RoleMapperAgent:
    """
    Input contract:
      analysis.signals should contain either:
        - "resolved_columns": [ ... ]
        or
        - "columns": [ ... ]

    Each column item should be a dict shaped approximately like:
      {
        "col_idx": 4,
        "role": "entity_value",
        "entity": "LTD",
        "currency": "USD",
        "period": "2022",
        "header_text": "LTD $",
        "formula_pattern": "=SUMIF(...)",
        "row_start": 11,
        "row_end": 199,
        "sheet_name": "BS",
        "confidence": 0.91,
        "evidence": ["header identifies entity", "formula references GL_LTD"]
      }

    Output:
      List[ColumnMapping]
    """

    def __init__(self, config: Optional[RoleMapperConfig] = None):
        self.config = config or RoleMapperConfig()

    def map_roles(
        self,
        task: Any,
        state: Any,
        analysis: Any,
    ) -> List[ColumnMapping]:
        raw_columns = self._extract_raw_columns(analysis)
        mapped: List[ColumnMapping] = []

        for item in raw_columns:
            cm = self._coerce_column_mapping(item=item, default_sheet_name=getattr(task, "sheet_name", ""))
            if cm is None:
                continue
            mapped.append(cm)

        mapped.sort(key=lambda c: c.col_idx)

        logger.info(
            "RoleMapper: mapped sheet=%s columns=%s",
            getattr(task, "sheet_name", "?"),
            [(c.col_letter, c.role, c.entity or "", c.currency or "") for c in mapped],
        )
        return mapped

    def _extract_raw_columns(self, analysis: Any) -> List[Dict[str, Any]]:
        signals = getattr(analysis, "signals", None) or {}
        if not isinstance(signals, dict):
            return []

        candidates = signals.get("resolved_columns")
        if isinstance(candidates, list):
            return [x for x in candidates if isinstance(x, dict)]

        candidates = signals.get("columns")
        if isinstance(candidates, list):
            return [x for x in candidates if isinstance(x, dict)]

        llm_payload = signals.get("llm_columns")
        if isinstance(llm_payload, list):
            return [x for x in llm_payload if isinstance(x, dict)]

        return []

    def _coerce_column_mapping(
        self,
        *,
        item: Dict[str, Any],
        default_sheet_name: str,
    ) -> Optional[ColumnMapping]:
        role = str(item.get("role", "") or "").strip()
        if role not in ALLOWED_COLUMN_ROLES:
            logger.info("RoleMapper: reject invalid role=%r", role)
            return None

        col_idx_raw = item.get("col_idx", None)
        try:
            col_idx = int(col_idx_raw)
        except Exception:
            logger.info("RoleMapper: reject missing/invalid col_idx=%r", col_idx_raw)
            return None

        if col_idx < 0:
            logger.info("RoleMapper: reject negative col_idx=%r", col_idx)
            return None

        col_letter = self._column_index_to_letter(col_idx)

        entity = self._clean_str(item.get("entity", ""))
        currency = self._clean_str(item.get("currency", ""))
        period = self._clean_str(item.get("period", ""))
        header_text = self._clean_str(item.get("header_text", item.get("header", "")))
        formula_pattern = self._clean_str(item.get("formula_pattern", item.get("formula", "")))
        sheet_name = self._clean_str(item.get("sheet_name", default_sheet_name)) or default_sheet_name

        row_start = self._to_optional_positive_int(item.get("row_start"))
        row_end = self._to_optional_positive_int(item.get("row_end"))

        confidence = self._to_confidence(item.get("confidence", 0.0))

        evidence_raw = item.get("evidence", [])
        if not isinstance(evidence_raw, list):
            evidence_raw = []
        evidence = [str(x) for x in evidence_raw[: self.config.max_evidence]]

        # Enforce spec rule:
        # entity is only allowed on entity-bearing roles.
        if role not in {"entity_value", "aje", "consolidated_aje", "consolidated"}:
            entity = ""

        # Defensive normalization:
        # consolidated columns should carry entity "Consolidated" if omitted.
        if role in {"consolidated", "consolidated_aje"} and not entity:
            entity = "Consolidated"

        try:
            return ColumnMapping(
                col_idx=col_idx,
                col_letter=col_letter,
                role=role,
                entity=entity,
                currency=currency,
                period=period,
                header_text=header_text,
                formula_pattern=formula_pattern,
                row_start=row_start,
                row_end=row_end,
                sheet_name=sheet_name,
                confidence=confidence,
                evidence=evidence,
            )
        except Exception as exc:
            logger.info("RoleMapper: reject column col_idx=%s role=%s err=%s", col_idx, role, type(exc).__name__)
            return None

    def _to_optional_positive_int(self, value: Any) -> Optional[int]:
        if value in (None, "", "null"):
            return None
        try:
            iv = int(value)
        except Exception:
            return None
        return iv if iv >= 1 else None

    def _to_confidence(self, value: Any) -> float:
        try:
            conf = float(value)
        except Exception:
            conf = 0.0
        if conf < self.config.clamp_confidence_min:
            return self.config.clamp_confidence_min
        if conf > self.config.clamp_confidence_max:
            return self.config.clamp_confidence_max
        return conf

    def _clean_str(self, value: Any) -> str:
        if value is None:
            return ""
        return str(value).strip()

    def _column_index_to_letter(self, col_idx: int) -> str:
        n = col_idx + 1
        letters: List[str] = []

        while n > 0:
            n, rem = divmod(n - 1, 26)
            letters.append(chr(ord("A") + rem))

        return "".join(reversed(letters))