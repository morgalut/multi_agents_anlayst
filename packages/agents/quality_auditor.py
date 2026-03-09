from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, List, Optional

logger = logging.getLogger("multi_agen.agents.quality_auditor")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)


@dataclass(frozen=True, slots=True)
class QualityAuditorConfig:
    """
    Structural audit configuration.

    This agent validates sheet extraction quality after column mapping.
    """

    require_coa_column: bool = True
    require_entity_column: bool = True
    require_row_bounds: bool = False
    warn_if_many_other_columns: bool = True

    max_other_ratio: float = 0.7


class QualityAuditorAgent:
    """
    Structural sheet-quality reviewer.

    Input:
        task
        state
        analysis
        tools

    Output:
        List[str] quality flags

    This agent is intentionally conservative:
    - it never modifies results
    - it only reports structural risks
    """

    def __init__(self, config: Optional[QualityAuditorConfig] = None):
        self.config = config or QualityAuditorConfig()

    def audit(
        self,
        task: Any,
        state: Any,
        analysis: Any,
        tools: Any,
        prompt_profile: Any = None,
    ) -> List[str]:

        logger.info("QualityAudit:start sheet=%s", getattr(task, "sheet_name", "?"))

        signals = getattr(analysis, "signals", None) or {}
        columns = signals.get("resolved_columns") or signals.get("columns") or []

        flags: List[str] = []

        if not columns:
            flags.append("no_columns_detected")
            return flags

        roles = [str(col.get("role", "")) for col in columns]

        # ------------------------------------------------------------
        # COA column check
        # ------------------------------------------------------------
        if self.config.require_coa_column:
            if "coa_name" not in roles:
                flags.append("missing_coa_column")

        # ------------------------------------------------------------
        # Entity column check
        # ------------------------------------------------------------
        if self.config.require_entity_column:
            if "entity_value" not in roles:
                flags.append("missing_entity_column")

        # ------------------------------------------------------------
        # Row bounds check
        # ------------------------------------------------------------
        if self.config.require_row_bounds:
            missing_bounds = True

            for col in columns:
                rs = col.get("row_start")
                re = col.get("row_end")
                if isinstance(rs, int) and isinstance(re, int):
                    missing_bounds = False
                    break

            if missing_bounds:
                flags.append("missing_row_bounds")

        # ------------------------------------------------------------
        # Consolidated sanity check
        # ------------------------------------------------------------
        if "consolidated" in roles and "entity_value" not in roles:
            flags.append("consolidated_without_entities")

        # ------------------------------------------------------------
        # AJE sanity check
        # ------------------------------------------------------------
        if "aje" in roles and "entity_value" not in roles:
            flags.append("aje_without_entity_columns")

        # ------------------------------------------------------------
        # Role distribution sanity check
        # ------------------------------------------------------------
        if self.config.warn_if_many_other_columns:
            total = len(roles)
            other_count = roles.count("other")

            if total > 0 and (other_count / total) > self.config.max_other_ratio:
                flags.append("too_many_other_columns")

        # ------------------------------------------------------------
        # Classification sanity
        # ------------------------------------------------------------
        classification = getattr(analysis, "classification", None)

        if classification is not None:
            types = getattr(classification, "types", [])

            if not types:
                flags.append("classification_empty")

        logger.info(
            "QualityAudit:done sheet=%s flags=%s",
            getattr(task, "sheet_name", "?"),
            flags,
        )

        return flags