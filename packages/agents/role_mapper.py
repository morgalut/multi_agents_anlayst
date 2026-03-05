from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional
import logging

from Multi_agen.packages.core import (
    RoleCandidate,
    ROLE_AJE,
    ROLE_CONSOLIDATED,
    ROLE_MAIN_COMPANY_DOLLAR,
    ROLE_MAIN_COMPANY_IL,
    ROLE_SUB_COMPANY,
)

logger = logging.getLogger("multi_agen.agents.role_mapper")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)


@dataclass(frozen=True, slots=True)
class RoleMapperConfig:
    """
    This agent does NOT decide final acceptance.
    It only converts LLM role JSON -> RoleCandidate objects.
    Acceptance thresholds are applied later in EntityColumnLetterResolver / confidence gates.
    """
    max_evidence: int = 10


class RoleMapperAgent:
    def __init__(self, config: Optional[RoleMapperConfig] = None):
        self.config = config or RoleMapperConfig()

    def map_roles(self, task, state, analysis) -> Dict[str, Optional[RoleCandidate]]:
        """
        Returns a canonical role map:
          {
            "main_company_dollar": RoleCandidate|None,
            "main_company_il": RoleCandidate|None,
            "sub_company": RoleCandidate|None,
            "aje": RoleCandidate|None,
            "consolidated": RoleCandidate|None,
          }
        """
        roles_obj: Any = (getattr(analysis, "signals", None) or {}).get("llm_roles", {})
        if not isinstance(roles_obj, dict):
            logger.info("RoleMapper: llm_roles missing or not dict sheet=%s", getattr(task, "sheet_name", "?"))
            roles_obj = {}

        def pick(role_key: str) -> Optional[RoleCandidate]:
            r = roles_obj.get(role_key)
            if not isinstance(r, dict):
                return None

            col_idx = r.get("col_idx", None)
            conf = r.get("confidence", 0.0)
            ev = r.get("evidence", [])

            if col_idx is None:
                return None

            try:
                col_i = int(col_idx)
                conf_f = float(conf)
            except Exception:
                return None

            if not isinstance(ev, list):
                ev = []

            return RoleCandidate(
                col_idx=col_i,
                confidence=conf_f,
                evidence=[str(x) for x in ev][: self.config.max_evidence],
            )

        mapped = {
            ROLE_MAIN_COMPANY_DOLLAR: pick(ROLE_MAIN_COMPANY_DOLLAR),
            ROLE_MAIN_COMPANY_IL: pick(ROLE_MAIN_COMPANY_IL),
            ROLE_SUB_COMPANY: pick(ROLE_SUB_COMPANY),
            ROLE_AJE: pick(ROLE_AJE),
            ROLE_CONSOLIDATED: pick(ROLE_CONSOLIDATED),
        }

        logger.info(
            "RoleMapper: mapped sheet=%s roles=%s",
            getattr(task, "sheet_name", "?"),
            {k: (v.col_idx if v else None) for k, v in mapped.items()},
        )

        return mapped