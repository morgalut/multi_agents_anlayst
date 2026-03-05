from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional
import logging

from Multi_agen.packages.core import (
    MissingValueSentinel,
    RoleCandidate,
    accept,
    T_CONSOLIDATED,
    T_ROLE,
    ROLE_CONSOLIDATED,
)

logger = logging.getLogger("multi_agen.agents.entity_resolver")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)


@dataclass(frozen=True, slots=True)
class EntityResolverConfig:
    """
    Thresholding is enforced here:
      - consolidated uses stricter threshold (T_CONSOLIDATED)
      - all others use T_ROLE
    """
    pass


class EntityColumnLetterResolver:
    """
    Converts role candidates (col_idx) into Excel column letters using:
      tools.excel_column_index_to_letter

    Missing/uncertain => MissingValueSentinel ("no")
    """

    def __init__(self, config: Optional[EntityResolverConfig] = None):
        self.config = config or EntityResolverConfig()

    def resolve(self, task, state, role_map: Dict[str, Optional[RoleCandidate]], tools) -> Dict[str, str]:
        out: Dict[str, str] = {}

        for role, cand in role_map.items():
            if cand is None:
                out[role] = MissingValueSentinel
                continue

            threshold = T_CONSOLIDATED if role == ROLE_CONSOLIDATED else T_ROLE
            if not accept(cand.confidence, threshold):
                logger.info(
                    "EntityResolver: reject role=%s conf=%.3f threshold=%.2f sheet=%s",
                    role,
                    cand.confidence,
                    threshold,
                    getattr(task, "sheet_name", "?"),
                )
                out[role] = MissingValueSentinel
                continue

            letter = tools.excel_column_index_to_letter(col_idx=cand.col_idx)
            out[role] = letter

            logger.info(
                "EntityResolver: accept role=%s col_idx=%d -> %s conf=%.3f sheet=%s",
                role,
                cand.col_idx,
                letter,
                cand.confidence,
                getattr(task, "sheet_name", "?"),
            )

        return out