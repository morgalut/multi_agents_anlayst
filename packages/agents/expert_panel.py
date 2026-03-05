from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional
import logging

from Multi_agen.packages.core import RoleCandidate

logger = logging.getLogger("multi_agen.agents.expert_panel")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)


@dataclass(frozen=True, slots=True)
class ExpertPanelConfig:
    """
    Future: can implement conflict resolution, tie-breaking, secondary tool checks, etc.
    Current: no-op.
    """
    enabled: bool = True


class ExpertPanelAgent:
    """
    Arbitration layer for conflicts/low confidence.

    For now:
      - If disabled -> returns role_map unchanged
      - If enabled  -> returns role_map unchanged (no-op)
    """

    def __init__(self, config: Optional[ExpertPanelConfig] = None):
        self.config = config or ExpertPanelConfig()

    def maybe_arbitrate(
        self,
        task,
        state,
        analysis,
        role_map: Dict[str, Optional[RoleCandidate]],
    ) -> Dict[str, Optional[RoleCandidate]]:
        if not self.config.enabled:
            logger.info("ExpertPanel: disabled sheet=%s", getattr(task, "sheet_name", "?"))
            return role_map

        # No-op arbitration for now; keep trace for debugging.
        logger.info("ExpertPanel: no-op sheet=%s", getattr(task, "sheet_name", "?"))
        return role_map