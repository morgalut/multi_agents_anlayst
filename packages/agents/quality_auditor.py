from __future__ import annotations

from dataclasses import dataclass
from typing import List


@dataclass(frozen=True, slots=True)
class QualityAuditorConfig:
    pass


class DataQualityAuditor:
    """
    Produces quality flags (corruption markers, image-like sheet, #REF! etc.)
    For now: minimal/no-op.
    """

    def __init__(self, config: QualityAuditorConfig | None = None):
        self.config = config or QualityAuditorConfig()

    def audit(self, task, state, analysis, tools) -> List[str]:
        flags: List[str] = []
        # Example future checks:
        # - if '#ref!' in grid -> flags.append("formula_corruption:#REF!")
        # - if many blanks / merged -> flags.append("image_like")
        return flags