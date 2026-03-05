from __future__ import annotations

from dataclasses import dataclass
from typing import Dict

from Multi_agen.packages.core import (
    MissingValueSentinel,
    RowResolvedOutput,
    ROLE_AJE,
    ROLE_CONSOLIDATED,
    ROLE_MAIN_COMPANY_DOLLAR,
    ROLE_MAIN_COMPANY_IL,
    ROLE_SUB_COMPANY,
)


@dataclass(frozen=True, slots=True)
class OutputRendererConfig:
    """
    PictureCompat renderer.
    """
    pass


class OutputRenderer:
    """
    Renders the per-row final output object the Summary expects.

    Important:
      - Filename/BS/P&L are sheet names in your picture (often same as task.sheet_name)
      - Role-derived fields are letters or "no"
      - Never guess: fallback is "no"
    """

    def __init__(self, config: OutputRendererConfig | None = None):
        self.config = config or OutputRendererConfig()

    def render(self, task, state, resolved_roles: Dict[str, str]) -> RowResolvedOutput:
        # Keep Filename/BS/P&L as sheet name by default (can be refined later)
        sheet = task.sheet_name or MissingValueSentinel

        return RowResolvedOutput(
            filename=sheet,
            bs=sheet,
            pl=sheet,
            main_company_il=resolved_roles.get(ROLE_MAIN_COMPANY_IL, MissingValueSentinel),
            main_company_dollar=resolved_roles.get(ROLE_MAIN_COMPANY_DOLLAR, MissingValueSentinel),
            aje=resolved_roles.get(ROLE_AJE, MissingValueSentinel),
            consolidated=resolved_roles.get(ROLE_CONSOLIDATED, MissingValueSentinel),
            sub_company=resolved_roles.get(ROLE_SUB_COMPANY, MissingValueSentinel),
        )