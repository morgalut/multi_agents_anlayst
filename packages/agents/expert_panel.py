from __future__ import annotations

from dataclasses import dataclass
from typing import Any, List, Optional, Dict, Tuple
import logging

from Multi_agen.packages.core import ColumnMapping

logger = logging.getLogger("multi_agen.agents.expert_panel")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)


@dataclass(frozen=True, slots=True)
class ExpertPanelConfig:
    """
    Conservative arbitration layer for structural column mappings.

    Rules:
    - never invent a column
    - prefer conservative downgrades over speculative upgrades
    - resolve obvious conflicts deterministically
    """
    enabled: bool = True
    prefer_single_consolidated: bool = True
    prefer_single_coa_name: bool = True
    drop_low_confidence_conflicts: bool = True
    min_confidence_keep: float = 0.35


class ExpertPanelAgent:
    """
    Arbitration layer for structural sheet extraction.

    Input:
      - task
      - state
      - analysis
      - columns: List[ColumnMapping]

    Output:
      - arbitrated List[ColumnMapping]

    Main responsibilities:
    - keep only one consolidated column when multiple candidates exist
    - keep only one primary coa_name column when duplicates conflict
    - resolve duplicate same-role same-column collisions
    - conservatively remove weak conflicting columns
    """

    def __init__(self, config: Optional[ExpertPanelConfig] = None):
        self.config = config or ExpertPanelConfig()

    def maybe_arbitrate(
        self,
        task: Any,
        state: Any,
        analysis: Any,
        columns: List[ColumnMapping],
        prompt_profile: Any = None,
    ) -> List[ColumnMapping]:
        if not self.config.enabled:
            logger.info("ExpertPanel: disabled sheet=%s", getattr(task, "sheet_name", "?"))
            return list(columns)

        logger.info(
            "ExpertPanel:start sheet=%s columns=%d",
            getattr(task, "sheet_name", "?"),
            len(columns),
        )

        out = list(columns)
        out = self._dedupe_exact_collisions(out)
        out = self._resolve_single_coa_name(out)
        out = self._resolve_single_consolidated(out)
        out = self._drop_low_confidence_conflicts(out)
        out.sort(key=lambda c: c.col_idx)

        logger.info(
            "ExpertPanel:done sheet=%s columns=%d",
            getattr(task, "sheet_name", "?"),
            len(out),
        )
        return out

    def _dedupe_exact_collisions(self, columns: List[ColumnMapping]) -> List[ColumnMapping]:
        """
        If multiple mappings point to the same physical column with conflicting duplicates,
        keep the strongest one per (col_idx, role, entity, currency, period).
        """
        best: Dict[Tuple[int, str, str, str, str], ColumnMapping] = {}

        for col in columns:
            key = (
                col.col_idx,
                col.role,
                col.entity,
                col.currency,
                col.period,
            )
            prev = best.get(key)
            if prev is None or col.confidence > prev.confidence:
                best[key] = col

        return list(best.values())

    def _resolve_single_coa_name(self, columns: List[ColumnMapping]) -> List[ColumnMapping]:
        if not self.config.prefer_single_coa_name:
            return columns

        coa_cols = [c for c in columns if c.role == "coa_name"]
        if len(coa_cols) <= 1:
            return columns

        winner = self._pick_best_coa_name(coa_cols)
        out = [c for c in columns if c.role != "coa_name"]
        out.append(winner)

        logger.info(
            "ExpertPanel: coa arbitration kept %s(%d)",
            winner.col_letter,
            winner.col_idx,
        )
        return out

    def _pick_best_coa_name(self, candidates: List[ColumnMapping]) -> ColumnMapping:
        """
        Prefer:
        1. higher confidence
        2. richer row coverage
        3. more descriptive header text
        4. leftmost column as stable tie-breaker
        """
        def score(c: ColumnMapping) -> Tuple[float, int, int, int]:
            coverage = 0
            if c.row_start is not None and c.row_end is not None and c.row_end >= c.row_start:
                coverage = c.row_end - c.row_start
            header_len = len((c.header_text or "").strip())
            return (
                float(c.confidence),
                coverage,
                header_len,
                -c.col_idx,
            )

        return sorted(candidates, key=score, reverse=True)[0]

    def _resolve_single_consolidated(self, columns: List[ColumnMapping]) -> List[ColumnMapping]:
        if not self.config.prefer_single_consolidated:
            return columns

        cons_cols = [c for c in columns if c.role == "consolidated"]
        if len(cons_cols) <= 1:
            return columns

        winner = self._pick_best_consolidated(cons_cols)
        out = [c for c in columns if c.role != "consolidated"]
        out.append(winner)

        logger.info(
            "ExpertPanel: consolidated arbitration kept %s(%d) formula=%r",
            winner.col_letter,
            winner.col_idx,
            winner.formula_pattern,
        )
        return out

    def _pick_best_consolidated(self, candidates: List[ColumnMapping]) -> ColumnMapping:
        """
        Prefer:
        1. columns with arithmetic-looking formula patterns
        2. higher confidence
        3. richer row coverage
        4. header mentioning consolidated
        5. leftmost stable tie-breaker
        """
        def arithmetic_bonus(formula: str) -> int:
            s = (formula or "").upper()
            if not s:
                return 0
            if "+" in s or "-" in s:
                return 3
            if "SUM(" in s:
                return 2
            if "SUMIF(" in s:
                return 1
            return 0

        def consolidated_header_bonus(header: str) -> int:
            h = (header or "").strip().lower()
            if "consolidated" in h or "cons." in h or "group" in h or "מאוחד" in h:
                return 1
            return 0

        def score(c: ColumnMapping) -> Tuple[int, float, int, int, int]:
            coverage = 0
            if c.row_start is not None and c.row_end is not None and c.row_end >= c.row_start:
                coverage = c.row_end - c.row_start
            return (
                arithmetic_bonus(c.formula_pattern),
                float(c.confidence),
                coverage,
                consolidated_header_bonus(c.header_text),
                -c.col_idx,
            )

        return sorted(candidates, key=score, reverse=True)[0]

    def _drop_low_confidence_conflicts(self, columns: List[ColumnMapping]) -> List[ColumnMapping]:
        if not self.config.drop_low_confidence_conflicts:
            return columns

        # Group by physical column.
        by_col: Dict[int, List[ColumnMapping]] = {}
        for col in columns:
            by_col.setdefault(col.col_idx, []).append(col)

        out: List[ColumnMapping] = []

        for col_idx, group in by_col.items():
            if len(group) == 1:
                c = group[0]
                if c.confidence >= self.config.min_confidence_keep:
                    out.append(c)
                continue

            # If multiple conflicting roles claim the same physical column,
            # keep only the strongest confident one.
            winner = sorted(
                group,
                key=lambda c: (
                    float(c.confidence),
                    self._role_priority(c.role),
                    -c.col_idx,
                ),
                reverse=True,
            )[0]

            if winner.confidence >= self.config.min_confidence_keep:
                out.append(winner)

            logger.info(
                "ExpertPanel: conflict on physical col %d kept role=%s conf=%.3f",
                col_idx,
                winner.role,
                winner.confidence,
            )

        return out

    def _role_priority(self, role: str) -> int:
        """
        Higher means more preferred in a same-column conflict.
        """
        priorities = {
            "coa_name": 9,
            "entity_value": 8,
            "consolidated": 7,
            "consolidated_aje": 6,
            "aje": 5,
            "prior_period": 4,
            "budget": 3,
            "debit": 2,
            "credit": 2,
            "other": 1,
        }
        return priorities.get(role, 0)