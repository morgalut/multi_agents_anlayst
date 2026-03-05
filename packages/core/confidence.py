"""
Confidence thresholds and helpers (SSOT).

Agents MUST import these thresholds and MUST NOT duplicate values elsewhere.
"""

from __future__ import annotations

T_ROLE: float = 0.75
T_CLASSIFICATION: float = 0.70
T_CONSOLIDATED: float = 0.80


def accept(conf: float, threshold: float) -> bool:
    try:
        return float(conf) >= float(threshold)
    except (TypeError, ValueError):
        return False