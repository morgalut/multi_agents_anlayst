"""
Evidence helpers: keep evidence strings consistent and easy to audit.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable, List, Optional, Sequence


def ev(msg: str) -> str:
    """Normalize evidence line formatting (single line, trimmed)."""
    s = str(msg or "").strip()
    s = " ".join(s.split())
    return s


def merge_evidence(*parts: Iterable[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for p in parts:
        for item in p or []:
            line = ev(item)
            if not line:
                continue
            if line in seen:
                continue
            seen.add(line)
            out.append(line)
    return out


@dataclass(frozen=True, slots=True)
class EvidenceBundle:
    """
    Common structure for returning (confidence, evidence).
    Used by analyzers for classification, role selection, etc.
    """
    confidence: float
    evidence: List[str] = field(default_factory=list)

    def with_more(self, more: Sequence[str]) -> "EvidenceBundle":
        return EvidenceBundle(self.confidence, merge_evidence(self.evidence, more))


def top_evidence(evidence: Sequence[str], limit: int = 8) -> List[str]:
    return list(evidence[: max(0, int(limit))])