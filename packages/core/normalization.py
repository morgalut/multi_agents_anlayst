"""
Text normalization helpers shared by schema detection + analyzers.

Keep these deterministic and conservative (no ML).
"""

from __future__ import annotations

import re
import unicodedata
from typing import Iterable


_WS_RE = re.compile(r"\s+")
_PUNCT_RE = re.compile(r"[^\w\u0590-\u05FF&]+", flags=re.UNICODE)  # keep Hebrew + word chars + &


def normalize_text(s: str) -> str:
    """
    Normalize header/anchor text for robust matching across:
    - case
    - whitespace
    - punctuation
    - unicode variants
    """
    if s is None:
        return ""
    s = str(s)
    s = unicodedata.normalize("NFKC", s)
    s = s.strip().lower()
    s = _WS_RE.sub(" ", s)

    # Replace common Excel-ish separators with spaces
    s = s.replace("\n", " ").replace("\r", " ").replace("\t", " ")
    s = s.replace("–", "-").replace("—", "-")

    # Remove most punctuation but keep Hebrew, alnum, underscore, and '&'
    s = _PUNCT_RE.sub(" ", s)
    s = _WS_RE.sub(" ", s).strip()
    return s


def normalize_tokens(s: str) -> list[str]:
    s = normalize_text(s)
    return [t for t in s.split(" ") if t]


def any_contains(haystack: str, needles: Iterable[str]) -> bool:
    h = normalize_text(haystack)
    for n in needles:
        if normalize_text(n) in h:
            return True
    return False