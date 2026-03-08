from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional
import unicodedata


@dataclass(frozen=True, slots=True)
class SheetNameResolution:
    raw_name: str
    resolved_name: Optional[str]
    matched_by: Optional[str] = None


class SheetNameResolver:
    """
    Resolves user/control-sheet candidate names to exact workbook worksheet names.

    Matching strategy:
    1) exact match
    2) exact trimmed match
    3) canonical Unicode/punctuation match
    """

    def resolve(self, raw_name: str, actual_sheet_names: List[str]) -> SheetNameResolution:
        raw = (raw_name or "")
        trimmed = raw.strip()

        # 1) exact
        if raw in actual_sheet_names:
            return SheetNameResolution(raw_name=raw_name, resolved_name=raw, matched_by="exact")

        # 2) trimmed exact
        if trimmed in actual_sheet_names:
            return SheetNameResolution(raw_name=raw_name, resolved_name=trimmed, matched_by="trimmed")

        # 3) canonicalized
        target = self._canonicalize(trimmed)
        for actual in actual_sheet_names:
            if self._canonicalize(actual) == target:
                return SheetNameResolution(
                    raw_name=raw_name,
                    resolved_name=actual,
                    matched_by="canonicalized",
                )

        return SheetNameResolution(raw_name=raw_name, resolved_name=None, matched_by=None)

    def _canonicalize(self, value: str) -> str:
        text = unicodedata.normalize("NFKC", (value or "").strip())

        replacements = {
            "״": '"',    # Hebrew gershayim
            "׳": "'",    # Hebrew geresh
            "\u00A0": " ",  # non-breaking space
            "\u200f": "",   # RTL mark
            "\u200e": "",   # LTR mark
            "\u202a": "",
            "\u202b": "",
            "\u202c": "",
            "\u202d": "",
            "\u202e": "",
        }

        for src, dst in replacements.items():
            text = text.replace(src, dst)

        text = " ".join(text.split())
        return text.casefold()