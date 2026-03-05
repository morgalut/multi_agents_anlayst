"""
Anchor phrases / synonyms for detecting statement types and key roles.

These are NOT "truth"; they're shared heuristics.
Prefer adding anchors here rather than scattering strings across agents.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List


@dataclass(frozen=True, slots=True)
class AnchorSet:
    # Statement type anchors
    bs: List[str]
    pl: List[str]

    # Role-ish anchors (entities/currency/adjustments)
    main_company: List[str]
    sub_company: List[str]
    consolidated: List[str]
    aje: List[str]

    # Currency anchors
    ils: List[str]
    usd: List[str]


DEFAULT_ANCHORS = AnchorSet(
    bs=[
        "balance sheet", "statement of financial position",
        "מאזן", "דוח על המצב הכספי",
        "assets", "liabilities", "equity",
        "נכסים", "התחייבויות", "הון",
    ],
    pl=[
        "profit and loss", "p&l", "income statement", "statement of profit or loss",
        "דוח רווח והפסד", "דו\"ח רווח והפסד", "דוח רווח והפסד",
        "revenue", "expenses", "gross profit", "operating profit",
        "הכנסות", "הוצאות", "רווח גולמי", "רווח תפעולי",
    ],
    main_company=[
        "main", "parent", "company", "entity", "mother",
        "חברה", "חברת אם", "החברה",
    ],
    sub_company=[
        "sub", "subsidiary", "subsidiaries",
        "חברת בת", "חברה בת", "בנות",
    ],
    consolidated=[
        "consolidated", "group",
        "מאוחד", "מאוחדים", "קבוצה",
    ],
    aje=[
        "aje", "adjusting journal entry", "adjustment", "adjustments",
        "פקודת יומן", "פקודות יומן", "התאמה", "התאמות",
    ],
    ils=[
        "ils", "nis", "₪", "shekel", "shekels",
        "ש\"ח", "שח", "שקל", "שקלים", "חדש",
    ],
    usd=[
        "usd", "$", "dollar", "dollars", "us dollar",
        "דולר", "דולרים",
    ],
)

# Optional: a small header synonym map used by schema detectors (if needed)
OUTPUT_HEADER_SYNONYMS: Dict[str, List[str]] = {
    "Filename": ["filename", "file name", "sheet", "sheet name", "שם גיליון", "קובץ", "שם קובץ"],
    "BS": ["bs", "balance sheet", "מאזן"],
    "P&L": ["p&l", "pl", "income statement", "רווח והפסד", "דוח רווח והפסד"],
    "Main Company IL": ["main company il", "main ils", "main nis", "חברה ש\"ח", "חברה בש\"ח"],
    "Main Company Dollar": ["main company dollar", "main usd", "main $", "חברה דולר", "חברה בדולר"],
    "AJE": ["aje", "adjustment", "adjustments", "פקודת יומן", "התאמות"],
    "Consolidated": ["consolidated", "group", "מאוחד", "קבוצה"],
    "Sub Company": ["sub company", "subsidiary", "חברת בת", "חברה בת"],
}