from __future__ import annotations

from typing import Any, Dict, List


SYSTEM_PROMPT = """You are an Excel financial statement analyzer.
Your job: infer sheet classification (BS, P&L) and likely columns for roles from a top-grid snapshot.

Rules:
- Never guess. If evidence is weak, return low confidence.
- Provide short evidence strings explaining signals (header matches, currency symbols, anchors).
- Output must be valid JSON ONLY.
"""

def build_sheet_analysis_prompt(sheet_name: str, grid: List[List[Any]], merged_ranges: List[Dict[str, Any]]) -> str:
    """
    Keep the prompt compact: only a top window, and small merged info.
    """
    # Convert grid to a compact text table
    lines = []
    max_rows = min(len(grid), 60)
    max_cols = min(len(grid[0]) if grid else 0, 20)

    for r in range(max_rows):
        row = grid[r] if r < len(grid) else []
        cells = []
        for c in range(max_cols):
            v = row[c] if c < len(row) else ""
            s = "" if v is None else str(v)
            s = s.replace("\n", " ").replace("\r", " ").strip()
            if len(s) > 40:
                s = s[:40] + "…"
            cells.append(s)
        lines.append(f"{r:02d} | " + " | ".join(cells))

    merged_preview = merged_ranges[:30] if isinstance(merged_ranges, list) else []

    schema = {
        "classification": {"types": ["BS","PL"], "confidence": 0.0, "evidence": []},
        "roles": {
            "main_company_dollar": {"col_idx": None, "confidence": 0.0, "evidence": []},
            "main_company_il": {"col_idx": None, "confidence": 0.0, "evidence": []},
            "sub_company": {"col_idx": None, "confidence": 0.0, "evidence": []},
            "aje": {"col_idx": None, "confidence": 0.0, "evidence": []},
            "consolidated": {"col_idx": None, "confidence": 0.0, "evidence": []},
        },
        "quality_flags": [],
    }

    return f"""
Sheet name: {sheet_name}

Top grid (rows x cols shown = {max_rows} x {max_cols}):
{chr(10).join(lines)}

Merged ranges preview:
{merged_preview}

Return JSON with this exact shape:
{schema}

Notes:
- col_idx must be 0-based integer if known, else null
- confidence is 0..1
- evidence is list of short strings
- types may include "BS" and/or "PL"
- quality_flags can include "image_like", "formula_corruption", "weak_headers"
""".strip()