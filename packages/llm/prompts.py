from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from Multi_agen.packages.llm.stage_prompts import (
    SHEET_ANALYSIS_PROFILE,
    StagePromptProfile,
)


def _clean_cell_text(value: Any, max_len: int = 40) -> str:
    s = "" if value is None else str(value)
    s = s.replace("\n", " ").replace("\r", " ").strip()
    if len(s) > max_len:
        s = s[:max_len] + "…"
    return s


def grid_to_text(grid: List[List[Any]], max_rows: int = 60, max_cols: int = 20) -> str:
    """
    Convert a grid to a compact preview string for prompt context.
    """
    if not grid:
        return ""

    lines: List[str] = []
    row_count = min(len(grid), max_rows)
    col_count = min(len(grid[0]) if grid else 0, max_cols)

    for r in range(row_count):
        row = grid[r] if r < len(grid) else []
        cells: List[str] = []
        for c in range(col_count):
            value = row[c] if c < len(row) else ""
            cells.append(_clean_cell_text(value))
        lines.append(f"{r:02d} | " + " | ".join(cells))

    return "\n".join(lines)


def build_sheet_analysis_messages(
    sheet_name: str,
    grid: List[List[Any]],
    merged_ranges: List[Dict[str, Any]],
    prompt_profile: Optional[StagePromptProfile] = None,
    max_rows: int = 60,
    max_cols: int = 20,
    max_merged_preview: int = 30,
) -> Tuple[str, str]:
    """
    Build system/user prompts for sheet analysis using a stage prompt profile.
    """
    profile = prompt_profile or SHEET_ANALYSIS_PROFILE

    grid_text = grid_to_text(grid, max_rows=max_rows, max_cols=max_cols)
    merged_preview = merged_ranges[:max_merged_preview] if isinstance(merged_ranges, list) else []

    context_block = f"""
Sheet name: {sheet_name}

Top grid (preview):
{grid_text}

Merged ranges preview:
{merged_preview}
""".strip()

    system_prompt = profile.compose_system_prompt()
    user_prompt = profile.compose_user_prompt(context_block)

    return system_prompt, user_prompt