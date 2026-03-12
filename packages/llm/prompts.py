from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from Multi_agen.packages.llm.stage_prompts import (
    SHEET_ANALYSIS_PROFILE,
    StagePromptProfile,
)


def _clean_cell_text(value: Any, max_len: int = 60) -> str:
    s = "" if value is None else str(value)
    s = s.replace("\n", " ").replace("\r", " ").strip()
    if len(s) > max_len:
        s = s[:max_len] + "…"
    return s


def grid_to_text(grid: List[List[Any]], max_rows: int = 60, max_cols: int = 20) -> str:
    """
    Convert a grid to a compact preview string for prompt context.

    Important:
    - width is derived from the widest visible preview row, not only grid[0]
    - this avoids hiding later header/value columns when the first row is sparse
    """
    if not grid:
        return ""

    lines: List[str] = []
    row_count = min(len(grid), max_rows)

    width = 0
    for r in range(row_count):
        row = grid[r] if isinstance(grid[r], list) else []
        width = max(width, len(row))
    col_count = min(width, max_cols)

    for r in range(row_count):
        row = grid[r] if r < len(grid) and isinstance(grid[r], list) else []
        cells: List[str] = []
        for c in range(col_count):
            value = row[c] if c < len(row) else ""
            cells.append(_clean_cell_text(value))
        lines.append(f"{r:02d} | " + " | ".join(cells))

    return "\n".join(lines)


def formulas_to_text(
    formulas: List[List[Any]],
    max_rows: int = 30,
    max_cols: int = 16,
    max_len: int = 80,
) -> str:
    """
    Convert a formula grid to a compact preview string.

    Empty formula cells are preserved as blanks so relative structure remains visible.
    """
    if not formulas:
        return ""

    lines: List[str] = []
    row_count = min(len(formulas), max_rows)

    width = 0
    for r in range(row_count):
        row = formulas[r] if isinstance(formulas[r], list) else []
        width = max(width, len(row))
    col_count = min(width, max_cols)

    for r in range(row_count):
        row = formulas[r] if r < len(formulas) and isinstance(formulas[r], list) else []
        cells: List[str] = []
        for c in range(col_count):
            value = row[c] if c < len(row) else ""
            cells.append(_clean_cell_text(value, max_len=max_len))
        lines.append(f"{r:02d} | " + " | ".join(cells))

    return "\n".join(lines)


def build_sheet_analysis_messages(
    sheet_name: str,
    grid: List[List[Any]],
    merged_ranges: List[Dict[str, Any]],
    prompt_profile: Optional[StagePromptProfile] = None,
    *,
    formulas: Optional[List[List[Any]]] = None,
    header_hint_rows: Optional[Tuple[int, int]] = None,
    # FIX PROMPT-3: new parameter — workbook_structure entities passed from ORC
    known_entities: Optional[List[Dict[str, Any]]] = None,
    max_rows: int = 60,
    max_cols: int = 20,
    max_formula_rows: int = 30,
    max_formula_cols: int = 16,
    max_merged_preview: int = 30,
) -> Tuple[str, str]:
    """
    Build system/user prompts for sheet analysis using a stage prompt profile.

    FIX PROMPT-3:
    - Accepts known_entities (list of {name, currency} dicts from workbook_structure)
    - Injects them into context_block so the LLM can see entity-column hints
    - Also builds an enriched prompt_profile if the registry supports it

    Added support:
    - formula preview
    - optional header hint rows
    - fixed-width preview based on widest visible row
    """
    profile = prompt_profile or SHEET_ANALYSIS_PROFILE

    grid_text = grid_to_text(grid, max_rows=max_rows, max_cols=max_cols)
    formula_text = formulas_to_text(
        formulas or [],
        max_rows=max_formula_rows,
        max_cols=max_formula_cols,
    )
    merged_preview = merged_ranges[:max_merged_preview] if isinstance(merged_ranges, list) else []

    header_hint_text = ""
    if header_hint_rows is not None:
        start_row, end_row = header_hint_rows
        header_hint_text = f"\nSuggested header-area rows: {start_row}-{end_row}"

    # FIX PROMPT-3: build entity hint block when known_entities are provided
    entity_hint_text = ""
    if known_entities:
        lines = ["Known entities from workbook structure (MUST search for these as entity_value columns):"]
        for ent in known_entities:
            name = str(getattr(ent, "name", None) or ent.get("name", "")).strip()
            currency = str(getattr(ent, "currency", None) or ent.get("currency", "") or "").strip()
            if name:
                lines.append(f"  - entity={name!r}  currency={(currency or 'unknown')!r}")
        lines += [
            "Action: for each entity above, find the column whose header contains that name.",
            "Assign role='entity_value', entity=<name>, currency=<currency> to that column.",
        ]
        entity_hint_text = "\n" + "\n".join(lines) + "\n"

    context_block = f"""
Sheet name: {sheet_name}
{entity_hint_text}
Top grid (preview):
{grid_text}
{header_hint_text}

Merged ranges preview:
{merged_preview}

Formula preview:
{formula_text}
""".strip()

    system_prompt = profile.compose_system_prompt()
    user_prompt = profile.compose_user_prompt(context_block)

    return system_prompt, user_prompt