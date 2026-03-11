from __future__ import annotations

from dataclasses import dataclass, field
from typing import Tuple


@dataclass(frozen=True, slots=True)
class ColumnSearchConfig:
    """
    Shared structural extraction/output contract.

    key_roles:
      Roles that must appear in the compact `key_columns` output when found.

    output_format:
      Keep stable as 'json_two_tables' so prompts/renderers stay aligned.
    """
    output_format: str = "json_two_tables"

    key_roles: Tuple[str, ...] = (
        "coa_name",
        "entity_value",
        "aje",
        "consolidated",
    )

    include_fields_key_columns: Tuple[str, ...] = (
        "column",
        "sheet",
        "role",
        "entity",
        "currency",
        "row_start",
        "row_end",
        "header",
        "formula",
    )

    include_fields_all_columns: Tuple[str, ...] = (
        "column",
        "index",
        "sheet",
        "role",
        "entity",
        "currency",
        "period",
        "row_start",
        "row_end",
        "header",
        "formula",
    )

    empty_string_for_missing: bool = True
    sort_by_index: bool = True