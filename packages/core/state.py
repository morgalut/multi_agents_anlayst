"""
Shared pipeline state for ORC/LangGraph-style orchestration.

This is intentionally minimal and serializable.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from .schemas import MainSheetSchema, RowResult, RowTask


@dataclass
class RunInput:
    workbook_path: str


@dataclass
class ToolingState:
    mcp_registry: List[str] = field(default_factory=list)
    available_capabilities: List[str] = field(default_factory=list)


@dataclass
class OutputState:
    workbook_out_path: Optional[str] = None


@dataclass
class PipelineState:
    """
    Minimal shared state (matches your README shape, pythonic).
    """
    run_id: str
    input: RunInput

    main_sheet: Optional[MainSheetSchema] = None
    row_tasks: List[RowTask] = field(default_factory=list)
    row_results: List[RowResult] = field(default_factory=list)

    tooling: ToolingState = field(default_factory=ToolingState)
    output: OutputState = field(default_factory=OutputState)

    # Scratch space for agents (avoid putting large grids here)
    memory: Dict[str, Any] = field(default_factory=dict)

    def add_result(self, rr: RowResult) -> None:
        self.row_results.append(rr)