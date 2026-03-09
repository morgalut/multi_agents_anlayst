"""
Shared pipeline state for workbook-structure extraction orchestration.

This state is designed for:
- workbook-level structure analysis
- sheet-centric task execution
- accumulation of per-sheet extraction results
- deterministic final rendering

It replaces the old row-centric summary-sheet workflow state.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from .schemas import (
    FinalRenderOutput,
    SheetExtractionResult,
    SheetTask,
    WorkbookExtractionResult,
    WorkbookStructure,
)


@dataclass
class RunInput:
    workbook_path: str
    workbook_out_path: Optional[str] = None


@dataclass
class ToolingState:
    mcp_registry: List[str] = field(default_factory=list)
    available_capabilities: List[str] = field(default_factory=list)


@dataclass
class PipelineState:
    """
    Shared orchestration state for workbook/sheet structural extraction.

    Main flow:
    1. workbook_structure is populated upstream
    2. sheet_tasks are created from workbook structure / workbook sheet inventory
    3. sheet_results are accumulated per analyzed sheet
    4. workbook_result is assembled from sheet_results
    5. final_render is produced as deterministic text

    Notes:
    - Keep large raw grids out of this state; store only compact metadata/results.
    - Use memory for temporary agent scratch data when needed.
    """
    run_id: str
    input: RunInput

    workbook_structure: Optional[WorkbookStructure] = None

    sheet_tasks: List[SheetTask] = field(default_factory=list)
    sheet_results: List[SheetExtractionResult] = field(default_factory=list)

    workbook_result: Optional[WorkbookExtractionResult] = None
    final_render: Optional[FinalRenderOutput] = None

    tooling: ToolingState = field(default_factory=ToolingState)

    # Provenance / audit helpers
    workbook_structure_provenance: Optional[Dict[str, Any]] = None
    task_provenance: List[Dict[str, Any]] = field(default_factory=list)

    # Scratch space for agents (avoid storing large workbook grids here)
    memory: Dict[str, Any] = field(default_factory=dict)

    def add_sheet_result(self, result: SheetExtractionResult) -> None:
        self.sheet_results.append(result)

    def set_workbook_result(self, result: WorkbookExtractionResult) -> None:
        self.workbook_result = result

    def set_final_render(self, render: FinalRenderOutput) -> None:
        self.final_render = render