"""
Shared pipeline state for workbook-structure extraction orchestration.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from .schemas import (
    FinalRenderOutput,
    SheetExtractionResult,
    SheetTask,
    WorkbookExtractionResult,
    WorkbookSheetProfilesResult,
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

    Two parallel output tracks:

    Track A (existing) — workbook extraction / comparison:
        workbook_result  →  FinalRenderOutput

    Track B (new) — per-sheet profile export:
        sheet_profiles_result  →  WorkbookSheetProfilesResult
        Serialises to the exmplete.json shape via .to_dict().
    """
    run_id: str
    input: RunInput

    workbook_structure: Optional[WorkbookStructure] = None

    sheet_tasks: List[SheetTask] = field(default_factory=list)
    sheet_results: List[SheetExtractionResult] = field(default_factory=list)

    workbook_result: Optional[WorkbookExtractionResult] = None
    final_render: Optional[FinalRenderOutput] = None

    # NEW: per-sheet profile export result (Track B)
    sheet_profiles_result: Optional[WorkbookSheetProfilesResult] = None

    tooling: ToolingState = field(default_factory=ToolingState)

    workbook_structure_provenance: Optional[Dict[str, Any]] = None
    task_provenance: List[Dict[str, Any]] = field(default_factory=list)

    memory: Dict[str, Any] = field(default_factory=dict)

    def add_sheet_result(self, result: SheetExtractionResult) -> None:
        self.sheet_results.append(result)

    def set_workbook_result(self, result: WorkbookExtractionResult) -> None:
        self.workbook_result = result

    def set_final_render(self, render: FinalRenderOutput) -> None:
        self.final_render = render

    def set_sheet_profiles_result(self, result: WorkbookSheetProfilesResult) -> None:
        """Store the per-sheet profile export result (Track B)."""
        self.sheet_profiles_result = result

    def get_sheet_profiles_dict(self) -> Optional[Dict[str, Any]]:
        """
        Convenience helper: return the profile export in serialisable dict form,
        ready for JSON encoding or API response serialisation.
        """
        if self.sheet_profiles_result is None:
            return None
        return self.sheet_profiles_result.to_dict()