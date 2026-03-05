from __future__ import annotations

from typing import Any, Dict

from ..types import ToolSpec


def tool_render_sheet_image(ocr_backend) -> ToolSpec:
    def handler(args: Dict[str, Any], ctx: Dict[str, Any]) -> Dict[str, Any]:
        workbook_path = args["workbook_path"]
        sheet_name = args["sheet_name"]
        return ocr_backend.render_sheet_image(workbook_path=workbook_path, sheet_name=sheet_name)

    return ToolSpec(
        name="ocr.render_sheet_image",
        description="Render an Excel sheet to an image file for OCR fallback.",
        input_schema={
            "type": "object",
            "properties": {
                "workbook_path": {"type": "string"},
                "sheet_name": {"type": "string"},
            },
            "required": ["workbook_path", "sheet_name"],
            "additionalProperties": False,
        },
        output_schema={
            "type": "object",
            "properties": {
                "image_path": {"type": "string"},
                "width": {"type": "integer"},
                "height": {"type": "integer"},
            },
            "required": ["image_path"],
            "additionalProperties": True,
        },
        capabilities=["ocr.render_sheet_image"],
        handler=handler,
    )