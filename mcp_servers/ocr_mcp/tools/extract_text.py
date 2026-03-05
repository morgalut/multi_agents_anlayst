from __future__ import annotations

from typing import Any, Dict

from ..server import ToolSpec


def tool_extract_text(ocr_backend) -> ToolSpec:
    def handler(args: Dict[str, Any], ctx: Dict[str, Any]) -> Dict[str, Any]:
        image_path = args["image_path"]
        # backend returns dict: {text: "...", blocks:[...]}
        return ocr_backend.extract_text(image_path=image_path)

    return ToolSpec(
        name="ocr.extract_text",
        description="Run OCR on an image and return extracted text + blocks.",
        input_schema={
            "type": "object",
            "properties": {"image_path": {"type": "string"}},
            "required": ["image_path"],
            "additionalProperties": False,
        },
        output_schema={
            "type": "object",
            "properties": {
                "text": {"type": "string"},
                "blocks": {"type": "array", "items": {"type": "object"}},
            },
            "required": ["text"],
            "additionalProperties": True,
        },
        capabilities=["ocr.extract_text"],
        handler=handler,
    )