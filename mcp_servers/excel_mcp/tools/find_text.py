from __future__ import annotations

from typing import Any, Dict, List

from ..server import ToolSpec


def tool_find_text(workbook_provider) -> ToolSpec:
    def handler(args: Dict[str, Any], ctx: Dict[str, Any]) -> Dict[str, Any]:
        sheet_name = args["sheet_name"]
        query = str(args["query"])
        max_hits = int(args.get("max_hits", 50))

        q = query.strip().lower()
        wb = workbook_provider.open(ctx["workbook_path"])
        ws = wb[sheet_name]

        hits: List[Dict[str, Any]] = []
        for row in ws.iter_rows(values_only=False):
            for cell in row:
                v = cell.value
                if v is None:
                    continue
                s = str(v).strip().lower()
                if q and q in s:
                    hits.append(
                        {
                            "row": cell.row - 1,
                            "col": cell.column - 1,
                            "value": cell.value,
                        }
                    )
                    if len(hits) >= max_hits:
                        return {"hits": hits}

        return {"hits": hits}

    return ToolSpec(
        name="excel.find_text",
        description="Find text occurrences in a sheet (substring match).",
        input_schema={
            "type": "object",
            "properties": {
                "sheet_name": {"type": "string"},
                "query": {"type": "string"},
                "max_hits": {"type": "integer", "minimum": 1},
            },
            "required": ["sheet_name", "query"],
            "additionalProperties": False,
        },
        output_schema={
            "type": "object",
            "properties": {"hits": {"type": "array", "items": {"type": "object"}}},
            "required": ["hits"],
            "additionalProperties": False,
        },
        capabilities=["excel.find_text"],
        handler=handler,
    )