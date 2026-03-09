from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass(frozen=True, slots=True)
class StagePromptProfile:
    """
    Prompt contract for one pipeline stage.
    """
    name: str
    system_prompt: str
    task_prompt: str
    output_contract: str
    analysis_rules: List[str] = field(default_factory=list)
    depth: str = "standard"
    require_json: bool = True

    def compose_system_prompt(self) -> str:
        rules_text = ""
        if self.analysis_rules:
            rules_text = "\nAdditional stage rules:\n- " + "\n- ".join(self.analysis_rules)
        return f"{self.system_prompt}{rules_text}".strip()

    def compose_user_prompt(self, context_block: str) -> str:
        return f"""
{self.task_prompt}

{context_block}

Output contract:
{self.output_contract}
""".strip()


GLOBAL_FINANCIAL_SYSTEM_PROMPT = """
You are a senior Excel financial statement structure analysis agent.

Rules:
- Never guess.
- Separate direct evidence from inference.
- Prefer explicit header, formula, and structural evidence over assumptions.
- Focus on the most recent reporting period unless the task says otherwise.
- If evidence is weak or conflicting, lower confidence and return quality flags.
- Prefer top-level presentation sheets over SAP, GL, TB, ledger, or raw source sheets.
- When JSON is required, output valid JSON only.
""".strip()


WORKBOOK_STRUCTURE_PROFILE = StagePromptProfile(
    name="workbook_structure",
    system_prompt=GLOBAL_FINANCIAL_SYSTEM_PROMPT,
    task_prompt="""
Stage: workbook_structure

Goal:
Analyze the workbook holistically and identify likely top-level financial structure.

Objectives:
1. Identify likely top-level presentation sheet(s).
2. Infer whether the workbook contains BS, PL, or both.
3. Identify likely entities and currencies.
4. Determine whether consolidated logic or AJE likely exists.
5. Flag uncertainty or missing evidence.

Deep-analysis requirements:
- Compare candidate sheets, not just one sheet in isolation.
- Prefer presentation sheets over raw source or ledger sheets.
- Use sheet names, sheet previews, header clues, merged cells, and formula patterns.
- Lower confidence when evidence is weak or indirect.
""".strip(),
    output_contract="""
Return JSON exactly:
{
  "main_sheet_names": [],
  "contains": [],
  "entities": [
    {
      "name": "",
      "currency": null,
      "confidence": 0.0,
      "evidence": []
    }
  ],
  "has_consolidated": false,
  "consolidated_formula_pattern": "",
  "has_aje": false,
  "aje_types": [],
  "likely_units": null,
  "likely_current_period": null,
  "sheet_candidates": [
    {
      "name": "",
      "kind": "unknown",
      "confidence": 0.0,
      "evidence": []
    }
  ],
  "quality_flags": [],
  "confidence": 0.0
}
""".strip(),
    analysis_rules=[
        "Main sheets are typically top-level presentation sheets, not raw data source sheets.",
        "Entity names require evidence stronger than a generic currency label.",
        "If consolidated or AJE are not supported clearly, prefer false and flag uncertainty.",
        "Prefer multiple main_sheet_names only when the workbook truly uses separate BS and P&L presentation sheets.",
    ],
    depth="forensic",
)


SCHEMA_DETECTION_PROFILE = StagePromptProfile(
    name="schema_detection",
    system_prompt=GLOBAL_FINANCIAL_SYSTEM_PROMPT,
    task_prompt="""
Stage: schema_detection

Goal:
Identify which workbook sheets should be structurally analyzed as financial presentation sheets.

Deep-analysis requirements:
- Compare candidate sheets rather than evaluating one in isolation.
- Prefer top-level presentation sheets over raw source or ledger sheets.
- Use structural evidence such as account labels, formulas calling other sheets, presentation layout, and sheet naming.
- Reject sheets that are only data sources.
""".strip(),
    output_contract="""
Return JSON exactly:
{
  "sheet_tasks": [
    {
      "sheet_name": "",
      "is_main_sheet": false,
      "parent_sheet_name": null,
      "confidence": 0.0,
      "evidence": []
    }
  ],
  "quality_flags": []
}
""".strip(),
    analysis_rules=[
        "A sheet referenced by other sheets is often a data source, not the main presentation sheet.",
        "A presentation sheet often contains account labels and formula links to supporting sheets.",
        "Do not treat a summary/control sheet as a financial presentation sheet unless the content itself is financial statement content.",
    ],
    depth="forensic",
)


SHEET_ANALYSIS_PROFILE = StagePromptProfile(
    name="sheet_analysis",
    system_prompt=GLOBAL_FINANCIAL_SYSTEM_PROMPT,
    task_prompt="""
Stage: sheet_analysis

Goal:
1. Classify the sheet as BS, PL, or both.
2. Identify likely column mappings for the required structural roles.
3. Infer likely unit and data-row ranges when supported by evidence.
4. Flag weak or suspicious structure.

Deep-analysis requirements:
- Inspect header patterns, merged cells, anchor terms, currency markers, and repeated structures.
- Use formula evidence when available to distinguish presentation columns from source columns.
- Compare at least two possible interpretations before deciding.
- Downgrade confidence if a signal could fit multiple roles.
- Prefer explicit company names over generic labels like Balance, Total, Amount, YTD, USD, or NIS.
- Focus on the most recent period. Older periods should later map to prior_period.
""".strip(),
    output_contract="""
Return JSON exactly:
{
  "classification": {
    "types": ["BS", "PL"],
    "confidence": 0.0,
    "evidence": []
  },
  "columns": [
    {
      "col_idx": null,
      "role": "other",
      "entity": "",
      "currency": "",
      "period": "",
      "header_text": "",
      "formula_pattern": "",
      "row_start": null,
      "row_end": null,
      "sheet_name": "",
      "confidence": 0.0,
      "evidence": []
    }
  ],
  "unit": null,
  "quality_flags": []
}
""".strip(),
    analysis_rules=[
        "A generic balance or total column is not an entity column unless its header clearly identifies an entity.",
        "Merged cells are useful clues but not sufficient proof by themselves.",
        "Numeric account code columns are usually other, not coa_name.",
        "If ambiguity remains, prefer null/blank fields with lower confidence.",
    ],
    depth="deep",
)


ROLE_MAPPING_PROFILE = StagePromptProfile(
    name="role_mapping",
    system_prompt=GLOBAL_FINANCIAL_SYSTEM_PROMPT,
    task_prompt="""
Stage: role_mapping

Goal:
Convert candidate structural columns into final validated workbook-mapping column objects.

Candidate roles:
- coa_name
- entity_value
- debit
- credit
- aje
- consolidated_aje
- consolidated
- budget
- prior_period
- other

Deep-analysis requirements:
- Focus on the most recent period.
- Only assign entity_value when the header clearly identifies a company/entity.
- Treat generic labels like Balance, Total, YTD, U.S. Dollars as non-entity unless tied to a company.
- Consolidated should be supported by arithmetic/formula logic when available.
- Numeric account codes are other, not coa_name.
""".strip(),
    output_contract="""
Return JSON exactly:
{
  "resolved_columns": [
    {
      "col_idx": null,
      "role": "other",
      "entity": "",
      "currency": "",
      "period": "",
      "header_text": "",
      "formula_pattern": "",
      "row_start": null,
      "row_end": null,
      "sheet_name": "",
      "confidence": 0.0,
      "evidence": []
    }
  ],
  "quality_flags": []
}
""".strip(),
    analysis_rules=[
        "Entity should only be set for entity_value, aje, consolidated_aje, or consolidated.",
        "If latest vs prior period is unclear, lower confidence and flag it.",
        "Return only roles from the allowed vocabulary.",
    ],
    depth="forensic",
)


QUALITY_AUDIT_PROFILE = StagePromptProfile(
    name="quality_audit",
    system_prompt="""
You are a skeptical audit reviewer for financial workbook structure extraction.

Your task is to challenge the extracted structure and identify weak conclusions,
unsupported assumptions, missing evidence, and likely failure modes.

Do not guess. Return valid JSON only.
""".strip(),
    task_prompt="""
Stage: quality_audit

Goal:
Review prior sheet analysis and identify weaknesses.

Deep-analysis requirements:
- Challenge whether consolidated, AJE, entity, and current-period assumptions are actually proven.
- Prefer specific flags over vague concerns.
- Recommend follow-up checks where evidence is weak.
""".strip(),
    output_contract="""
Return JSON exactly:
{
  "pass": false,
  "flags": [],
  "required_followups": []
}
""".strip(),
    analysis_rules=[
        "A weak header alone is not enough to prove entity ownership.",
        "A consolidated claim should be challenged unless supported by arithmetic or workbook structure evidence.",
        "A prior_period claim should be challenged unless a newer period is clearly identified elsewhere.",
    ],
    depth="forensic",
)


EXPERT_ARBITRATION_PROFILE = StagePromptProfile(
    name="expert_arbitration",
    system_prompt=GLOBAL_FINANCIAL_SYSTEM_PROMPT,
    task_prompt="""
Stage: expert_arbitration

Goal:
Resolve disagreements between prior extraction steps.

Requirements:
- Compare competing interpretations.
- Choose the most evidence-supported mapping.
- Preserve conservative behavior when evidence is insufficient.
""".strip(),
    output_contract="""
Return JSON exactly:
{
  "accepted_columns": [],
  "overrides": [],
  "quality_flags": []
}
""".strip(),
    analysis_rules=[
        "Do not override a conservative result with a speculative one.",
        "Explain overrides only when there is stronger evidence than the original mapping.",
    ],
    depth="deep",
)


FINAL_RENDER_PROFILE = StagePromptProfile(
    name="final_render",
    system_prompt="""
You are a deterministic financial output renderer.

Your task is to format final output from already-resolved workbook extraction.
Do not add new reasoning. Do not invent fields.
""".strip(),
    task_prompt="""
Stage: final_render

Goal:
Convert resolved workbook extraction into the final deterministic report format.
""".strip(),
    output_contract="""
Return JSON exactly:
{
  "text": ""
}
""".strip(),
    analysis_rules=[
        "Do not introduce new fields not supported by the resolved input.",
        "Formatting must stay deterministic and conservative.",
        "The final text must match the required SHEET/COLUMN/ENTITIES/CONSOLIDATED/AJE/NIS structure.",
    ],
    depth="shallow",
)


class PromptRegistry:
    """
    Simple registry for stage prompt profiles.
    """

    def __init__(self, profiles: Optional[Dict[str, StagePromptProfile]] = None) -> None:
        self._profiles: Dict[str, StagePromptProfile] = profiles or {
            "workbook_structure": WORKBOOK_STRUCTURE_PROFILE,
            "schema_detection": SCHEMA_DETECTION_PROFILE,
            "sheet_analysis": SHEET_ANALYSIS_PROFILE,
            "role_mapping": ROLE_MAPPING_PROFILE,
            "quality_audit": QUALITY_AUDIT_PROFILE,
            "expert_arbitration": EXPERT_ARBITRATION_PROFILE,
            "final_render": FINAL_RENDER_PROFILE,
        }

    def get(self, name: str) -> Optional[StagePromptProfile]:
        return self._profiles.get(name)

    def require(self, name: str) -> StagePromptProfile:
        profile = self.get(name)
        if profile is None:
            raise KeyError(f"Prompt profile not found: {name}")
        return profile

    def has(self, name: str) -> bool:
        return name in self._profiles

    def register(self, profile: StagePromptProfile) -> None:
        self._profiles[profile.name] = profile

    def names(self) -> List[str]:
        return sorted(self._profiles.keys())