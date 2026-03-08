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
You are a senior Excel financial statement analysis agent.

Rules:
- Never guess.
- Separate direct evidence from inference.
- Prefer explicit header, formula, and structural evidence over assumptions.
- Focus on the most recent reporting period unless the task says otherwise.
- If evidence is weak or conflicting, lower confidence and return quality flags.
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
1. Identify the likely main presentation sheet.
2. Infer whether the workbook contains BS, PL, or both.
3. Identify likely entities and currencies.
4. Determine whether consolidated logic or AJE likely exists.
5. Flag uncertainty or missing evidence.

Deep-analysis requirements:
- Compare candidate sheets, not just one sheet in isolation.
- Prefer presentation sheets over raw source or ledger sheets.
- Use sheet names, sheet previews, header clues, and structural patterns.
- Lower confidence when evidence is weak or indirect.
""".strip(),
    output_contract="""
Return JSON exactly:
{
  "main_sheet_name": "",
  "header_row_index": null,
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
        "Main sheet is typically the top-level presentation sheet, not the raw data source sheet.",
        "Entity names require evidence stronger than a generic currency label.",
        "If consolidated or AJE are not supported clearly, prefer false and flag uncertainty.",
    ],
    depth="forensic",
)


SCHEMA_DETECTION_PROFILE = StagePromptProfile(
    name="schema_detection",
    system_prompt=GLOBAL_FINANCIAL_SYSTEM_PROMPT,
    task_prompt="""
Stage: schema_detection

Goal:
Identify the main presentation sheet and its likely header row.

Deep-analysis requirements:
- Compare candidate sheets rather than evaluating one in isolation.
- Prefer top-level presentation sheets over raw source or ledger sheets.
- Use structural evidence such as account labels, formulas calling other sheets, and visible layout clues.
- Reject sheets that are only data sources.
""".strip(),
    output_contract="""
Return JSON exactly:
{
  "main_sheet": {
    "name": "",
    "header_row_index": null,
    "confidence": 0.0,
    "evidence": []
  },
  "alternatives": [],
  "quality_flags": []
}
""".strip(),
    analysis_rules=[
        "The main sheet typically contains COA labels and presentation-ready structure.",
        "A sheet referenced by other sheets is often a data source, not the main sheet.",
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
2. Identify likely candidate columns for key roles.
3. Provide short evidence strings.
4. Flag weak or suspicious structure.

Deep-analysis requirements:
- Inspect header patterns, merged cells, anchor terms, currency markers, and repeated structures.
- Compare at least two possible interpretations before deciding.
- Downgrade confidence if a signal could fit multiple roles.
- Prefer explicit company names over generic labels like Balance, Total, Amount, YTD, USD, or NIS.
""".strip(),
    output_contract="""
Return JSON exactly:
{
  "classification": {
    "types": ["BS", "PL"],
    "confidence": 0.0,
    "evidence": []
  },
  "roles": {
    "main_company_dollar": {"col_idx": null, "confidence": 0.0, "evidence": []},
    "main_company_il": {"col_idx": null, "confidence": 0.0, "evidence": []},
    "sub_company": {"col_idx": null, "confidence": 0.0, "evidence": []},
    "aje": {"col_idx": null, "confidence": 0.0, "evidence": []},
    "consolidated": {"col_idx": null, "confidence": 0.0, "evidence": []}
  },
  "quality_flags": []
}
""".strip(),
    analysis_rules=[
        "A generic balance or total column is not an entity column unless its header clearly identifies an entity.",
        "Merged cells are useful clues but not sufficient proof by themselves.",
        "If ambiguity remains, prefer null with low confidence.",
    ],
    depth="deep",
)


ROLE_MAPPING_PROFILE = StagePromptProfile(
    name="role_mapping",
    system_prompt=GLOBAL_FINANCIAL_SYSTEM_PROMPT,
    task_prompt="""
Stage: role_mapping

Goal:
Assign structural roles to relevant columns.

Candidate roles:
- coa_name
- entity_value
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
- Numeric account codes are 'other', not 'coa_name'.
""".strip(),
    output_contract="""
Return JSON exactly:
{
  "columns": [
    {
      "col_idx": null,
      "role": "other",
      "entity_name": null,
      "currency": null,
      "period": "current",
      "header_text": "",
      "formula_pattern": "",
      "confidence": 0.0,
      "evidence": []
    }
  ],
  "entities": [],
  "has_consolidated": false,
  "consolidated_formula_pattern": "",
  "has_aje": false,
  "aje_types": [],
  "quality_flags": []
}
""".strip(),
    analysis_rules=[
        "'other' columns must not contain entity_name.",
        "If latest vs prior period is unclear, lower confidence and flag it.",
    ],
    depth="forensic",
)


ENTITY_RESOLUTION_PROFILE = StagePromptProfile(
    name="entity_resolution",
    system_prompt=GLOBAL_FINANCIAL_SYSTEM_PROMPT,
    task_prompt="""
Stage: entity_resolution

Goal:
Normalize resolved entities and column coordinates.

Requirements:
- Resolve ambiguous entity names conservatively.
- Keep the output deterministic.
- Do not invent entities.
""".strip(),
    output_contract="""
Return JSON exactly:
{
  "resolved_columns": [],
  "entities": [],
  "quality_flags": []
}
""".strip(),
    analysis_rules=[
        "Do not normalize two different company names into one unless supported by explicit evidence.",
        "If entity identity is unclear, preserve ambiguity and flag it.",
    ],
    depth="standard",
)


QUALITY_AUDIT_PROFILE = StagePromptProfile(
    name="quality_audit",
    system_prompt="""
You are a skeptical audit reviewer for financial workbook analysis.

Your task is to challenge the extracted structure and identify weak conclusions,
unsupported assumptions, missing evidence, and likely failure modes.

Do not guess. Return valid JSON only.
""".strip(),
    task_prompt="""
Stage: quality_audit

Goal:
Review prior analysis and identify weaknesses.

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
  "accepted_role_map": {},
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

Your task is to format final output from already-resolved analysis.
Do not add new reasoning. Do not invent fields.
""".strip(),
    task_prompt="""
Stage: final_render

Goal:
Convert resolved analysis into final render payload only.
""".strip(),
    output_contract="""
Return JSON exactly:
{
  "output": {}
}
""".strip(),
    analysis_rules=[
        "Do not introduce new fields not supported by the resolved input.",
        "Formatting must stay deterministic and conservative.",
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
            "entity_resolution": ENTITY_RESOLUTION_PROFILE,
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