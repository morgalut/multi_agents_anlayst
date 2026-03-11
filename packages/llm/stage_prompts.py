# Multi_agen\packages\llm\stage_prompts.py
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

# ColumnSearchConfig drives dynamic prompt injection.
try:
    from Multi_agen.packages.core.search_config import ColumnSearchConfig
    _SEARCH_CONFIG_AVAILABLE = True
except ImportError:
    ColumnSearchConfig = None  # type: ignore[assignment,misc]
    _SEARCH_CONFIG_AVAILABLE = False


# ---------------------------------------------------------------------------
# StagePromptProfile — contract for a single pipeline stage
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class StagePromptProfile:
    """
    Prompt contract for one pipeline stage.

    Fields
    ------
    name            Unique stage identifier (matches PromptRegistry key).
    system_prompt   Base system-level instructions for the LLM.
    task_prompt     Goal and requirements injected as user context.
    output_contract Exact JSON schema the LLM must return.
    analysis_rules  Additional stage-specific constraints appended to system_prompt.
    depth           Hint to the caller about LLM effort level.
    require_json    When True, callers should enforce JSON-only output mode.
    """

    name: str
    system_prompt: str
    task_prompt: str
    output_contract: str
    analysis_rules: List[str] = field(default_factory=list)
    depth: str = "standard"
    require_json: bool = True

    def compose_system_prompt(self) -> str:
        """Append analysis_rules to system_prompt (if any)."""
        if not self.analysis_rules:
            return self.system_prompt
        rules_block = "\nAdditional stage rules:\n- " + "\n- ".join(self.analysis_rules)
        return f"{self.system_prompt}{rules_block}".strip()

    def compose_user_prompt(self, context_block: str) -> str:
        """
        Assemble the full user turn:
            task_prompt
            <context_block supplied by the caller>
            output_contract
        """
        return (
            f"{self.task_prompt}\n\n"
            f"{context_block}\n\n"
            f"Output contract:\n{self.output_contract}"
        ).strip()


# ---------------------------------------------------------------------------
# Shared system prompt reused across most stages
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Stage profile constants (base templates — may be enriched at runtime)
# ---------------------------------------------------------------------------

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
{
  "main_sheet_names": [],
  "contains": [],
  "entities": [
    { "name": "", "currency": null, "confidence": 0.0, "evidence": [] }
  ],
  "has_consolidated": false,
  "consolidated_formula_pattern": "",
  "has_aje": false,
  "aje_types": [],
  "likely_units": null,
  "likely_current_period": null,
  "sheet_candidates": [
    { "name": "", "kind": "unknown", "confidence": 0.0, "evidence": [] }
  ],
  "quality_flags": [],
  "confidence": 0.0
}
""".strip(),
    analysis_rules=[
        "Main sheets are typically top-level presentation sheets, not raw data source sheets.",
        "Entity names require evidence stronger than a generic currency label.",
        "If consolidated or AJE are not clearly supported, prefer false and flag uncertainty.",
        "Prefer multiple main_sheet_names only when the workbook truly uses separate BS and P&L sheets.",
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
- Use structural evidence: account labels, cross-sheet formulas, layout, and sheet naming.
- Reject sheets that are only data sources.
""".strip(),
    output_contract="""
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
        "A sheet referenced by other sheets is often a data source, not a presentation sheet.",
        "A presentation sheet often contains account labels and formula links to supporting sheets.",
        "Do not treat a summary/control sheet as a financial presentation sheet unless its content is financial statement content.",
    ],
    depth="forensic",
)


# ---------------------------------------------------------------------------
# New stage: sheet_company
# ---------------------------------------------------------------------------

SHEET_COMPANY_PROFILE = StagePromptProfile(
    name="sheet_company",
    system_prompt="""
You are a financial Excel entity/company detection agent.

Rules:
- Identify only legal entity or company labels — not generic column headers.
- Prefer workbook-known entity names supplied in context over pure guesses.
- Search: header cells (top 10 rows), merged range labels, formula source references.
- Formula patterns like GL_LTD, WP_INC, '[LTD]Sheet1' are strong entity signals.
- Return valid JSON only.
""".strip(),
    task_prompt="""
Stage: sheet_company

Goal:
Detect company / legal-entity labels in the main financial statement sheet
and resolve each to a physical column index.

Inputs provided in context:
  - sheet_name         : the sheet being analyzed
  - known_entities     : entity names already found by WorkbookStructureAgent
  - entity_currencies  : workbook-level entity → currency priors
  - grid_preview       : top N rows of cell values (row | col0 | col1 | …)
  - merged_preview     : merged cell ranges with text labels
  - formula_preview    : top N rows of formula strings

Detection strategy:
1. Scan header rows for cells that contain a known_entity name.
2. Scan formula strings for entity-encoding patterns (GL_LTD, WP_INC …).
3. Scan merged cells whose label matches a known entity.
4. Only propose new (unlisted) entities when evidence is very strong.

For each hit, record:
  - the exact entity name
  - the zero-based col_idx of the column where the entity header appears
  - the Excel column letter (col_letter)
  - the row_idx of the header cell (or null for merged ranges)
  - the raw header_text
  - a confidence score (0.0 – 1.0)
  - evidence strings explaining the finding
""".strip(),
    output_contract="""
{
  "entities": [
    {
      "entity": "",
      "col_idx": null,
      "col_letter": "",
      "row_idx": null,
      "header_text": "",
      "confidence": 0.0,
      "evidence": []
    }
  ],
  "quality_flags": []
}
""".strip(),
    analysis_rules=[
        "Do not emit an entity hit for a generic label like 'Balance', 'Total', 'Amount', 'YTD', 'USD', 'NIS'.",
        "A col_idx of null is not acceptable — always provide the best-guess column index.",
        "If zero entity hits are found, return an empty entities list and add 'no_entity_hits_found' to quality_flags.",
        "If two entities share the same column, keep only the higher-confidence hit.",
    ],
    depth="forensic",
)


# ---------------------------------------------------------------------------
# New stage: sheet_currency
# ---------------------------------------------------------------------------

SHEET_CURRENCY_PROFILE = StagePromptProfile(
    name="sheet_currency",
    system_prompt="""
You are a financial Excel currency detection agent.

Rules:
- Identify currency markers only: ISO codes (USD, NIS, ILS, EUR …) or symbols ($, ₪, €, £ …).
- Align each currency to the nearest entity column found by SheetCompanyAgent.
- Use workbook-structure entity→currency priors as a fallback when no in-sheet evidence exists.
- ILS and NIS are the same currency — normalise to NIS.
- Return valid JSON only.
""".strip(),
    task_prompt="""
Stage: sheet_currency

Goal:
Detect currency markers in the main financial statement sheet and align
each marker to a physical column and entity.

Inputs provided in context:
  - sheet_name             : the sheet being analyzed
  - entity_currency_prior  : workbook-level entity → currency map
  - known_entity_columns   : entity hits from SheetCompanyAgent
                             [{entity, col_idx, col_letter, header_text}, …]
  - grid_preview           : top N rows of cell values
  - merged_preview         : merged cell ranges with text labels

Detection strategy:
1. Scan header rows for ISO currency codes (USD, NIS/ILS, EUR …) or symbols ($, ₪ …).
2. Align each hit to the nearest entity column (within 3 columns).
3. Scan merged cells for combined labels like 'LTD | NIS', 'INC | $'.
4. For entity columns with no in-sheet currency evidence, apply the
   entity_currency_prior as a low-confidence fallback.

For each hit, record:
  - the normalised currency string (uppercase, ILS → NIS)
  - the zero-based col_idx
  - the Excel column letter (col_letter)
  - the row_idx (or null for merged ranges)
  - the raw header_text
  - the entity name this currency applies to (empty if sheet-wide)
  - a confidence score (0.0 – 1.0)
  - evidence strings

Multi-currency entities (e.g. LTD with both NIS and USD columns) are valid —
do NOT collapse them into one hit.
""".strip(),
    output_contract="""
{
  "currencies": [
    {
      "currency": "",
      "col_idx": null,
      "col_letter": "",
      "row_idx": null,
      "header_text": "",
      "entity": "",
      "confidence": 0.0,
      "evidence": []
    }
  ],
  "quality_flags": []
}
""".strip(),
    analysis_rules=[
        "ILS must be returned as NIS.",
        "A col_idx of null is not acceptable — always provide the best-guess column index.",
        "Prior-based hits (from entity_currency_prior) should have confidence ≤ 0.65.",
        "If zero currency hits are found, return an empty currencies list and add 'no_currency_hits_found' to quality_flags.",
    ],
    depth="forensic",
)


# ---------------------------------------------------------------------------
# Dynamic stage profiles (sheet_analysis, final_render)
# ---------------------------------------------------------------------------

def _build_sheet_analysis_profile(search_config: Optional[object] = None) -> StagePromptProfile:
    search_block = ""
    if search_config is not None:
        fmt = getattr(search_config, "output_format", "json")
        key_roles = getattr(search_config, "key_roles", [])
        key_roles_text = ", ".join(key_roles) if key_roles else "coa_name, entity_value, aje, consolidated"
        search_block = f"""
Search configuration:
- output_format : {fmt}
- key_roles     : {key_roles_text}
  (prioritize these roles; all others map to 'other' or are omitted)
""".strip()

    base_task = """
Stage: sheet_analysis

Goal:
1. Classify the sheet as BS, PL, or both.
2. Identify column mappings for the required structural roles.
3. Infer likely unit and data-row ranges when supported by evidence.
4. Flag weak or suspicious structure.

Deep-analysis requirements:
- Inspect header patterns, merged cells, anchor terms, currency markers, and repeated structures.
- Use formula evidence to distinguish presentation columns from source columns.
- Compare at least two possible interpretations before deciding.
- Downgrade confidence when a signal fits multiple roles.
- Prefer explicit company names over generic labels (Balance, Total, Amount, YTD, USD, NIS).
- Focus on the most recent period; older periods map to prior_period.
""".strip()

    task_prompt = f"{base_task}\n\n{search_block}".strip() if search_block else base_task

    return StagePromptProfile(
        name="sheet_analysis",
        system_prompt=GLOBAL_FINANCIAL_SYSTEM_PROMPT,
        task_prompt=task_prompt,
        output_contract="""
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
            "A generic balance or total column is not an entity column unless the header clearly identifies an entity.",
            "Merged cells are useful clues but not sufficient proof by themselves.",
            "Numeric account code columns are usually 'other', not 'coa_name'.",
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
  coa_name, entity_value, debit, credit, aje, consolidated_aje,
  consolidated, budget, prior_period, other

Inputs now include focused agent outputs:
  - company_extraction : entity hits from SheetCompanyAgent
                         (entity name → col_idx, col_letter, confidence)
  - currency_extraction: currency hits from SheetCurrencyAgent
                         (currency → entity, col_idx, col_letter, confidence)

Use these targeted findings to set entity= and currency= on entity_value,
aje, consolidated_aje, and consolidated columns.  Prefer focused-agent
evidence over general header inference when confidence is ≥ 0.65.

Deep-analysis requirements:
- Focus on the most recent period.
- Only assign entity_value when the header clearly identifies a company/entity.
- Treat generic labels (Balance, Total, YTD, U.S. Dollars) as non-entity unless tied to a company.
- Consolidated should be supported by arithmetic/formula logic when available.
- Numeric account codes are 'other', not 'coa_name'.
""".strip(),
    output_contract="""
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
        "entity is only valid for: entity_value, aje, consolidated_aje, consolidated.",
        "If latest vs prior period is unclear, lower confidence and flag it.",
        "Return only roles from the allowed vocabulary.",
        "Promote focused-agent entity/currency assignments over weaker general-analysis guesses.",
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


def _build_final_render_profile(search_config: Optional[object] = None) -> StagePromptProfile:
    if search_config is not None:
        fmt = getattr(search_config, "output_format", "json")
        key_roles = getattr(search_config, "key_roles", [])
        key_roles_text = ", ".join(key_roles) if key_roles else "coa_name, entity_value, aje, consolidated"

        task_prompt = f"""
Stage: final_render

Goal:
Convert resolved workbook extraction into the final deterministic report format.

Output format: {fmt}
Key roles (key_columns filter): {key_roles_text}

Rules:
- key_columns contains only key-role columns.
- all_columns contains every detected column.
- companies_table contains one row per detected entity.
- currencies_table contains one row per (entity, currency) pair.
- Use empty string "" for missing scalar values — never null for strings.
- Do not add markdown tables or prose.
""".strip()

        output_contract = """
{
  "text": "",
  "key_columns": [
    {
      "column": "",
      "sheet": "",
      "role": "",
      "entity": "",
      "currency": "",
      "row_start": null,
      "row_end": null,
      "header": "",
      "formula": ""
    }
  ],
  "all_columns": [
    {
      "column": "",
      "index": null,
      "sheet": "",
      "role": "",
      "entity": "",
      "currency": "",
      "period": "",
      "row_start": null,
      "row_end": null,
      "header": "",
      "formula": ""
    }
  ],
  "companies_table": [
    {
      "entity": "",
      "sheet": "",
      "source": "",
      "header": "",
      "column": "",
      "confidence": 0.0
    }
  ],
  "currencies_table": [
    {
      "entity": "",
      "currency": "",
      "sheet": "",
      "header": "",
      "column": "",
      "confidence": 0.0,
      "source": ""
    }
  ]
}
""".strip()

    else:
        task_prompt = """
Stage: final_render

Goal:
Convert resolved workbook extraction into the final deterministic report format.
""".strip()
        output_contract = '{"text": ""}'

    return StagePromptProfile(
        name="final_render",
        system_prompt="""
You are a deterministic financial output renderer.

Your task is to format final output from already-resolved workbook extraction.
Do not add new reasoning. Do not invent fields.
""".strip(),
        task_prompt=task_prompt,
        output_contract=output_contract,
        analysis_rules=[
            "Do not introduce new fields not supported by the resolved input.",
            "Formatting must stay deterministic and conservative.",
            "The final text must match the required SHEET/COLUMN/ENTITIES/CONSOLIDATED/AJE/NIS/COMPANIES/CURRENCIES structure.",
        ],
        depth="shallow",
    )


# ---------------------------------------------------------------------------
# PromptRegistry
# ---------------------------------------------------------------------------

class PromptRegistry:
    """
    Registry for pipeline stage prompt profiles.

    v0.5.0 additions:
      "sheet_company"  → SHEET_COMPANY_PROFILE
      "sheet_currency" → SHEET_CURRENCY_PROFILE

    Usage (basic — no search config):
        registry = PromptRegistry()
        profile = registry.get("sheet_company")   # → StagePromptProfile

    Usage (with search config):
        from Multi_agen.packages.core.search_config import ColumnSearchConfig
        cfg = ColumnSearchConfig(output_format="json", key_roles=["coa_name", "entity_value", ...])
        registry = PromptRegistry(search_config=cfg)
    """

    def __init__(
        self,
        profiles: Optional[Dict[str, StagePromptProfile]] = None,
        search_config: Optional[object] = None,
    ) -> None:
        if search_config is not None and _SEARCH_CONFIG_AVAILABLE:
            if not isinstance(search_config, ColumnSearchConfig):  # type: ignore[arg-type]
                raise TypeError(
                    f"search_config must be a ColumnSearchConfig instance, got {type(search_config).__name__}"
                )

        self._search_config = search_config

        if profiles is not None:
            self._profiles: Dict[str, StagePromptProfile] = dict(profiles)
        else:
            self._profiles = self._build_default_profiles(search_config)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get(self, name: str) -> Optional[StagePromptProfile]:
        return self._profiles.get(name)

    def require(self, name: str) -> StagePromptProfile:
        profile = self._profiles.get(name)
        if profile is None:
            raise KeyError(f"Prompt profile not found: {name!r}")
        return profile

    def has(self, name: str) -> bool:
        return name in self._profiles

    def register(self, profile: StagePromptProfile) -> None:
        self._profiles[profile.name] = profile

    def names(self) -> List[str]:
        return sorted(self._profiles.keys())

    def system_prompt(self, name: str) -> Optional[str]:
        p = self.get(name)
        return p.compose_system_prompt() if p is not None else None

    def user_prompt(self, name: str, context_block: str = "") -> Optional[str]:
        p = self.get(name)
        return p.compose_user_prompt(context_block) if p is not None else None

    # ------------------------------------------------------------------
    # Internal builders
    # ------------------------------------------------------------------

    @staticmethod
    def _build_default_profiles(
        search_config: Optional[object],
    ) -> Dict[str, StagePromptProfile]:
        return {
            "workbook_structure":  WORKBOOK_STRUCTURE_PROFILE,
            "schema_detection":    SCHEMA_DETECTION_PROFILE,
            "sheet_analysis":      _build_sheet_analysis_profile(search_config),
            "sheet_company":       SHEET_COMPANY_PROFILE,
            "sheet_currency":      SHEET_CURRENCY_PROFILE,
            "role_mapping":        ROLE_MAPPING_PROFILE,
            "quality_audit":       QUALITY_AUDIT_PROFILE,
            "expert_arbitration":  EXPERT_ARBITRATION_PROFILE,
            "final_render":        _build_final_render_profile(search_config),
        }


# ---------------------------------------------------------------------------
# Module-level convenience aliases (for direct imports)
# ---------------------------------------------------------------------------

SHEET_ANALYSIS_PROFILE: StagePromptProfile = _build_sheet_analysis_profile(None)
FINAL_RENDER_PROFILE: StagePromptProfile = _build_final_render_profile(None)