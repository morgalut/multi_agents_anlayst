# Multi_agen\packages\agents\orc.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional
import inspect
import logging
import time

from Multi_agen.packages.core import (
    FinalRenderOutput,
    PipelineState,
    SheetExtractionResult,
    SheetTask,
    WorkbookExtractionResult,
)
from Multi_agen.packages.core.schemas import (
    EntityCurrencyPair,
    SheetCompanyExtraction,
    SheetCurrencyExtraction,
    WorkbookEntity,
    WorkbookSheetProfilesResult,
)

logger = logging.getLogger("multi_agen.agents.orc")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)


@dataclass(frozen=True, slots=True)
class OrcConfig:
    require_capabilities: tuple[str, ...] = (
        "excel.list_sheets",
        "excel.read_sheet_range",
        "excel.find_text",
        "excel.column_index_to_letter",
        "excel.detect_merged_cells",
        "excel.get_formulas",
    )
    continue_on_sheet_error: bool = False
    enable_workbook_structure_pass: bool = True
    enable_quality_audit: bool = True
    enable_expert_arbitration: bool = True
    enable_company_extraction: bool = True
    enable_currency_extraction: bool = True
    # NEW: toggle for the parallel sheet-profile export path
    enable_sheet_profile_export: bool = True


@dataclass(frozen=True, slots=True)
class OrcPromptPolicy:
    workbook_structure: str = "workbook_structure"
    schema_detection: str = "schema_detection"
    sheet_analysis: str = "sheet_analysis"
    sheet_company: str = "sheet_company"
    sheet_currency: str = "sheet_currency"
    role_mapping: str = "role_mapping"
    quality_audit: str = "quality_audit"
    expert_arbitration: str = "expert_arbitration"
    final_render: str = "final_render"


class ORCAgent:
    """
    Workbook/sheet-centric orchestrator.

    Pipeline:
      1. validate capabilities
      2. workbook structure analysis
      3. detect sheet tasks
      4. per-sheet: analyze → company → currency → quality_audit → map_roles → arbitrate
         ↳ NEW: also map_sheet_profile (parallel export path)
      5. aggregate WorkbookExtractionResult  (existing path)
         aggregate WorkbookSheetProfilesResult  (NEW path)
      6. final render (existing path)
         sheet-profile result stored on state (NEW path)
    """

    def __init__(
        self,
        tools: Any,
        schema_detector: Any,
        sheet_analyzer: Any,
        role_mapper: Any,
        output_renderer: Any,
        workbook_structure_agent: Optional[Any] = None,
        expert_panel: Optional[Any] = None,
        quality_auditor: Optional[Any] = None,
        company_agent: Optional[Any] = None,
        currency_agent: Optional[Any] = None,
        prompt_registry: Optional[Any] = None,
        prompt_policy: Optional[OrcPromptPolicy] = None,
        config: Optional[OrcConfig] = None,
        # NEW: optional sheet-profile mapper (injected; falls back to built-in)
        sheet_profile_mapper: Optional[Any] = None,
    ) -> None:
        self.tools = tools
        self.schema_detector = schema_detector
        self.sheet_analyzer = sheet_analyzer
        self.role_mapper = role_mapper
        self.output_renderer = output_renderer
        self.workbook_structure_agent = workbook_structure_agent
        self.expert_panel = expert_panel
        self.quality_auditor = quality_auditor
        self.company_agent = company_agent
        self.currency_agent = currency_agent
        self.prompt_registry = prompt_registry
        self.prompt_policy = prompt_policy or OrcPromptPolicy()
        self.config = config or OrcConfig()
        self._sheet_profile_mapper = sheet_profile_mapper or self._build_default_profile_mapper()
        logger.info("ORC initialized")

    # ------------------------------------------------------------------
    # Profile mapper construction
    # ------------------------------------------------------------------

    @staticmethod
    def _build_default_profile_mapper() -> Any:
        """Lazy-import the mapper so it's optional at import time."""
        try:
            from Multi_agen.packages.agents.sheet_profile_mapper import SheetProfileMapper
            return SheetProfileMapper()
        except ImportError:
            logger.warning("SheetProfileMapper not available; sheet-profile export disabled")
            return None

    # ------------------------------------------------------------------
    # Capability check
    # ------------------------------------------------------------------

    def _check_capabilities(self, state: PipelineState) -> None:
        caps = set(getattr(state.tooling, "available_capabilities", []) or [])
        missing = [c for c in self.config.require_capabilities if c not in caps]
        if missing:
            raise RuntimeError(f"Missing required MCP capabilities: {missing}")

    # ------------------------------------------------------------------
    # Prompt / invocation helpers
    # ------------------------------------------------------------------

    def _get_prompt_profile(self, stage_name: str) -> Any:
        if self.prompt_registry is None:
            return None
        try:
            return self.prompt_registry.get(stage_name)
        except Exception:
            logger.exception("Failed to resolve prompt profile stage=%s", stage_name)
            return None

    def _invoke_stage_method(
        self, method: Any, *args: Any, prompt_profile: Any = None, **kwargs: Any
    ) -> Any:
        try:
            sig = inspect.signature(method)
            if "prompt_profile" in sig.parameters:
                return method(*args, prompt_profile=prompt_profile, **kwargs)
            return method(*args, **kwargs)
        except TypeError as exc:
            msg = str(exc)
            if "prompt_profile" in msg and (
                "unexpected keyword argument" in msg
                or "got an unexpected keyword argument" in msg
            ):
                logger.exception("Stage method rejected prompt_profile; retrying without")
                return method(*args, **kwargs)
            raise

    # ------------------------------------------------------------------
    # Main run
    # ------------------------------------------------------------------

    def run(self, state: PipelineState) -> PipelineState:
        t0 = time.perf_counter()
        logger.info("ORC pipeline start run_id=%s", state.run_id)
        self._check_capabilities(state)

        # Stage 1: workbook structure
        if self.config.enable_workbook_structure_pass and self.workbook_structure_agent is not None:
            try:
                ws = self._invoke_stage_method(
                    self.workbook_structure_agent.analyze_workbook,
                    state, self.tools,
                    prompt_profile=self._get_prompt_profile(self.prompt_policy.workbook_structure),
                )
                state.workbook_structure = ws
                logger.info("Workbook structure done main_sheets=%s", getattr(ws, "main_sheet_names", None))
            except Exception:
                logger.exception("Workbook structure failed")
                if not self.config.continue_on_sheet_error:
                    raise

        # Stage 2: sheet tasks
        sheet_tasks: List[SheetTask] = self._invoke_stage_method(
            self.schema_detector.detect, state, self.tools,
            prompt_profile=self._get_prompt_profile(self.prompt_policy.schema_detection),
        )
        state.sheet_tasks = sheet_tasks
        logger.info("Sheet tasks detected count=%d", len(sheet_tasks))

        # Stage 3: process each sheet
        # NEW: accumulate per-sheet profile results in parallel
        sheet_profile_map: Dict[str, Any] = {}  # sheet_name -> SheetProfileResult

        for i, task in enumerate(sheet_tasks):
            logger.info("Processing %d/%d sheet=%s", i + 1, len(sheet_tasks), task.sheet_name)
            try:
                result, analysis = self._process_one_sheet_with_analysis(task, state)
            except Exception as exc:
                logger.exception("Sheet failed sheet=%s", task.sheet_name)
                if not self.config.continue_on_sheet_error:
                    raise
                result = SheetExtractionResult(
                    sheet_name=task.sheet_name,
                    contains="BS+PL",
                    columns=[],
                    quality_flags=[f"sheet_processing_error:{type(exc).__name__}"],
                    confidence=0.0,
                )
                analysis = None

            state.add_sheet_result(result)

            # NEW: build sheet profile (parallel export track)
            if (
                self.config.enable_sheet_profile_export
                and self._sheet_profile_mapper is not None
                and analysis is not None
            ):
                try:
                    profile = self._sheet_profile_mapper.map_profile(
                        task=task,
                        analysis=analysis,
                        is_main_sheet=getattr(task, "is_main_sheet", False),
                    )
                    sheet_profile_map[task.sheet_name] = profile
                    logger.info("SheetProfile built sheet=%s", task.sheet_name)
                except Exception:
                    logger.exception("SheetProfile failed sheet=%s", task.sheet_name)

        # Stage 4: aggregate workbook result (existing path)
        workbook_result = self._aggregate_workbook_result(state)
        state.set_workbook_result(workbook_result)

        # Stage 5: aggregate sheet-profile result (NEW path)
        if self.config.enable_sheet_profile_export and sheet_profile_map:
            sheet_profiles_result = WorkbookSheetProfilesResult(profiles=sheet_profile_map)
            self._stash(state, "sheet_profiles_result", "_", sheet_profiles_result)
            # Also set as a top-level attribute for convenient access
            try:
                object.__setattr__(state, "sheet_profiles_result", sheet_profiles_result)
            except Exception:
                pass
            logger.info("WorkbookSheetProfiles built sheets=%s", list(sheet_profile_map.keys()))

        # Stage 6: render (existing path)
        final_render: FinalRenderOutput = self._invoke_stage_method(
            self.output_renderer.render, workbook_result,
            prompt_profile=self._get_prompt_profile(self.prompt_policy.final_render),
        )
        state.set_final_render(final_render)

        logger.info(
            "ORC done sheets=%d elapsed_ms=%.1f",
            len(state.sheet_results),
            (time.perf_counter() - t0) * 1000,
        )
        return state

    # ------------------------------------------------------------------
    # Per-sheet processing  (NEW: returns analysis alongside result)
    # ------------------------------------------------------------------

    def _process_one_sheet_with_analysis(
        self, task: SheetTask, state: PipelineState
    ) -> tuple:  # -> (SheetExtractionResult, SheetAnalysis)
        """
        Thin wrapper that returns BOTH the extraction result AND the raw analysis,
        so the profile-export track can consume the analysis signals directly.
        """
        # A: broad analysis
        analysis = self._invoke_stage_method(
            self.sheet_analyzer.analyze, task, state, self.tools,
            prompt_profile=self._get_prompt_profile(self.prompt_policy.sheet_analysis),
        )

        # B: company extraction (main sheet only)
        company_extraction: Optional[SheetCompanyExtraction] = None
        if task.is_main_sheet and self.config.enable_company_extraction and self.company_agent:
            try:
                company_extraction = self._invoke_stage_method(
                    self.company_agent.extract, task, state, analysis, self.tools,
                    prompt_profile=self._get_prompt_profile(self.prompt_policy.sheet_company),
                )
                self._stash(state, "company_extractions", task.sheet_name, company_extraction)
            except Exception:
                logger.exception("SheetCompanyAgent failed sheet=%s", task.sheet_name)

        # C: currency extraction (main sheet only)
        currency_extraction: Optional[SheetCurrencyExtraction] = None
        if task.is_main_sheet and self.config.enable_currency_extraction and self.currency_agent:
            try:
                currency_extraction = self._invoke_stage_method(
                    self.currency_agent.extract, task, state, analysis, self.tools,
                    company_extraction,
                    prompt_profile=self._get_prompt_profile(self.prompt_policy.sheet_currency),
                )
                self._stash(state, "currency_extractions", task.sheet_name, currency_extraction)
            except Exception:
                logger.exception("SheetCurrencyAgent failed sheet=%s", task.sheet_name)

        # D: quality audit
        quality_flags: List[str] = []
        if self.config.enable_quality_audit and self.quality_auditor:
            try:
                raw = self._invoke_stage_method(
                    self.quality_auditor.audit, task, state, analysis, self.tools,
                    prompt_profile=self._get_prompt_profile(self.prompt_policy.quality_audit),
                )
                quality_flags = raw if isinstance(raw, list) else ([str(raw)] if raw else [])
            except Exception:
                logger.exception("Quality audit failed sheet=%s", task.sheet_name)

        # E: role mapping
        columns = self._invoke_role_mapper(task, state, analysis, company_extraction, currency_extraction)

        # F: expert arbitration
        if self.config.enable_expert_arbitration and self.expert_panel:
            try:
                columns = self._invoke_stage_method(
                    self.expert_panel.maybe_arbitrate, task, state, analysis, columns,
                    prompt_profile=self._get_prompt_profile(self.prompt_policy.expert_arbitration),
                ) or columns
            except Exception:
                logger.exception("Expert arbitration failed sheet=%s", task.sheet_name)

        result = SheetExtractionResult(
            sheet_name=task.sheet_name,
            contains=self._resolve_contains(analysis),
            unit=self._resolve_unit(analysis),
            data_row_start=self._resolve_data_row_bounds(columns)[0],
            data_row_end=self._resolve_data_row_bounds(columns)[1],
            columns=list(columns),
            quality_flags=list(quality_flags),
            confidence=self._resolve_confidence(analysis, columns),
        )
        return result, analysis

    # Keep old signature for backward compatibility
    def _process_one_sheet(self, task: SheetTask, state: PipelineState) -> SheetExtractionResult:
        result, _ = self._process_one_sheet_with_analysis(task, state)
        return result

    # ------------------------------------------------------------------
    # Role mapper invocation (backward-compatible)
    # ------------------------------------------------------------------

    def _invoke_role_mapper(
        self,
        task: SheetTask,
        state: PipelineState,
        analysis: Any,
        company_extraction: Optional[SheetCompanyExtraction],
        currency_extraction: Optional[SheetCurrencyExtraction],
    ) -> List[Any]:
        profile = self._get_prompt_profile(self.prompt_policy.role_mapping)
        try:
            sig = inspect.signature(self.role_mapper.map_roles)
            params = set(sig.parameters.keys())
            if "company_extraction" in params and "currency_extraction" in params:
                return self._invoke_stage_method(
                    self.role_mapper.map_roles, task, state, analysis,
                    company_extraction=company_extraction,
                    currency_extraction=currency_extraction,
                    prompt_profile=profile,
                ) or []
        except Exception:
            pass
        return self._invoke_stage_method(
            self.role_mapper.map_roles, task, state, analysis,
            prompt_profile=profile,
        ) or []

    # ------------------------------------------------------------------
    # Aggregation
    # ------------------------------------------------------------------

    def _aggregate_workbook_result(self, state: PipelineState) -> WorkbookExtractionResult:
        sheet_results = list(getattr(state, "sheet_results", []) or [])

        entities_seen: set[str] = set()
        entities: List[str] = []
        has_consolidated = False
        consolidated_patterns: List[str] = []
        has_aje = False
        has_per_entity_aje = False
        has_consolidated_aje = False
        has_nis = False
        quality_flags: List[str] = []

        for sheet in sheet_results:
            quality_flags.extend(list(getattr(sheet, "quality_flags", []) or []))
            for col in getattr(sheet, "columns", []) or []:
                role = str(getattr(col, "role", "") or "")
                entity = str(getattr(col, "entity", "") or "").strip()
                currency = str(getattr(col, "currency", "") or "").strip().upper()
                formula_pattern = str(getattr(col, "formula_pattern", "") or "").strip()

                if role == "entity_value" and entity and entity not in entities_seen:
                    entities_seen.add(entity)
                    entities.append(entity)
                if role in {"consolidated", "consolidated_aje"}:
                    has_consolidated = True
                    if formula_pattern:
                        consolidated_patterns.append(formula_pattern)
                if role == "aje":
                    has_aje = True
                    has_per_entity_aje = True
                if role == "consolidated_aje":
                    has_aje = True
                    has_consolidated_aje = True
                if currency == "NIS" or "NIS" in str(getattr(col, "header_text", "") or "").upper():
                    has_nis = True

        consolidated_formula_pattern = consolidated_patterns[0] if consolidated_patterns else ""
        aje_types: List[str] = []
        if has_per_entity_aje and has_consolidated_aje:
            aje_types = ["both"]
        elif has_per_entity_aje:
            aje_types = ["per-entity"]
        elif has_consolidated_aje:
            aje_types = ["consolidated"]

        ws = getattr(state, "workbook_structure", None)
        ws_entities: List[WorkbookEntity] = []

        if ws is not None:
            ws_entities = list(getattr(ws, "entities", []) or [])

            if not entities:
                for ent in ws_entities:
                    name = str(getattr(ent, "name", "") or "").strip()
                    if name and name not in entities_seen:
                        entities_seen.add(name)
                        entities.append(name)
            if not has_consolidated:
                has_consolidated = bool(getattr(ws, "has_consolidated", False))
            if not consolidated_formula_pattern:
                consolidated_formula_pattern = str(
                    getattr(ws, "consolidated_formula_pattern", "") or ""
                ).strip()
            if not has_aje:
                has_aje = bool(getattr(ws, "has_aje", False))
            if not aje_types:
                ws_aje = [str(x).strip() for x in (getattr(ws, "aje_types", []) or []) if str(x).strip()]
                if ws_aje:
                    aje_types = ws_aje
            quality_flags.extend(list(getattr(ws, "quality_flags", []) or []))

            for ent in ws_entities:
                if str(getattr(ent, "currency", "") or "").upper() in ("NIS", "ILS"):
                    has_nis = True

        qf_out: List[str] = []
        seen_qf: set[str] = set()
        for f in quality_flags:
            s = str(f).strip()
            if s and s not in seen_qf:
                seen_qf.add(s)
                qf_out.append(s)

        main_sheet_name = self._resolve_main_sheet_name(state)

        entity_currency_pairs, companies_table, currencies_table = (
            self._build_entity_currency_tables(state, sheet_results, ws)
        )

        for pair in entity_currency_pairs:
            if pair.entity and pair.entity not in entities_seen:
                entities_seen.add(pair.entity)
                entities.append(pair.entity)
            if pair.currency and pair.currency.upper() in ("NIS", "ILS"):
                has_nis = True

        return WorkbookExtractionResult(
            sheets=sheet_results,
            entities=entities,
            has_consolidated=has_consolidated,
            consolidated_formula_pattern=consolidated_formula_pattern,
            has_aje=has_aje,
            aje_types=aje_types,
            has_nis=has_nis,
            quality_flags=qf_out,
            main_sheet_name=main_sheet_name,
            entity_currency_pairs=entity_currency_pairs,
            companies_table=companies_table,
            currencies_table=currencies_table,
            workbook_structure_entities=ws_entities,
        )

    # ------------------------------------------------------------------
    # Entity/currency tables
    # ------------------------------------------------------------------

    def _resolve_main_sheet_name(self, state: PipelineState) -> str:
        for task in (getattr(state, "sheet_tasks", []) or []):
            if getattr(task, "is_main_sheet", False):
                return str(getattr(task, "sheet_name", "") or "")
        ws = getattr(state, "workbook_structure", None)
        if ws:
            names = getattr(ws, "main_sheet_names", []) or []
            if names:
                return str(names[0])
        return ""

    def _build_entity_currency_tables(
        self,
        state: PipelineState,
        sheet_results: List[SheetExtractionResult],
        ws: Any,
    ) -> tuple[List[EntityCurrencyPair], List[Dict[str, Any]], List[Dict[str, Any]]]:

        company_extractions: Dict[str, Any] = getattr(state, "company_extractions", {}) or {}
        currency_extractions: Dict[str, Any] = getattr(state, "currency_extractions", {}) or {}

        entity_map: Dict[str, Dict[str, Any]] = {}
        for sheet_name, ce in company_extractions.items():
            for eh in (getattr(ce, "entities", []) or []):
                key = str(getattr(eh, "entity", "") or "").strip()
                if not key:
                    continue
                conf = float(getattr(eh, "confidence", 0.0))
                prev = entity_map.get(key)
                if prev is None or conf > prev["confidence"]:
                    entity_map[key] = {
                        "entity": key,
                        "sheet": sheet_name,
                        "col_idx": getattr(eh, "col_idx", 0),
                        "col_letter": str(getattr(eh, "col_letter", "") or ""),
                        "header": str(getattr(eh, "header_text", "") or ""),
                        "confidence": conf,
                        "source": "sheet_company",
                    }

        currency_map: Dict[tuple[str, str], Dict[str, Any]] = {}
        for sheet_name, cex in currency_extractions.items():
            for ch in (getattr(cex, "currencies", []) or []):
                currency = str(getattr(ch, "currency", "") or "").strip().upper()
                entity = str(getattr(ch, "entity", "") or "").strip()
                if not currency:
                    continue
                key = (currency, entity)
                conf = float(getattr(ch, "confidence", 0.0))
                prev = currency_map.get(key)
                if prev is None or conf > prev["confidence"]:
                    currency_map[key] = {
                        "currency": currency,
                        "entity": entity,
                        "sheet": sheet_name,
                        "col_idx": getattr(ch, "col_idx", 0),
                        "col_letter": str(getattr(ch, "col_letter", "") or ""),
                        "header": str(getattr(ch, "header_text", "") or ""),
                        "confidence": conf,
                        "source": "sheet_currency",
                    }

        if ws is not None:
            main_sheet = self._resolve_main_sheet_name(state)
            for ent_obj in (getattr(ws, "entities", []) or []):
                ent_name = str(getattr(ent_obj, "name", "") or "").strip()
                ent_currency = str(getattr(ent_obj, "currency", "") or "").strip().upper()
                ent_conf = float(getattr(ent_obj, "confidence", 0.0))

                if ent_name and ent_name not in entity_map:
                    entity_map[ent_name] = {
                        "entity": ent_name,
                        "sheet": main_sheet,
                        "col_idx": -1,
                        "col_letter": "",
                        "header": "",
                        "confidence": ent_conf * 0.7,
                        "source": "workbook_structure",
                    }
                elif ent_name in entity_map:
                    prev_source = entity_map[ent_name].get("source", "")
                    if "workbook_structure" not in prev_source:
                        entity_map[ent_name] = dict(
                            entity_map[ent_name],
                            source=_merge_source(prev_source, "workbook_structure"),
                        )

                if ent_name and ent_currency:
                    key = (ent_currency, ent_name)
                    if key not in currency_map:
                        ent_data = entity_map.get(ent_name, {})
                        currency_map[key] = {
                            "currency": ent_currency,
                            "entity": ent_name,
                            "sheet": ent_data.get("sheet", main_sheet),
                            "col_idx": ent_data.get("col_idx", -1),
                            "col_letter": ent_data.get("col_letter", ""),
                            "header": ent_data.get("header", ""),
                            "confidence": ent_conf * 0.7,
                            "source": "workbook_structure",
                        }
                    else:
                        prev = currency_map[key]
                        if "workbook_structure" not in prev.get("source", ""):
                            currency_map[key] = dict(
                                prev,
                                source=_merge_source(prev.get("source", ""), "workbook_structure"),
                            )

        pairs: List[EntityCurrencyPair] = []
        for ent_name, ent_data in entity_map.items():
            best_key = max(
                (k for k in currency_map if k[1].upper() == ent_name.upper()),
                key=lambda k: currency_map[k]["confidence"],
                default=None,
            )
            if best_key is None:
                pairs.append(EntityCurrencyPair(
                    entity=ent_name, currency="",
                    sheet_name=ent_data.get("sheet", ""),
                    header=ent_data.get("header", ""),
                    column=ent_data.get("col_letter", ""),
                    confidence=ent_data.get("confidence", 0.0),
                    source=ent_data.get("source", ""),
                ))
            else:
                c_data = currency_map[best_key]
                pairs.append(EntityCurrencyPair(
                    entity=ent_name,
                    currency=c_data["currency"],
                    sheet_name=ent_data.get("sheet", ""),
                    header=ent_data.get("header", ""),
                    column=ent_data.get("col_letter", ""),
                    confidence=min(ent_data.get("confidence", 0.0), c_data.get("confidence", 0.0)),
                    source=_merge_source(ent_data.get("source", ""), c_data.get("source", "")),
                ))

        companies_table: List[Dict[str, Any]] = [
            {
                "entity":     d["entity"],
                "sheet":      d.get("sheet", ""),
                "source":     d.get("source", ""),
                "header":     d.get("header", ""),
                "column":     d.get("col_letter", ""),
                "confidence": round(d.get("confidence", 0.0), 3),
            }
            for d in sorted(entity_map.values(), key=lambda x: x["entity"])
        ]

        currencies_table: List[Dict[str, Any]] = sorted(
            [
                {
                    "entity":     p.entity,
                    "currency":   p.currency,
                    "sheet":      p.sheet_name,
                    "header":     p.header,
                    "column":     p.column,
                    "confidence": round(p.confidence, 3),
                    "source":     p.source,
                }
                for p in pairs if p.currency
            ],
            key=lambda r: (r["entity"], r["currency"]),
        )

        return pairs, companies_table, currencies_table

    # ------------------------------------------------------------------
    # Resolution helpers
    # ------------------------------------------------------------------

    def _resolve_contains(self, analysis: Any) -> str:
        raw = list(getattr(getattr(analysis, "classification", None), "types", []) or [])
        if "BS" in raw and "PL" in raw:
            return "BS+PL"
        if "BS" in raw:
            return "BS"
        if "PL" in raw:
            return "PL"
        return "BS+PL"

    def _resolve_unit(self, analysis: Any) -> str:
        signals = getattr(analysis, "signals", None) or {}
        if not isinstance(signals, dict):
            return "units"
        for key in ("unit", "likely_unit", "units"):
            v = signals.get(key)
            if v:
                return str(v).strip()
        llm_raw = str(signals.get("llm_raw", "") or "").lower()
        if "thousand" in llm_raw:
            return "thousands"
        if "million" in llm_raw:
            return "millions"
        return "units"

    def _resolve_data_row_bounds(
        self, columns: List[Any]
    ) -> tuple[Optional[int], Optional[int]]:
        starts, ends = [], []
        for col in columns:
            rs = getattr(col, "row_start", None)
            re = getattr(col, "row_end", None)
            if isinstance(rs, int) and rs >= 1:
                starts.append(rs)
            if isinstance(re, int) and re >= 1:
                ends.append(re)
        return (min(starts) if starts else None), (max(ends) if ends else None)

    def _resolve_confidence(self, analysis: Any, columns: List[Any]) -> float:
        try:
            cls_conf = float(
                getattr(getattr(analysis, "classification", None), "confidence", 0.0)
            )
        except Exception:
            cls_conf = 0.0
        col_confs = []
        for col in columns:
            try:
                col_confs.append(float(getattr(col, "confidence", 0.0) or 0.0))
            except Exception:
                pass
        if not col_confs:
            return max(0.0, min(1.0, cls_conf))
        return max(0.0, min(1.0, cls_conf * 0.4 + (sum(col_confs) / len(col_confs)) * 0.6))

    # ------------------------------------------------------------------
    # Utility
    # ------------------------------------------------------------------

    @staticmethod
    def _stash(state: Any, attr: str, key: str, value: Any) -> None:
        if not hasattr(state, attr):
            try:
                object.__setattr__(state, attr, {})
            except Exception:
                return
        try:
            getattr(state, attr)[key] = value
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Module-level helper
# ---------------------------------------------------------------------------

def _merge_source(a: str, b: str) -> str:
    parts = set((a or "").split("+")) | set((b or "").split("+"))
    parts.discard("")
    return "+".join(sorted(parts))