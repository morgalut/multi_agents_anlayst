from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple
import logging
import re

from Multi_agen.packages.core import (
    PipelineState,
    SheetTask,
    WorkbookStructure,
    normalize_text,
)

logger = logging.getLogger("multi_agen.agents.schema_detector")


@dataclass(frozen=True, slots=True)
class SchemaDetectorConfig:
    """
    Detect presentation-sheet tasks for structural extraction.

    Two-pass strategy:
      Pass A: grid-only scoring for all candidate sheets (cheap).
      Pass B: formula-assisted scoring only for top shortlist (expensive, gated).

    This avoids burning 60-second timeout penalties on every sheet.
    """
    # Pass A – grid-only scan dimensions (cheap, applied to all sheets)
    grid_only_scan_rows: int = 35
    grid_only_scan_cols: int = 20

    # Pass B – formula probe dimensions (expensive, applied to shortlist only)
    formula_probe_rows: int = 12
    formula_probe_cols: int = 12
    max_formula_validation_candidates: int = 3

    max_tasks: int = 2

    min_preferred_score: int = 4
    min_fallback_score: int = 5
    min_signal_families: int = 2

    workbook_candidate_min_confidence: float = 0.50
    allow_fallback_supplement: bool = True
    secondary_score_gap: int = 1


@dataclass
class CandidateValidationResult:
    sheet_name: str
    ok: bool
    total_score: int
    signal_families: int
    evidence: List[str] = field(default_factory=list)

    has_statement_signal: bool = False
    has_account_signal: bool = False
    has_formula_signal: bool = False

    is_derivative: bool = False
    distinct_ref_sheets: Tuple[str, ...] = ()
    total_formula_refs: int = 0


class MainSheetSchemaDetector:
    """
    Two-pass sheet detector:

    Pass A (cheap, all sheets):
      - read a small grid window
      - score using sheet name + content signals
      - no formula reads in this pass

    Pass B (expensive, shortlist only):
      - read a narrow formula window
      - augment scores for top N candidates only

    This changes detector cost from:
      O(all sheets × expensive formula read)
    to:
      O(all sheets × cheap grid read) + O(shortlist × narrow formula read)
    """

    _SHEET_REF_RE = re.compile(
        r"(?:^|[=+\-*/,( ])(?:'([^']+)'|([A-Za-z_][A-Za-z0-9_ .&-]*))!"
    )

    def __init__(self, config: Optional[SchemaDetectorConfig] = None) -> None:
        self.config = config or SchemaDetectorConfig()

    def detect(
        self,
        state: PipelineState,
        tools: Any,
        prompt_profile: Any = None,
    ) -> List[SheetTask]:
        logger.info("SchemaDetector:start run_id=%s", getattr(state, "run_id", None))

        workbook_structure = getattr(state, "workbook_structure", None)

        # exact_sheets: raw names exactly as the server returned them (may have trailing spaces)
        exact_sheets = self._safe_list_sheets(tools)
        if not exact_sheets:
            raise RuntimeError("Failed to detect presentation sheets: no sheets found.")

        # canonical_to_exact: stripped-name -> exact-server-name
        # Lets us resolve "FS" -> "FS " when the server has a trailing-space variant,
        # avoiding HTTP 400s from whitespace mismatches.
        canonical_to_exact: Dict[str, str] = {}
        for name in exact_sheets:
            canonical = name.strip()
            if canonical not in canonical_to_exact:
                canonical_to_exact[canonical] = name

        sheet_set = set(exact_sheets)   # exact names for membership tests
        sheets = exact_sheets           # alias used below

        # ----------------------------------------------------------------
        # Workbook-preferred candidates (from LLM workbook analysis)
        # ----------------------------------------------------------------
        preferred_names_raw = self._extract_preferred_sheet_names(workbook_structure)
        # Resolve each preferred name to its exact server-side spelling
        preferred_names = [
            canonical_to_exact.get(n.strip(), n)
            for n in preferred_names_raw
        ]
        preferred_results = self._validate_named_candidates_grid_only(
            preferred_names,
            sheet_set,
            tools,
            min_score=self.config.min_preferred_score,
        )

        # ----------------------------------------------------------------
        # Fallback: grid-only scan of all sheets, then narrow formula probe
        # ----------------------------------------------------------------
        fallback_results: List[CandidateValidationResult] = []
        if self.config.allow_fallback_supplement or not preferred_results:
            grid_ranked = self._rank_all_sheets_grid_only(
                sheets=sheets,
                tools=tools,
            )
            fallback_results = self._augment_shortlist_with_formulas(
                candidates=grid_ranked,
                tools=tools,
                min_score=self.config.min_fallback_score,
            )

        final_results = self._select_candidates(
            preferred_results=preferred_results,
            fallback_results=fallback_results,
            workbook_structure=workbook_structure,
        )

        if not final_results:
            raise RuntimeError("Failed to detect any usable presentation-sheet tasks.")

        tasks, task_provenance = self._build_tasks_and_provenance(
            final_results=final_results,
            workbook_structure=workbook_structure,
        )

        try:
            state.sheet_tasks = tasks
            state.task_provenance = task_provenance
            state.workbook_structure_provenance = self._make_workbook_structure_provenance(
                workbook_structure=workbook_structure,
                selected_tasks=tasks,
            )
        except Exception:
            logger.exception("SchemaDetector:failed attaching tasks to state")

        logger.info(
            "SchemaDetector:done selected=%s main=%s",
            [t.sheet_name for t in tasks],
            tasks[0].sheet_name if tasks else None,
        )
        return tasks

    # ------------------------------------------------------------------
    # Sheet listing
    # ------------------------------------------------------------------

    def _safe_list_sheets(self, tools: Any) -> List[str]:
        """
        Return sheet names exactly as the server reported them.

        The server is authoritative — do NOT strip trailing/leading whitespace
        from the names used in tool calls.  Stripping here caused HTTP 400s
        when the workbook contains names like 'שע"ח +סבירות ' (trailing space).

        We preserve the raw names so every subsequent read_sheet_range /
        get_formulas call uses the exact string the server expects.
        """
        try:
            sheets = tools.excel_list_sheets()
        except Exception as exc:
            raise RuntimeError("Failed to list workbook sheets.") from exc

        if not isinstance(sheets, list):
            return []

        out: List[str] = []
        for item in sheets:
            raw = str(item)          # ← do NOT strip here
            if raw.strip():          # skip truly blank entries
                out.append(raw)
        return out

    # ------------------------------------------------------------------
    # Preferred-name extraction from workbook structure
    # ------------------------------------------------------------------

    def _extract_preferred_sheet_names(
        self, workbook_structure: Optional[WorkbookStructure]
    ) -> List[str]:
        if workbook_structure is None:
            return []

        out: List[str] = []
        seen: Set[str] = set()

        for name in getattr(workbook_structure, "main_sheet_names", []) or []:
            s = str(name).strip()
            if not s or s in seen:
                continue
            seen.add(s)
            out.append(s)

        if out:
            return out

        for cand in getattr(workbook_structure, "sheet_candidates", []) or []:
            name = str(getattr(cand, "name", "") or "").strip()
            kind = str(getattr(cand, "kind", "unknown") or "unknown").strip().lower()
            try:
                conf = float(getattr(cand, "confidence", 0.0) or 0.0)
            except Exception:
                conf = 0.0

            if not name or name in seen:
                continue

            if kind == "presentation" and conf >= self.config.workbook_candidate_min_confidence:
                seen.add(name)
                out.append(name)

        return out

    # ------------------------------------------------------------------
    # Pass A: grid-only validation (all sheets, cheap)
    # ------------------------------------------------------------------

    def _validate_named_candidates_grid_only(
        self,
        candidate_names: List[str],
        sheet_set: Set[str],
        tools: Any,
        min_score: int,
    ) -> List[CandidateValidationResult]:
        results: List[CandidateValidationResult] = []

        for sheet_name in candidate_names:
            if sheet_name not in sheet_set:
                continue

            result = self._score_grid_only(
                sheet_name=sheet_name,
                tools=tools,
                min_score=min_score,
            )
            if result.ok:
                results.append(result)
            else:
                logger.info(
                    "SchemaDetector:preferred candidate rejected sheet=%s score=%d evidence=%s",
                    sheet_name,
                    result.total_score,
                    result.evidence,
                )

        return results

    def _rank_all_sheets_grid_only(
        self,
        sheets: List[str],
        tools: Any,
    ) -> List[CandidateValidationResult]:
        """
        Score every sheet using only grid reads (cheap).
        Returns all results sorted best-first regardless of min_score,
        so the formula-probe pass can pick from the true top N.
        """
        ranked: List[CandidateValidationResult] = []

        for sheet_name in sheets:
            result = self._score_grid_only(
                sheet_name=sheet_name,
                tools=tools,
                min_score=0,  # no cutoff here; filter after formula pass
            )
            ranked.append(result)

        ranked.sort(key=self._candidate_sort_key)
        logger.info(
            "SchemaDetector:grid_only_ranked candidates=%s",
            [
                {"sheet_name": r.sheet_name, "score": r.total_score, "families": r.signal_families, "derivative": r.is_derivative}
                for r in ranked[:10]
            ],
        )
        return ranked

    def _score_grid_only(
        self,
        *,
        sheet_name: str,
        tools: Any,
        min_score: int,
    ) -> CandidateValidationResult:
        """
        Pass A: score a single sheet using only a small grid read.
        No formula reads here.
        """
        grid = self._safe_read_grid(
            sheet_name=sheet_name,
            tools=tools,
            nrows=self.config.grid_only_scan_rows,
            ncols=self.config.grid_only_scan_cols,
        )

        top_blob = self._grid_text_blob(grid[: min(8, len(grid))])
        full_blob = self._grid_text_blob(grid)

        evidence: List[str] = []
        score = 0

        # Statement signals
        bs_top_hits = self._count_terms(
            top_blob,
            ["balance sheet", "statement of financial position", "assets", "liabilities",
             "equity", "מאזן", "נכסים", "התחייבויות", "הון"],
        )
        pl_top_hits = self._count_terms(
            top_blob,
            ["profit and loss", "p&l", "income statement", "revenue", "expenses",
             "gross profit", "operating profit", "רווח והפסד", "הכנסות", "הוצאות"],
        )
        bs_full_hits = self._count_terms(
            full_blob,
            ["balance sheet", "statement of financial position", "assets", "liabilities",
             "equity", "מאזן", "נכסים", "התחייבויות", "הון"],
        )
        pl_full_hits = self._count_terms(
            full_blob,
            ["profit and loss", "p&l", "income statement", "revenue", "expenses",
             "gross profit", "operating profit", "רווח והפסד", "הכנסות", "הוצאות"],
        )

        has_statement_signal = False
        if bs_top_hits > 0 or pl_top_hits > 0:
            score += 3
            has_statement_signal = True
            evidence.append(f"top_statement:bs={bs_top_hits},pl={pl_top_hits}")
        elif bs_full_hits > 0 or pl_full_hits > 0:
            score += 2
            has_statement_signal = True
            evidence.append(f"full_statement:bs={bs_full_hits},pl={pl_full_hits}")

        # Account/description signals
        account_hits = self._count_terms(
            full_blob,
            ["account", "account name", "account description", "description",
             "coa", "details", "תיאור", "חשבון"],
        )
        has_account_signal = account_hits > 0
        if has_account_signal:
            score += 2
            evidence.append(f"account_hits:{account_hits}")

        # Name bonuses and penalties
        presentation_bonus = self._presentation_sheet_name_bonus(sheet_name)
        if presentation_bonus:
            score += presentation_bonus
            evidence.append(f"presentation_bonus:{presentation_bonus}")

        source_penalty = self._source_sheet_name_penalty(sheet_name)
        if source_penalty:
            score -= source_penalty
            evidence.append(f"source_penalty:{source_penalty}")

        derivative_penalty = self._derivative_sheet_name_penalty(sheet_name)
        is_derivative = derivative_penalty > 0
        if derivative_penalty:
            score -= derivative_penalty
            evidence.append(f"derivative_penalty:{derivative_penalty}")

        signal_families = int(has_statement_signal) + int(has_account_signal)
        structural_support = has_statement_signal or has_account_signal

        ok = (
            score >= min_score
            and signal_families >= self.config.min_signal_families
            and structural_support
        )

        return CandidateValidationResult(
            sheet_name=sheet_name,
            ok=ok,
            total_score=score,
            signal_families=signal_families,
            evidence=evidence,
            has_statement_signal=has_statement_signal,
            has_account_signal=has_account_signal,
            has_formula_signal=False,  # will be updated in Pass B
            is_derivative=is_derivative,
            distinct_ref_sheets=(),
            total_formula_refs=0,
        )

    # ------------------------------------------------------------------
    # Pass B: formula probe (shortlist only, targeted narrow reads)
    # ------------------------------------------------------------------

    def _augment_shortlist_with_formulas(
        self,
        candidates: List[CandidateValidationResult],
        tools: Any,
        min_score: int,
    ) -> List[CandidateValidationResult]:
        """
        Pass B: read a narrow formula window for the top N shortlisted sheets only.
        Augments their scores and re-filters with min_score.
        """
        if not hasattr(tools, "excel_get_formulas"):
            # Formula tool unavailable — filter grid-only results
            return [c for c in candidates if c.ok and c.total_score >= min_score]

        shortlist = candidates[: self.config.max_formula_validation_candidates]
        augmented: List[CandidateValidationResult] = []

        for candidate in shortlist:
            augmented_result = self._augment_with_formula_probe(
                candidate=candidate,
                tools=tools,
            )
            if augmented_result.total_score >= min_score:
                augmented.append(augmented_result)
            else:
                logger.info(
                    "SchemaDetector:shortlist candidate rejected after formula probe sheet=%s score=%d",
                    augmented_result.sheet_name,
                    augmented_result.total_score,
                )

        augmented.sort(key=self._candidate_sort_key)
        logger.info(
            "SchemaDetector:ranked candidates=%s",
            [
                {"sheet_name": r.sheet_name, "score": r.total_score, "families": r.signal_families, "derivative": r.is_derivative}
                for r in augmented
            ],
        )
        return augmented

    def _augment_with_formula_probe(
        self,
        *,
        candidate: CandidateValidationResult,
        tools: Any,
    ) -> CandidateValidationResult:
        """
        Read a narrow formula window and update the candidate score.
        Uses small probe dimensions to avoid 60-second timeouts.
        """
        sheet_name = candidate.sheet_name

        formulas = self._safe_read_formulas(
            sheet_name=sheet_name,
            tools=tools,
            nrows=self.config.formula_probe_rows,
            ncols=self.config.formula_probe_cols,
        )

        if not formulas:
            # Formula read failed or timed out — keep grid-only result
            return candidate

        ref_sheets = self._extract_formula_ref_sheets(formulas)
        distinct_ref_sheets = tuple(sorted(set(ref_sheets)))
        total_formula_refs = len(ref_sheets)

        evidence = list(candidate.evidence)
        score = candidate.total_score
        has_formula_signal = False

        if len(distinct_ref_sheets) >= 2:
            score += 4
            has_formula_signal = True
            evidence.append(f"distinct_formula_refs:{len(distinct_ref_sheets)}")
        elif total_formula_refs >= 2:
            score += 2
            has_formula_signal = True
            evidence.append(f"formula_ref_count:{total_formula_refs}")

        # Extra derivative penalty: relies on a single sheet heavily
        is_derivative = candidate.is_derivative
        if is_derivative and len(distinct_ref_sheets) <= 1 and total_formula_refs >= 4:
            score -= 2
            evidence.append("single_sheet_dependency_penalty:2")

        signal_families = candidate.signal_families + int(has_formula_signal)
        structural_support = (
            candidate.has_statement_signal
            or candidate.has_account_signal
            or has_formula_signal
        )

        ok = (
            score >= self.config.min_fallback_score
            and signal_families >= self.config.min_signal_families
            and structural_support
        )

        return CandidateValidationResult(
            sheet_name=sheet_name,
            ok=ok,
            total_score=score,
            signal_families=signal_families,
            evidence=evidence,
            has_statement_signal=candidate.has_statement_signal,
            has_account_signal=candidate.has_account_signal,
            has_formula_signal=has_formula_signal,
            is_derivative=is_derivative,
            distinct_ref_sheets=distinct_ref_sheets,
            total_formula_refs=total_formula_refs,
        )

    # ------------------------------------------------------------------
    # Candidate selection
    # ------------------------------------------------------------------

    def _candidate_sort_key(
        self, result: CandidateValidationResult
    ) -> Tuple[int, int, int, int, str]:
        return (
            0 if result.is_derivative else -1,
            -result.total_score,
            -result.signal_families,
            -len(result.distinct_ref_sheets),
            result.sheet_name.lower(),
        )

    def _select_candidates(
        self,
        *,
        preferred_results: List[CandidateValidationResult],
        fallback_results: List[CandidateValidationResult],
        workbook_structure: Optional[WorkbookStructure],
    ) -> List[CandidateValidationResult]:
        merged: Dict[str, CandidateValidationResult] = {}

        for result in preferred_results + fallback_results:
            prev = merged.get(result.sheet_name)
            if prev is None or result.total_score > prev.total_score:
                merged[result.sheet_name] = result

        if not merged:
            return []

        all_results = sorted(merged.values(), key=self._candidate_sort_key)

        # Non-derivative sheets always preferred as primary
        primary_pool = [r for r in all_results if not r.is_derivative] or all_results
        if not primary_pool:
            return []

        primary = primary_pool[0]
        selected = [primary]

        if self.config.max_tasks <= 1:
            return selected

        # Use stripped names for membership test — exact names may have trailing spaces
        preferred_names = {n.strip() for n in self._extract_preferred_sheet_names(workbook_structure)}

        for cand in primary_pool[1:]:
            if len(selected) >= self.config.max_tasks:
                break

            if cand.sheet_name == primary.sheet_name:
                continue

            score_threshold = max(
                self.config.min_fallback_score,
                primary.total_score - self.config.secondary_score_gap,
            )
            if cand.total_score < score_threshold:
                continue

            if (
                self._is_complementary_sheet(primary.sheet_name, cand.sheet_name)
                or cand.sheet_name.strip() in preferred_names
            ):
                selected.append(cand)

        return selected

    def _is_complementary_sheet(self, a: str, b: str) -> bool:
        def family(name: str) -> str:
            lower = (name or "").strip().lower()
            if any(t in lower for t in ("balance", "bs")):
                return "BS"
            if any(t in lower for t in ("p&l", "pnl", "pl", "income", "profit", "loss")):
                return "PL"
            return "MIXED"

        return {family(a), family(b)} == {"BS", "PL"}

    # ------------------------------------------------------------------
    # Task building
    # ------------------------------------------------------------------

    def _build_tasks_and_provenance(
        self,
        *,
        final_results: List[CandidateValidationResult],
        workbook_structure: Optional[WorkbookStructure],
    ) -> Tuple[List[SheetTask], List[Dict[str, Any]]]:
        tasks: List[SheetTask] = []
        provenance: List[Dict[str, Any]] = []

        for idx, result in enumerate(final_results):
            task = SheetTask(
                sheet_name=result.sheet_name,
                is_main_sheet=(idx == 0),
                parent_sheet_name=None,
            )
            tasks.append(task)
            provenance.append(
                self._make_task_provenance(
                    sheet_name=result.sheet_name,
                    matched_by="grid_first_then_formula_probe",
                    score=result.total_score,
                    evidence=result.evidence,
                )
            )

        return tasks, provenance

    # ------------------------------------------------------------------
    # Safe tool reads
    # ------------------------------------------------------------------

    def _safe_read_grid(
        self,
        *,
        sheet_name: str,
        tools: Any,
        nrows: int,
        ncols: int,
    ) -> List[List[Any]]:
        try:
            return tools.excel_read_sheet_range(
                sheet_name=sheet_name,
                row0=0,
                col0=0,
                nrows=nrows,
                ncols=ncols,
            )
        except Exception:
            logger.exception("SchemaDetector:grid_read_failed sheet=%s", sheet_name)
            return []

    def _safe_read_formulas(
        self,
        *,
        sheet_name: str,
        tools: Any,
        nrows: int,
        ncols: int,
    ) -> List[List[Optional[str]]]:
        try:
            return tools.excel_get_formulas(
                sheet_name=sheet_name,
                row0=0,
                col0=0,
                nrows=nrows,
                ncols=ncols,
            )
        except Exception:
            logger.exception("SchemaDetector:formula_read_failed sheet=%s", sheet_name)
            return []

    # ------------------------------------------------------------------
    # Text helpers
    # ------------------------------------------------------------------

    def _grid_text_blob(self, grid: List[List[Any]]) -> str:
        parts: List[str] = []
        for row in grid:
            if not isinstance(row, list):
                continue
            for cell in row:
                if cell is None:
                    continue
                txt = normalize_text(str(cell))
                if txt:
                    parts.append(txt)
        return " | ".join(parts)

    def _count_terms(self, haystack: str, needles: List[str]) -> int:
        h = normalize_text(haystack)
        count = 0
        for needle in needles:
            n = normalize_text(needle)
            if n and n in h:
                count += 1
        return count

    def _extract_formula_ref_sheets(
        self, formulas: List[List[Optional[str]]]
    ) -> List[str]:
        refs: List[str] = []

        for row in formulas:
            if not isinstance(row, list):
                continue
            for cell in row:
                if not cell:
                    continue
                s = str(cell)
                for match in self._SHEET_REF_RE.finditer(s):
                    name = (match.group(1) or match.group(2) or "").strip()
                    if name:
                        refs.append(name)

        return refs

    # ------------------------------------------------------------------
    # Sheet name scoring
    # ------------------------------------------------------------------

    def _presentation_sheet_name_bonus(self, sheet_name: str) -> int:
        lower = (sheet_name or "").strip().lower()
        score = 0
        if any(t in lower for t in ("fs", "financial", "statement", "statements",
                                     "balance", "p&l", "pnl", "income")):
            score += 1
        if "consolidated" in lower:
            score += 1
        return score

    def _source_sheet_name_penalty(self, sheet_name: str) -> int:
        lower = (sheet_name or "").strip().lower()
        score = 0
        for term in ("sap", "gl", "ledger", "trial", "tb", "dump",
                     "raw", "data", "mapping", "control", "tmp"):
            if term in lower:
                score += 2
        return min(score, 4)

    def _derivative_sheet_name_penalty(self, sheet_name: str) -> int:
        lower = (sheet_name or "").strip().lower()
        score = 0
        for term in ("slides", "slide", "report", "reports", "summary",
                     "dashboard", "bod", "actual vs budget", "presentation"):
            if term in lower:
                score += 2
        return min(score, 4)

    # ------------------------------------------------------------------
    # Provenance
    # ------------------------------------------------------------------

    def _make_task_provenance(
        self,
        *,
        sheet_name: str,
        matched_by: str,
        score: int,
        evidence: List[str],
    ) -> Dict[str, Any]:
        return {
            "sheet_name": sheet_name,
            "provenance": {
                "matched_by": matched_by,
                "score": score,
                "evidence": list(evidence),
                "source_code": {
                    "file": "Multi_agen/packages/agents/schema_detector.py",
                },
            },
        }

    def _make_workbook_structure_provenance(
        self,
        *,
        workbook_structure: Optional[WorkbookStructure],
        selected_tasks: List[SheetTask],
    ) -> Dict[str, Any]:
        return {
            "workbook_structure_main_sheet_names": list(
                getattr(workbook_structure, "main_sheet_names", []) or []
            ) if workbook_structure is not None else [],
            "selected_sheet_tasks": [t.sheet_name for t in selected_tasks],
            "source_code": {
                "file": "Multi_agen/packages/agents/schema_detector.py",
            },
        }