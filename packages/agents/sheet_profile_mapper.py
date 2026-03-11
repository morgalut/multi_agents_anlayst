# Multi_agen\packages\agents\sheet_profile_mapper.py

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger("multi_agen.agents.sheet_profile_mapper")

# ---------------------------------------------------------------------------
# Lazy import to avoid circular dependencies at module load time
# ---------------------------------------------------------------------------
def _import_profile_types():
    from Multi_agen.packages.core.schemas import SheetProfileColumn, SheetProfileResult
    return SheetProfileColumn, SheetProfileResult


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class SheetProfileMapperConfig:
    """Tuning knobs for sheet-profile classification heuristics."""
    # Sheet-name fragments that identify GL sheets
    gl_name_fragments: Tuple[str, ...] = ("gl", "ledger", "general ledger")
    # Sheet-name fragments that identify AJE card sheets
    aje_card_name_fragments: Tuple[str, ...] = ("ae", "aje", "adjust", "adjusting")
    # Sheet-name fragments that identify generic card / TB sheets
    card_name_fragments: Tuple[str, ...] = ("tb", "trial balance", "card", "כרטיס")
    # Minimum debit+credit column count to infer is_aje_card_sheet from content
    aje_card_debit_credit_min: int = 2


# ---------------------------------------------------------------------------
# Main agent
# ---------------------------------------------------------------------------

class SheetProfileMapper:
    """
    Adapter: SheetAnalysis  →  SheetProfileResult

    Usage (called by ORCAgent after per-sheet analysis):

        mapper = SheetProfileMapper()
        profile = mapper.map_profile(task=task, analysis=analysis, is_main_sheet=True)
    """

    # Maps existing column roles to (column_type_template, is_tb, debit_flag, credit_flag)
    _ROLE_TYPE_MAP: Dict[str, Tuple[str, Optional[bool], Optional[bool], Optional[bool]]] = {
        "coa_name":         ("Account name",    False,  None,  None),
        "entity_value":     ("{entity} {curr}", True,   None,  None),
        "debit":            ("Debit {curr}",    False,  True,  False),
        "credit":           ("Credit {curr}",   False,  False, True),
        "aje":              ("AJE {entity}",    False,  None,  None),
        "consolidated_aje": ("AJE Consolidated",False,  None,  None),
        "consolidated":     ("Consolidated",    False,  None,  None),
        "budget":           ("Budget",          False,  None,  None),
        "prior_period":     ("Prior Period",    False,  None,  None),
        "other":            ("Details",         False,  None,  None),
    }

    # Currency symbols used in human-readable labels
    _CURRENCY_SYMBOL: Dict[str, str] = {
        "USD": "$",
        "NIS": "₪",
        "ILS": "₪",
        "EUR": "€",
        "GBP": "£",
    }

    def __init__(self, config: Optional[SheetProfileMapperConfig] = None) -> None:
        self.config = config or SheetProfileMapperConfig()

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def map_profile(
        self,
        *,
        task: Any,
        analysis: Any,
        is_main_sheet: bool = False,
    ) -> Any:  # returns SheetProfileResult
        SheetProfileColumn, SheetProfileResult = _import_profile_types()

        sheet_name: str = str(getattr(task, "sheet_name", "") or "")
        signals: Dict[str, Any] = getattr(analysis, "signals", None) or {}
        raw_columns: List[Dict[str, Any]] = (
            signals.get("columns") or signals.get("normalized_columns") or []
        )
        confidence: float = float(
            getattr(getattr(analysis, "classification", None), "confidence", 0.0) or 0.0
        )

        # ------------------------------------------------------------------
        # 1. Sheet-level flags
        # ------------------------------------------------------------------
        is_gl        = self._infer_is_gl(sheet_name, raw_columns)
        is_aje_card  = self._infer_is_aje_card(sheet_name, raw_columns)
        is_card      = is_aje_card or self._infer_is_card(sheet_name, raw_columns)

        # ------------------------------------------------------------------
        # 2. Per-column profiles
        # ------------------------------------------------------------------
        profile_columns: List[Any] = []  # List[SheetProfileColumn]
        for col in raw_columns:
            pc = self._map_column(col, sheet_name=sheet_name)
            if pc is not None:
                profile_columns.append(pc)

        # ------------------------------------------------------------------
        # 3. Narrative description
        # ------------------------------------------------------------------
        additional_info = self._build_narrative(
            sheet_name=sheet_name,
            is_gl=is_gl,
            is_aje_card=is_aje_card,
            is_card=is_card,
            columns=raw_columns,
            signals=signals,
        )

        return SheetProfileResult(
            sheet_name=sheet_name,
            is_main_sheet=is_main_sheet,
            is_card_sheet=is_card,
            is_aje_card_sheet=is_aje_card,
            is_gl_sheet=is_gl,
            confidence=round(min(max(confidence, 0.0), 1.0), 4),
            columns=profile_columns,
            additional_info_on_sheet=additional_info,
        )

    # ------------------------------------------------------------------
    # Sheet-level flag inference
    # ------------------------------------------------------------------

    def _infer_is_gl(self, sheet_name: str, columns: List[Dict[str, Any]]) -> bool:
        lower = sheet_name.lower()
        return any(frag in lower for frag in self.config.gl_name_fragments)

    def _infer_is_aje_card(
        self, sheet_name: str, columns: List[Dict[str, Any]]
    ) -> bool:
        lower = sheet_name.lower()
        name_hit = any(frag in lower for frag in self.config.aje_card_name_fragments)
        if name_hit:
            return True

        # Content heuristic: sheet has both debit and credit AJE columns
        has_debit  = any(c.get("role") in ("debit", "aje") and "debit" in str(c.get("header_text", "")).lower() for c in columns)
        has_credit = any(c.get("role") in ("credit", "aje") and "credit" in str(c.get("header_text", "")).lower() for c in columns)
        debit_credit_count = sum(
            1 for c in columns if c.get("role") in ("debit", "credit")
        )
        return has_debit and has_credit and debit_credit_count >= self.config.aje_card_debit_credit_min

    def _infer_is_card(
        self, sheet_name: str, columns: List[Dict[str, Any]]
    ) -> bool:
        lower = sheet_name.lower()
        return any(frag in lower for frag in self.config.card_name_fragments)

    # ------------------------------------------------------------------
    # Column mapping
    # ------------------------------------------------------------------

    def _map_column(
        self,
        col: Dict[str, Any],
        *,
        sheet_name: str,
    ) -> Optional[Any]:  # returns Optional[SheetProfileColumn]
        SheetProfileColumn, _ = _import_profile_types()

        role      = str(col.get("role", "") or "").strip()
        col_idx   = col.get("col_idx")
        entity    = str(col.get("entity", "") or "").strip()
        currency  = str(col.get("currency", "") or "").strip().upper()
        header    = str(col.get("header_text", "") or "").strip()

        if col_idx is None:
            return None

        col_letter = self._idx_to_letter(int(col_idx))

        # ── column_type ────────────────────────────────────────────────
        column_type = self._derive_column_type(
            role=role, entity=entity, currency=currency, header=header
        )

        # ── currency/company for profile ───────────────────────────────
        profile_currency = currency or None
        profile_company  = (entity if entity and entity.lower() != "consolidated" else None) or None

        # ── boolean annotations ────────────────────────────────────────
        is_tb, is_aje_debit, is_aje_credit, is_consolidated, is_final = (
            self._derive_booleans(role=role, entity=entity, header=header, currency=currency)
        )
        is_account_number, is_account_description = self._derive_account_flags(
            role=role, header=header
        )

        return SheetProfileColumn(
            column_letter=col_letter,
            column_type=column_type,
            company=profile_company,
            currency=profile_currency if profile_currency else None,
            is_tb=is_tb,
            is_aje_debit=is_aje_debit,
            is_aje_credit=is_aje_credit,
            is_consolidated=is_consolidated,
            is_final=is_final,
            is_account_number=is_account_number,
            is_account_description=is_account_description,
        )

    # ------------------------------------------------------------------
    # Column-type string derivation
    # ------------------------------------------------------------------

    def _derive_column_type(
        self,
        *,
        role: str,
        entity: str,
        currency: str,
        header: str,
    ) -> str:
        """
        Produce a human-readable column_type label matching the example format.

        Priority: explicit header keywords > role-based template.
        """
        header_lower = header.lower()
        curr_sym = self._currency_symbol(currency)

        # Explicit debit / credit keywords in header beat role
        if "debit" in header_lower:
            return f"Debit {curr_sym}".strip() if curr_sym else "Debit"
        if "credit" in header_lower:
            return f"Credit {curr_sym}".strip() if curr_sym else "Credit"

        # Account-number / AE number signals
        if re.search(r"\b(ae|aje)\s*#", header_lower):
            return "AE #"
        if re.search(r"\b(account|acc)\s*#", header_lower) or re.search(r"\b(account|acc)\s+number", header_lower):
            return "Account #"
        if "account name" in header_lower or "account description" in header_lower:
            return "Account name"
        if "details" in header_lower or "remarks" in header_lower or "description" in header_lower:
            return "Details"
        if "expense" in header_lower and ("amount" in header_lower or "total" in header_lower):
            return "Expense Amount"

        # Role-based template
        tpl_entry = self._ROLE_TYPE_MAP.get(role)
        if tpl_entry is None:
            return header or "Column"

        tpl = tpl_entry[0]
        label = tpl.replace("{entity}", entity or "").replace("{curr}", curr_sym or currency).strip()
        # Clean up redundant whitespace
        label = re.sub(r"\s{2,}", " ", label).strip()
        return label or (header or role or "Column")

    def _currency_symbol(self, currency: str) -> str:
        return self._CURRENCY_SYMBOL.get(currency.upper(), currency)

    # ------------------------------------------------------------------
    # Boolean annotation derivation
    # ------------------------------------------------------------------

    def _derive_booleans(
        self,
        *,
        role: str,
        entity: str,
        header: str,
        currency: str,
    ) -> Tuple[
        Optional[bool],   # is_tb
        Optional[bool],   # is_aje_debit
        Optional[bool],   # is_aje_credit
        Optional[bool],   # is_consolidated
        Optional[bool],   # is_final
    ]:
        h = header.lower()

        is_tb:           Optional[bool] = None
        is_aje_debit:    Optional[bool] = None
        is_aje_credit:   Optional[bool] = None
        is_consolidated: Optional[bool] = None
        is_final:        Optional[bool] = None

        if role == "coa_name":
            is_tb = False
            is_aje_debit  = None
            is_aje_credit = None
            is_consolidated = None
            is_final = False

        elif role == "entity_value":
            is_tb = True
            is_aje_debit  = False
            is_aje_credit = False
            is_consolidated = False
            is_final = False

        elif role == "debit":
            is_tb = False
            is_aje_debit  = True
            is_aje_credit = False
            is_consolidated = False
            is_final = False

        elif role == "credit":
            is_tb = False
            is_aje_debit  = False
            is_aje_credit = True
            is_consolidated = False
            is_final = False

        elif role == "aje":
            # Determine debit vs credit from header
            if "debit" in h:
                is_aje_debit  = True
                is_aje_credit = False
            elif "credit" in h:
                is_aje_debit  = False
                is_aje_credit = True
            else:
                is_aje_debit  = None
                is_aje_credit = None
            is_tb = False
            is_consolidated = False
            is_final = False

        elif role in ("consolidated", "consolidated_aje"):
            is_tb = False
            is_consolidated = True
            is_final = role == "consolidated"  # consolidated (non-AJE) is often the final column
            if role == "consolidated_aje":
                if "debit" in h:
                    is_aje_debit  = True
                    is_aje_credit = False
                elif "credit" in h:
                    is_aje_debit  = False
                    is_aje_credit = True

        elif role == "budget":
            is_tb = False
            is_final = False
            is_consolidated = False

        elif role == "prior_period":
            is_tb = True   # prior period TB columns are still TB
            is_final = False
            is_consolidated = False

        elif role == "other":
            is_tb = False
            is_aje_debit  = None
            is_aje_credit = None
            is_consolidated = False
            is_final = False

        return is_tb, is_aje_debit, is_aje_credit, is_consolidated, is_final

    def _derive_account_flags(
        self,
        *,
        role: str,
        header: str,
    ) -> Tuple[Optional[bool], Optional[bool]]:
        """Returns (is_account_number, is_account_description)."""
        h = header.lower()

        if role == "coa_name":
            # Account names: could be description or number depending on header
            if re.search(r"\bnumber\b|\b#\b|\bcode\b|\bnum\b", h):
                return True, False
            return False, True

        if re.search(r"\baccount\b|\bacc\b|\bcoa\b", h):
            if re.search(r"\bnumber\b|\b#\b|\bcode\b", h):
                return True, False
            return False, True

        if role == "other":
            # Details / description columns
            return False, True

        # Default: not an account column
        return False, False

    # ------------------------------------------------------------------
    # Narrative description
    # ------------------------------------------------------------------

    def _build_narrative(
        self,
        *,
        sheet_name: str,
        is_gl: bool,
        is_aje_card: bool,
        is_card: bool,
        columns: List[Dict[str, Any]],
        signals: Dict[str, Any],
    ) -> str:
        """
        Build a plain-English summary of what this sheet appears to contain.

        Mirrors the style of additional_info_on_sheet in exmplete.json.
        """
        parts: List[str] = []

        # Sheet type sentence
        if is_aje_card:
            parts.append(
                f"This sheet called [{sheet_name}] appears to contain Adjusting Entries (AJE) information,"
                " showing accounts and corresponding debit and credit amounts"
            )
        elif is_gl:
            parts.append(
                f"This sheet called [{sheet_name}] appears to be a General Ledger source sheet"
                " containing transaction-level detail"
            )
        elif is_card:
            parts.append(
                f"This sheet called [{sheet_name}] appears to be a structured card-format sheet"
                " containing account-level data"
            )
        else:
            parts.append(
                f"This sheet called [{sheet_name}] appears to contain financial statement data"
            )

        # Currency detail
        currencies: List[str] = sorted({
            str(c.get("currency", "") or "").upper()
            for c in columns
            if c.get("currency")
        })
        if currencies:
            curr_str = " and ".join(currencies)
            parts.append(f"with amounts in {curr_str} currencies")

        # Column range summary
        roles = [str(c.get("role", "")) for c in columns]
        has_coa    = "coa_name"     in roles
        has_entity = "entity_value" in roles
        has_aje    = any(r in roles for r in ("aje", "debit", "credit"))
        has_consol = any(r in roles for r in ("consolidated", "consolidated_aje"))
        has_other  = "other"        in roles

        notes: List[str] = []
        if has_coa:
            notes.append("account identifiers and descriptions")
        if has_entity:
            notes.append("entity value columns")
        if has_aje:
            notes.append("debit and credit adjustment amounts")
        if has_consol:
            notes.append("consolidated figures")
        if has_other:
            notes.append("qualitative descriptions and details")

        if notes:
            parts.append("— columns cover " + ", ".join(notes))

        return ". ".join(p.strip() for p in parts if p.strip()).strip() + "."

    # ------------------------------------------------------------------
    # Utility
    # ------------------------------------------------------------------

    @staticmethod
    def _idx_to_letter(col_idx: int) -> str:
        n = col_idx + 1
        letters: List[str] = []
        while n > 0:
            n, rem = divmod(n - 1, 26)
            letters.append(chr(ord("A") + rem))
        return "".join(reversed(letters))
