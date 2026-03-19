"""Deterministic candidate retrieval — no LLM involvement here.

Strategy:
  1. Exact match on order_id / purchase_order_id → return immediately.
  2. Else: token-LIKE matching on normalized name fields + date window filter.
     Score is computed in SQL; ORDER BY score DESC LIMIT :max_candidates.
"""

import re
import unicodedata
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from api.core.config import settings
from api.schemas.domain import CandidateSummary
from api.schemas.search import SearchFields, SearchRequest


@dataclass
class NormalizedQuery:
    order_id: Optional[str]
    purchase_order_id: Optional[str]
    customer_tokens: List[str]
    facility_tokens: List[str]
    contact_tokens: List[str]
    date_start: Optional[str]    # ISO string or None
    date_end: Optional[str]
    status: Optional[str]
    free_text_tokens: List[str]
    raw_free_text: Optional[str]


@dataclass
class CandidateQueryPlan:
    sql: str
    params: Dict[str, Any]
    is_exact: bool               # True → skip rerank, return directly
    score_expressions: List[str] = field(default_factory=list)


def _normalize(text: str) -> str:
    """Lower, strip accents, collapse whitespace, remove punctuation."""
    text = unicodedata.normalize("NFKD", text)
    text = text.encode("ascii", "ignore").decode("ascii")
    text = re.sub(r"[^\w\s]", " ", text.lower())
    return re.sub(r"\s+", " ", text).strip()


def _tokenize(text: Optional[str]) -> List[str]:
    if not text:
        return []
    return [t for t in _normalize(text).split() if len(t) >= 2]


# Common abbreviation expansions for facility matching
_EXPANSIONS = {
    "st": ["st", "saint"],
    "saint": ["saint", "st"],
    "hosp": ["hosp", "hospital"],
    "med": ["med", "medical", "medicine"],
    "ctr": ["ctr", "center", "centre"],
    "univ": ["univ", "university"],
    "gen": ["gen", "general"],
}

# Stop words — common English words that won't appear in searchable data fields
_STOP_WORDS = frozenset({
    "find", "show", "get", "give", "list", "display", "fetch", "pull",
    "me", "my", "the", "for", "and", "with", "from", "about", "what",
    "where", "when", "how", "any", "all", "need", "want", "can", "please",
    "orders", "order", "recent", "latest", "last",
    "look", "looking", "search", "check", "tell", "update", "info",
})


def _expand_tokens(tokens: List[str]) -> List[str]:
    expanded = []
    for t in tokens:
        expanded.append(t)
        for variant in _EXPANSIONS.get(t, []):
            if variant != t:
                expanded.append(variant)
    return list(dict.fromkeys(expanded))  # deduplicate while preserving order


class FuzzyService:

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def normalize_inputs(self, request: SearchRequest) -> NormalizedQuery:
        f: SearchFields = request.fields
        free_text_tokens = _tokenize(request.free_text)
        return NormalizedQuery(
            order_id=f.order_id.strip() if f.order_id else None,
            purchase_order_id=f.purchase_order_id.strip() if f.purchase_order_id else None,
            customer_tokens=_expand_tokens(_tokenize(f.customer_name)),
            facility_tokens=_expand_tokens(_tokenize(f.facility_name)),
            contact_tokens=_tokenize(f.contact_name),
            status=f.status.strip().lower() if f.status else None,
            date_start=str(f.date_start) if f.date_start else None,
            date_end=str(f.date_end) if f.date_end else None,
            free_text_tokens=free_text_tokens,
            raw_free_text=request.free_text,
        )

    def build_candidate_query(self, normalized: NormalizedQuery) -> CandidateQueryPlan:
        # ── Exact match path ───────────────────────────────────────────
        if normalized.order_id and re.match(r"^[A-Za-z0-9\-]+$", normalized.order_id):
            sql = """
                SELECT
                    order_id, purchase_order_id, status, status_last_updated_ts,
                    customer_name, facility_name, promised_delivery_date,
                    tracking_number, carrier, actual_ship_ts, actual_delivery_date,
                    priority_flag, requested_ship_date, total_amount_usd, currency,
                    sales_region,
                    100.0 AS candidate_score
                FROM DEMO_BSC.ORDER_SEARCH_V
                WHERE order_id = %(order_id)s
                LIMIT 1
            """
            return CandidateQueryPlan(
                sql=sql,
                params={"order_id": normalized.order_id},
                is_exact=True,
            )

        if normalized.purchase_order_id:
            sql = """
                SELECT
                    order_id, purchase_order_id, status, status_last_updated_ts,
                    customer_name, facility_name, promised_delivery_date,
                    tracking_number, carrier, actual_ship_ts, actual_delivery_date,
                    priority_flag, requested_ship_date, total_amount_usd, currency,
                    sales_region,
                    90.0 AS candidate_score
                FROM DEMO_BSC.ORDER_SEARCH_V
                WHERE purchase_order_id = %(purchase_order_id)s
                LIMIT %(max_candidates)s
            """
            return CandidateQueryPlan(
                sql=sql,
                params={
                    "purchase_order_id": normalized.purchase_order_id,
                    "max_candidates": settings.max_candidates,
                },
                is_exact=True,
            )

        # ── Fuzzy match path ───────────────────────────────────────────
        where_clauses: List[str] = ["1=1"]
        params: Dict[str, Any] = {"max_candidates": settings.max_candidates}
        score_parts: List[str] = ["0"]

        # Date window filter
        if normalized.date_start:
            where_clauses.append("order_created_ts >= %(date_start)s::DATE")
            params["date_start"] = normalized.date_start
        if normalized.date_end:
            where_clauses.append("order_created_ts <= %(date_end)s::DATE")
            params["date_end"] = normalized.date_end

        # Status filter — exact match on normalized status value
        if normalized.status:
            where_clauses.append("LOWER(status) = %(status_filter)s")
            params["status_filter"] = normalized.status
            score_parts.append(
                "CASE WHEN LOWER(status) = %(status_filter)s THEN 25 ELSE 0 END"
            )

        # Facility token scoring — build one LIKE per token; also filter to rows
        # that match at least one facility OR customer token (if tokens provided).
        fac_like_parts: List[str] = []
        for i, tok in enumerate(normalized.facility_tokens[:5]):  # cap at 5 tokens
            param_name = f"fac_tok_{i}"
            params[param_name] = f"%{tok}%"
            fac_like_parts.append(f"facility_name_norm LIKE %({param_name})s")
            score_parts.append(
                f"CASE WHEN facility_name_norm LIKE %({param_name})s THEN 20 ELSE 0 END"
            )

        cust_like_parts: List[str] = []
        for i, tok in enumerate(normalized.customer_tokens[:5]):
            param_name = f"cust_tok_{i}"
            params[param_name] = f"%{tok}%"
            cust_like_parts.append(f"customer_name_norm LIKE %({param_name})s")
            score_parts.append(
                f"CASE WHEN customer_name_norm LIKE %({param_name})s THEN 15 ELSE 0 END"
            )

        # At least one token must match (facility OR customer OR free text blob)
        match_conditions: List[str] = []
        if fac_like_parts:
            match_conditions.append(f"({' OR '.join(fac_like_parts)})")
        if cust_like_parts:
            match_conditions.append(f"({' OR '.join(cust_like_parts)})")

        # Free-text blob fallback tokens — filter stop words and use ALL
        # remaining tokens as OR'd match conditions (not just the first).
        blob_like_parts: List[str] = []
        for i, tok in enumerate(normalized.free_text_tokens[:8]):
            param_name = f"blob_tok_{i}"
            params[param_name] = f"%{tok}%"
            score_parts.append(
                f"CASE WHEN search_blob LIKE %({param_name})s THEN 5 ELSE 0 END"
            )
            # Only use data-bearing tokens (not stop words) in the WHERE clause
            if tok not in _STOP_WORDS:
                blob_like_parts.append(f"search_blob LIKE %({param_name})s")

        if not match_conditions and blob_like_parts:
            match_conditions.append(f"({' OR '.join(blob_like_parts)})")

        if match_conditions:
            where_clauses.append(f"({' OR '.join(match_conditions)})")

        # Recency boost — orders updated in last 30 days
        score_parts.append(
            "CASE WHEN DATEDIFF('day', status_last_updated_ts, CURRENT_TIMESTAMP()) <= 30 "
            "THEN 5 ELSE 0 END"
        )

        # Priority boost
        score_parts.append("CASE WHEN priority_flag = TRUE THEN 3 ELSE 0 END")

        score_expr = " + ".join(score_parts)
        where_expr = " AND ".join(where_clauses)

        sql = f"""
            SELECT
                order_id, purchase_order_id, status, status_last_updated_ts,
                customer_name, facility_name, promised_delivery_date,
                tracking_number, carrier, actual_ship_ts, actual_delivery_date,
                priority_flag, requested_ship_date, total_amount_usd, currency,
                sales_region,
                ({score_expr}) AS candidate_score
            FROM DEMO_BSC.ORDER_SEARCH_V
            WHERE {where_expr}
            ORDER BY candidate_score DESC
            LIMIT %(max_candidates)s
        """

        return CandidateQueryPlan(sql=sql, params=params, is_exact=False)

    def score_candidates(self, rows: List[Dict]) -> List[CandidateSummary]:
        candidates = []
        for row in rows:
            candidates.append(
                CandidateSummary(
                    order_id=row["ORDER_ID"],
                    purchase_order_id=row.get("PURCHASE_ORDER_ID"),
                    status=row["STATUS"],
                    status_last_updated_ts=row["STATUS_LAST_UPDATED_TS"],
                    customer_name=row["CUSTOMER_NAME"],
                    facility_name=row["FACILITY_NAME"],
                    promised_delivery_date=row.get("PROMISED_DELIVERY_DATE"),
                    tracking_number=row.get("TRACKING_NUMBER"),
                    score=float(row.get("CANDIDATE_SCORE", 0)),
                )
            )
        return sorted(candidates, key=lambda c: c.score, reverse=True)
