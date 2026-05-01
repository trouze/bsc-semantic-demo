"""OrderLookupSkill — deterministic + LLM-reranked order retrieval.

Ported from api/services/semantic_service.py (_handle_order_lookup) and
api/services/cortex_service.py (_RERANK_PROMPT_TEMPLATE).
"""
from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Optional, TYPE_CHECKING

from pydantic import Field

if TYPE_CHECKING:
    from agent.types import ContextPack, SkillResult
    from snowflake.snowpark import Session

try:
    from agent.skills.base import SlotSpec
except ImportError:
    from pydantic import BaseModel

    class SlotSpec(BaseModel):  # type: ignore[no-redef]
        model_config = {"extra": "ignore"}


_FETCH_ORDERS_SQL = """
    SELECT
        order_id, purchase_order_id, status, status_last_updated_ts,
        customer_name, facility_name, promised_delivery_date,
        carrier, tracking_number, actual_ship_ts, actual_delivery_date,
        priority_flag, requested_ship_date, total_amount_usd, currency,
        sales_region
    FROM DEMO_BSC.ORDER_SEARCH_V
    WHERE order_id IN ({placeholders})
"""

_RERANK_PROMPT = """\
You are an order matching assistant for a medical device fulfillment company.
A customer support rep is on a live call and needs the most relevant orders.

User query: {query}
{semantic_context}
Candidate orders (JSON):
{candidates_json}

Instructions:
1. Rank the candidates by relevance to the user query (most relevant first).
2. You MUST only use order_ids from the candidates list above.
3. Return at most {top_n} order_ids.
4. For each chosen order, give a SHORT reason (1 sentence) why it matches.

Return ONLY a valid JSON object in this exact format:
{{
  "ranked_ids": ["order_id_1", "order_id_2"],
  "rationale": {{
    "order_id_1": "Matches facility name and date window",
    "order_id_2": "Status recently updated, facility name partial match"
  }}
}}"""


class OrderLookupSlots(SlotSpec):  # noqa: F821 — SlotSpec from agent.skills.base
    order_id: Optional[str] = None
    purchase_order_id: Optional[str] = None
    customer_name: Optional[str] = None
    facility_name: Optional[str] = None
    status: Optional[str] = None
    date_start: Optional[str] = None
    date_end: Optional[str] = None
    free_text_residual: Optional[str] = None
    top_n: int = Field(default=5, ge=1, le=20)


class OrderLookupSkill:
    """Look up Boston Scientific orders using fuzzy matching + Cortex reranking."""

    name = "order_lookup"
    version = "v1"
    description = (
        "Look up Boston Scientific orders by order ID, PO number, customer name, "
        "facility name, status, or free-text description. Returns matching orders."
    )
    slot_schema = OrderLookupSlots

    def __init__(self, session: "Session") -> None:
        from agent.skills._fuzzy import FuzzyService
        from agent.semantic.executor import SnowflakeExecutor
        from agent.cortex.client import CortexClient

        self._fuzzy = FuzzyService()
        self._executor = SnowflakeExecutor(session)
        self._cortex = CortexClient(session)

    def validate(self, slots: OrderLookupSlots, ctx: "ContextPack") -> list[str]:
        from agent.guardrails.slot_validators import validate_status_value, validate_date_range

        errors: list[str] = []
        errors.extend(validate_status_value(slots.status, ctx.status_values))
        errors.extend(validate_date_range(slots.date_start, slots.date_end))
        return errors

    def execute(self, slots: OrderLookupSlots, ctx: "ContextPack") -> "SkillResult":
        from agent.types import SkillResult

        candidate_query = self._fuzzy.build_candidate_query(slots)
        raw_candidates: List[Dict[str, Any]] = self._executor.run(
            candidate_query.sql, candidate_query.params
        )
        candidates = self._fuzzy.score_candidates(raw_candidates)

        query_str = slots.free_text_residual or _slots_summary(slots)
        is_exact = candidate_query.is_exact or _has_exact_id_match(
            candidates, slots.order_id, slots.purchase_order_id
        )

        top_ids: List[str] = []
        rerank_rationale: Dict[str, str] = {}

        if is_exact or len(candidates) == 0:
            top_ids = [c.order_id for c in candidates[: slots.top_n]]
            rerank_rationale = {oid: "Exact ID match" for oid in top_ids}
        else:
            rerank_pool = candidates[:20]
            pool_ids = [c.order_id for c in rerank_pool]
            candidates_json = _serialize_candidates(rerank_pool)
            semantic_context = getattr(ctx, "semantic_context_str", "") or ""
            ctx_block = f"\n{semantic_context}\n" if semantic_context else ""

            prompt = _RERANK_PROMPT.format(
                query=query_str,
                candidates_json=candidates_json,
                top_n=slots.top_n,
                semantic_context=ctx_block,
            )

            raw_response = self._cortex.complete(prompt)
            top_ids, rerank_rationale = _parse_rerank_response(
                raw_response,
                valid_ids=set(pool_ids),
                top_n=slots.top_n,
                fallback_ids=pool_ids[: slots.top_n],
            )

        ordered_rows: List[Dict[str, Any]] = []
        if top_ids:
            # Parameterized IN clause — avoids SQL injection from order ID values
            placeholders = ", ".join(f"%(id_{i})s" for i in range(len(top_ids)))
            fetch_sql = _FETCH_ORDERS_SQL.format(placeholders=placeholders)
            fetch_params = {f"id_{i}": oid for i, oid in enumerate(top_ids)}
            fetch_rows = self._executor.run(fetch_sql, fetch_params)

            rows_by_id = {row.get("ORDER_ID", row.get("order_id")): row for row in fetch_rows}
            score_map = {c.order_id: c.score for c in candidates}
            for oid in top_ids:
                row = rows_by_id.get(oid)
                if row is not None:
                    row = dict(row)
                    row["_match_score"] = round(score_map.get(oid, 0.0), 2)
                    row["_rerank_rationale"] = rerank_rationale.get(oid, "")
                    ordered_rows.append(row)

        return SkillResult(
            skill_name=self.name,
            skill_version=self.version,
            data={
                "rows": ordered_rows,
                "rerank_rationale": rerank_rationale,
                "candidate_count": len(candidates),
            },
            status="ok" if ordered_rows else "empty",
        )

    def present(self, result: "SkillResult", ctx: "ContextPack") -> None:
        try:
            import pandas as pd
            import streamlit as st
        except ImportError:
            return  # non-Streamlit context (tests, stored procs)

        rows = result.data.get("rows", [])
        if not rows:
            st.info("No matching orders found.")
            return

        # Strip internal metadata columns before displaying
        display_rows = [
            {k: v for k, v in row.items() if not k.startswith("_")}
            for row in rows
        ]
        df = pd.DataFrame(display_rows)
        st.dataframe(df, use_container_width=True)

        rationale = result.data.get("rerank_rationale", {})
        if rationale:
            with st.expander("Why these results? (rerank rationale)"):
                for oid, reason in rationale.items():
                    st.write(f"**{oid}**: {reason}")


def _slots_summary(slots: OrderLookupSlots) -> str:
    """Build a short human-readable query string from slot values."""
    parts = []
    if slots.order_id:
        parts.append(f"order {slots.order_id}")
    if slots.purchase_order_id:
        parts.append(f"PO {slots.purchase_order_id}")
    if slots.customer_name:
        parts.append(f"customer {slots.customer_name}")
    if slots.facility_name:
        parts.append(f"facility {slots.facility_name}")
    if slots.status:
        parts.append(f"status {slots.status}")
    if slots.date_start:
        parts.append(f"from {slots.date_start}")
    if slots.date_end:
        parts.append(f"to {slots.date_end}")
    return " ".join(parts) or "order lookup"


def _has_exact_id_match(
    candidates: Any,
    order_id: Optional[str],
    purchase_order_id: Optional[str],
) -> bool:
    """Return True if any candidate is an exact match on the provided IDs."""
    if not order_id and not purchase_order_id:
        return False
    for c in candidates:
        if order_id and getattr(c, "order_id", None) == order_id:
            return True
        if purchase_order_id and getattr(c, "purchase_order_id", None) == purchase_order_id:
            return True
    return False


def _serialize_candidates(candidates: Any) -> str:
    return json.dumps(
        [
            {
                "order_id": getattr(c, "order_id", None),
                "purchase_order_id": getattr(c, "purchase_order_id", None),
                "status": getattr(c, "status", None),
                "status_last_updated_ts": str(getattr(c, "status_last_updated_ts", "")),
                "customer_name": getattr(c, "customer_name", None),
                "facility_name": getattr(c, "facility_name", None),
                "promised_delivery_date": str(getattr(c, "promised_delivery_date", "") or ""),
                "tracking_number": getattr(c, "tracking_number", None),
                "candidate_score": round(getattr(c, "score", 0.0), 2),
            }
            for c in candidates
        ],
        indent=2,
    )


def _parse_rerank_response(
    raw: str,
    valid_ids: set,
    top_n: int,
    fallback_ids: List[str],
) -> tuple[List[str], Dict[str, str]]:
    """Parse Cortex rerank JSON; fall back to original candidate order on any error."""
    try:
        text = re.sub(r"```(?:json)?\s*", "", raw, flags=re.IGNORECASE)
        text = text.replace("```", "").strip()
        found = re.search(r"\{.*\}", text, re.DOTALL)
        data = json.loads(found.group(0) if found else text)

        ranked_ids = [oid for oid in data.get("ranked_ids", []) if oid in valid_ids][:top_n]
        rationale = {
            oid: str(reason)
            for oid, reason in data.get("rationale", {}).items()
            if oid in valid_ids
        }
        return ranked_ids, rationale
    except Exception:
        return fallback_ids[:top_n], {}
