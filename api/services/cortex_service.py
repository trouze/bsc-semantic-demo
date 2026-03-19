"""Cortex integration — LLM usage is strictly bounded to:
  1. parse_user_input  → extract structured fields from free text
  2. rerank_candidates → sort candidate list by relevance

Guardrails:
  - Cortex cannot invent order IDs; reranker may only pick from provided candidates.
  - All unknown IDs returned by the model are silently filtered out.
  - Results are cached by a normalized cache key (TTL configurable).
"""

import hashlib
import json
import re
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from cachetools import TTLCache

from api.core.config import settings
from api.core.errors import CortexError
from api.core.log import get_logger
from api.schemas.domain import CandidateSummary
from api.schemas.search import SearchFields
from api.services.snowflake_service import SnowflakeService

logger = get_logger(__name__)


@dataclass
class ParsedIntent:
    intent: str = "order_lookup"  # "order_lookup" or "metric_query"
    order_id: Optional[str] = None
    purchase_order_id: Optional[str] = None
    customer_name: Optional[str] = None
    facility_name: Optional[str] = None
    date_start: Optional[str] = None
    date_end: Optional[str] = None
    contact_name: Optional[str] = None
    metric_question: Optional[str] = None
    metric_params: Optional[Dict[str, Any]] = None
    raw_response: str = ""


@dataclass
class RerankResult:
    ranked_ids: List[str]
    rationale: Dict[str, str]   # order_id -> reason
    prompt_used: str
    elapsed_ms: float = 0.0


_PARSE_PROMPT_TEMPLATE = """\
You are an order lookup assistant for a medical device company.
Classify the user's query and extract structured fields. Return ONLY a valid JSON object.
Do not explain. Do not add markdown. Return null for unknown fields.

First, determine the intent:
  - "order_lookup": the user is looking for a SPECIFIC order, shipment, tracking info,
    or asking about a particular customer/facility's order. Keywords: order, tracking,
    shipment, status, delivery, PO, purchase order.
  - "metric_query": the user is asking an AGGREGATE or ANALYTICAL question about
    orders — counts, averages, trends, comparisons, totals, breakdowns by status/region/time.
    Keywords: how many, total, average, count, trend, breakdown, by status, by region.
{semantic_context}
Fields to extract:
  intent: "order_lookup" or "metric_query"
  order_id: string or null (e.g. "SO-2026-001234" or partial like "01234")
  purchase_order_id: string or null
  customer_name: string or null
  facility_name: string or null (hospital / clinic / facility name)
  date_start: ISO date string or null (start of date window)
  date_end: ISO date string or null (end of date window)
  contact_name: string or null
  metric_question: string or null (for metric_query intent, rephrase the question clearly)

Today's date: {today}

User query: {query}

Return JSON only."""


_RERANK_PROMPT_TEMPLATE = """\
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


_COMBINED_PARSE_TEMPLATE = """\
Classify and return JSON only.
Intent: "order_lookup"=specific order. "metric_query"=aggregate/count/trend.
Metrics: {metric_names}
Dims: {dim_names}
Rules: order_count=all orders, fulfilled_order_count=shipped/delivered only.
group_by: type="dimension" grain=null, or type="time_dimension" grain="MONTH"/"YEAR".
Today:{today} Query:{query}
JSON:{{"intent":"...","order_id":null,"customer_name":null,"facility_name":null,"date_start":null,"date_end":null,"contact_name":null,"purchase_order_id":null,"metric_question":null,"metric_params":{{"metrics":[],"group_by":[],"order_by":null,"where":null,"limit":null}}}}"""


_METRIC_QUERY_BUILDER_TEMPLATE = """\
You are a data analyst assistant. Given a user question and the available metrics
and dimensions from a semantic layer, produce the EXACT parameters needed to
query the metrics API.

Available metrics:
{metrics_json}

Available dimensions:
{dimensions_json}

Rules:
1. Pick the most relevant metric(s) from the list above.
2. Pick group_by dimensions that answer the question. Each group_by item needs:
   - "name": dimension name from the list
   - "type": "dimension" for CATEGORICAL, "time_dimension" for TIME
   - "grain": null for CATEGORICAL, or "DAY"/"WEEK"/"MONTH"/"QUARTER"/"YEAR" for TIME
3. Add order_by if the question implies sorting (most, top, recent, etc.).
   Each item needs "name" and "descending" (boolean).
4. Add limit if the question asks for top N.
5. Add where clause only if the question specifies a filter.
   Use Dimension/TimeDimension syntax: {{{{ Dimension('name') }}}} or {{{{ TimeDimension('name', 'GRAIN') }}}}.

Return ONLY a valid JSON object:
{{
  "metrics": ["metric_name"],
  "group_by": [{{"name": "dim", "type": "dimension", "grain": null}}],
  "order_by": [{{"name": "metric_name", "descending": true}}],
  "where": null,
  "limit": null
}}

User question: {question}

Return JSON only."""


def format_semantic_context(ctx: Dict[str, Any]) -> str:
    """Format a semantic model context dict into a prompt-friendly string."""
    lines: List[str] = []

    if ctx.get("status_values"):
        lines.append("Valid order status values: " + ", ".join(ctx["status_values"]))

    if ctx.get("business_terms"):
        lines.append("Business term definitions:")
        for term, defn in ctx["business_terms"].items():
            lines.append(f"  - {term}: {defn}")

    if ctx.get("entity_relationships"):
        lines.append("Entity relationships:")
        for rel in ctx["entity_relationships"]:
            lines.append(f"  - {rel}")

    if ctx.get("dimensions"):
        named = [d for d in ctx["dimensions"] if d.get("description")]
        if named:
            lines.append("Dimension descriptions:")
            for d in named:
                lines.append(f"  - {d['name']}: {d['description']}")

    if ctx.get("metrics"):
        lines.append("Available metrics:")
        for m in ctx["metrics"]:
            desc = m.get("description", "")
            lines.append(f"  - {m['name']}: {desc}" if desc else f"  - {m['name']}")

    return "\n".join(lines)


def _cache_key(*parts: Any) -> str:
    raw = json.dumps(parts, default=str, sort_keys=True)
    return hashlib.sha256(raw.encode()).hexdigest()


class CortexService:

    def __init__(self, snowflake: SnowflakeService):
        self._sf = snowflake
        self._rerank_cache: TTLCache = TTLCache(
            maxsize=512, ttl=settings.rerank_cache_ttl_s
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def parse_user_input(
        self,
        text: str,
        available_metrics: Optional[List[Dict[str, Any]]] = None,
        available_dimensions: Optional[List[Dict[str, Any]]] = None,
        semantic_context: Optional[str] = None,
    ) -> ParsedIntent:
        """Call Cortex to classify intent and extract fields.

        When metrics/dimensions are provided, the LLM also builds query_metrics
        params for metric_query intents in the same call (saving a round trip).

        When semantic_context is provided, it is injected into the parse prompt
        to give the LLM knowledge of the data model (valid statuses, business
        terms, entity relationships, dimension descriptions).
        """
        from datetime import date
        today = date.today().isoformat()

        ctx_block = f"\n{semantic_context}\n" if semantic_context else ""

        if available_metrics and available_dimensions:
            metric_names = ", ".join(m.get("name", "") for m in available_metrics)
            dim_names = ", ".join(d.get("name", "") for d in available_dimensions)
            prompt = _COMBINED_PARSE_TEMPLATE.format(
                today=today,
                query=text,
                metric_names=metric_names,
                dim_names=dim_names,
            )
        else:
            prompt = _PARSE_PROMPT_TEMPLATE.format(
                today=today, query=text, semantic_context=ctx_block,
            )

        raw = self._complete(prompt, label="parse_user_input")
        intent = self._safe_parse_intent(raw)
        intent.raw_response = raw
        return intent

    def rerank_candidates(
        self,
        query: str,
        candidates: List[CandidateSummary],
        top_n: int,
        semantic_context: Optional[str] = None,
    ) -> RerankResult:
        """Rerank candidates using Cortex. Results are cached by (query, candidate_ids).

        When semantic_context is provided, it is injected into the rerank prompt
        to help Cortex reason about field meanings and status values.
        """
        candidate_ids = [c.order_id for c in candidates]
        cache_key = _cache_key(query, candidate_ids, top_n)

        if cache_key in self._rerank_cache:
            logger.info("rerank_cache_hit", extra={"extra": {"key": cache_key[:12]}})
            return self._rerank_cache[cache_key]

        candidates_json = json.dumps(
            [
                {
                    "order_id": c.order_id,
                    "purchase_order_id": c.purchase_order_id,
                    "status": c.status,
                    "status_last_updated_ts": str(c.status_last_updated_ts),
                    "customer_name": c.customer_name,
                    "facility_name": c.facility_name,
                    "promised_delivery_date": str(c.promised_delivery_date) if c.promised_delivery_date else None,
                    "tracking_number": c.tracking_number,
                    "candidate_score": round(c.score, 2),
                }
                for c in candidates
            ],
            indent=2,
        )

        ctx_block = f"\n{semantic_context}\n" if semantic_context else ""

        prompt = _RERANK_PROMPT_TEMPLATE.format(
            query=query,
            candidates_json=candidates_json,
            top_n=top_n,
            semantic_context=ctx_block,
        )

        t0 = time.perf_counter()
        raw = self._complete(prompt, label="rerank_candidates")
        elapsed_ms = (time.perf_counter() - t0) * 1000

        result = self._safe_parse_rerank(raw, valid_ids=set(candidate_ids), top_n=top_n)
        result.prompt_used = prompt
        result.elapsed_ms = round(elapsed_ms, 1)

        self._rerank_cache[cache_key] = result
        return result

    def build_metric_query_params(
        self,
        question: str,
        available_metrics: List[Dict[str, Any]],
        available_dimensions: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Use Cortex to map a natural-language question to query_metrics params."""
        metrics_desc = json.dumps(
            [{"name": m.get("name"), "description": m.get("description", "")} for m in available_metrics],
            indent=2,
        )
        dims_desc = json.dumps(
            [{"name": d.get("name"), "type": d.get("type", "CATEGORICAL")} for d in available_dimensions],
            indent=2,
        )
        prompt = _METRIC_QUERY_BUILDER_TEMPLATE.format(
            question=question,
            metrics_json=metrics_desc,
            dimensions_json=dims_desc,
        )
        raw = self._complete(prompt, label="metric_query_builder")
        try:
            return json.loads(self._extract_json_block(raw))
        except json.JSONDecodeError:
            # LLM sometimes truncates closing braces — try to repair
            repaired = self._repair_json(raw)
            if repaired:
                return repaired
            logger.warning(f"metric_query_builder_failed: cannot parse | raw={raw[:300]}")
            return {}

    @staticmethod
    def _repair_json(raw: str) -> Optional[Dict[str, Any]]:
        """Attempt to repair truncated JSON from LLM output."""
        text = re.sub(r"```(?:json)?\s*", "", raw, flags=re.IGNORECASE)
        text = text.replace("```", "").strip()
        start = text.find("{")
        if start < 0:
            return None
        fragment = text[start:]
        suffixes = [
            "}",
            "]}",
            "null}",
            "null]}",
            "\"}",
            "\"]}"
        ]
        for suffix in suffixes:
            try:
                return json.loads(fragment + suffix)
            except json.JSONDecodeError:
                continue
        return None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _complete(self, prompt: str, *, label: str = "cortex") -> str:
        """Call SNOWFLAKE.CORTEX.COMPLETE and return the text response."""
        sql = """
            SELECT SNOWFLAKE.CORTEX.COMPLETE(%(model)s, %(prompt)s) AS response
        """
        try:
            result = self._sf.execute(
                sql,
                {"model": settings.cortex_model, "prompt": prompt},
                label=label,
            )
            if not result.rows:
                raise CortexError(f"Empty response from Cortex [{label}]")
            raw = result.rows[0].get("RESPONSE", "") or ""
            return self._extract_content(raw)
        except Exception as exc:
            raise CortexError(str(exc)) from exc

    @staticmethod
    def _extract_content(raw: str) -> str:
        """Pull text from Cortex COMPLETE JSON envelope if present."""
        try:
            parsed = json.loads(raw)
            # Standard Cortex envelope: {"choices": [{"messages": "..."}]}
            if isinstance(parsed, dict) and "choices" in parsed:
                return parsed["choices"][0].get("messages", raw)
        except (json.JSONDecodeError, KeyError, IndexError):
            pass
        return raw

    @staticmethod
    def _extract_json_block(text: str) -> str:
        """Strip markdown fences and return first JSON block found."""
        # Remove ```json ... ``` fences
        text = re.sub(r"```(?:json)?\s*", "", text, flags=re.IGNORECASE)
        text = text.replace("```", "").strip()
        # Find first { ... } block
        match = re.search(r"\{.*\}", text, re.DOTALL)
        return match.group(0) if match else text

    def _safe_parse_intent(self, raw: str) -> ParsedIntent:
        try:
            block = self._extract_json_block(raw)
            try:
                data = json.loads(block)
            except json.JSONDecodeError:
                repaired = self._repair_json(raw)
                if repaired:
                    data = repaired
                else:
                    raise
            def _clean(val):
                """Treat string 'null'/'None' as actual None."""
                if isinstance(val, str) and val.strip().lower() in ("null", "none", ""):
                    return None
                return val

            return ParsedIntent(
                intent=data.get("intent", "order_lookup"),
                order_id=_clean(data.get("order_id")),
                purchase_order_id=_clean(data.get("purchase_order_id")),
                customer_name=_clean(data.get("customer_name")),
                facility_name=_clean(data.get("facility_name")),
                date_start=_clean(data.get("date_start")),
                date_end=_clean(data.get("date_end")),
                contact_name=_clean(data.get("contact_name")),
                metric_question=_clean(data.get("metric_question")),
                metric_params=data.get("metric_params"),
            )
        except Exception as exc:
            logger.warning(f"parse_intent_failed: {exc} | raw={raw[:200]}")
            return ParsedIntent()

    def _safe_parse_rerank(
        self, raw: str, valid_ids: set, top_n: int
    ) -> RerankResult:
        try:
            data = json.loads(self._extract_json_block(raw))
            ranked_ids = [
                oid for oid in data.get("ranked_ids", []) if oid in valid_ids
            ][:top_n]
            rationale = {
                oid: str(reason)
                for oid, reason in data.get("rationale", {}).items()
                if oid in valid_ids
            }
            return RerankResult(ranked_ids=ranked_ids, rationale=rationale, prompt_used="")
        except Exception as exc:
            logger.warning(f"rerank_parse_failed: {exc} | raw={raw[:200]}")
            # Fallback: return ids in original order
            return RerankResult(
                ranked_ids=list(valid_ids)[:top_n],
                rationale={},
                prompt_used="",
            )
