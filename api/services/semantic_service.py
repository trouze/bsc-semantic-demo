"""SemanticService — stable orchestration contract.

This is the single entry point for all search logic.
Future Agentforce / Tableau Next clients call the same methods.

Two pipelines depending on intent:

  ORDER LOOKUP (intent="order_lookup"):
    1. FuzzyService  → deterministic candidate SQL + scoring
    2. SnowflakeService → execute candidate query
    3. CortexService → parse free-text (if needed) + rerank
    4. SnowflakeService → fetch full payload for top N
    5. ExplainService → package artifacts

  METRIC QUERY (intent="metric_query"):
    1. CortexService → classify intent + build query_metrics params
    2. DbtMcpService → query_metrics via Semantic Layer
    3. Return tabular metric results

Both paths log a trace (trace_id + timings).
"""

import hashlib
import json
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from api.core.config import settings
from api.core.errors import OrderNotFoundError
from api.core.log import get_logger
from api.core.timing import Timer
from api.schemas.domain import CandidateSummary, OrderStatusPayload
from api.schemas.explain import ExplainResponse
from api.schemas.search import (
    MatchedOrder,
    MetricResult,
    SearchRequest,
    SearchResponse,
    TimingsMs,
)
from api.schemas.trace import TraceLog
from api.services.cortex_service import CortexService, RerankResult, format_semantic_context
from api.services.dbt_mcp_service import DbtMcpService
from api.services.explain_service import ExplainService
from api.services.fuzzy_service import FuzzyService, NormalizedQuery
from api.services.snowflake_service import SnowflakeService

logger = get_logger(__name__)

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

_INSERT_TRACE_SQL = """
    INSERT INTO DEMO_BSC.DEMO_TRACE_LOG (
        trace_id, created_at, mode, normalized_request_summary,
        parse_prompt_version, rerank_prompt_version,
        candidate_sql_hash, fetch_sql_hash,
        snowflake_qid_candidate, snowflake_qid_fetch,
        candidate_count, chosen_order_ids,
        sql_candidate_ms, cortex_rerank_ms, sql_fetch_top_ms, total_ms,
        error
    ) SELECT
        %(trace_id)s, %(created_at)s, %(mode)s, %(normalized_request_summary)s,
        %(parse_prompt_version)s, %(rerank_prompt_version)s,
        %(candidate_sql_hash)s, %(fetch_sql_hash)s,
        %(snowflake_qid_candidate)s, %(snowflake_qid_fetch)s,
        %(candidate_count)s, %(chosen_order_ids)s,
        %(sql_candidate_ms)s, %(cortex_rerank_ms)s, %(sql_fetch_top_ms)s, %(total_ms)s,
        %(error)s
"""


class SemanticService:
    """Stable serving contract — do not change method signatures."""

    def __init__(
        self,
        snowflake: SnowflakeService,
        cortex: CortexService,
        fuzzy: FuzzyService,
        explain: ExplainService,
        dbt_mcp: Optional[DbtMcpService] = None,
    ):
        self._sf = snowflake
        self._cortex = cortex
        self._fuzzy = fuzzy
        self._explain = explain
        self._dbt_mcp = dbt_mcp
        self._explain_store: Dict[str, ExplainResponse] = {}
        self._dbt_mcp_available = False

        if self._dbt_mcp and settings.semantic_backend == "dbt_mcp":
            self._dbt_mcp_available = self._dbt_mcp.check_availability()
            if self._dbt_mcp_available:
                logger.info("dbt_mcp_backend_active")
            else:
                logger.info("dbt_mcp_backend_unavailable, falling back to direct_sql")

    # ------------------------------------------------------------------
    # Primary entry points (stable contract)
    # ------------------------------------------------------------------

    def search_orders(self, request: SearchRequest) -> SearchResponse:
        trace_id = str(uuid.uuid4())
        timer = Timer()

        if request.mode == "free_text" and request.free_text:
            is_metric = self._classify_intent(request.free_text)

            if is_metric and self._dbt_mcp_available:
                return self._handle_metric_query(
                    trace_id=trace_id,
                    timer=timer,
                    question=request.free_text,
                    original_request=request,
                )

            # Order lookup (with Cortex parse for field extraction)
            with timer.segment("cortex_parse"):
                intent = self._cortex.parse_user_input(request.free_text)
            return self._handle_order_lookup(
                trace_id=trace_id,
                timer=timer,
                request=request,
                pre_parsed_intent=intent,
            )

        return self._handle_order_lookup(
            trace_id=trace_id,
            timer=timer,
            request=request,
        )

    @staticmethod
    def _classify_intent(text: str) -> bool:
        """Fast keyword-based intent classification. Returns True for metric queries."""
        t = text.lower()
        METRIC_PATTERNS = [
            "how many", "count", "total", "average", "avg ",
            "trend", "breakdown", "by status", "by region",
            "by month", "by year", "by quarter", "by week",
            "revenue", "fulfillment rate", "top ", "sum ",
            "percentage", "units ordered", "line items",
            "over time", "compare", "statistics", "aggregate",
        ]
        ORDER_PATTERNS = [
            "order for", "find order", "track", "where is",
            "status of", "shipping", "delivery for", "po-",
            "so-", "purchase order", "specific order",
        ]
        metric_score = sum(1 for p in METRIC_PATTERNS if p in t)
        order_score = sum(1 for p in ORDER_PATTERNS if p in t)
        return metric_score > order_score

    def get_order_status(self, order_id: str) -> OrderStatusPayload:
        """Direct single-order lookup by exact order_id."""
        sql = """
            SELECT
                order_id, purchase_order_id, status, status_last_updated_ts,
                customer_name, facility_name, promised_delivery_date,
                carrier, tracking_number, actual_ship_ts, actual_delivery_date,
                priority_flag, requested_ship_date, total_amount_usd, currency,
                sales_region
            FROM DEMO_BSC.ORDER_SEARCH_V
            WHERE order_id = %(order_id)s
            LIMIT 1
        """
        result = self._sf.execute(sql, {"order_id": order_id}, label="get_order_status")
        if not result.rows:
            raise OrderNotFoundError(order_id)
        row = result.rows[0]
        return OrderStatusPayload(
            order_id=row["ORDER_ID"],
            purchase_order_id=row.get("PURCHASE_ORDER_ID"),
            status=row["STATUS"],
            status_last_updated_ts=row["STATUS_LAST_UPDATED_TS"],
            customer_name=row["CUSTOMER_NAME"],
            facility_name=row["FACILITY_NAME"],
            promised_delivery_date=row.get("PROMISED_DELIVERY_DATE"),
            carrier=row.get("CARRIER"),
            tracking_number=row.get("TRACKING_NUMBER"),
            actual_ship_ts=row.get("ACTUAL_SHIP_TS"),
            actual_delivery_date=row.get("ACTUAL_DELIVERY_DATE"),
            priority_flag=row.get("PRIORITY_FLAG"),
            requested_ship_date=row.get("REQUESTED_SHIP_DATE"),
            total_amount_usd=row.get("TOTAL_AMOUNT_USD"),
            currency=row.get("CURRENCY"),
            sales_region=row.get("SALES_REGION"),
        )

    def explain(self, trace_id: str) -> ExplainResponse:
        resp = self._explain_store.get(trace_id)
        if resp is None:
            raise OrderNotFoundError(f"trace:{trace_id}")
        return resp

    # ------------------------------------------------------------------
    # METRIC QUERY pipeline
    # ------------------------------------------------------------------

    def _handle_metric_query(
        self,
        *,
        trace_id: str,
        timer: Timer,
        question: str,
        original_request: SearchRequest,
        parse_prompt_used: Optional[str] = None,
    ) -> SearchResponse:
        error_str: Optional[str] = None
        metric_result: Optional[MetricResult] = None
        semantic_objects_used: List[str] = []

        try:
            metrics_list = self._dbt_mcp.list_metrics()
            metric_names = [m.get("name", "") for m in metrics_list]
            sample_metrics = metric_names[:5] if metric_names else []
            dims_list = self._dbt_mcp.get_dimensions(sample_metrics) if sample_metrics else []

            # Map dimension types for the LLM
            mapped_dims = []
            for d in dims_list:
                dtype = d.get("type", "CATEGORICAL")
                mapped_type = "time_dimension" if dtype == "TIME" else "dimension"
                mapped_dims.append({"name": d.get("name", ""), "type": mapped_type})

            with timer.segment("cortex_parse"):
                query_params = self._cortex.build_metric_query_params(
                    question=question,
                    available_metrics=metrics_list,
                    available_dimensions=mapped_dims,
                )
            query_params = self._normalize_metric_params(query_params) or {}

            if not query_params.get("metrics"):
                logger.warning("metric_query_no_metrics_matched", extra={
                    "extra": {"question": question[:100]}
                })
                return SearchResponse(
                    trace_id=trace_id,
                    response_type="metric_query",
                    timings_ms=TimingsMs(total_ms=timer.total_ms(), cortex_parse_ms=timer.get("cortex_parse")),
                    semantic_backend="dbt_mcp",
                )

            semantic_objects_used = query_params.get("metrics", [])

            with timer.segment("mcp_query"):
                rows = self._dbt_mcp.query_metrics(
                    metrics=query_params["metrics"],
                    group_by=query_params.get("group_by"),
                    order_by=query_params.get("order_by"),
                    where=query_params.get("where"),
                    limit=query_params.get("limit"),
                )

            columns = list(rows[0].keys()) if rows else []
            dimensions_used = [
                g.get("name", "") for g in (query_params.get("group_by") or [])
            ]

            # Fetch MetricFlow-compiled SQL for explainability
            compiled_sql = ""
            try:
                compiled_sql = self._dbt_mcp.get_compiled_sql(
                    metrics=query_params["metrics"],
                    group_by=query_params.get("group_by"),
                    order_by=query_params.get("order_by"),
                    where=query_params.get("where"),
                    limit=query_params.get("limit"),
                )
            except Exception as sql_exc:
                logger.warning(f"compiled_sql_fetch_failed: {sql_exc}")

            metric_result = MetricResult(
                columns=columns,
                rows=rows,
                row_count=len(rows),
                metrics_used=query_params["metrics"],
                dimensions_used=dimensions_used,
                compiled_sql=compiled_sql or None,
            )

        except Exception as exc:
            error_str = str(exc)
            logger.error(f"metric_query_error: {exc}", extra={"extra": {"trace_id": trace_id}})

        timings = TimingsMs(
            cortex_parse_ms=timer.get("cortex_parse"),
            mcp_query_ms=timer.get("mcp_query"),
            total_ms=timer.total_ms(),
        )

        self._write_trace(
            trace_id=trace_id,
            request=original_request,
            timings=timings,
            candidate_count=metric_result.row_count if metric_result else 0,
            chosen_ids=[],
            candidate_sql_hash="",
            fetch_sql_hash="",
            snowflake_query_ids={},
            error=error_str,
        )

        metricflow_sql = ""
        if metric_result and metric_result.compiled_sql:
            metricflow_sql = metric_result.compiled_sql

        explain_resp = self._explain.build_explain_response(
            trace_id=trace_id,
            candidate_sql=metricflow_sql or "(metric query via dbt Semantic Layer)",
            candidate_count=metric_result.row_count if metric_result else 0,
            top_candidates_pre_rerank=[],
            rerank_result=RerankResult(ranked_ids=[], rationale={}, prompt_used=""),
            fetch_sql="",
            timings_ms=timings,
            prompt_versions={
                "parse": settings.parse_prompt_version,
                "rerank": settings.rerank_prompt_version,
            },
            snowflake_query_ids={},
            parse_prompt_used=parse_prompt_used,
            normalized_request={"intent": "metric_query", "question": question},
            semantic_objects_used=semantic_objects_used,
            semantic_backend="dbt_mcp",
        )
        self._explain_store[trace_id] = explain_resp

        return SearchResponse(
            trace_id=trace_id,
            response_type="metric_query",
            metric_result=metric_result,
            timings_ms=timings,
            semantic_backend="dbt_mcp",
        )

    # ------------------------------------------------------------------
    # ORDER LOOKUP pipeline (existing)
    # ------------------------------------------------------------------

    def _handle_order_lookup(
        self,
        *,
        trace_id: str,
        timer: Timer,
        request: SearchRequest,
        pre_parsed_intent=None,
    ) -> SearchResponse:
        snowflake_query_ids: Dict[str, str] = {}
        error_str: Optional[str] = None
        rerank_result: RerankResult = RerankResult(
            ranked_ids=[], rationale={}, prompt_used=""
        )
        normalized: Optional[NormalizedQuery] = None
        candidate_sql = ""
        fetch_sql = ""
        candidate_count = 0
        candidates: List[CandidateSummary] = []
        semantic_objects_used: List[str] = []
        parse_prompt_used: Optional[str] = None
        semantic_context_str: Optional[str] = None
        data_freshness: Optional[Dict[str, Any]] = None
        model_health: Optional[Dict[str, Any]] = None
        lineage: Optional[Dict[str, Any]] = None

        try:
            # Step 0a: dbt MCP semantic context (if available)
            if self._dbt_mcp_available and self._dbt_mcp:
                try:
                    ctx = self._dbt_mcp.get_semantic_context_for_search()
                    semantic_objects_used = ctx.get("metrics", [])
                except Exception as exc:
                    logger.warning(f"dbt_mcp_context_failed: {exc}")

                # Fetch rich semantic context for prompt enrichment
                try:
                    sem_ctx = self._dbt_mcp.get_semantic_model_context()
                    semantic_context_str = format_semantic_context(sem_ctx)
                except Exception as exc:
                    logger.warning(f"dbt_mcp_semantic_model_context_failed: {exc}")

                # Fetch model health, freshness, and lineage for explain panel
                try:
                    model_health = self._dbt_mcp.get_model_health("fct_orders")
                except Exception as exc:
                    logger.warning(f"dbt_mcp_model_health_failed: {exc}")
                try:
                    sources = self._dbt_mcp.get_sources_freshness()
                    if sources:
                        data_freshness = {"sources": sources}
                except Exception as exc:
                    logger.warning(f"dbt_mcp_sources_freshness_failed: {exc}")
                try:
                    lineage = self._dbt_mcp.get_lineage("fct_orders")
                except Exception as exc:
                    logger.warning(f"dbt_mcp_lineage_failed: {exc}")

            # Step 0: parse free text if needed
            if request.mode == "free_text" and request.free_text:
                if pre_parsed_intent:
                    intent = pre_parsed_intent
                    parse_prompt_used = intent.raw_response
                else:
                    with timer.segment("cortex_parse"):
                        intent = self._cortex.parse_user_input(
                            request.free_text,
                            semantic_context=semantic_context_str,
                        )
                        parse_prompt_used = intent.raw_response

                f = request.fields
                f.order_id = f.order_id or intent.order_id
                f.purchase_order_id = f.purchase_order_id or intent.purchase_order_id
                f.customer_name = f.customer_name or intent.customer_name
                f.facility_name = f.facility_name or intent.facility_name
                if not f.date_start and intent.date_start:
                    from datetime import date
                    try:
                        f.date_start = date.fromisoformat(intent.date_start)
                    except ValueError:
                        pass
                if not f.date_end and intent.date_end:
                    from datetime import date
                    try:
                        f.date_end = date.fromisoformat(intent.date_end)
                    except ValueError:
                        pass
                f.contact_name = f.contact_name or intent.contact_name
                f.status = f.status or intent.status

            # Step 1: deterministic candidate retrieval
            normalized = self._fuzzy.normalize_inputs(request)
            plan = self._fuzzy.build_candidate_query(normalized)
            candidate_sql = plan.sql

            with timer.segment("sql_candidate"):
                cand_result = self._sf.execute(
                    plan.sql, plan.params, label="candidate_query"
                )
                snowflake_query_ids["candidate"] = cand_result.query_id

            candidates = self._fuzzy.score_candidates(cand_result.rows)
            candidate_count = len(candidates)

            # Step 2: Cortex reranking
            query_str = request.free_text or self._summarize_fields(request)

            if plan.is_exact or candidate_count == 0:
                rerank_result = RerankResult(
                    ranked_ids=[c.order_id for c in candidates[: request.top_n]],
                    rationale={c.order_id: "Exact ID match" for c in candidates[: request.top_n]},
                    prompt_used="",
                    elapsed_ms=0.0,
                )
            else:
                rerank_pool = candidates[:20]
                with timer.segment("cortex_rerank"):
                    rerank_result = self._cortex.rerank_candidates(
                        query=query_str,
                        candidates=rerank_pool,
                        top_n=request.top_n,
                        semantic_context=semantic_context_str,
                    )

            # Step 3: final fetch
            top_ids = rerank_result.ranked_ids or [
                c.order_id for c in candidates[: request.top_n]
            ]

            matched_orders: List[MatchedOrder] = []
            if top_ids:
                fetch_sql = self._build_fetch_sql(top_ids)
                with timer.segment("sql_fetch_top"):
                    fetch_result = self._sf.execute(
                        fetch_sql,
                        {f"id_{i}": oid for i, oid in enumerate(top_ids)},
                        label="fetch_top",
                    )
                    snowflake_query_ids["fetch_top"] = fetch_result.query_id

                score_map = {c.order_id: c.score for c in candidates}
                rows_by_id = {row["ORDER_ID"]: row for row in fetch_result.rows}

                for order_id in top_ids:
                    row = rows_by_id.get(order_id)
                    if row is None:
                        continue
                    reasons = self._build_match_reasons(
                        row=row,
                        normalized=normalized,
                        rationale=rerank_result.rationale.get(order_id, ""),
                        is_exact=plan.is_exact,
                    )
                    matched_orders.append(
                        MatchedOrder(
                            order_id=row["ORDER_ID"],
                            purchase_order_id=row.get("PURCHASE_ORDER_ID"),
                            status=row["STATUS"],
                            status_last_updated_ts=row["STATUS_LAST_UPDATED_TS"],
                            customer_name=row["CUSTOMER_NAME"],
                            facility_name=row["FACILITY_NAME"],
                            promised_delivery_date=row.get("PROMISED_DELIVERY_DATE"),
                            carrier=row.get("CARRIER"),
                            tracking_number=row.get("TRACKING_NUMBER"),
                            actual_ship_ts=row.get("ACTUAL_SHIP_TS"),
                            actual_delivery_date=row.get("ACTUAL_DELIVERY_DATE"),
                            priority_flag=row.get("PRIORITY_FLAG"),
                            requested_ship_date=row.get("REQUESTED_SHIP_DATE"),
                            total_amount_usd=row.get("TOTAL_AMOUNT_USD"),
                            currency=row.get("CURRENCY"),
                            sales_region=row.get("SALES_REGION"),
                            match_score=round(score_map.get(order_id, 0.0), 2),
                            match_reasons=reasons,
                        )
                    )

        except Exception as exc:
            error_str = str(exc)
            logger.error(f"search_orders_error: {exc}", extra={"extra": {"trace_id": trace_id}})
            raise

        finally:
            timings = TimingsMs(
                sql_candidate_ms=timer.get("sql_candidate"),
                cortex_rerank_ms=timer.get("cortex_rerank") + getattr(rerank_result, "elapsed_ms", 0.0),
                sql_fetch_top_ms=timer.get("sql_fetch_top"),
                cortex_parse_ms=timer.get("cortex_parse"),
                total_ms=timer.total_ms(),
            )
            self._write_trace(
                trace_id=trace_id,
                request=request,
                timings=timings,
                candidate_count=candidate_count,
                chosen_ids=rerank_result.ranked_ids,
                candidate_sql_hash=hashlib.sha256(candidate_sql.encode()).hexdigest()[:16],
                fetch_sql_hash=hashlib.sha256(fetch_sql.encode()).hexdigest()[:16],
                snowflake_query_ids=snowflake_query_ids,
                error=error_str,
            )

        response = SearchResponse(
            trace_id=trace_id,
            response_type="order_lookup",
            results=matched_orders,
            timings_ms=timings,
            candidate_count=candidate_count,
            candidate_sql=candidate_sql.strip(),
            fetch_sql=fetch_sql.strip(),
            semantic_backend=settings.semantic_backend,
        )

        explain_resp = self._explain.build_explain_response(
            trace_id=trace_id,
            candidate_sql=candidate_sql,
            candidate_count=candidate_count,
            top_candidates_pre_rerank=candidates[:20],
            rerank_result=rerank_result,
            fetch_sql=fetch_sql,
            timings_ms=timings,
            prompt_versions={
                "parse": settings.parse_prompt_version,
                "rerank": settings.rerank_prompt_version,
            },
            snowflake_query_ids=snowflake_query_ids,
            parse_prompt_used=parse_prompt_used,
            normalized_request=vars(normalized) if normalized else None,
            semantic_objects_used=semantic_objects_used,
            semantic_backend=settings.semantic_backend,
            data_freshness=data_freshness,
            model_health=model_health,
            lineage=lineage,
        )
        self._explain_store[trace_id] = explain_resp

        return response

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _normalize_metric_params(params: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """Ensure LLM-produced metric params match the MCP tool schema."""
        if not params or not params.get("metrics"):
            return params
        TYPE_MAP = {"CATEGORICAL": "dimension", "TIME": "time_dimension"}
        if params.get("group_by"):
            normalized_gb = []
            for gb in params["group_by"]:
                raw_type = gb.get("type", "dimension")
                gb_type = TYPE_MAP.get(raw_type.upper(), raw_type)
                if gb_type not in ("dimension", "time_dimension", "entity"):
                    gb_type = "dimension"
                normalized_gb.append({
                    "name": gb["name"],
                    "type": gb_type,
                    "grain": gb.get("grain"),
                })
            params["group_by"] = normalized_gb
        if params.get("order_by"):
            normalized_ob = []
            for ob in params["order_by"]:
                normalized_ob.append({
                    "name": ob["name"],
                    "descending": ob.get("descending", False),
                })
            params["order_by"] = normalized_ob
        return params

    @staticmethod
    def _build_fetch_sql(order_ids: List[str]) -> str:
        placeholders = ", ".join(f"%(id_{i})s" for i in range(len(order_ids)))
        return _FETCH_ORDERS_SQL.format(placeholders=placeholders)

    @staticmethod
    def _summarize_fields(request: SearchRequest) -> str:
        parts = []
        f = request.fields
        if f.order_id:
            parts.append(f"order {f.order_id}")
        if f.facility_name:
            parts.append(f"facility {f.facility_name}")
        if f.customer_name:
            parts.append(f"customer {f.customer_name}")
        if f.date_start or f.date_end:
            parts.append(f"dates {f.date_start} to {f.date_end}")
        return " ".join(parts) or "order lookup"

    @staticmethod
    def _build_match_reasons(
        row: dict,
        normalized: NormalizedQuery,
        rationale: str,
        is_exact: bool,
    ) -> List[str]:
        reasons: List[str] = []
        if is_exact:
            reasons.append("Exact ID match")
        if rationale:
            reasons.append(rationale)
        fac_norm = (row.get("FACILITY_NAME") or "").lower()
        for tok in normalized.facility_tokens:
            if tok in fac_norm:
                reasons.append(f"Facility token match: '{tok}'")
                break
        cust_norm = (row.get("CUSTOMER_NAME") or "").lower()
        for tok in normalized.customer_tokens:
            if tok in cust_norm:
                reasons.append(f"Customer name match: '{tok}'")
                break
        return reasons or ["Candidate score match"]

    def _write_trace(
        self,
        *,
        trace_id: str,
        request: SearchRequest,
        timings: TimingsMs,
        candidate_count: int,
        chosen_ids: List[str],
        candidate_sql_hash: str,
        fetch_sql_hash: str,
        snowflake_query_ids: Dict[str, str],
        error: Optional[str],
    ) -> None:
        try:
            summary = f"mode={request.mode} top_n={request.top_n}"
            if request.fields.facility_name:
                summary += f" facility={request.fields.facility_name[:20]}"

            array_literal = (
                "ARRAY_CONSTRUCT(" + ", ".join(f"'{oid}'" for oid in chosen_ids) + ")"
                if chosen_ids
                else "PARSE_JSON('[]')"
            )
            conn = self._sf._get_conn()
            cur = conn.cursor()
            cur.execute(
                _INSERT_TRACE_SQL.replace("%(chosen_order_ids)s", array_literal),
                {
                    "trace_id": trace_id,
                    "created_at": datetime.now(timezone.utc),
                    "mode": request.mode,
                    "normalized_request_summary": summary,
                    "parse_prompt_version": settings.parse_prompt_version,
                    "rerank_prompt_version": settings.rerank_prompt_version,
                    "candidate_sql_hash": candidate_sql_hash,
                    "fetch_sql_hash": fetch_sql_hash,
                    "snowflake_qid_candidate": snowflake_query_ids.get("candidate", ""),
                    "snowflake_qid_fetch": snowflake_query_ids.get("fetch_top", ""),
                    "candidate_count": candidate_count,
                    "sql_candidate_ms": timings.sql_candidate_ms,
                    "cortex_rerank_ms": timings.cortex_rerank_ms,
                    "sql_fetch_top_ms": timings.sql_fetch_top_ms,
                    "total_ms": timings.total_ms,
                    "error": error,
                },
            )
            cur.close()
        except Exception as exc:
            logger.warning(f"trace_write_failed: {exc}")
