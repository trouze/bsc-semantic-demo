"""Packages all explainability artifacts for a completed request."""

from typing import Dict, List, Optional

from api.schemas.domain import CandidateSummary
from api.schemas.explain import ExplainResponse
from api.schemas.search import TimingsMs
from api.services.cortex_service import RerankResult


class ExplainService:

    def build_explain_response(
        self,
        *,
        trace_id: str,
        candidate_sql: str,
        candidate_count: int,
        top_candidates_pre_rerank: List[CandidateSummary],
        rerank_result: RerankResult,
        fetch_sql: str,
        timings_ms: TimingsMs,
        prompt_versions: Dict[str, str],
        snowflake_query_ids: Dict[str, str],
        parse_prompt_used: Optional[str] = None,
        normalized_request: Optional[dict] = None,
        semantic_objects_used: Optional[List[str]] = None,
        semantic_backend: Optional[str] = None,
        data_freshness: Optional[dict] = None,
        model_health: Optional[dict] = None,
        lineage: Optional[dict] = None,
    ) -> ExplainResponse:
        return ExplainResponse(
            trace_id=trace_id,
            candidate_sql=candidate_sql.strip(),
            candidate_count=candidate_count,
            top_candidates_pre_rerank=top_candidates_pre_rerank[:20],
            rerank_rationale=rerank_result.rationale,
            rerank_order=rerank_result.ranked_ids,
            fetch_sql=fetch_sql.strip(),
            timings_ms=timings_ms,
            prompt_versions=prompt_versions,
            snowflake_query_ids=snowflake_query_ids,
            parse_prompt_used=parse_prompt_used,
            rerank_prompt_used=rerank_result.prompt_used,
            normalized_request=normalized_request,
            semantic_objects_used=semantic_objects_used or [],
            semantic_backend=semantic_backend,
            data_freshness=data_freshness,
            model_health=model_health,
            lineage=lineage,
        )
