from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from .domain import CandidateSummary
from .search import TimingsMs


class ExplainResponse(BaseModel):
    trace_id: str
    candidate_sql: str
    candidate_count: int
    top_candidates_pre_rerank: List[CandidateSummary]
    rerank_rationale: Dict[str, str]       # order_id -> reason string
    rerank_order: List[str]                # order_ids in reranked order
    fetch_sql: str
    timings_ms: TimingsMs
    prompt_versions: Dict[str, str]        # key -> version id
    snowflake_query_ids: Dict[str, str]    # label -> Snowflake query id
    parse_prompt_used: Optional[str] = None
    rerank_prompt_used: Optional[str] = None
    normalized_request: Optional[Dict[str, Any]] = None
    semantic_objects_used: List[str] = []   # dbt semantic layer objects referenced
    semantic_backend: Optional[str] = None  # 'dbt_mcp' or 'direct_sql'
    data_freshness: Optional[Dict[str, Any]] = None   # source freshness info
    model_health: Optional[Dict[str, Any]] = None      # dbt model test results
    lineage: Optional[Dict[str, Any]] = None           # data lineage graph
