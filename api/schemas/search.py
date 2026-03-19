from pydantic import BaseModel, Field
from typing import Any, Dict, Optional, List, Literal
from datetime import date
from .domain import OrderStatusPayload


class SearchFields(BaseModel):
    order_id: Optional[str] = None
    purchase_order_id: Optional[str] = None
    customer_name: Optional[str] = None
    facility_name: Optional[str] = None
    date_start: Optional[date] = None
    date_end: Optional[date] = None
    contact_name: Optional[str] = None
    status: Optional[str] = None


class SearchRequest(BaseModel):
    mode: Literal["structured", "free_text"] = "structured"
    free_text: Optional[str] = None
    fields: SearchFields = Field(default_factory=SearchFields)
    top_n: int = Field(default=5, ge=1, le=20)


class MatchedOrder(OrderStatusPayload):
    match_score: float
    match_reasons: List[str]


class TimingsMs(BaseModel):
    sql_candidate_ms: float = 0.0
    cortex_rerank_ms: float = 0.0
    sql_fetch_top_ms: float = 0.0
    cortex_parse_ms: float = 0.0
    mcp_query_ms: float = 0.0
    total_ms: float = 0.0


class MetricResult(BaseModel):
    """Tabular result from a dbt Semantic Layer metric query."""
    columns: List[str]
    rows: List[Dict[str, Any]]
    row_count: int
    metrics_used: List[str]
    dimensions_used: List[str]
    compiled_sql: Optional[str] = None


class SearchResponse(BaseModel):
    trace_id: str
    response_type: Literal["order_lookup", "metric_query"] = "order_lookup"
    results: List[MatchedOrder] = []
    metric_result: Optional[MetricResult] = None
    timings_ms: TimingsMs
    candidate_count: int = 0
    candidate_sql: Optional[str] = None
    fetch_sql: Optional[str] = None
    semantic_backend: Optional[str] = None
