"""dbt Cloud Semantic Layer integration via MCP Streamable HTTP transport.

Maintains a persistent MCP session on a background asyncio loop so that
sync FastAPI handlers can call Semantic Layer tools without per-request
connection overhead.  Typical tool latency: 500-1700 ms.

Exposed tools (all sync wrappers):
  - list_metrics()
  - get_dimensions(metrics)
  - get_entities(metrics)
  - query_metrics(...)   → returns rows directly from dbt Cloud
  - get_compiled_sql(...)→ returns compiled SQL string
  - text_to_sql(text)    → natural-language → SQL

Falls back gracefully when dbt Cloud is unreachable.
"""

import asyncio
import json
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from api.core.config import settings
from api.core.log import get_logger

logger = get_logger(__name__)

_SESSION_LOCK = threading.Lock()
_MCP_CALL_TIMEOUT = 15.0  # seconds per tool call
_CACHE_TTL = 300  # 5 min cache for metadata


@dataclass
class SemanticObject:
    name: str
    object_type: str
    description: str = ""
    expr: Optional[str] = None
    meta: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SemanticQueryResult:
    sql: str
    semantic_objects_used: List[str]
    compile_ms: float = 0.0
    meta: Dict[str, Any] = field(default_factory=dict)


class _McpLoop:
    """Manages a background asyncio loop + persistent MCP session."""

    def __init__(self, url: str, headers: Dict[str, str]):
        self._url = url
        self._headers = headers
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None
        self._session = None  # mcp.ClientSession
        self._read = None
        self._write = None
        self._ctx_stack = None
        self._connected = False
        self._lock = threading.Lock()

    def start(self) -> bool:
        """Start the background loop and connect.  Returns True on success."""
        if self._connected:
            return True
        with self._lock:
            if self._connected:
                return True
            self._loop = asyncio.new_event_loop()
            self._thread = threading.Thread(
                target=self._loop.run_forever, daemon=True, name="mcp-loop"
            )
            self._thread.start()
            try:
                fut = asyncio.run_coroutine_threadsafe(self._connect(), self._loop)
                fut.result(timeout=20)
                return self._connected
            except Exception as exc:
                logger.warning(f"mcp_connect_failed: {exc}")
                return False

    async def _connect(self):
        from mcp.client.streamable_http import streamablehttp_client
        from mcp import ClientSession

        try:
            cm = streamablehttp_client(self._url, headers=self._headers)
            self._read, self._write, _ = await cm.__aenter__()
            self._ctx_stack = cm

            session = ClientSession(self._read, self._write)
            self._session = await session.__aenter__()
            await self._session.initialize()
            self._connected = True
            logger.info("mcp_session_connected", extra={"extra": {"url": self._url}})
        except Exception as exc:
            self._connected = False
            logger.error(f"mcp_session_error: {exc}")
            raise

    def call_tool(self, name: str, arguments: Dict[str, Any]) -> Any:
        """Sync wrapper: call an MCP tool and return parsed result."""
        if not self._connected:
            raise RuntimeError("MCP session not connected")
        fut = asyncio.run_coroutine_threadsafe(
            self._session.call_tool(name, arguments), self._loop
        )
        result = fut.result(timeout=_MCP_CALL_TIMEOUT)

        if getattr(result, "isError", False):
            error_text = ""
            for block in (result.content or []):
                if hasattr(block, "text"):
                    error_text += block.text
            logger.error(f"mcp_tool_returned_error: {name}: {error_text[:500]}")
            raise RuntimeError(f"MCP tool error ({name}): {error_text[:300]}")

        return self._parse_content(result)

    @staticmethod
    def _parse_content(result) -> Any:
        """Extract text content from MCP tool result."""
        if not result or not result.content:
            return None
        texts = []
        for block in result.content:
            if hasattr(block, "text"):
                try:
                    texts.append(json.loads(block.text))
                except (json.JSONDecodeError, TypeError):
                    texts.append(block.text)
        if len(texts) == 1:
            return texts[0]
        return texts

    @property
    def connected(self) -> bool:
        return self._connected

    def stop(self):
        if self._loop and self._loop.is_running():
            self._loop.call_soon_threadsafe(self._loop.stop)


class DbtMcpService:
    """Sync adapter for dbt Cloud Semantic Layer via persistent MCP session.

    Call `connect()` once at startup.  All tool methods are sync and safe
    to call from FastAPI request handlers.
    """

    def __init__(self):
        self._host = (settings.dbt_cloud_host or "").rstrip("/")
        self._token = settings.dbt_cloud_token
        self._env_id = settings.dbt_cloud_environment_id
        self._mcp: Optional[_McpLoop] = None
        self._available = False

        self._metrics_cache: Optional[List[Dict]] = None
        self._metrics_cache_ts: float = 0.0
        self._dimensions_cache: Dict[str, List[Dict]] = {}
        self._entities_cache: Dict[str, List[Dict]] = {}

    def _mcp_url(self) -> str:
        base = self._host
        if not base.endswith("/api/ai/v1/mcp/"):
            if not base.endswith("/"):
                base += "/"
            base += "api/ai/v1/mcp/"
        return base

    def _mcp_headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Token {self._token}",
            "x-dbt-prod-environment-id": self._env_id,
        }

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def connect(self) -> bool:
        if not self._host or not self._token or not self._env_id:
            logger.info("dbt_cloud_not_configured")
            return False
        self._mcp = _McpLoop(self._mcp_url(), self._mcp_headers())
        self._available = self._mcp.start()
        return self._available

    def check_availability(self) -> bool:
        if self._available and self._mcp and self._mcp.connected:
            return True
        return self.connect()

    @property
    def is_available(self) -> bool:
        return self._available

    # ------------------------------------------------------------------
    # Semantic Layer tools (sync)
    # ------------------------------------------------------------------

    def list_metrics(self, search: Optional[str] = None, refresh: bool = False) -> List[Dict]:
        if (
            not refresh
            and self._metrics_cache is not None
            and (time.time() - self._metrics_cache_ts) < _CACHE_TTL
        ):
            metrics = self._metrics_cache
            if search:
                s = search.lower()
                metrics = [m for m in metrics if s in m.get("name", "").lower()]
            return metrics

        args: Dict[str, Any] = {}
        if search:
            args["search"] = search
        raw = self._call("list_metrics", args)
        metrics = raw if isinstance(raw, list) else []
        self._metrics_cache = metrics
        self._metrics_cache_ts = time.time()
        if search:
            s = search.lower()
            metrics = [m for m in metrics if s in m.get("name", "").lower()]
        return metrics

    def get_dimensions(self, metrics: List[str]) -> List[Dict]:
        cache_key = ",".join(sorted(metrics))
        if cache_key in self._dimensions_cache:
            return self._dimensions_cache[cache_key]
        raw = self._call("get_dimensions", {"metrics": metrics})
        dims = raw if isinstance(raw, list) else []
        self._dimensions_cache[cache_key] = dims
        return dims

    def get_entities(self, metrics: List[str]) -> List[Dict]:
        cache_key = ",".join(sorted(metrics))
        if cache_key in self._entities_cache:
            return self._entities_cache[cache_key]
        raw = self._call("get_entities", {"metrics": metrics})
        ents = raw if isinstance(raw, list) else []
        self._entities_cache[cache_key] = ents
        return ents

    def query_metrics(
        self,
        metrics: List[str],
        group_by: Optional[List[Dict]] = None,
        order_by: Optional[List[Dict]] = None,
        where: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[Dict]:
        args: Dict[str, Any] = {"metrics": metrics}
        if group_by:
            args["group_by"] = group_by
        if order_by:
            args["order_by"] = order_by
        if where:
            args["where"] = where
        if limit is not None:
            args["limit"] = limit
        logger.info("mcp_query_metrics_args", extra={"extra": {
            "metrics": metrics,
            "group_by": group_by,
            "has_order_by": order_by is not None,
            "has_where": where is not None,
            "limit": limit,
        }})
        raw = self._call("query_metrics", args)
        row_count = len(raw) if isinstance(raw, list) else 0
        logger.info(f"mcp_query_metrics_result: {row_count} rows, type={type(raw).__name__}")
        return raw if isinstance(raw, list) else []

    def get_compiled_sql(
        self,
        metrics: List[str],
        group_by: Optional[List[Dict]] = None,
        order_by: Optional[List[Dict]] = None,
        where: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> str:
        args: Dict[str, Any] = {"metrics": metrics}
        if group_by:
            args["group_by"] = group_by
        if order_by:
            args["order_by"] = order_by
        if where:
            args["where"] = where
        if limit is not None:
            args["limit"] = limit
        raw = self._call("get_metrics_compiled_sql", args)
        if isinstance(raw, str):
            return raw
        if isinstance(raw, dict):
            return raw.get("sql", raw.get("compiled_sql", str(raw)))
        return str(raw) if raw else ""

    def text_to_sql(self, text: str) -> str:
        raw = self._call("text_to_sql", {"text": text})
        if isinstance(raw, str):
            return raw
        return str(raw) if raw else ""

    # ------------------------------------------------------------------
    # High-level helpers
    # ------------------------------------------------------------------

    def get_semantic_context_for_search(self) -> Dict[str, Any]:
        metrics = self.list_metrics()
        metric_names = [m.get("name", "") for m in metrics]
        return {
            "metrics": metric_names,
            "metric_details": metrics,
            "object_count": len(metrics),
        }

    def get_semantic_model_context(self) -> Dict[str, Any]:
        """Fetch rich semantic context for prompt enrichment.

        Returns dimension descriptions, entity relationships, metric
        definitions, and hardcoded domain knowledge (valid status values,
        business term mappings).  Result is cached via the underlying
        list_metrics / get_dimensions / get_entities caches.
        """
        metrics = self.list_metrics()
        metric_names = [m.get("name", "") for m in metrics]
        sample = metric_names[:5] if metric_names else []

        dims = self.get_dimensions(sample) if sample else []
        entities = self.get_entities(sample) if sample else []

        # --- business vocabulary (domain knowledge not in MCP) ---
        status_values = [
            "CREATED", "ALLOCATED", "PICKED", "SHIPPED",
            "DELIVERED", "BACKORDERED", "CANCELLED", "ON_HOLD",
        ]
        business_terms = {
            "is_fulfilled": "order has been SHIPPED or DELIVERED",
            "priority_flag": "TRUE when the order is flagged as urgent / priority",
            "fulfillment_rate": "percentage of orders that are shipped or delivered",
            "priority_rate": "percentage of orders flagged as priority",
            "days_to_last_update": "calendar days from order creation to most recent status change",
        }
        entity_relationships = [
            "Each order belongs to exactly one customer (customer_account_id)",
            "Each order ships to exactly one facility (facility_id)",
            "Facilities have city, state, and zip attributes",
        ]

        return {
            "status_values": status_values,
            "business_terms": business_terms,
            "entity_relationships": entity_relationships,
            "dimensions": [
                {"name": d.get("name", ""), "type": d.get("type", ""), "description": d.get("description", "")}
                for d in dims
            ],
            "metrics": [
                {"name": m.get("name", ""), "description": m.get("description", "")}
                for m in metrics
            ],
            "entities": [
                {"name": e.get("name", ""), "type": e.get("type", ""), "description": e.get("description", "")}
                for e in entities
            ],
        }

    def get_model_health(self, model_name: str) -> Optional[Dict[str, Any]]:
        """Fetch model health / test status via MCP.  Returns None on failure."""
        try:
            raw = self._call("get_model_health", {"model_name": model_name})
            return raw if isinstance(raw, dict) else {"raw": raw}
        except Exception as exc:
            logger.warning(f"get_model_health_failed: {exc}")
            return None

    def get_sources_freshness(self) -> Optional[List[Dict[str, Any]]]:
        """Fetch source freshness info via MCP.  Returns None on failure."""
        try:
            raw = self._call("get_all_sources", {})
            if isinstance(raw, list):
                return raw
            if isinstance(raw, dict):
                return [raw]
            return None
        except Exception as exc:
            logger.warning(f"get_sources_freshness_failed: {exc}")
            return None

    def get_lineage(self, model_name: str) -> Optional[Dict[str, Any]]:
        """Fetch lineage graph for a model via MCP.  Returns None on failure."""
        try:
            raw = self._call("get_lineage", {"model_name": model_name})
            return raw if isinstance(raw, dict) else {"raw": raw}
        except Exception as exc:
            logger.warning(f"get_lineage_failed: {exc}")
            return None

    def list_semantic_objects(self, refresh: bool = False) -> List[SemanticObject]:
        objects: List[SemanticObject] = []
        for m in self.list_metrics(refresh=refresh):
            objects.append(SemanticObject(
                name=m.get("name", ""),
                object_type="metric",
                description=m.get("description", ""),
                meta=m,
            ))
        return objects

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _call(self, tool_name: str, args: Dict[str, Any]) -> Any:
        if not self._mcp or not self._mcp.connected:
            if not self.connect():
                raise RuntimeError("dbt Cloud MCP not connected")

        t0 = time.perf_counter()
        try:
            result = self._mcp.call_tool(tool_name, args)
            elapsed_ms = (time.perf_counter() - t0) * 1000
            logger.info("mcp_tool_call", extra={"extra": {
                "tool": tool_name,
                "elapsed_ms": round(elapsed_ms, 1),
            }})
            return result
        except Exception as exc:
            elapsed_ms = (time.perf_counter() - t0) * 1000
            logger.error(f"mcp_tool_error: {tool_name} ({elapsed_ms:.0f}ms): {exc}")
            self._available = False
            raise


_dbt_mcp_service: Optional[DbtMcpService] = None


def get_dbt_mcp_service() -> DbtMcpService:
    global _dbt_mcp_service
    if _dbt_mcp_service is None:
        _dbt_mcp_service = DbtMcpService()
    return _dbt_mcp_service
