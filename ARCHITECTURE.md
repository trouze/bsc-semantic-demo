# Architecture — Order Status Assistant (BSC Semantic Demo)

## System Overview

This application is a **medical device order fulfillment assistant** built for Boston Scientific. It provides two capabilities through a single API:

1. **Order Lookup** — fuzzy/exact search for specific orders with AI-powered reranking
2. **Metric Queries** — natural-language analytical questions answered via the dbt Semantic Layer

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           PRESENTATION LAYER                                │
│                                                                             │
│  ┌──────────────────┐    ┌──────────────────┐    ┌──────────────────────┐   │
│  │   Streamlit UI   │    │   Agentforce     │    │   Tableau Next       │   │
│  │   (implemented)  │    │   (future)       │    │   (future)           │   │
│  └────────┬─────────┘    └────────┬─────────┘    └──────────┬──────────┘   │
│           │                       │                          │              │
│           └───────────────────────┼──────────────────────────┘              │
│                                   │ HTTP (JSON)                             │
└───────────────────────────────────┼─────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         FastAPI APPLICATION (port 8000)                      │
│                                                                             │
│  Routers:                                                                   │
│  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────┐              │
│  │ POST /search/    │  │ GET /orders/     │  │ GET /explain/ │              │
│  │      orders      │  │      {id}        │  │    {trace_id} │              │
│  └────────┬─────────┘  └────────┬─────────┘  └──────┬───────┘              │
│           │                     │                     │                      │
│           └─────────────────────┼─────────────────────┘                      │
│                                 │                                            │
│                                 ▼                                            │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │                      SemanticService (orchestrator)                   │   │
│  │                                                                      │   │
│  │  Entry: search_orders(request) → routes to one of two pipelines:     │   │
│  │                                                                      │   │
│  │  ┌─────────────────────────┐    ┌──────────────────────────────┐     │   │
│  │  │   ORDER LOOKUP Pipeline │    │   METRIC QUERY Pipeline      │     │   │
│  │  │   (intent=order_lookup) │    │   (intent=metric_query)      │     │   │
│  │  └─────────────┬───────────┘    └──────────────┬───────────────┘     │   │
│  │                │                                │                     │   │
│  └────────────────┼────────────────────────────────┼─────────────────────┘   │
│                   │                                │                         │
│  ┌────────────────┼────────────────────────────────┼─────────────────────┐   │
│  │           SERVICE LAYER                         │                     │   │
│  │                │                                │                     │   │
│  │  ┌─────────────┼──────────────┐   ┌─────────────┼──────────────┐     │   │
│  │  │ FuzzyService│ CortexService│   │CortexService│ DbtMcpService│     │   │
│  │  │ ExplainSvc  │ SnowflakeSvc │   │             │              │     │   │
│  │  └─────────────┼──────────────┘   └─────────────┼──────────────┘     │   │
│  │                │                                │                     │   │
│  └────────────────┼────────────────────────────────┼─────────────────────┘   │
└───────────────────┼────────────────────────────────┼─────────────────────────┘
                    │                                │
                    ▼                                ▼
┌──────────────────────────────────┐  ┌────────────────────────────────────────┐
│       SNOWFLAKE                  │  │       dbt CLOUD                        │
│                                  │  │                                        │
│  ┌────────────────────────────┐  │  │  ┌──────────────────────────────────┐  │
│  │  Snowflake Cortex LLM     │  │  │  │  dbt Semantic Layer (MCP)        │  │
│  │  (mistral-7b)             │  │  │  │                                  │  │
│  │                           │  │  │  │  Tools exposed:                  │  │
│  │  • parse_user_input       │  │  │  │  • list_metrics                  │  │
│  │    (NL → structured JSON) │  │  │  │  • get_dimensions                │  │
│  │  • rerank_candidates      │  │  │  │  • get_entities                  │  │
│  │    (sort by relevance)    │  │  │  │  • query_metrics → rows          │  │
│  │  • build_metric_query     │  │  │  │  • get_metrics_compiled_sql      │  │
│  │    (NL → query params)    │  │  │  │  • text_to_sql (NL → SQL)       │  │
│  └────────────────────────────┘  │  │  └──────────────────────────────────┘  │
│                                  │  │                                        │
│  ┌────────────────────────────┐  │  │  Transport: MCP Streamable HTTP        │
│  │  Snowflake Tables/Views   │  │  │  URL: {host}/api/ai/v1/mcp/            │
│  │                           │  │  │  Auth: Token + environment ID header    │
│  │  DEMO_BSC.ORDER_SEARCH_V  │  │  │  Session: persistent background loop   │
│  │  DEMO_BSC.DEMO_TRACE_LOG  │  │  └────────────────────────────────────────┘
│  │  DEMO_BSC.FCT_ORDERS     │  │
│  │  (+ staging tables)       │  │
│  └────────────────────────────┘  │
└──────────────────────────────────┘
```

---

## Pipeline 1: Order Lookup (Detailed Flow)

When a user asks about a specific order (e.g., *"Where is the shipment for St. Mary's Hospital?"*):

```
User Query (free text or structured fields)
    │
    ▼
┌─────────────────────────────────────┐
│ 1. INTENT CLASSIFICATION            │
│    Keyword heuristic (no LLM call)  │
│    → "order_lookup" or              │
│      "metric_query"                 │
└──────────────┬──────────────────────┘
               │ order_lookup
               ▼
┌─────────────────────────────────────┐
│ 2. CORTEX PARSE  (if free_text)     │
│    Snowflake Cortex COMPLETE()      │
│    mistral-7b                       │
│    NL → {order_id, customer_name,   │
│           facility_name, dates...}  │
└──────────────┬──────────────────────┘
               ▼
┌─────────────────────────────────────┐
│ 3. FUZZY CANDIDATE RETRIEVAL        │
│    FuzzyService (deterministic SQL) │
│                                     │
│    Exact path: order_id/PO match    │
│    Fuzzy path: token-LIKE scoring   │
│      • facility_name_norm tokens    │
│      • customer_name_norm tokens    │
│      • search_blob fallback         │
│      • date window filter           │
│      • recency + priority boosts    │
│                                     │
│    Query: ORDER_SEARCH_V            │
│    Limit: 200 candidates            │
└──────────────┬──────────────────────┘
               ▼
┌─────────────────────────────────────┐
│ 4. CORTEX RERANK                    │
│    Snowflake Cortex COMPLETE()      │
│    Sends top 20 candidates + query  │
│    → Returns ranked_ids + rationale │
│    GUARDRAIL: can only pick from    │
│    provided candidate IDs           │
└──────────────┬──────────────────────┘
               ▼
┌─────────────────────────────────────┐
│ 5. FINAL FETCH                      │
│    SELECT full payload for top N    │
│    from ORDER_SEARCH_V              │
│    → OrderStatusPayload per match   │
└──────────────┬──────────────────────┘
               ▼
┌─────────────────────────────────────┐
│ 6. EXPLAIN + TRACE                  │
│    ExplainService packages:         │
│    • candidate SQL, fetch SQL       │
│    • rerank rationale               │
│    • prompt versions                │
│    • Snowflake query IDs            │
│    • timing breakdown               │
│                                     │
│    Trace written to:                │
│    DEMO_BSC.DEMO_TRACE_LOG          │
└─────────────────────────────────────┘
```

---

## Pipeline 2: Metric Query (Detailed Flow)

When a user asks an analytical question (e.g., *"How many orders by status this quarter?"*):

```
User Query (natural language)
    │
    ▼
┌──────────────────────────────────────┐
│ 1. INTENT CLASSIFICATION             │
│    Keyword heuristic → "metric_query"│
└──────────────┬───────────────────────┘
               ▼
┌──────────────────────────────────────┐
│ 2. FETCH SEMANTIC METADATA (MCP)     │
│    DbtMcpService.list_metrics()      │
│    DbtMcpService.get_dimensions()    │
│    → Available metrics + dimensions  │
│    (cached 5 min)                    │
└──────────────┬───────────────────────┘
               ▼
┌──────────────────────────────────────┐
│ 3. CORTEX QUERY BUILDER              │
│    Snowflake Cortex COMPLETE()       │
│    mistral-7b                        │
│    NL + metrics catalog + dims       │
│    → {metrics, group_by, order_by,   │
│       where, limit}                  │
│    Normalizes types for MCP schema   │
└──────────────┬───────────────────────┘
               ▼
┌──────────────────────────────────────┐
│ 4. QUERY METRICS (MCP)               │
│    DbtMcpService.query_metrics()     │
│    → dbt Cloud compiles semantic SQL │
│    → Executes against Snowflake      │
│    → Returns tabular rows            │
└──────────────┬───────────────────────┘
               ▼
┌──────────────────────────────────────┐
│ 5. RESPONSE                          │
│    MetricResult:                     │
│    • columns, rows, row_count        │
│    • metrics_used, dimensions_used   │
│    • compiled_sql (optional)         │
└──────────────────────────────────────┘
```

---

## dbt MCP Integration (Deep Dive)

The dbt Cloud Semantic Layer is accessed via the **Model Context Protocol (MCP)** over Streamable HTTP transport:

```
┌─────────────────────────┐         ┌──────────────────────────────────┐
│  FastAPI (sync handlers) │         │  dbt Cloud                       │
│                          │         │                                  │
│  DbtMcpService           │         │  /api/ai/v1/mcp/                 │
│    │                     │         │                                  │
│    ├─ _McpLoop           │  MCP    │  ┌────────────────────────────┐  │
│    │   (background       │◄──────►│  │  Semantic Layer Engine     │  │
│    │    asyncio loop)    │ HTTP   │  │                            │  │
│    │                     │         │  │  Semantic Models:          │  │
│    │   ClientSession     │         │  │  • orders (fct_orders)     │  │
│    │    .call_tool()     │         │  │  • order_items             │  │
│    │                     │         │  │    (stg_order_items)       │  │
│    │                     │         │  │                            │  │
│    │  Threading model:   │         │  │  Metrics:                  │  │
│    │  sync wrapper calls │         │  │  • order_count             │  │
│    │  run_coroutine_     │         │  │  • revenue                 │  │
│    │  threadsafe() into  │         │  │  • average_order_value     │  │
│    │  background loop    │         │  │  • fulfilled_order_count   │  │
│    │                     │         │  │  • priority_order_count    │  │
│    └─ Cache (5m TTL):    │         │  │  • fulfillment_rate (derived) │
│       metrics, dims,     │         │  │  • priority_rate (derived) │  │
│       entities           │         │  │  • line_item_count         │  │
└─────────────────────────┘         │  │  • units_ordered           │  │
                                     │  │  • line_item_revenue       │  │
                                     │  └────────────────────────────┘  │
                                     │                                  │
                                     │  Compiles metric queries →       │
                                     │  Executes SQL on Snowflake →     │
                                     │  Returns result rows via MCP     │
                                     └──────────────────────────────────┘
```

**Key architectural detail:** The MCP connection runs on a dedicated background `asyncio` event loop in a daemon thread. The sync `call_tool()` wrapper uses `asyncio.run_coroutine_threadsafe()` to bridge the sync FastAPI handlers to the async MCP session. The session is persistent (not per-request), with a 15-second per-call timeout.

---

## dbt Data Model Layer

```
         RAW SOURCE TABLES (Snowflake: global_supply_chain.DEMO_BSC)
    ┌──────────────┬──────────────┬──────────────┬──────────────┬──────────────┐
    │ CUSTOMER_DIM │ FACILITY_DIM │ PRODUCT_DIM  │ ORDER_FACT   │ORDER_ITEM_   │
    │              │              │              │              │   FACT        │
    └──────┬───────┴──────┬───────┴──────┬───────┴──────┬───────┴──────┬───────┘
           │              │              │              │              │
           ▼              ▼              ▼              ▼              ▼
    ┌──────────────────────────────────────────────────────────────────────────┐
    │  STAGING LAYER (stg_*)                                                   │
    │  stg_customers · stg_facilities · stg_products · stg_orders ·            │
    │  stg_order_items · stg_contacts                                          │
    │  (normalization, type casting, naming conventions)                        │
    └──────────────────────────────────────────┬───────────────────────────────┘
                                               │
                                               ▼
    ┌──────────────────────────────────────────────────────────────────────────┐
    │  MARTS LAYER                                                             │
    │                                                                          │
    │  fct_orders (TABLE, clustered by date + status)                          │
    │    = stg_orders ⟕ stg_customers ⟕ stg_facilities                       │
    │    Denormalized: customer_name, facility_name, geo, fulfillment flags    │
    │                                                                          │
    │  order_search_v (VIEW over fct_orders)                                   │
    │    Adds: search_blob (concatenated normalized text for LIKE matching)     │
    │                                                                          │
    │  time_spine_daily (for MetricFlow time-series joins)                      │
    └──────────────────────────────────────────────────────────────────────────┘
                                               │
                                               ▼
    ┌──────────────────────────────────────────────────────────────────────────┐
    │  SEMANTIC MODELS (dbt Semantic Layer / MetricFlow)                        │
    │                                                                          │
    │  sem_orders (on fct_orders)          sem_order_items (on stg_order_items) │
    │    Entities: order, customer,          Entities: order_item, order,       │
    │              facility                               product              │
    │    Dimensions: 13 (categorical+time)   Dimensions: 3                     │
    │    Measures: 6                         Measures: 4                        │
    │    Metrics: 7 (5 simple, 2 derived)    Metrics: 3 (all simple)           │
    └──────────────────────────────────────────────────────────────────────────┘
```

---

## Snowflake Cortex Usage

Cortex is used exclusively through `SNOWFLAKE.CORTEX.COMPLETE()` SQL function calls routed through the existing Snowflake connection. Three distinct LLM tasks:

| Task | Prompt Template | Purpose | When Called |
|------|----------------|---------|-------------|
| `parse_user_input` | `_PARSE_PROMPT_TEMPLATE` or `_COMBINED_PARSE_TEMPLATE` | Extract structured fields from free text; classify intent | Free-text order lookups |
| `rerank_candidates` | `_RERANK_PROMPT_TEMPLATE` | Rank candidate orders by relevance to query | After fuzzy retrieval returns >0 non-exact candidates |
| `build_metric_query_params` | `_METRIC_QUERY_BUILDER_TEMPLATE` | Map NL question → `query_metrics` API params | Metric query pipeline |

**Model:** `mistral-7b` (configurable via `CORTEX_MODEL` env var)

**Guardrails:**
- Reranker can only select from provided candidate IDs (hallucinated IDs are filtered out)
- Schema allowlist prevents SQL access outside `DEMO_BSC`
- DML/DDL statements are blocked at the query layer
- All Cortex outputs are JSON-parsed with repair logic for truncated responses

---

## API Contract (Stable Interface)

```
FastAPI (port 8000)
│
├── GET  /health                    → {status, snowflake, dbt_cloud, semantic_backend}
│
├── POST /search/orders             → SearchResponse
│       Body: {mode, free_text?, fields?, top_n}
│       Returns: order_lookup results OR metric_query results
│
├── GET  /orders/{order_id}         → OrderStatusPayload
│       Direct single-order lookup
│
└── GET  /explain/{trace_id}        → ExplainResponse
        Full audit trail: SQL, prompts, timings, Snowflake query IDs
```

The API is designed as the **stable serving contract** — all current and future clients consume it identically.

---

## Deployment Architecture

```
docker-compose.yml
│
├── api  (FastAPI, port 8000)
│    ├── Healthcheck: GET /health every 30s
│    ├── Connects to: Snowflake (direct), dbt Cloud (MCP)
│    └── .env for credentials
│
└── ui   (Streamlit, port 8501)
     ├── Depends on: api (healthy)
     ├── API_BASE_URL=http://api:8000
     └── Displays: results, metrics, explain panel, performance traces
```

---

## Interoperability Capabilities NOT Currently Leveraged

The codebase has several integration points that are wired up but unused:

### 1. Agentforce Integration (Designed, Not Connected)
- `api/main.py` line 5: *"Agentforce (future)"*
- `semantic_service.py` line 4: *"Future Agentforce / Tableau Next clients call the same methods."*
- The API's `POST /search/orders` endpoint is explicitly designed as the Agentforce action target. The stable `SearchResponse` schema returns everything an agent needs: matched orders, match reasons, timing, and trace IDs.
- **What's needed:** Register the FastAPI endpoint as a Salesforce External Service, then expose it as an Agentforce Action.

### 2. Tableau Next Integration (Designed, Not Connected)
- Same stable API contract applies. Tableau Next could call `/search/orders` or consume metric results directly.
- The `MetricResult` schema (columns + rows) is already structured for tabular rendering.

### 3. dbt Semantic Layer `text_to_sql` Tool
- `DbtMcpService.text_to_sql(text)` is **implemented but never called** from any pipeline.
- This could enable a third pipeline: direct NL-to-SQL without going through the Cortex query builder, fully delegating SQL generation to dbt Cloud's AI.

### 4. dbt Semantic Layer `get_compiled_sql` Tool
- `DbtMcpService.get_compiled_sql(...)` is **implemented but never called** in the metric pipeline.
- The `MetricResult.compiled_sql` field exists in the response schema but is always `None`.
- Could be used to show users the exact SQL that dbt generated, improving transparency.

### 5. Semantic Context Enrichment for Order Lookup
- `_handle_order_lookup` calls `dbt_mcp.get_semantic_context_for_search()` to fetch metric names, but only uses them for the explain artifact — they don't influence the actual search logic.
- **Opportunity:** Use semantic metadata to enhance fuzzy matching or provide metric-aware context in the reranker prompt.

### 6. Combined Parse Template (Partially Used)
- `_COMBINED_PARSE_TEMPLATE` exists to do intent classification + metric param building in a single Cortex call, but the metric pipeline currently uses the two-step approach: keyword classification → separate `build_metric_query_params` call.
- The single-call path via `parse_user_input(text, available_metrics, available_dimensions)` is implemented but only invoked for order lookups, not metrics.

### 7. Entity Relationships via MCP
- `DbtMcpService.get_entities(metrics)` is implemented with caching, but **never called** outside the service itself.
- The `order_items` semantic model defines entity relationships (order_item → order → product) that could enable cross-model queries (e.g., "top products by revenue" joining orders and line items).

### 8. Evaluation Harness (Built, Manual Only)
- `evaluation/run_eval.py` is a complete eval framework with golden prompts and expected results.
- Not integrated into CI/CD — runs manually against a live API.
- Could be wired into a GitHub Actions workflow for regression testing.

### 9. CORS Wildcard → Production Lock-down
- CORS is set to `allow_origins=["*"]` — appropriate for demo, but would need scoping for production deployment with Agentforce/Tableau.

---

## Component Dependency Map

```
                    SemanticService
                   /    |    |     \
                  /     |    |      \
     FuzzyService  CortexService  DbtMcpService  ExplainService
                        |              |
                   SnowflakeService    |
                        |              |
                   ┌────┴────┐    ┌────┴────────┐
                   │Snowflake│    │dbt Cloud MCP │
                   │ Cortex  │    │Semantic Layer│
                   │ Tables  │    └──────────────┘
                   └─────────┘
```

All services are instantiated as singletons via `deps.py` and injected through FastAPI's dependency system. The `SemanticService` is the only orchestrator — no service-to-service calls exist outside of it.
