# Redesign: Streamlit-in-Snowflake Agent over dbt Semantic Layer

## Context

A working demo exists in this repo: a FastAPI + local-Streamlit app that lets users ask natural-language questions about Boston Scientific orders. The current architecture is sound for a one-off demo (well-shaped pipelines, real Cortex calls, real dbt SL via MCP) but takes shortcuts that block the path to production: it runs in Docker locally, authenticates via password, hits dbt Cloud at runtime for every metric query, has no feedback capture, and embeds business glossary as Python constants.

We're redesigning so the same demo runs **fully on Snowflake infra** (Streamlit-in-Snowflake + Cortex), uses the **dbt Semantic Layer to compile SQL but routes execution directly to the warehouse**, and ships with the four production-shaped concepts you care about as first-class primitives: **curated context, bounded skills, explicit guardrails, and a Snowflake-native feedback loop**. POC scope, but the framework should survive contact with real users.

Confirmed user decisions (from clarifying questions):
1. Kill FastAPI; collapse everything into the SiS app.
2. Cortex Complete + our own router (not Cortex Agents).
3. Capture-only feedback in v1 + scheduled evals via Snowflake Tasks.
4. Compile via SL SDK, execute on Snowflake direct.

## Architecture Overview

```
┌──────────────────────────────────────────────────────────────────┐
│                 Streamlit-in-Snowflake (single app)              │
│                                                                  │
│  pages/             agent/                                       │
│  ├─ chat            ├─ context/    (curated semantic catalog)    │
│  ├─ traces          ├─ router/     (LLM skill router)            │
│  └─ eval            ├─ skills/     (bounded recipes)             │
│                     ├─ guardrails/ (pre/post validators)         │
│                     ├─ semantic/   (dbt-sl-sdk wrapper)          │
│                     ├─ cortex/     (Cortex Complete client)      │
│                     ├─ feedback/   (trace + thumbs writers)      │
│                     └─ eval/       (golden-set runner)           │
│                                                                  │
│   ▲                                                              │
│   │ Snowpark Session (in-process)                                │
│   ▼                                                              │
│  ┌──────────────────┐    ┌──────────────────┐                    │
│  │ Cortex Complete  │    │ SQL execution    │                    │
│  │ (LLM)            │    │ (warehouse)      │                    │
│  └──────────────────┘    └──────────────────┘                    │
│                                                                  │
│   ▲ External Access Integration                                  │
│   │ semantic-layer.cloud.getdbt.com                              │
│   ▼                                                              │
│  ┌──────────────────────────────────────────┐                    │
│  │ dbt Semantic Layer (compile only)        │                    │
│  │ • catalog metadata (eager-loaded)        │                    │
│  │ • compile_sql() → MetricFlow SQL         │                    │
│  └──────────────────────────────────────────┘                    │
│                                                                  │
│  Snowflake tables (state):                                       │
│  ├─ DEMO_BSC.AGENT_TRACE          (every turn)                   │
│  ├─ DEMO_BSC.AGENT_FEEDBACK       (thumbs/corrections)           │
│  ├─ DEMO_BSC.AGENT_GOLDEN         (eval set)                     │
│  ├─ DEMO_BSC.AGENT_EVAL_RUNS      (per-run x prompt outcomes)    │
│  ├─ DEMO_BSC.AGENT_GLOSSARY       (curated business terms)       │
│  └─ DEMO_BSC.SEMANTIC_CATALOG_CACHE (materialized SL catalog)    │
└──────────────────────────────────────────────────────────────────┘
```

**Request flow** (one turn):
1. User input → SiS app stores in `st.session_state.history`.
2. `ContextBuilder` assembles a `ContextPack`: curated catalog + glossary + last 3 turns + user identity.
3. `SkillRouter.route(turn, ctx)` calls Cortex Complete with the skill catalog; parses + validates JSON.
4. Selected `Skill.validate(slots, ctx)` checks slots against catalog allowlists.
5. `Skill.execute(slots, ctx)` runs:
   - Order lookup: deterministic SQL → Cortex rerank → fetch.
   - Metric query: `SLClient.compile_sql(...)` → `SnowflakeExecutor.run(sql)`.
6. `Skill.present(result, ctx)` renders. Trace row written. UI shows result + 👍👎 + "edit" affordance.
7. Feedback writes to `AGENT_FEEDBACK` keyed on `trace_id`.

## Architecture decisions (with rationale)

### A1. SiS only, no FastAPI
Justified: Streamlit reruns in-process and SiS gives Snowpark `Session` for free, removing the need for a separate orchestration tier. Future non-Snowflake clients (Agentforce, Tableau Next) can wrap the `agent/` package in a thin FastAPI later — building it now is YAGNI. Throw away `api/main.py`, `api/routers/*`, `docker-compose.yml`, `ui/`.

### A2. SPCS-hosted Streamlit + dbt-sl-sdk
The classic SiS warehouse runtime is restricted to the Snowflake Anaconda channel, which does **not** include `dbt-sl-sdk`. So we move the Streamlit app to **Snowpark Container Services**: a Docker image we control with full pip access, deployed as a service against a compute pool.

Justified:
- We get the SDK's eager catalog loading + ergonomic `compile_sql`/`query` methods without writing the GraphQL plumbing ourselves.
- SPCS containers give us a real filesystem for caching (pickled catalog, prompt-template files, etc.) that persists across container restarts via mounted block storage.
- Container memory/CPU is sized for the workload (SiS classic uses warehouse memory which can be wasteful or starved depending on warehouse size).
- Long-term we can add sidecar processes (e.g., a metric-cache pre-warmer) without changing the deploy model.

Tradeoffs we're accepting:
- More infra: image registry, compute pool, service spec, image build/push.
- Compute pool spend is separate from warehouse spend.
- Slightly more deploy friction than `snow streamlit deploy` for classic SiS.

**Wrap behind an interface anyway**: `agent/semantic/sl_client.py` exposes `list_metrics`, `get_dimensions`, `get_entities`, `compile_sql` regardless of transport. The SDK is the v1 implementation, but if we ever revisit (e.g., dbt-sl-sdk drops a feature, or the SL API surface changes), the swap stays cheap.

**Caching pattern in SPCS**:
- In-memory: `@st.cache_resource` for the `SemanticLayerClient` instance and the catalog dataclass — survives reruns within a session.
- On-disk: pickled catalog at `/data/catalog_cache.pkl` (volume-mounted) — survives container restart.
- Snowflake table: `SEMANTIC_CATALOG_CACHE` for cross-instance consistency and as the source of truth for the eval runner stored proc, which can't import the SDK from inside Snowflake's stored-proc runtime.

### A3. Compile via SL, execute on Snowflake direct
Justified: kills the dbt Cloud round-trip on every metric query (~500–1700ms savings) and lets Snowflake's caching + warehouse acceleration apply. dbt Cloud becomes a metadata/compile-time dependency, not a runtime executor.

**Caveat to handle**: the compiled MetricFlow SQL references fully-qualified analytics tables that likely live **outside** `DEMO_BSC` (they're produced by your real dbt project). Our schema-allowlist guardrail (`api/services/snowflake_service.py:_assert_schema_safe` lines 126–155) currently blocks this. We'll extend the allowlist to include the analytics database/schema where the dbt models materialize, while keeping the DML/DDL block.

### A4. Cortex Complete + our own router
Justified: maximum control over skill routing for guardrails, prompt versioning, and A/B eval. We pay the cost of writing a router, but it's small (one Cortex call returning a JSON envelope `{skill, slots, rationale}` validated against an allowlist).

**Model upgrade**: replace `mistral-7b` (current default in `api/core/config.py:16`) with **Llama 3.3 70B** as the router model — JSON tool-calling fidelity at mistral-7b is poor and is the reason `_repair_json` exists. Skills can keep using a smaller model for narrow extraction tasks where reliability is high.

### A5. Snowflake-native state (no external store)
Trace, feedback, golden, and eval-run tables live in `DEMO_BSC`. Eval runs scheduled via `CREATE TASK` calling a Python stored procedure that imports the `agent` package directly. No CI/CD dependency for the eval loop — Snowflake is the substrate.

## Module layout

```
app/                                    # SiS deploy root
  app.py                                # entry: page config, session bootstrap
  environment.yml                       # SiS conda packages
  packages.yml                          # SiS extra packages (dbt-sl-sdk if available)
  pages/
    01_chat.py                          # primary chat UI + thumbs/corrections
    02_traces.py                        # AGENT_TRACE browser, replay-from-trace
    03_eval.py                          # read-only AGENT_EVAL_RUNS dashboard
  agent/
    config.py                           # settings (no secrets)
    secrets.py                          # os.getenv('DBT_CLOUD_TOKEN') + local fallback
    session.py                          # @st.cache_resource get_session()
    types.py                            # AgentTurn, ContextPack, ToolCall, SkillResult
    context/
      builder.py                        # ContextBuilder.build(turn, history) -> ContextPack
      catalog.py                        # SemanticCatalog (reads SEMANTIC_CATALOG_CACHE)
      glossary.py                       # reads AGENT_GLOSSARY (status_values, business_terms)
    cortex/
      client.py                         # CortexClient.complete(prompt, model, label)
      json_repair.py                    # ported _repair_json + _extract_json_block
    semantic/
      sl_client.py                      # SLClient — wraps dbt-sl-sdk SemanticLayerClient
      executor.py                       # SnowflakeExecutor.run(sql) — Snowpark + sql_guard
    router/
      router.py                         # SkillRouter.route() -> SkillCall
      registry.py                       # SkillRegistry — name -> Skill
    skills/
      base.py                           # Skill protocol + SlotSpec + SkillResult
      order_lookup.py                   # OrderLookupSkill (port of Pipeline 1)
      metric_query.py                   # MetricQuerySkill (compile-then-execute)
      metric_compare.py                 # MetricCompareSkill (new — period/segment delta)
      clarify.py                        # ClarifySkill (router fallback when ambiguous)
      _fuzzy.py                         # private helper (port of FuzzyService)
    guardrails/
      sql_guard.py                      # ported _assert_schema_safe with extended allowlist
      slot_validators.py                # date ranges, enum membership, metric/dim allowlist
      output_guard.py                   # row caps, redaction hooks, confidence escalation
    feedback/
      trace_writer.py                   # write AGENT_TRACE row
      feedback_writer.py                # write AGENT_FEEDBACK row
    eval/
      runner.py                         # in-process EvalRunner -> AGENT_EVAL_RUNS
      promoter.py                       # AGENT_FEEDBACK -> AGENT_GOLDEN (manual SQL stub in v1)
```

## Skills v1

The `Skill` protocol (`agent/skills/base.py`):

```python
class Skill(Protocol):
    name: str                       # e.g. "metric_query"
    version: str                    # bumped per prompt/code change; logged in trace
    description: str                # LLM-facing capability paragraph
    slot_schema: type[SlotSpec]     # Pydantic subclass

    def validate(self, slots: SlotSpec, ctx: ContextPack) -> list[str]: ...
    def execute(self, slots: SlotSpec, ctx: ContextPack) -> SkillResult: ...
    def present(self, result: SkillResult, ctx: ContextPack) -> None: ...  # st.* renders
```

### `OrderLookupSkill` (port from Pipeline 1)
- Slot schema: `order_id`, `purchase_order_id`, `customer_name`, `facility_name`, `status`, `date_start`, `date_end`, `free_text_residual`, `top_n`.
- Validate: `status` in `ctx.glossary.status_values`; `date_end >= date_start`.
- Execute: ports `FuzzyService.build_candidate_query` (`api/services/fuzzy_service.py`) lift-and-shift; uses Cortex rerank prompt (port of `_RERANK_PROMPT_TEMPLATE` from `api/services/cortex_service.py`) only when no exact ID match.
- Present: `st.dataframe` of matches + expander showing rerank rationale.

### `MetricQuerySkill` (port from Pipeline 2, **architecturally changed**)
- Slot schema: `question`, `metrics: list[str]`, `group_by: list[GroupBySpec]`, `order_by`, `where`, `limit`.
- Validate: every `metric` in `ctx.catalog.metric_names`; every `group_by.name` in `ctx.catalog.dimension_names`; grain only on `time_dimension` slots.
- Execute: 
  1. `compiled_sql = SLClient.compile_sql(metrics, group_by, where, order_by, limit)` (no execute!)
  2. `result = SnowflakeExecutor.run(compiled_sql)` — direct warehouse execution.
- Present: `st.dataframe` + `st.line_chart` for time grouping + expander showing compiled MetricFlow SQL.

### `MetricCompareSkill` (new — proves the framework)
- Slot schema: `metric: str`, `compare_dim: GroupBySpec`, `period_a: PeriodSpec`, `period_b: PeriodSpec`, `filter`.
- Execute: two `compile_sql` calls (one per period) → two warehouse executions → in-Python merge on `compare_dim.name` → compute delta + pct_change.
- Present: side-by-side table + bar chart of deltas + expander with both SQL strings.
- Demonstrates that skills are not thin LLM wrappers; they own multi-step orchestration.

### `ClarifySkill` (router fallback)
- Triggered when router returns invalid JSON, unknown skill name, or low-confidence routing.
- Asks one disambiguation question; never executes anything.

## Context as a first-class concept

Two layers, both eager-loaded:

**Static catalog** (reloads nightly via Snowflake Task):
- `SEMANTIC_CATALOG_CACHE` table: one row per metric/dimension/entity, populated by a stored proc that calls SL `list_metrics`/`get_dimensions`/`get_entities` and writes to the table.
- App reads on first request via `@st.cache_resource(ttl=3600)`. Live SL hits limited to a manual "refresh" button + per-query `compile_sql`.
- This shifts SL latency off the hot path.

**Curated business glossary** (`AGENT_GLOSSARY` table — replaces hardcoded constants):
- Currently in `api/services/dbt_mcp_service.py` lines 337–353: status enums, business term definitions, entity relationships.
- Migrate to a YAML seed loaded into `AGENT_GLOSSARY` so SMEs can edit without a code change.
- Long-term ambition: push these into the dbt SL `meta` blocks. v2 task; v1 keeps them in the table.

**Conversation context** (per session):
- `st.session_state.history`: last N turns (input, routed_skill, slots, result_summary).
- `ContextBuilder.build(turn, history)` produces a single `ContextPack` consumed by router and skills — one source of truth for what the LLM "sees."
- Cross-session continuity (returning user, shared link) reads from `AGENT_TRACE` by `trace_id` query param. `session_state` holds only the active conversation buffer.

## Guardrails

**Pre-execution** (in `agent/guardrails/`):
- `sql_guard.assert_safe(sql)`: ported `_assert_schema_safe` (`api/services/snowflake_service.py:126–155`); extended allowlist for the analytics database where dbt-built models live.
- `slot_validators`: every slot's value must be in the catalog (metric exists, dimension exists, status enum valid, date range bounded).
- Skill-name allowlist: router output's `skill_name` must match a registered skill; otherwise route to `ClarifySkill`.

**Execution**:
- Snowpark `Session.sql(...)` always with parameter binding (no string interpolation).
- Per-skill row cap (configurable via `AGENT_CONFIG` table — runtime-reloadable, no redeploy).
- Per-skill query timeout (Snowpark statement timeout).

**Post-execution**:
- Result row count cap (truncate + flag in trace).
- Confidence scoring: skill computes a self-score (e.g. order rerank confidence, metric query coverage); below threshold escalates to "I'm not sure — here are 3 interpretations" pattern using `ClarifySkill`-style UI.
- PII redaction hook (no-op in v1; placeholder for production).

**Escalation paths**:
- Validation failure → `ClarifySkill` with the validation error in the prompt.
- Execution error → render error + log trace with `status='error'`; do not retry silently.
- Low confidence → render result with a banner: "I picked X but wasn't sure between X and Y. Was this right?"

## Feedback + eval pipeline

### Snowflake schema (DDL in `infra/sql/`)

```sql
-- AGENT_TRACE: one row per turn
CREATE TABLE DEMO_BSC.AGENT_TRACE (
  trace_id           STRING        PRIMARY KEY,
  session_id         STRING        NOT NULL,
  user_email         STRING,
  created_at         TIMESTAMP_TZ  DEFAULT CURRENT_TIMESTAMP(),
  user_input         STRING,
  routed_skill       STRING,
  router_raw         VARIANT,
  slots              VARIANT,
  context_pack_hash  STRING,
  prompt_version     STRING,
  model_version      STRING,
  skill_version      STRING,
  compiled_sql       STRING,
  executed_sql_hash  STRING,
  snowflake_qid      STRING,
  result_summary     VARIANT,
  timings_ms         VARIANT,
  total_ms           NUMBER(10,1),
  error              STRING,
  status             STRING                                  -- 'ok'|'error'|'clarify'
);

-- AGENT_FEEDBACK: 1:N to trace (revisions allowed)
CREATE TABLE DEMO_BSC.AGENT_FEEDBACK (
  feedback_id        STRING        PRIMARY KEY,
  trace_id           STRING        NOT NULL,
  created_at         TIMESTAMP_TZ  DEFAULT CURRENT_TIMESTAMP(),
  user_email         STRING,
  rating             STRING,                                 -- 'up'|'down'
  correction_text    STRING,
  corrected_slots    VARIANT,
  expected_skill     STRING,
  notes              STRING
);

-- AGENT_GOLDEN: promoted ground truth
CREATE TABLE DEMO_BSC.AGENT_GOLDEN (
  golden_id              STRING       PRIMARY KEY,
  source_trace_id        STRING,
  source_feedback_id     STRING,
  promoted_at            TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
  promoted_by            STRING,
  user_input             STRING       NOT NULL,
  expected_skill         STRING       NOT NULL,
  expected_slots         VARIANT,
  expected_checks        VARIANT,                            -- ports existing checks JSON
  tags                   ARRAY,
  active                 BOOLEAN      DEFAULT TRUE,
  prompt_version_at_promotion STRING
);

-- AGENT_EVAL_RUNS: long-format (run x prompt)
CREATE TABLE DEMO_BSC.AGENT_EVAL_RUNS (
  run_id           STRING        NOT NULL,
  golden_id        STRING        NOT NULL,
  ran_at           TIMESTAMP_TZ  DEFAULT CURRENT_TIMESTAMP(),
  prompt_version   STRING,
  model_version    STRING,
  skill_version    STRING,
  routed_skill     STRING,
  passed           BOOLEAN,
  check_details    VARIANT,
  latency_ms       NUMBER(10,1),
  error            STRING,
  trace_id         STRING,
  PRIMARY KEY (run_id, golden_id)
);

CREATE VIEW DEMO_BSC.AGENT_EVAL_SUMMARY AS
SELECT run_id, prompt_version, model_version, skill_version,
       COUNT(*) AS total, SUM(IFF(passed,1,0)) AS passed,
       APPROX_PERCENTILE(latency_ms, 0.5) AS p50_ms,
       APPROX_PERCENTILE(latency_ms, 0.95) AS p95_ms
FROM DEMO_BSC.AGENT_EVAL_RUNS GROUP BY 1,2,3,4;

-- Bonus: AGENT_GLOSSARY + AGENT_CONFIG
CREATE TABLE DEMO_BSC.AGENT_GLOSSARY (
  term            STRING PRIMARY KEY,
  category        STRING,                                    -- 'status_value'|'business_term'|'entity_rel'
  definition      STRING,
  metadata        VARIANT,
  active          BOOLEAN DEFAULT TRUE
);

CREATE TABLE DEMO_BSC.AGENT_CONFIG (
  key             STRING PRIMARY KEY,
  value           VARIANT,
  description     STRING
);
```

### Feedback UX

Every result in `01_chat.py` ships with:
- 👍 / 👎 buttons writing `rating` to `AGENT_FEEDBACK` linked by `trace_id`.
- "Was the interpretation right?" expander showing the routed skill + slots; user can click "edit" → reveals a free-text correction box and (for slot-fixable cases) form fields to amend specific slots → writes `correction_text` and/or `corrected_slots`.
- "Save as eval example" → writes a `AGENT_FEEDBACK` row with `rating='up'` and a flag we can filter on for promotion.

### Eval automation

- **Runner**: `agent/eval/runner.py` — `EvalRunner.run(golden_ids=None)`. Reads `AGENT_GOLDEN WHERE active=TRUE`; invokes the `agent` package in-process (no httpx, no localhost API); writes `AGENT_EVAL_RUNS`. Reuses `evaluation/run_eval.py:evaluate_checks` (lines 54–118) and the metric computation (lines 206–215) — those are pure functions and worth keeping.
- **Schedule**: a Python stored procedure `RUN_NIGHTLY_EVAL()` imports the `agent` package and calls `EvalRunner.run()`. Fired by:
  ```sql
  CREATE TASK NIGHTLY_AGENT_EVAL
    WAREHOUSE = DEMO_WH
    SCHEDULE = 'USING CRON 0 4 * * * America/Los_Angeles'
  AS CALL DEMO_BSC.RUN_NIGHTLY_EVAL();
  ```
- **Regression detection**: a second view + Task scans `AGENT_EVAL_SUMMARY` for prompt-version regressions and inserts into `AGENT_INCIDENTS` (post-v1).

### Promotion (manual in v1)

A reviewer runs SQL like:
```sql
INSERT INTO DEMO_BSC.AGENT_GOLDEN (golden_id, source_feedback_id, user_input, expected_skill, expected_slots, expected_checks, promoted_by, prompt_version_at_promotion)
SELECT UUID_STRING(), f.feedback_id, t.user_input, t.routed_skill, t.slots, ARRAY_CONSTRUCT(...), CURRENT_USER(), t.prompt_version
FROM AGENT_FEEDBACK f JOIN AGENT_TRACE t USING (trace_id)
WHERE f.feedback_id = '...';
```

v2 task: a Streamlit reviewer page that wraps this in a UI with HITL approval queues.

## Snowflake plumbing (handled by `infra/setup.sh`)

Key DDL the script runs:

```sql
-- 04_secrets_eai.sql (templated; ${DBT_CLOUD_TOKEN} from env)
CREATE OR REPLACE SECRET DEMO_BSC.DBT_CLOUD_TOKEN
  TYPE = GENERIC_STRING SECRET_STRING = '${DBT_CLOUD_TOKEN}';

CREATE OR REPLACE NETWORK RULE DEMO_BSC.DBT_CLOUD_RULE
  TYPE = HOST_PORT MODE = EGRESS
  VALUE_LIST = ('${DBT_SL_HOST}:443', 'metadata.cloud.getdbt.com:443');

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION DBT_CLOUD_EAI
  ALLOWED_NETWORK_RULES = (DEMO_BSC.DBT_CLOUD_RULE)
  ALLOWED_AUTHENTICATION_SECRETS = (DEMO_BSC.DBT_CLOUD_TOKEN)
  ENABLED = TRUE;

-- 05_compute_pool.sql
CREATE COMPUTE POOL IF NOT EXISTS DEMO_AGENT_POOL
  MIN_NODES = 1 MAX_NODES = 2
  INSTANCE_FAMILY = CPU_X64_S
  AUTO_RESUME = TRUE
  AUTO_SUSPEND_SECS = 600;

-- 06_image_repo.sql
CREATE IMAGE REPOSITORY IF NOT EXISTS DEMO_BSC.AGENT_REPO;
```

In the SPCS container, the dbt token is available as an env var via the secret injection in `infra/spec/streamlit_service.yaml` (the `secrets` block). `agent/secrets.py` reads `os.getenv('DBT_CLOUD_TOKEN')` — no `_snowflake` module needed since SPCS injects secrets as env vars rather than via the SiS-classic `_snowflake` API.

## Infra setup script

A single shell script at `infra/setup.sh` provisions everything via the Snowflake CLI (`snow`) plus Docker for the image build. Idempotent — safe to re-run.

```
infra/
  setup.sh                              # entry point — orchestrates the rest
  teardown.sh                           # symmetric removal
  sql/
    01_database_schema.sql              # CREATE DATABASE/SCHEMA
    02_warehouse.sql                    # CREATE WAREHOUSE DEMO_WH
    03_tables.sql                       # AGENT_TRACE, AGENT_FEEDBACK, AGENT_GOLDEN,
                                        #   AGENT_EVAL_RUNS, AGENT_GLOSSARY, AGENT_CONFIG,
                                        #   SEMANTIC_CATALOG_CACHE
    04_secrets_eai.sql                  # SECRET, NETWORK RULE, EAI (templated)
    05_compute_pool.sql                 # CREATE COMPUTE POOL for SPCS
    06_image_repo.sql                   # CREATE IMAGE REPOSITORY
    07_seed_glossary.sql                # AGENT_GLOSSARY rows from dbt_mcp_service.py:337–353
    08_seed_golden.sql                  # AGENT_GOLDEN rows from evaluation/datasets/*.jsonl
    09_stored_procs.sql                 # REFRESH_SEMANTIC_CATALOG, RUN_NIGHTLY_EVAL
    10_tasks.sql                        # NIGHTLY_AGENT_EVAL, REFRESH_CATALOG_DAILY
    11_service.sql                      # CREATE SERVICE for the SPCS-hosted Streamlit
                                        #   (binds EAI + secret + compute pool)
  spec/
    streamlit_service.yaml              # SPCS service spec — image, ports, volumes, EAI
  app/
    Dockerfile                          # python:3.11-slim + requirements.txt + app code
    requirements.txt                    # streamlit, dbt-sl-sdk, snowflake-snowpark-python,
                                        #   pyarrow, pandas, pydantic, requests, cachetools
    app.py
    pages/...
    agent/...
```

`setup.sh` flow:

```bash
#!/usr/bin/env bash
set -euo pipefail

: "${DBT_CLOUD_TOKEN:?must be set in env}"
: "${DBT_SL_HOST:=semantic-layer.cloud.getdbt.com}"  # override per region
: "${SF_ACCOUNT:?}"; : "${SF_ROLE:?}"

# 1. Snowflake objects (DDL + secrets + EAI + compute pool + image repo)
snow sql -f infra/sql/01_database_schema.sql
snow sql -f infra/sql/02_warehouse.sql
snow sql -f infra/sql/03_tables.sql
envsubst < infra/sql/04_secrets_eai.sql | snow sql -i
snow sql -f infra/sql/05_compute_pool.sql
snow sql -f infra/sql/06_image_repo.sql

# 2. Build + push the SPCS image
REGISTRY_URL=$(snow sql -q "SHOW IMAGE REPOSITORIES IN SCHEMA DEMO_BSC" \
  --format json | jq -r '.[] | select(.name=="AGENT_REPO") | .repository_url')
docker build -t "${REGISTRY_URL}/agent_app:latest" -f app/Dockerfile app/
snow spcs image-registry login
docker push "${REGISTRY_URL}/agent_app:latest"

# 3. Seeds + procs + tasks
snow sql -f infra/sql/07_seed_glossary.sql
snow sql -f infra/sql/08_seed_golden.sql
snow sql -f infra/sql/09_stored_procs.sql
snow sql -f infra/sql/10_tasks.sql

# 4. Create or replace the SPCS service from the spec
envsubst < infra/spec/streamlit_service.yaml > /tmp/service.yaml
snow spcs service create AGENT_APP \
  --compute-pool DEMO_AGENT_POOL \
  --spec-path /tmp/service.yaml \
  --replace --query-warehouse DEMO_WH \
  --external-access-integrations DBT_CLOUD_EAI

# 5. Print the public endpoint
snow sql -q "DESC SERVICE DEMO_BSC.AGENT_APP" --format json | jq '.public_endpoints'
echo "✅ Service running. Open the public URL printed above."
```

The service spec (`infra/spec/streamlit_service.yaml`) skeleton:

```yaml
spec:
  containers:
    - name: agent
      image: /demo_db/demo_bsc/agent_repo/agent_app:latest
      env:
        DBT_SL_HOST: ${DBT_SL_HOST}
        DBT_ENVIRONMENT_ID: ${DBT_ENVIRONMENT_ID}
      secrets:
        - snowflakeSecret: DEMO_BSC.DBT_CLOUD_TOKEN
          envVarName: DBT_CLOUD_TOKEN
      volumeMounts:
        - name: catalog-cache
          mountPath: /data
  endpoints:
    - name: streamlit
      port: 8501
      public: true
  volumes:
    - name: catalog-cache
      source: block
      size: 5Gi
```

Notes:
- The token is referenced via SPCS's secret-injection mechanism — it lands as an env var in the container, not embedded in the image.
- `volumes.source: block` gives us a 5Gi persistent disk for the pickled catalog cache.
- EAI binding lives in the `snow spcs service create --external-access-integrations` flag, not in the spec.
- `infra/teardown.sh` reverses: drop service → drop image repo → drop compute pool → drop EAI/secret → drop warehouse → drop database.

## Implementation phases

Sequential milestones; each ends in a working, demoable state.

**Phase 1 — Infra script + Snowflake substrate + image plumbing**
- Author `infra/setup.sh`, `infra/teardown.sh`, the `infra/sql/*.sql` files, and `infra/spec/streamlit_service.yaml`.
- Author `app/Dockerfile` and `app/requirements.txt` (with `dbt-sl-sdk`).
- Run end-to-end against a dev account: verify database, tables, secret + EAI, compute pool, image repo, image build/push, service create, and a stub Streamlit app reachable on the public endpoint.
- Seed `AGENT_GLOSSARY` from `api/services/dbt_mcp_service.py:337–353`.
- Bulk-load `evaluation/datasets/*.jsonl` into `AGENT_GOLDEN` (one row per prompt with `expected_checks` carrying the existing checks JSON).
- Build the catalog-refresh stored proc (Python stored proc that calls dbt SL GraphQL via `requests` — stored procs cannot import `dbt-sl-sdk` from outside Anaconda, so the *proc* uses raw GraphQL while the *app* uses the SDK).

**Phase 2 — Agent core package** (`app/agent/*`, no UI yet)
- Port `FuzzyService` → `agent/skills/_fuzzy.py`.
- Port Cortex client + `_repair_json` → `agent/cortex/`.
- Implement `SLClient` (dbt-sl-sdk wrapper or GraphQL fallback) → `agent/semantic/sl_client.py`.
- Implement `SnowflakeExecutor` with extended `sql_guard` allowlist → `agent/semantic/executor.py`.
- Implement `ContextBuilder`, `SemanticCatalog`, `Glossary` → `agent/context/`.
- Implement `Skill` protocol + `OrderLookupSkill` + `MetricQuerySkill` + `ClarifySkill` → `agent/skills/`.
- Implement `SkillRouter` + `SkillRegistry` → `agent/router/`.
- Implement `TraceWriter` and `FeedbackWriter` → `agent/feedback/`.
- Cover with unit tests where pure (validators, json_repair, fuzzy normalize). Integration tests deferred to Phase 4.

**Phase 3 — Streamlit app pages** (`app/app.py`, `app/pages/*`)
- `01_chat.py`: chat + thumbs/correction UI. Replaces `ui/`.
- `02_traces.py`: trace browser + replay-from-trace_id deep links.
- `03_eval.py`: read-only `AGENT_EVAL_SUMMARY` dashboard.
- Bake into the SPCS image via `app/Dockerfile`; redeploy by running the image-build + push + `snow spcs service upgrade` steps in `setup.sh`.
- Smoke-test that the catalog cache pickles to `/data/` and survives a `snow spcs service suspend/resume` cycle.

**Phase 4 — Eval automation**
- Port `evaluation/run_eval.py` → `agent/eval/runner.py`. Reuse `evaluate_checks` and metric computation.
- Bulk-load `evaluation/datasets/golden_prompts.jsonl` + `expected_results.jsonl` into `AGENT_GOLDEN`.
- Wrap runner in a Python stored proc; create `NIGHTLY_AGENT_EVAL` task.
- Smoke-test by running once manually and inspecting `AGENT_EVAL_SUMMARY`.

**Phase 5 — Polish**
- Add `MetricCompareSkill` to demonstrate framework extensibility.
- Add the "Save as eval example" affordance.
- Tune router prompt against the golden set; promote to a stable `prompt_version`.

## Critical files to reference / port / discard

### Port (rewrite-with-purpose)
- `api/services/cortex_service.py:365–381` (`_complete`) → `agent/cortex/client.py`. Keep `_repair_json` (337–359), `_extract_json_block` (395–403), `_extract_content` (383–393).
- `api/services/cortex_service.py` prompt templates → split: `_PARSE_PROMPT_TEMPLATE` becomes router prompt; `_RERANK_PROMPT_TEMPLATE` stays in `OrderLookupSkill`; `_METRIC_QUERY_BUILDER_TEMPLATE` folds into `MetricQuerySkill`.
- `api/services/snowflake_service.py:126–155` (`_assert_schema_safe`) → `agent/guardrails/sql_guard.py` with extended allowlist.
- `api/services/dbt_mcp_service.py:337–353` (hardcoded glossary) → `AGENT_GLOSSARY` table seed.
- `api/services/dbt_mcp_service.py:208–308` (catalog/query/compile methods) → `agent/semantic/sl_client.py`, rewritten as a `dbt-sl-sdk` `SemanticLayerClient` wrapper (sync). MCP transport machinery deleted.
- `api/services/semantic_service.py` orchestration → split between `agent/router/router.py` and the skills. Class itself dies.
- `evaluation/run_eval.py:54–118` (`evaluate_checks`) and `206–215` (metric computation) → `agent/eval/runner.py` — these are pure, lift verbatim.

### Lift (light edits only)
- `api/services/fuzzy_service.py` → `agent/skills/_fuzzy.py`.
- `api/core/timing.py` → `agent/types.py` (Timer class).
- `dbt/models/semantic_models/sem_orders.yml` and `sem_order_items.yml` — keep as-is.
- `evaluation/datasets/golden_prompts.jsonl` and `expected_results.jsonl` — bulk-load into `AGENT_GOLDEN`.

### Discard
- `api/main.py`, `api/routers/*`, `api/core/errors.py` — entire FastAPI surface.
- `api/services/dbt_mcp_service.py:_McpLoop` (52–148) — MCP transport, unused.
- `api/services/explain_service.py` — replaced by reading `AGENT_TRACE` directly.
- `ui/app.py`, `ui/components/*` — rewrite as native SiS pages.
- `docker-compose.yml`, `infra/Dockerfile*` — irrelevant in SiS.
- `api/core/config.py` `pydantic-settings` usage — replace with simple module reading `_snowflake.get_generic_secret_string` + `st.secrets`.
- The keyword-based `_classify_intent` (`api/services/semantic_service.py:143–162`) — kill it; trust the router. Only fast-path: a single regex for exact `order_id`/`PO_*` IDs that bypasses the LLM and routes to `OrderLookupSkill` directly.

## Open questions / verifications before commit

1. **VERIFY**: which dbt analytics database/schema the compiled MetricFlow SQL references — needed to extend `sql_guard` allowlist before metric queries can execute.
2. **VERIFY**: which Cortex models are available in your Snowflake region (target: Llama 3.3 70B for router; mistral-7b is fine for narrow extraction inside skills).
3. **VERIFY**: SPCS service spec format for secret injection (env var vs file mount) and the exact `--external-access-integrations` flag name on the version of `snow` we're using.
4. **VERIFY**: GraphQL endpoint for the user's dbt Cloud tenant — multi-region tenants use different hosts (`semantic-layer.cloud.getdbt.com` vs region-specific). Update network rule accordingly.
5. **VERIFY**: `dbt-sl-sdk` version that pins `pyarrow` compatible with the Python 3.11 base image we'll use — pin both in `requirements.txt`.

## Verification (how to test end-to-end)

1. **Local agent core sanity**: run `pytest app/agent/` against unit tests for validators, json_repair, fuzzy normalize, and a smoke test for `OrderLookupSkill` against a Snowflake dev account (Docker-Compose locally with the same image, env-var secrets).
2. **SPCS deployment smoke test**: run `infra/setup.sh` end-to-end; visit the public endpoint; run a known-good order lookup ("status of order ORD-12345") and a known-good metric query ("orders by status this quarter"). Verify `AGENT_TRACE` row written, compiled SQL captured, `total_ms < 5s`. Confirm `/data/catalog_cache.pkl` exists in the container after first request.
3. **Compile-then-execute correctness**: pick 5 metric queries that the existing demo produces correct answers for; rerun via the new pipeline; assert row-level equality between dbt-Cloud-executed (old) and Snowflake-direct-executed (new).
4. **Feedback loop**: submit a 👎 with correction; query `AGENT_FEEDBACK` to confirm the row exists with `trace_id` linking back to `AGENT_TRACE`.
5. **Eval task**: manually `CALL DEMO_BSC.RUN_NIGHTLY_EVAL()`; assert `AGENT_EVAL_SUMMARY` shows non-zero `passed` count for the bulk-loaded golden set; assert latency p95 < 5s.
6. **Router robustness**: feed 10 deliberately ambiguous prompts; verify `ClarifySkill` triggers (not silent execution of the wrong skill).
7. **Guardrail enforcement**: hand-craft a prompt that would coerce the router into picking a non-existent skill; verify routing fails closed and `AGENT_TRACE.status='clarify'`.

End-state: a single SiS app, with curated context, four bounded skills, explicit guardrails, and a Snowflake-native feedback + eval loop running nightly. The framework is ready to grow more skills and graduate to a real agentic system without architectural rework.
