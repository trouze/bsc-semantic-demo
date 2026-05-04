# BSci Semantic Demo

Natural-language analytics on Snowflake for Boston Scientific. Users ask questions in plain English — the app routes through a dbt Semantic Layer–powered agent, compiles SQL via the dbt SL SDK, executes it directly on Snowflake, and renders charts automatically.

**Stack:** Streamlit · Snowflake Cortex · dbt Semantic Layer SDK · Snowpark Container Services (SPCS)

---

## Architecture

```
User query (plain English)
        │
        ▼
┌───────────────────────────────────────────────┐
│  CortexAgent  (app/agents/cortex.py)          │
│  SNOWFLAKE.CORTEX.COMPLETE → JSON query plan  │
│  { skill, metrics[], group_by[], where[],     │
│    order_by[], limit, confidence }            │
└──────────────────┬────────────────────────────┘
                   │
                   ▼
┌───────────────────────────────────────────────┐
│  GuardrailsValidator  (app/guardrails/)        │
│  • Metrics exist in catalog                   │
│  • Dimensions valid for requested metric set  │
│  • PII dimension block list                   │
│  • Confidence ≥ 0.45 floor                    │
│  • ≤ 5 metrics per query                      │
└──────────────────┬────────────────────────────┘
                   │
                   ▼
┌───────────────────────────────────────────────┐
│  SkillRegistry → Skill  (app/skills/)          │
│  5 built-in skills:                           │
│    trend      time-series line chart          │
│    compare    side-by-side bar chart          │
│    breakdown  ranked bar / pie chart          │
│    rank       sorted table with top-N filter  │
│    summary    single-metric KPI card          │
│  Skill.build_query() → QueryPlan              │
└──────────────────┬────────────────────────────┘
                   │
                   ▼
┌───────────────────────────────────────────────┐
│  SLExecutor  (app/semantic/executor.py)        │
│  AsyncSemanticLayerClient.compile_sql()       │
│    → SQL string (dbt SL gRPC, no data yet)   │
│  Snowflake connector.execute(sql)             │
│    → DataFrame                               │
└──────────────────┬────────────────────────────┘
                   │
                   ▼
┌───────────────────────────────────────────────┐
│  UI  (app/ui/)                                 │
│  Plotly chart auto-selected by skill hint     │
│  Query details expander (SQL, timing, conf.)  │
│  👍 / 👎 rating buttons                       │
└──────────────────┬────────────────────────────┘
                   │
                   ▼
┌───────────────────────────────────────────────┐
│  FeedbackCollector  (app/feedback/)            │
│  Every interaction logged to Snowflake:       │
│  INTERACTIONS · RATINGS · GOLDEN_SET          │
│  Views: SKILL_PERFORMANCE · GOLDEN_CANDIDATES │
└───────────────────────────────────────────────┘
```

**Key design decisions:**
- `compile_sql()` over `query()` — dbt SL compiles the SQL but Snowflake executes it directly (speed + observability)
- Streamlit is sync; dbt SL SDK is async-only → `ThreadPoolExecutor` with an isolated `asyncio` event loop per thread
- Every query routes through a named, bounded skill — no free-form SQL generation
- Feedback errors are silently swallowed and never break the main query flow

---

## Semantic Catalog

`SemanticCatalog` eagerly loads all metrics and dimensions from the dbt Semantic Layer at startup (`lazy=False`). The LLM only ever sees catalog-registered names — the raw Snowflake schema is never exposed.

Real BSci catalog: **13 metrics · 62 dimensions** across inventory, DIOH, and supply chain domains.

---

## Quick Start

### Prerequisites

- [Snowflake CLI v2+](https://docs.snowflake.com/developer-guide/snowflake-cli/installation) with a configured connection
- [Docker Desktop](https://docs.docker.com/get-docker/)
- [uv](https://docs.astral.sh/uv/getting-started/installation/)
- A dbt Semantic Layer service token (from dbt Cloud)

### Local development

```bash
# Install dependencies
uv sync

# Copy and fill in credentials
cp .streamlit/secrets.toml.example .streamlit/secrets.toml
# edit .streamlit/secrets.toml

# Bootstrap feedback tables (run once)
snow sql -f app/feedback/schema.sql --connection <your-connection>

# Run the app
uv run streamlit run app/streamlit_app.py
```

Open **http://localhost:8501**.

### Deploy to Snowflake (SPCS)

```bash
export SNOW_CONNECTION="my_connection"
export DBT_SL_ENVIRONMENT_ID="335860"
export DBT_SL_AUTH_TOKEN="dbtu_..."

chmod +x scripts/deploy.sh
./scripts/deploy.sh
```

The script handles everything: Snowflake object creation, Docker build + push, SPCS service deployment, and feedback table bootstrap. It prints the public HTTPS endpoint when the service becomes active (~5 min first cold start).

```bash
# Re-deploy after code changes (rebuild image + upgrade service)
./scripts/deploy.sh --upgrade

# Tear down all created resources
./scripts/deploy.sh --teardown
```

---

## Configuration

All settings are read from `.streamlit/secrets.toml` (local) or environment variables (SPCS). See `.streamlit/secrets.toml.example` for a full template.

| Variable | Required | Default | Description |
|---|---|---|---|
| `DBT_SL_ENVIRONMENT_ID` | Yes | — | dbt Cloud environment ID |
| `DBT_SL_AUTH_TOKEN` | Yes | — | dbt Cloud service token |
| `DBT_SL_HOST` | Yes | — | Semantic Layer gRPC host |
| `SNOWFLAKE_ACCOUNT` | Yes* | — | Snowflake account identifier |
| `SNOWFLAKE_USER` | Yes* | — | Snowflake username |
| `SNOWFLAKE_PASSWORD` | Yes* | — | Snowflake password |
| `SNOWFLAKE_WAREHOUSE` | No | `COMPUTE_WH` | Query warehouse |
| `SNOWFLAKE_DATABASE` | No | `ANALYTICS` | Default database |
| `SNOWFLAKE_SCHEMA` | No | `PUBLIC` | Default schema |
| `SNOWFLAKE_ROLE` | No | — | Snowflake role override |
| `SNOWFLAKE_FEEDBACK_DB` | No | `ANALYTICS` | Feedback tables database |
| `SNOWFLAKE_FEEDBACK_SCHEMA` | No | `SEMANTIC_DEMO_FEEDBACK` | Feedback tables schema |
| `CORTEX_MODEL` | No | `claude-3-5-sonnet` | Cortex LLM model name |
| `MAX_RESULT_ROWS` | No | `10000` | Max rows returned per query |

*Not required when running inside SPCS — Snowflake credentials come from the active session via `get_active_session()`.

---

## Feedback & Eval

Every interaction is automatically logged to Snowflake. Users rate responses with 👍 / 👎. High-rated successful interactions surface in `GOLDEN_CANDIDATES` as eval set candidates.

```sql
-- Skill-level success rates and avg confidence
SELECT * FROM ANALYTICS.SEMANTIC_DEMO_FEEDBACK.SKILL_PERFORMANCE;

-- All interactions with explicit ratings joined
SELECT * FROM ANALYTICS.SEMANTIC_DEMO_FEEDBACK.RATED_INTERACTIONS;

-- Promote to golden set for regression evals
SELECT * FROM ANALYTICS.SEMANTIC_DEMO_FEEDBACK.GOLDEN_CANDIDATES;
```

`EvalRunner` (`app/feedback/evaluator.py`) scores golden cases on skill match, metrics match, and SQL fragment presence. Wire it into CI to gate deploys on `pass_rate ≥ 0.85`.

---

## Repository Structure

```
bsc-semantic-demo/
  pyproject.toml              # uv project definition
  Dockerfile                  # SPCS-ready, linux/amd64, uv-based install
  scripts/
    deploy.sh                 # Full Snowflake deployment (infra + image + service + feedback DDL)

  app/
    streamlit_app.py          # Entry point — session state, chat loop, cached catalog
    config.py                 # Settings — reads st.secrets then env vars
    async_utils.py            # ThreadPoolExecutor bridge (Streamlit sync ↔ async SDK)

    agents/
      cortex.py               # CortexAgent — CORTEX.COMPLETE → JSON query plan
      orchestrator.py         # Orchestrator.process() — plan → guardrails → skill → execute

    semantic/
      catalog.py              # SemanticCatalog — eager load, keyword search, LLM format method
      executor.py             # SLExecutor — compile_sql() then Snowflake execute

    skills/
      base.py                 # Skill ABC + ChartHint enum
      builtin.py              # TrendSkill, CompareSkill, BreakdownSkill, RankSkill, SummarySkill
      registry.py             # SkillRegistry

    guardrails/
      validator.py            # GuardrailsValidator — whitelist, PII block, confidence floor

    feedback/
      schema.sql              # DDL — INTERACTIONS, RATINGS, GOLDEN_SET tables + analytical views
      collector.py            # FeedbackCollector — log_interaction, log_rating
      evaluator.py            # EvalRunner — golden set scoring

    ui/
      chat.py                 # render_message, render_feedback_buttons
      results.py              # render_result — Plotly chart auto-selected by skill hint
      sidebar.py              # Catalog panel + skill guide

  .streamlit/
    secrets.toml.example      # Credential template (copy to secrets.toml, never commit)
```
