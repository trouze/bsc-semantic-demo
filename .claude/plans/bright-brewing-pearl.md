# Parallel Work Plan: SiS / SPCS Agent Redesign

## Context

The plan in `so-a-colleague-of-wiggly-cherny.md` describes a full redesign: kill FastAPI + local Streamlit, replace with a Snowpark Container Services (SPCS) hosted Streamlit app backed by the dbt Semantic Layer SDK (compile) + direct Snowflake execution. The `app/` directory does not exist yet; `infra/` has only legacy SQL stubs. All 17 units below create **new files only** — no existing files are modified in any unit, so PRs are conflict-free and independently mergeable.

## Research Findings

| File | Lines | Porting destination |
|------|-------|---------------------|
| `api/services/cortex_service.py` | 460 | `app/agent/cortex/` |
| `api/services/snowflake_service.py:126-155` | 167 | `app/agent/guardrails/sql_guard.py` |
| `api/services/dbt_mcp_service.py:208-308` | 448 | `app/agent/semantic/sl_client.py` |
| `api/services/dbt_mcp_service.py:337-353` | — | `infra/sql/07_seed_glossary.sql` |
| `api/services/semantic_service.py` | 713 | split → router + skills |
| `api/services/fuzzy_service.py` | 263 | `app/agent/skills/_fuzzy.py` |
| `api/core/timing.py` | 29 | `app/agent/types.py` (inline) |
| `evaluation/run_eval.py:54-118,206-215` | 713 | `app/agent/eval/runner.py` |

## e2e Test Recipe

Since SPCS deployment requires a live Snowflake account (not available in CI), the per-worker test is:

1. **Syntax check**: `python -m py_compile <all new .py files in this unit>`
2. **Unit tests** (for units with pure logic): `cd app && python -m pytest tests/<module>/ -x -q` — each worker that creates testable pure logic should also create matching tests in `app/tests/`.
3. **Import smoke**: `python -c "from agent.<module> import <MainClass>"` — verifies all intra-package imports resolve.
4. **Skip full SPCS e2e** — this is scaffolding; deployment smoke test is a manual post-merge step described in the original plan.

## Work Units

| # | Title | Files | Description |
|---|-------|-------|-------------|
| 1 | Infra DDL | `infra/sql/01_database_schema.sql` through `infra/sql/11_service.sql` | Create all 11 SQL files: database/schema, warehouse, all agent tables + views, secret + EAI (templated), compute pool, image repo, glossary seed (from `dbt_mcp_service.py:337-353`), golden seed, stored procs (REFRESH_SEMANTIC_CATALOG + RUN_NIGHTLY_EVAL), tasks, service DDL |
| 2 | Infra scripts + spec | `infra/setup.sh`, `infra/teardown.sh`, `infra/spec/streamlit_service.yaml` | Shell orchestration script, symmetric teardown, and SPCS service YAML spec (image, ports, volumes, secret injection, EAI) |
| 3 | Container files | `app/Dockerfile`, `app/requirements.txt`, `app/environment.yml` | python:3.11-slim Dockerfile, pinned requirements (streamlit, dbt-sl-sdk, snowflake-snowpark-python, pyarrow, pandas, pydantic, requests, cachetools), conda environment file |
| 4 | Agent foundation | `app/agent/__init__.py`, `app/agent/types.py`, `app/agent/config.py`, `app/agent/secrets.py`, `app/agent/session.py` | Shared dataclasses (`AgentTurn`, `ContextPack`, `ToolCall`, `SkillResult`, Timer inline from `api/core/timing.py`), settings (no pydantic-settings; plain module), `os.getenv('DBT_CLOUD_TOKEN')` secrets, `@st.cache_resource get_session()` Snowpark getter |
| 5 | Cortex module | `app/agent/cortex/__init__.py`, `app/agent/cortex/client.py`, `app/agent/cortex/json_repair.py` | Port `_complete()` (lines 365-381) → `CortexClient.complete(prompt, model, label)`; port `_repair_json` (337-359) + `_extract_json_block` (395-403) + `_extract_content` (383-393). Model default: `llama3.3-70b` (replacing mistral-7b). Unit tests in `app/tests/test_json_repair.py`. |
| 6 | Semantic SL client | `app/agent/semantic/__init__.py`, `app/agent/semantic/sl_client.py` | Wrap `dbt-sl-sdk` `SemanticLayerClient` exposing `list_metrics()`, `get_dimensions()`, `get_entities()`, `compile_sql()` (no execute). Port logic from `dbt_mcp_service.py:208-308` but drop `_McpLoop`; use the SDK's sync client + `@st.cache_resource`. |
| 7 | Executor + sql_guard | `app/agent/semantic/executor.py`, `app/agent/guardrails/__init__.py`, `app/agent/guardrails/sql_guard.py` | `SnowflakeExecutor.run(sql)` via Snowpark with parameter binding and statement timeout; port `_assert_schema_safe` from `snowflake_service.py:126-155`, extend allowlist to include the dbt analytics database/schema (configurable env var `ANALYTICS_DB`). Unit tests in `app/tests/test_sql_guard.py`. |
| 8 | Context module | `app/agent/context/__init__.py`, `app/agent/context/builder.py`, `app/agent/context/catalog.py`, `app/agent/context/glossary.py` | `ContextBuilder.build(turn, history) -> ContextPack`; `SemanticCatalog` reads `SEMANTIC_CATALOG_CACHE` table via Snowpark; `Glossary` reads `AGENT_GLOSSARY` table. Both use `@st.cache_resource(ttl=3600)`. |
| 9 | Guardrails: slots + output | `app/agent/guardrails/slot_validators.py`, `app/agent/guardrails/output_guard.py` | `slot_validators`: date range check, enum membership (status in glossary), metric/dimension allowlist (from catalog). `output_guard`: row cap, confidence threshold, PII redaction no-op placeholder. Unit tests in `app/tests/test_slot_validators.py`. |
| 10 | Fuzzy helper + base skill | `app/agent/skills/__init__.py`, `app/agent/skills/_fuzzy.py`, `app/agent/skills/base.py` | Lift `fuzzy_service.py` verbatim → `_fuzzy.py` (keep `_normalize`, `_tokenize`, `_expand_tokens`, `FuzzyService`, `NormalizedQuery`); define `Skill` Protocol + `SlotSpec` (Pydantic) + `SkillResult` dataclass in `base.py`. Unit tests in `app/tests/test_fuzzy.py`. |
| 11 | Order lookup + clarify skills | `app/agent/skills/order_lookup.py`, `app/agent/skills/clarify.py` | Port `_handle_order_lookup` from `semantic_service.py:351-586` into `OrderLookupSkill.execute()`; slot schema (order_id, purchase_order_id, customer_name, facility_name, status, date_start, date_end, free_text_residual, top_n). Port `_RERANK_PROMPT_TEMPLATE`. `ClarifySkill` for router fallback — one disambiguation question, never executes. |
| 12 | Metric skills | `app/agent/skills/metric_query.py`, `app/agent/skills/metric_compare.py` | Port `_handle_metric_query` from `semantic_service.py:210-345` → `MetricQuerySkill` (compile via `SLClient`, execute via `SnowflakeExecutor`); new `MetricCompareSkill` (two compile calls → two executions → Python merge, delta + pct_change). Both present with `st.dataframe` + chart + SQL expander. |
| 13 | Router | `app/agent/router/__init__.py`, `app/agent/router/router.py`, `app/agent/router/registry.py` | `SkillRouter.route(turn, ctx) -> SkillCall` — one Cortex Complete call returning `{skill, slots, rationale}` JSON validated against `SkillRegistry`; regex fast-path for exact `ORD-*`/`PO-*` IDs that bypasses LLM. `SkillRegistry` maps name → `Skill` instance. Port `_PARSE_PROMPT_TEMPLATE` as router prompt. |
| 14 | Feedback writers | `app/agent/feedback/__init__.py`, `app/agent/feedback/trace_writer.py`, `app/agent/feedback/feedback_writer.py` | `TraceWriter.write(turn, result, timings) -> trace_id` — inserts into `AGENT_TRACE`; `FeedbackWriter.write(trace_id, rating, correction_text, corrected_slots, expected_skill)` — inserts into `AGENT_FEEDBACK`. Both use Snowpark session. |
| 15 | Eval module | `app/agent/eval/__init__.py`, `app/agent/eval/runner.py`, `app/agent/eval/promoter.py` | Port `evaluate_checks` (lines 54-118) and metric computation (206-215) from `evaluation/run_eval.py` → `runner.py`; `EvalRunner.run(golden_ids=None)` reads `AGENT_GOLDEN WHERE active=TRUE`, invokes agent in-process, writes `AGENT_EVAL_RUNS`. `promoter.py` stub for v2 HITL promotion queue. |
| 16 | Chat page | `app/app.py`, `app/pages/01_chat.py` | `app.py`: page config, session bootstrap, `st.session_state.history` init. `01_chat.py`: full chat UI — input box, per-result thumbs/correction widget (👍/👎 → `FeedbackWriter`), "Was the interpretation right?" slot-edit expander, "Save as eval example" button. |
| 17 | Trace + eval pages | `app/pages/02_traces.py`, `app/pages/03_eval.py` | `02_traces.py`: paginated `AGENT_TRACE` browser + replay-from-trace_id deep link via `?trace_id=` query param. `03_eval.py`: read-only `AGENT_EVAL_SUMMARY` view dashboard — pass rate, p50/p95 per prompt_version. |

## Shared Conventions Workers Must Follow

- All new files live under `app/` (no changes to `api/`, `ui/`, `evaluation/`, `dbt/`, `docker-compose.yml`)
- `app/agent/` is a Python package; every `__init__.py` is empty or re-exports the public API
- Type hints everywhere; no `Any` unless porting verbatim from source
- No `pydantic-settings` / `BaseSettings` — config reads `os.getenv(...)` directly in `app/agent/config.py`
- Snowpark session always obtained via `app/agent/session.get_session()` (never create ad-hoc)
- All SQL to Snowflake via Snowpark's parameterized `.sql(...).collect()` — no f-string interpolation
- Stubs are acceptable for cross-module imports that don't exist yet (e.g., `from agent.types import ContextPack  # type: ignore` with a `TYPE_CHECKING` guard); each unit is responsible for its own files only
- Test files go in `app/tests/` and import modules via `sys.path.insert(0, str(Path(__file__).parents[1]))` so they work without install

## Worker Instructions Template

> After you finish implementing the change:
> 1. **Simplify** — Invoke the `Skill` tool with `skill: "simplify"` to review and clean up your changes.
> 2. **Run unit tests** — `cd /Users/tylerrouze/dev/clients/bsci/bsc-semantic-demo/app && python -m py_compile $(find . -name '*.py' | tr '\n' ' ')` then, if your unit includes pure-logic tests: `python -m pytest tests/ -x -q 2>/dev/null || echo "no tests yet"`.
> 3. **Test end-to-end** — Run `python -c "import sys, pathlib; sys.path.insert(0, 'app'); from agent.<your_module> import <MainClass>"` to verify imports resolve. For SQL/shell files run a linter or syntax check instead (e.g., `bash -n infra/setup.sh`).
> 4. **Commit and push** — Commit all changes with a clear message, push the branch, and create a PR with `gh pr create`. Use a descriptive title.
> 5. **Report** — End with a single line: `PR: <url>`.
