-- AGENT_TRACE
CREATE TABLE IF NOT EXISTS DEMO_BSC.AGENT_TRACE (
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
  status             STRING
);

-- AGENT_FEEDBACK
CREATE TABLE IF NOT EXISTS DEMO_BSC.AGENT_FEEDBACK (
  feedback_id        STRING        PRIMARY KEY,
  trace_id           STRING        NOT NULL,
  created_at         TIMESTAMP_TZ  DEFAULT CURRENT_TIMESTAMP(),
  user_email         STRING,
  rating             STRING,
  correction_text    STRING,
  corrected_slots    VARIANT,
  expected_skill     STRING,
  notes              STRING
);

-- AGENT_GOLDEN
CREATE TABLE IF NOT EXISTS DEMO_BSC.AGENT_GOLDEN (
  golden_id              STRING       PRIMARY KEY,
  source_trace_id        STRING,
  source_feedback_id     STRING,
  promoted_at            TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
  promoted_by            STRING,
  user_input             STRING       NOT NULL,
  expected_skill         STRING       NOT NULL,
  expected_slots         VARIANT,
  expected_checks        VARIANT,
  tags                   ARRAY,
  active                 BOOLEAN      DEFAULT TRUE,
  prompt_version_at_promotion STRING
);

-- AGENT_EVAL_RUNS
CREATE TABLE IF NOT EXISTS DEMO_BSC.AGENT_EVAL_RUNS (
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

CREATE VIEW IF NOT EXISTS DEMO_BSC.AGENT_EVAL_SUMMARY AS
SELECT run_id, prompt_version, model_version, skill_version,
       COUNT(*) AS total, SUM(IFF(passed,1,0)) AS passed,
       APPROX_PERCENTILE(latency_ms, 0.5) AS p50_ms,
       APPROX_PERCENTILE(latency_ms, 0.95) AS p95_ms
FROM DEMO_BSC.AGENT_EVAL_RUNS GROUP BY 1,2,3,4;

-- AGENT_GLOSSARY
CREATE TABLE IF NOT EXISTS DEMO_BSC.AGENT_GLOSSARY (
  term            STRING PRIMARY KEY,
  category        STRING,
  definition      STRING,
  metadata        VARIANT,
  active          BOOLEAN DEFAULT TRUE
);

-- AGENT_CONFIG
CREATE TABLE IF NOT EXISTS DEMO_BSC.AGENT_CONFIG (
  key             STRING PRIMARY KEY,
  value           VARIANT,
  description     STRING
);

-- SEMANTIC_CATALOG_CACHE
CREATE TABLE IF NOT EXISTS DEMO_BSC.SEMANTIC_CATALOG_CACHE (
  object_name     STRING        NOT NULL,
  object_type     STRING        NOT NULL,
  description     STRING,
  expr            STRING,
  meta            VARIANT,
  refreshed_at    TIMESTAMP_TZ  DEFAULT CURRENT_TIMESTAMP(),
  PRIMARY KEY (object_name, object_type)
);
