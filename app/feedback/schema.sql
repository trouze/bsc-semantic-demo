-- Feedback schema: run once against your Snowflake account before starting the app.
-- Replace ANALYTICS.SEMANTIC_DEMO_FEEDBACK with your chosen DB.SCHEMA.

CREATE DATABASE IF NOT EXISTS ANALYTICS;
CREATE SCHEMA IF NOT EXISTS ANALYTICS.SEMANTIC_DEMO_FEEDBACK;

-- Every agent interaction is logged here regardless of outcome.
CREATE TABLE IF NOT EXISTS ANALYTICS.SEMANTIC_DEMO_FEEDBACK.INTERACTIONS (
    INTERACTION_ID   VARCHAR(36)   NOT NULL PRIMARY KEY,
    SESSION_ID       VARCHAR(36)   NOT NULL,
    USER_ID          VARCHAR(255)  NOT NULL,
    USER_QUERY       TEXT          NOT NULL,
    SKILL            VARCHAR(64),
    METRICS          VARIANT,      -- JSON array of metric names
    DIMENSIONS       VARIANT,      -- JSON array of dimension names
    CONFIDENCE       FLOAT,
    COMPILED_SQL     TEXT,
    EXECUTION_TIME_MS INTEGER,
    ROW_COUNT        INTEGER,
    STATUS           VARCHAR(64),  -- success | clarification | out_of_scope | blocked | *_error
    CREATED_AT       TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

-- Explicit thumbs-up/thumbs-down ratings attached to interactions.
CREATE TABLE IF NOT EXISTS ANALYTICS.SEMANTIC_DEMO_FEEDBACK.RATINGS (
    RATING_ID        VARCHAR(36)   NOT NULL PRIMARY KEY,
    INTERACTION_ID   VARCHAR(36)   NOT NULL REFERENCES ANALYTICS.SEMANTIC_DEMO_FEEDBACK.INTERACTIONS(INTERACTION_ID),
    RATING           VARCHAR(8)    NOT NULL,  -- 'up' | 'down'
    COMMENT          TEXT,
    CREATED_AT       TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

-- Golden set: manually curated query/expected-result pairs used for evals.
CREATE TABLE IF NOT EXISTS ANALYTICS.SEMANTIC_DEMO_FEEDBACK.GOLDEN_SET (
    GOLDEN_ID        VARCHAR(36)   NOT NULL PRIMARY KEY,
    QUERY            TEXT          NOT NULL,
    EXPECTED_SKILL   VARCHAR(64),
    EXPECTED_METRICS VARIANT,
    EXPECTED_SQL_FRAGMENT TEXT,    -- partial SQL that should appear in compiled output
    NOTES            TEXT,
    CREATED_AT       TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

-- ─── Analytical views ─────────────────────────────────────────────────────────

-- Per-skill success rate and avg confidence.
CREATE OR REPLACE VIEW ANALYTICS.SEMANTIC_DEMO_FEEDBACK.SKILL_PERFORMANCE AS
SELECT
    SKILL,
    COUNT(*)                                          AS total_queries,
    SUM(CASE WHEN STATUS = 'success' THEN 1 ELSE 0 END) AS successful,
    ROUND(100.0 * SUM(CASE WHEN STATUS = 'success' THEN 1 ELSE 0 END) / COUNT(*), 1) AS success_pct,
    ROUND(AVG(CONFIDENCE), 3)                         AS avg_confidence,
    ROUND(AVG(EXECUTION_TIME_MS), 0)                  AS avg_exec_ms,
    ROUND(AVG(ROW_COUNT), 0)                          AS avg_rows
FROM ANALYTICS.SEMANTIC_DEMO_FEEDBACK.INTERACTIONS
GROUP BY SKILL
ORDER BY total_queries DESC;

-- Interactions with explicit ratings joined in.
CREATE OR REPLACE VIEW ANALYTICS.SEMANTIC_DEMO_FEEDBACK.RATED_INTERACTIONS AS
SELECT
    i.*,
    r.RATING,
    r.COMMENT
FROM ANALYTICS.SEMANTIC_DEMO_FEEDBACK.INTERACTIONS i
LEFT JOIN ANALYTICS.SEMANTIC_DEMO_FEEDBACK.RATINGS r USING (INTERACTION_ID);

-- High-value golden candidates: successful interactions with thumbs-up.
CREATE OR REPLACE VIEW ANALYTICS.SEMANTIC_DEMO_FEEDBACK.GOLDEN_CANDIDATES AS
SELECT
    i.INTERACTION_ID,
    i.USER_QUERY,
    i.SKILL,
    i.METRICS,
    i.COMPILED_SQL
FROM ANALYTICS.SEMANTIC_DEMO_FEEDBACK.INTERACTIONS i
JOIN ANALYTICS.SEMANTIC_DEMO_FEEDBACK.RATINGS r USING (INTERACTION_ID)
WHERE i.STATUS = 'success'
  AND r.RATING = 'up'
ORDER BY i.CREATED_AT DESC;

-- ─── Eval task (run weekly) ────────────────────────────────────────────────────
-- Schedules a lightweight eval that uses Cortex to cluster failure patterns.
-- Uncomment and adjust when ready.

/*
CREATE OR REPLACE TASK ANALYTICS.SEMANTIC_DEMO_FEEDBACK.WEEKLY_EVAL
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON 0 6 * * 1 UTC'  -- every Monday at 06:00 UTC
AS
INSERT INTO ANALYTICS.SEMANTIC_DEMO_FEEDBACK.EVAL_RUNS (
    RUN_DATE,
    FAILURE_SUMMARY,
    SKILL_PERFORMANCE_SNAPSHOT
)
SELECT
    CURRENT_DATE(),
    SNOWFLAKE.CORTEX.COMPLETE(
        'claude-3-5-sonnet',
        CONCAT(
            'Summarize the following failed query patterns and suggest prompt improvements. '
            'Failures from the last 7 days:\n',
            (SELECT LISTAGG(USER_QUERY, '\n') WITHIN GROUP (ORDER BY CREATED_AT)
             FROM ANALYTICS.SEMANTIC_DEMO_FEEDBACK.INTERACTIONS
             WHERE STATUS NOT IN ('success', 'clarification', 'out_of_scope')
               AND CREATED_AT >= DATEADD(day, -7, CURRENT_TIMESTAMP()))
        )
    ),
    (SELECT OBJECT_CONSTRUCT(*) FROM ANALYTICS.SEMANTIC_DEMO_FEEDBACK.SKILL_PERFORMANCE LIMIT 20);
ALTER TASK ANALYTICS.SEMANTIC_DEMO_FEEDBACK.WEEKLY_EVAL RESUME;
*/
