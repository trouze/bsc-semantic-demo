-- Bulk-load evaluation/datasets/golden_prompts.jsonl into AGENT_GOLDEN.
--
-- Manual steps required before running this file:
--   1. Create an internal stage (if it doesn't exist):
--        CREATE STAGE IF NOT EXISTS DEMO_BSC.EVAL_STAGE;
--   2. Upload the file using SnowSQL or the snow CLI:
--        snow sql --query "PUT file://evaluation/datasets/golden_prompts.jsonl @DEMO_BSC.EVAL_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE;"
--   3. Run this file:
--        snow sql -f infra/sql/08_seed_golden.sql
--
-- Schema of golden_prompts.jsonl:
--   {"id": "gp-NNN", "mode": "free_text"|"structured",
--    "free_text": "...",           -- present when mode=free_text
--    "fields": {"facility_name": "...", "date_start": "...", ...}}  -- present when mode=structured
--
-- COPY INTO loads each JSON line into a temporary table; MERGE upserts into AGENT_GOLDEN.

CREATE TEMPORARY TABLE DEMO_BSC._GOLDEN_STAGE_TMP (raw VARIANT);

COPY INTO DEMO_BSC._GOLDEN_STAGE_TMP (raw)
FROM @DEMO_BSC.EVAL_STAGE/golden_prompts.jsonl
FILE_FORMAT = (TYPE = JSON STRIP_OUTER_ARRAY = FALSE)
ON_ERROR = ABORT_STATEMENT;

MERGE INTO DEMO_BSC.AGENT_GOLDEN AS tgt
USING (
  SELECT
    raw:id::STRING                                          AS golden_id,
    NULL::STRING                                            AS source_trace_id,
    NULL::STRING                                            AS source_feedback_id,
    CURRENT_TIMESTAMP()                                     AS promoted_at,
    'seed'                                                  AS promoted_by,
    COALESCE(raw:free_text::STRING,
             raw:fields::STRING)                            AS user_input,
    'order_lookup'                                          AS expected_skill,
    raw:fields                                              AS expected_slots,
    NULL::VARIANT                                           AS expected_checks,
    ARRAY_CONSTRUCT(raw:mode::STRING)                       AS tags,
    TRUE                                                    AS active,
    NULL::STRING                                            AS prompt_version_at_promotion
  FROM DEMO_BSC._GOLDEN_STAGE_TMP
) AS src
ON tgt.golden_id = src.golden_id
WHEN NOT MATCHED THEN
  INSERT (golden_id, source_trace_id, source_feedback_id, promoted_at, promoted_by,
          user_input, expected_skill, expected_slots, expected_checks, tags, active,
          prompt_version_at_promotion)
  VALUES (src.golden_id, src.source_trace_id, src.source_feedback_id, src.promoted_at,
          src.promoted_by, src.user_input, src.expected_skill, src.expected_slots,
          src.expected_checks, src.tags, src.active, src.prompt_version_at_promotion);

DROP TABLE IF EXISTS DEMO_BSC._GOLDEN_STAGE_TMP;
