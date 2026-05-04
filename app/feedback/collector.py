"""Feedback collector — writes interaction data and ratings to Snowflake.

Every agent turn is logged (regardless of outcome), then users optionally
rate responses. This data drives the eval pipeline and continuous improvement.
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime
from typing import Any, Optional


class FeedbackCollector:
    def __init__(self, sf_conn: Any, db: str, schema: str):
        self._conn = sf_conn
        self._db = db
        self._schema = schema
        self._fq = f"{db}.{schema}"

    def log_interaction(
        self,
        interaction_id: str,
        session_id: str,
        user_id: str,
        user_query: str,
        skill: Optional[str],
        metrics: list[str],
        dimensions: list[str],
        confidence: Optional[float],
        sql: Optional[str],
        execution_time_ms: Optional[int],
        row_count: Optional[int],
        status: str,
    ) -> None:
        cur = self._conn.cursor()
        try:
            cur.execute(
                f"""
                INSERT INTO {self._fq}.INTERACTIONS (
                    INTERACTION_ID, SESSION_ID, USER_ID, USER_QUERY,
                    SKILL, METRICS, DIMENSIONS, CONFIDENCE,
                    COMPILED_SQL, EXECUTION_TIME_MS, ROW_COUNT, STATUS,
                    CREATED_AT
                ) VALUES (
                    %s, %s, %s, %s,
                    %s, PARSE_JSON(%s), PARSE_JSON(%s), %s,
                    %s, %s, %s, %s,
                    CURRENT_TIMESTAMP()
                )
                """,
                (
                    interaction_id,
                    session_id,
                    user_id,
                    user_query,
                    skill,
                    json.dumps(metrics),
                    json.dumps(dimensions),
                    confidence,
                    sql,
                    execution_time_ms,
                    row_count,
                    status,
                ),
            )
        except Exception:
            pass  # never propagate feedback errors
        finally:
            cur.close()

    def log_rating(
        self,
        interaction_id: str,
        rating: str,          # "up" | "down"
        comment: str = "",
    ) -> None:
        cur = self._conn.cursor()
        try:
            cur.execute(
                f"""
                INSERT INTO {self._fq}.RATINGS (
                    RATING_ID, INTERACTION_ID, RATING, COMMENT, CREATED_AT
                ) VALUES (
                    %s, %s, %s, %s, CURRENT_TIMESTAMP()
                )
                """,
                (str(uuid.uuid4()), interaction_id, rating, comment),
            )
        except Exception:
            pass
        finally:
            cur.close()

    def refresh_connection(self, sf_conn: Any) -> None:
        self._conn = sf_conn
