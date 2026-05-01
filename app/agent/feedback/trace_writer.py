"""TraceWriter — writes one row per agent turn to AGENT_TRACE."""
from __future__ import annotations
import json
import logging
import uuid
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from snowflake.snowpark import Session

logger = logging.getLogger(__name__)

_INSERT_SQL = """
INSERT INTO DEMO_BSC.AGENT_TRACE (
  trace_id, session_id, user_email, user_input,
  routed_skill, router_raw, slots, context_pack_hash,
  prompt_version, model_version, skill_version,
  compiled_sql, executed_sql_hash, snowflake_qid,
  result_summary, timings_ms, total_ms, error, status
) VALUES (
  ?, ?, ?, ?,
  ?, PARSE_JSON(?), PARSE_JSON(?), ?,
  ?, ?, ?,
  ?, ?, ?,
  PARSE_JSON(?), PARSE_JSON(?), ?, ?, ?
)
"""


class TraceWriter:
    """Writes one AGENT_TRACE row per completed agent turn."""

    def __init__(self, session: "Session") -> None:
        self._session = session

    def write(
        self,
        turn: Any,          # AgentTurn
        skill_call: Any,    # SkillCall
        result: Any,        # SkillResult
        ctx: Any,           # ContextPack
    ) -> str:
        """Insert a trace row. Returns the trace_id (UUID string)."""
        trace_id = str(uuid.uuid4())
        try:
            result_summary = json.dumps(
                {"row_count": len(result.data) if isinstance(result.data, list) else 1,
                 "status": result.status}
            )
            self._session.sql(_INSERT_SQL, params=[
                trace_id,
                turn.session_id,
                turn.user_email or "",
                turn.user_input,
                result.skill_name,
                skill_call.router_raw or "{}",
                json.dumps(skill_call.slots),
                ctx.catalog_hash,
                getattr(ctx, "prompt_version", ""),
                "",  # model_version — filled from result in v2
                result.skill_version,
                result.compiled_sql or "",
                result.executed_sql_hash or "",
                result.snowflake_qid or "",
                result_summary,
                json.dumps(result.timings),
                result.timings.get("total_ms", 0.0),
                result.error or "",
                result.status,
            ]).collect()
        except Exception:
            logger.exception("TraceWriter.write failed for turn=%s", getattr(turn, "turn_id", "?"))
        return trace_id
