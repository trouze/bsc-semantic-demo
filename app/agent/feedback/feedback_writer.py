"""FeedbackWriter — writes thumbs/correction rows to AGENT_FEEDBACK."""
from __future__ import annotations
import json
import logging
import uuid
from typing import Literal, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from snowflake.snowpark import Session

logger = logging.getLogger(__name__)

_INSERT_SQL = """
INSERT INTO DEMO_BSC.AGENT_FEEDBACK (
  feedback_id, trace_id, user_email,
  rating, correction_text, corrected_slots, expected_skill, notes
) VALUES (
  ?, ?, ?,
  ?, ?, PARSE_JSON(?), ?, ?
)
"""


class FeedbackWriter:
    """Writes AGENT_FEEDBACK rows from thumbs/corrections submitted in the UI."""

    def __init__(self, session: "Session") -> None:
        self._session = session

    def write(
        self,
        trace_id: str,
        rating: Literal["up", "down"],
        user_email: str = "",
        correction_text: str = "",
        corrected_slots: Optional[dict] = None,
        expected_skill: str = "",
        notes: str = "",
    ) -> str:
        """Insert a feedback row. Returns feedback_id."""
        feedback_id = str(uuid.uuid4())
        try:
            self._session.sql(_INSERT_SQL, params=[
                feedback_id,
                trace_id,
                user_email,
                rating,
                correction_text,
                json.dumps(corrected_slots or {}),
                expected_skill,
                notes,
            ]).collect()
        except Exception:
            logger.exception("FeedbackWriter.write failed for trace_id=%s", trace_id)
        return feedback_id
