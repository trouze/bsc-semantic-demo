"""Promoter — stub for v2 HITL feedback-to-golden promotion.

v1: reviewers run the SQL in the plan doc manually.
v2: this module will wrap that SQL in a UI with approval queues.
"""
from __future__ import annotations
import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from snowflake.snowpark import Session

logger = logging.getLogger(__name__)


class Promoter:
    """Promotes AGENT_FEEDBACK rows to AGENT_GOLDEN. Stub in v1."""

    def __init__(self, session: "Session") -> None:
        self._session = session

    def promote(self, feedback_id: str, promoted_by: str) -> str:
        """Promote a feedback row to AGENT_GOLDEN. Returns new golden_id."""
        raise NotImplementedError(
            "Manual promotion SQL is in the plan doc. "
            "Automated HITL promotion is a v2 feature."
        )
