"""Snowflake query executor — runs SQL via Snowpark with safety checks.

All SQL passes through sql_guard.assert_safe() before execution.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from snowflake.snowpark import Session

from agent.guardrails.sql_guard import assert_safe, sql_hash

logger = logging.getLogger(__name__)


@dataclass
class QueryResult:
    rows: list[dict[str, Any]]
    query_id: str
    elapsed_ms: float
    sql_hash: str
    row_count: int


class SnowflakeExecutor:
    """Executes parameterized SQL against Snowflake via Snowpark session."""

    def __init__(
        self,
        session: "Session",
        max_rows: int = 500,
        timeout_s: int = 30,
    ) -> None:
        self._session = session
        self._max_rows = max_rows
        self._timeout_s = timeout_s  # reserved for future query-level timeout enforcement

    def run(self, sql: str, params: list[Any] | None = None) -> QueryResult:
        """Execute sql after safety check. Returns QueryResult.

        Uses Snowpark session.sql(...).collect() for parameterized execution.
        Never uses string interpolation — params go through Snowpark binding.

        Args:
            sql: Pre-compiled SQL to execute (must pass assert_safe).
            params: Optional positional bind parameters for Snowpark.

        Returns:
            QueryResult with rows, query_id, elapsed_ms, sql_hash, row_count.

        Raises:
            ValueError: If the SQL fails the safety check.
            Exception: Any Snowpark / Snowflake execution error.
        """
        assert_safe(sql)

        h = sql_hash(sql)
        t0 = time.monotonic()

        try:
            if params:
                sf_df = self._session.sql(sql, params=params)
            else:
                sf_df = self._session.sql(sql)

            rows_raw = sf_df.limit(self._max_rows).collect()
            rows = [r.as_dict() for r in rows_raw]
            row_count = len(rows)

            # Retrieve the query ID from Snowflake's session metadata.
            query_id = ""
            try:
                query_id = (
                    self._session.sql("SELECT LAST_QUERY_ID()").collect()[0][0] or ""
                )
            except Exception:
                pass

            elapsed = round((time.monotonic() - t0) * 1000, 1)
            logger.info(
                "snowflake_executor.run",
                extra={
                    "sql_hash": h,
                    "query_id": query_id,
                    "row_count": row_count,
                    "elapsed_ms": elapsed,
                },
            )
            return QueryResult(
                rows=rows,
                query_id=query_id,
                elapsed_ms=elapsed,
                sql_hash=h,
                row_count=row_count,
            )
        except Exception:
            logger.exception("SnowflakeExecutor.run failed for sql_hash=%s", h)
            raise
