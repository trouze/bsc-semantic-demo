"""Query executor: compile SQL via dbt SL, execute directly on Snowflake.

This is the performance-critical path. compile_sql() asks the dbt Semantic Layer
to translate metric + dimension intent into warehouse SQL, then we run that SQL
ourselves on a direct Snowflake connection, bypassing the SL's execution proxy.
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Optional

import pandas as pd

from dbtsl.asyncio import AsyncSemanticLayerClient
from dbtsl.api.shared.query_params import OrderByGroupBy, OrderByMetric


@dataclass
class QueryPlan:
    """Structured query parameters ready for the dbt SL."""
    metrics: list[str]
    group_by: list[str]                     # qualified dimension names + grain tokens
    where: list[str]                        # Jinja filter strings
    order_by: list[Any]                     # OrderByMetric | OrderByGroupBy
    limit: Optional[int]


@dataclass
class ExecutionResult:
    df: pd.DataFrame
    sql: str
    execution_time_ms: int
    row_count: int


class SLExecutor:
    """Compiles metric queries via dbt SL, executes on Snowflake connector."""

    def __init__(self, environment_id: int, auth_token: str, host: str, sf_conn: Any):
        self._sl_cfg = dict(environment_id=environment_id, auth_token=auth_token, host=host)
        self._conn = sf_conn

    async def compile_and_run(self, plan: QueryPlan) -> ExecutionResult:
        """Compile SQL through the SL, then run it on Snowflake directly."""
        sql = await self._compile(plan)
        start = time.monotonic()
        df = self._execute_sql(sql, plan.limit)
        elapsed_ms = int((time.monotonic() - start) * 1000)
        return ExecutionResult(
            df=df,
            sql=sql,
            execution_time_ms=elapsed_ms,
            row_count=len(df),
        )

    async def _compile(self, plan: QueryPlan) -> str:
        client = AsyncSemanticLayerClient(**self._sl_cfg)
        async with client.session():
            sql = await client.compile_sql(
                metrics=plan.metrics,
                group_by=plan.group_by or None,
                where=plan.where or None,
                order_by=plan.order_by or None,
                limit=plan.limit,
            )
        return sql

    def _execute_sql(self, sql: str, limit: Optional[int]) -> pd.DataFrame:
        cur = self._conn.cursor()
        try:
            if limit:
                # Wrap in a subquery if limit not already baked in
                effective_sql = f"SELECT * FROM ({sql}) __q LIMIT {int(limit)}"
            else:
                effective_sql = sql
            cur.execute(effective_sql)
            return cur.fetch_pandas_all()
        finally:
            cur.close()

    def refresh_connection(self, sf_conn: Any) -> None:
        self._conn = sf_conn
