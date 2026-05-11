"""Query executor: compile SQL via dbt SL, execute directly on Snowflake.

This is the performance-critical path. compile_sql() asks the dbt Semantic Layer
to translate metric + dimension intent into warehouse SQL, then we run that SQL
ourselves on a direct Snowflake connection, bypassing the SL's execution proxy.

`compile_sql_async` / `compile_sql_sync` compile only (no warehouse) — used by
the Streamlit explorer and anywhere else you need the rendered SQL string.
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Optional, Sequence

import pandas as pd

from dbtsl.asyncio import AsyncSemanticLayerClient

from app.async_utils import run_async


@dataclass
class QueryPlan:
    """Structured query parameters ready for the dbt SL."""
    metrics: list[str]
    group_by: list[Any]                     # str | GroupByParam
    where: list[str]                        # semantic-layer SQL snippets
    order_by: list[Any]                     # OrderByMetric | OrderByGroupBy
    limit: Optional[int]


async def compile_sql_async(
    environment_id: int,
    auth_token: str,
    host: str,
    *,
    metrics: Optional[list[str]] = None,
    group_by: Optional[list[Any]] = None,
    where: Optional[list[str]] = None,
    order_by: Optional[list[Any]] = None,
    limit: Optional[int] = None,
    read_cache: bool = True,
) -> str:
    """Return warehouse SQL (GraphQL compileSql).

    Pass **group_by only** for non-aggregated grain/list queries; add **metrics** for KPIs.
    At least one of metrics or group_by must be provided.
    """
    if not metrics and (group_by is None or len(group_by) == 0):
        raise ValueError("compile_sql requires metrics and/or a non-empty group_by")

    client = AsyncSemanticLayerClient(
        environment_id=environment_id,
        auth_token=auth_token,
        host=host,
        lazy=False,
    )
    payload: dict[str, Any] = {"read_cache": read_cache}
    if metrics:
        payload["metrics"] = metrics
    if group_by is not None:
        payload["group_by"] = group_by
    if where:
        payload["where"] = where
    if order_by:
        payload["order_by"] = order_by
    if limit is not None:
        payload["limit"] = limit
    async with client.session():
        return await client.compile_sql(**payload)


def compile_sql_sync(
    environment_id: int,
    auth_token: str,
    host: str,
    *,
    metrics: Optional[list[str]] = None,
    group_by: Optional[list[Any]] = None,
    where: Optional[list[str]] = None,
    order_by: Optional[list[Any]] = None,
    limit: Optional[int] = None,
    read_cache: bool = True,
) -> str:
    """Sync wrapper for Streamlit and other non-async callers."""
    return run_async(
        compile_sql_async(
            environment_id,
            auth_token,
            host,
            metrics=metrics,
            group_by=group_by,
            where=where,
            order_by=order_by,
            limit=limit,
            read_cache=read_cache,
        )
    )


async def entities_async(
    environment_id: int,
    auth_token: str,
    host: str,
    *,
    anchor_metrics: Sequence[str],
) -> list[Any]:
    """Semantic entities reachable from the given metrics (same graph / joins)."""
    client = AsyncSemanticLayerClient(
        environment_id=environment_id,
        auth_token=auth_token,
        host=host,
        lazy=False,
    )
    async with client.session():
        return await client.entities(metrics=list(anchor_metrics))


def entities_sync(
    environment_id: int,
    auth_token: str,
    host: str,
    *,
    anchor_metrics: Sequence[str],
) -> list[Any]:
    return run_async(
        entities_async(
            environment_id,
            auth_token,
            host,
            anchor_metrics=anchor_metrics,
        )
    )


async def dimension_values_async(
    environment_id: int,
    auth_token: str,
    host: str,
    *,
    metrics: Sequence[str],
    group_by: str,
) -> list[str]:
    """Distinct values for a dimension in the context of the given metrics (ADBC → PyArrow)."""
    client = AsyncSemanticLayerClient(
        environment_id=environment_id,
        auth_token=auth_token,
        host=host,
        lazy=False,
    )
    async with client.session():
        table = await client.dimension_values(metrics=list(metrics), group_by=group_by)
    if table.num_rows == 0 or table.num_columns == 0:
        return []
    col = table.column(0)
    return [str(x) for x in col.to_pylist() if x is not None]


def dimension_values_sync(
    environment_id: int,
    auth_token: str,
    host: str,
    *,
    metrics: Sequence[str],
    group_by: str,
) -> list[str]:
    return run_async(
        dimension_values_async(
            environment_id,
            auth_token,
            host,
            metrics=metrics,
            group_by=group_by,
        )
    )


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
        return await compile_sql_async(
            self._sl_cfg["environment_id"],
            self._sl_cfg["auth_token"],
            self._sl_cfg["host"],
            metrics=plan.metrics,
            group_by=plan.group_by or None,
            where=plan.where or None,
            order_by=plan.order_by or None,
            limit=plan.limit,
        )

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
