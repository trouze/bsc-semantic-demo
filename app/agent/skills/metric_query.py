"""MetricQuerySkill — compile via dbt SL, execute on Snowflake directly."""
from __future__ import annotations
from typing import Optional, TYPE_CHECKING

from pydantic import Field

if TYPE_CHECKING:
    from agent.types import ContextPack, SkillResult
    from snowflake.snowpark import Session

try:
    from agent.skills.base import SlotSpec
except ImportError:
    from pydantic import BaseModel
    class SlotSpec(BaseModel):  # type: ignore
        model_config = {"extra": "ignore"}


class GroupBySpec(SlotSpec):
    name: str
    grain: Optional[str] = None  # time dimensions: 'day'|'week'|'month'|'quarter'|'year'

    def qualified_name(self) -> str:
        """Return MetricFlow-qualified name, e.g. metric_time__month."""
        return f"{self.name}__{self.grain}" if self.grain else self.name


class MetricQuerySlots(SlotSpec):
    question: str = ""
    metrics: list[str] = Field(default_factory=list)
    group_by: list[GroupBySpec] = Field(default_factory=list)
    order_by: Optional[list[str]] = None
    where: Optional[list[str]] = None
    limit: int = Field(default=100, ge=1, le=500)


class MetricQuerySkill:
    name = "metric_query"
    version = "v1"
    description = (
        "Query dbt Semantic Layer metrics. Compiles MetricFlow SQL via dbt SL SDK "
        "then executes directly on Snowflake. Use for aggregated business metrics "
        "like orders by status, revenue by region, etc."
    )
    slot_schema = MetricQuerySlots

    def __init__(self, session: "Session") -> None:
        from agent.semantic.sl_client import SLClient
        from agent.semantic.executor import SnowflakeExecutor
        from agent import config, secrets
        self._sl = SLClient(
            host=config.DBT_SL_HOST,
            environment_id=config.DBT_ENVIRONMENT_ID,
            token=secrets.get_dbt_cloud_token(),
        )
        self._executor = SnowflakeExecutor(session)

    def validate(self, slots: MetricQuerySlots, ctx: "ContextPack") -> list[str]:
        from agent.guardrails.slot_validators import (
            validate_metric_names, validate_dimension_names
        )
        errors = []
        if not slots.metrics:
            errors.append("No metrics specified.")
        errors.extend(validate_metric_names(slots.metrics, ctx.metric_names))
        dim_names = [g.name for g in slots.group_by]
        errors.extend(validate_dimension_names(dim_names, ctx.dimension_names))
        return errors

    def execute(self, slots: MetricQuerySlots, ctx: "ContextPack") -> "SkillResult":
        from agent.types import SkillResult
        import time
        t0 = time.monotonic()

        group_by_names = [g.qualified_name() for g in slots.group_by]

        compiled_sql = self._sl.compile_sql(
            metrics=slots.metrics,
            group_by=group_by_names,
            where=slots.where or [],
            order_by=slots.order_by or [],
            limit=slots.limit,
        )
        compile_ms = round((time.monotonic() - t0) * 1000, 1)

        result = self._executor.run(compiled_sql)
        total_ms = round((time.monotonic() - t0) * 1000, 1)

        return SkillResult(
            skill_name=self.name,
            skill_version=self.version,
            data=result.rows,
            compiled_sql=compiled_sql,
            executed_sql_hash=result.sql_hash,
            snowflake_qid=result.query_id,
            timings={"compile_ms": compile_ms, "execute_ms": result.elapsed_ms, "total_ms": total_ms},
        )

    def present(self, result: "SkillResult", ctx: "ContextPack") -> None:
        try:
            import streamlit as st
            import pandas as pd
            if not result.data:
                st.info("No results returned.")
                return
            df = pd.DataFrame(result.data)
            st.dataframe(df, use_container_width=True)
            date_cols = [c for c in df.columns if any(kw in c.lower() for kw in ["date", "period", "month", "week", "day"])]
            if date_cols and len(df.columns) >= 2:
                numeric_cols = df.select_dtypes(include="number").columns.tolist()
                if numeric_cols:
                    st.line_chart(df.set_index(date_cols[0])[numeric_cols])
            with st.expander("Compiled MetricFlow SQL"):
                st.code(result.compiled_sql or "", language="sql")
        except ImportError:
            pass
