"""MetricCompareSkill — compare a metric across two time periods or segments."""
from __future__ import annotations
from typing import Optional, TYPE_CHECKING

from pydantic import Field

if TYPE_CHECKING:
    from agent.types import ContextPack, SkillResult
    from snowflake.snowpark import Session

try:
    from agent.skills.base import SlotSpec
    from agent.skills.metric_query import GroupBySpec
except ImportError:
    from pydantic import BaseModel
    class SlotSpec(BaseModel):  # type: ignore
        model_config = {"extra": "ignore"}
    class GroupBySpec(SlotSpec):  # type: ignore
        name: str
        grain: Optional[str] = None

        def qualified_name(self) -> str:
            return f"{self.name}__{self.grain}" if self.grain else self.name


class PeriodSpec(SlotSpec):
    label: str           # e.g. "Q1 2024"
    where_filter: str    # MetricFlow WHERE clause fragment, e.g. "metric_time >= '2024-01-01'"


class MetricCompareSlots(SlotSpec):
    metric: str
    compare_dim: GroupBySpec
    period_a: PeriodSpec
    period_b: PeriodSpec
    filter: Optional[list[str]] = None


class MetricCompareSkill:
    """Demonstrates multi-step orchestration: two compile+execute calls, then Python merge."""
    name = "metric_compare"
    version = "v1"
    description = (
        "Compare a metric across two time periods or segments. "
        "Returns side-by-side table with delta and percent change."
    )
    slot_schema = MetricCompareSlots

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

    def validate(self, slots: MetricCompareSlots, ctx: "ContextPack") -> list[str]:
        from agent.guardrails.slot_validators import validate_metric_names, validate_dimension_names
        errors = []
        errors.extend(validate_metric_names([slots.metric], ctx.metric_names))
        errors.extend(validate_dimension_names([slots.compare_dim.name], ctx.dimension_names))
        return errors

    def execute(self, slots: MetricCompareSlots, ctx: "ContextPack") -> "SkillResult":
        from agent.types import SkillResult
        import pandas as pd

        dim_name = slots.compare_dim.qualified_name()
        base_where = slots.filter or []

        sql_a = self._sl.compile_sql(
            metrics=[slots.metric],
            group_by=[dim_name],
            where=base_where + [slots.period_a.where_filter],
        )
        sql_b = self._sl.compile_sql(
            metrics=[slots.metric],
            group_by=[dim_name],
            where=base_where + [slots.period_b.where_filter],
        )

        result_a = self._executor.run(sql_a)
        result_b = self._executor.run(sql_b)

        df_a = pd.DataFrame(result_a.rows).rename(columns={slots.metric: f"{slots.metric}_a"})
        df_b = pd.DataFrame(result_b.rows).rename(columns={slots.metric: f"{slots.metric}_b"})

        if df_a.empty and df_b.empty:
            return SkillResult(
                skill_name=self.name,
                skill_version=self.version,
                data=[],
                compiled_sql=f"-- Period A:\n{sql_a}\n\n-- Period B:\n{sql_b}",
            )

        merge_col = slots.compare_dim.name
        if merge_col not in df_a.columns or merge_col not in df_b.columns:
            # fallback: use first column
            if not df_a.empty:
                merge_col = df_a.columns[0]

        merged = df_a.merge(df_b, on=merge_col, how="outer").fillna(0)
        col_a = f"{slots.metric}_a"
        col_b = f"{slots.metric}_b"
        if col_a in merged.columns and col_b in merged.columns:
            merged["delta"] = merged[col_b] - merged[col_a]
            merged["pct_change"] = (
                (merged["delta"] / merged[col_a].replace(0, float("nan"))) * 100
            ).round(1)

        return SkillResult(
            skill_name=self.name,
            skill_version=self.version,
            data=merged.to_dict(orient="records"),
            compiled_sql=f"-- Period A:\n{sql_a}\n\n-- Period B:\n{sql_b}",
        )

    def present(self, result: "SkillResult", ctx: "ContextPack") -> None:
        try:
            import streamlit as st
            import pandas as pd
            if not result.data:
                st.info("No comparison data.")
                return
            df = pd.DataFrame(result.data)
            st.dataframe(df, use_container_width=True)
            delta_col = "delta"
            dim_col = df.columns[0]
            if delta_col in df.columns:
                st.bar_chart(df.set_index(dim_col)[delta_col])
            with st.expander("SQL for both periods"):
                st.code(result.compiled_sql or "", language="sql")
        except ImportError:
            pass
