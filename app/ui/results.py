"""Result rendering — translates DataFrames into charts or tables based on skill hint."""
from __future__ import annotations

from typing import TYPE_CHECKING, Optional

import pandas as pd
import streamlit as st

from app.skills.base import ChartHint

if TYPE_CHECKING:
    from app.agents.orchestrator import QueryResult


def render_result(qr: "QueryResult") -> None:
    """Render the execution result portion of an assistant turn."""
    if qr.error:
        st.error(qr.error)
        return

    if qr.clarification_needed:
        st.info(f"**Clarification needed:** {qr.clarification_needed}")
        return

    if qr.out_of_scope:
        st.warning("This question falls outside the available metrics. Try rephrasing or asking about a different topic.")
        return

    if qr.blocked:
        st.warning(f"**Blocked by data governance:** {qr.error}")
        return

    if not qr.execution or qr.execution.df.empty:
        st.info("Query returned no data. Try adjusting the time range or filters.")
        return

    df = qr.execution.df
    skill = qr.plan.get("skill", "summary")
    hint = _chart_hint_from_skill(skill)

    _render_chart_or_table(df, hint, qr.plan)

    # Metadata expander
    with st.expander("Query details", expanded=False):
        col1, col2, col3 = st.columns(3)
        col1.metric("Rows", f"{qr.execution.row_count:,}")
        col2.metric("Exec time", f"{qr.execution.execution_time_ms}ms")
        col3.metric("Confidence", f"{qr.plan.get('confidence', 0):.0%}")
        st.caption("**Compiled SQL**")
        st.code(qr.execution.sql, language="sql")


def _chart_hint_from_skill(skill: str) -> ChartHint:
    return {
        "trend": ChartHint.LINE,
        "compare": ChartHint.BAR,
        "breakdown": ChartHint.PIE,
        "rank": ChartHint.BAR_HORIZONTAL,
        "summary": ChartHint.METRIC_CARDS,
    }.get(skill, ChartHint.TABLE)


def _render_chart_or_table(df: pd.DataFrame, hint: ChartHint, plan: dict) -> None:
    import plotly.express as px

    metrics = plan.get("metrics", [])
    group_by = plan.get("group_by", [])

    # Identify the metric column(s): try exact match then case-insensitive
    metric_cols = _find_columns(df, metrics)
    dim_cols = [c for c in df.columns if c not in metric_cols]

    if not metric_cols:
        st.dataframe(df, use_container_width=True)
        return

    y_col = metric_cols[0]

    if hint == ChartHint.LINE:
        x_col = _pick_time_col(df, dim_cols)
        color_col = dim_cols[1] if len(dim_cols) > 1 and x_col == dim_cols[0] else (dim_cols[0] if dim_cols and dim_cols[0] != x_col else None)
        fig = px.line(df, x=x_col, y=y_col, color=color_col, markers=True,
                      title=_chart_title(plan))
        st.plotly_chart(fig, use_container_width=True)

    elif hint == ChartHint.BAR:
        x_col = dim_cols[0] if dim_cols else y_col
        color_col = dim_cols[1] if len(dim_cols) > 1 else None
        fig = px.bar(df, x=x_col, y=y_col, color=color_col,
                     title=_chart_title(plan), barmode="group")
        st.plotly_chart(fig, use_container_width=True)

    elif hint == ChartHint.BAR_HORIZONTAL:
        x_col = dim_cols[0] if dim_cols else y_col
        fig = px.bar(df, x=y_col, y=x_col, orientation="h",
                     title=_chart_title(plan))
        st.plotly_chart(fig, use_container_width=True)

    elif hint == ChartHint.PIE:
        names_col = dim_cols[0] if dim_cols else None
        if names_col:
            fig = px.pie(df, names=names_col, values=y_col, title=_chart_title(plan))
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.dataframe(df, use_container_width=True)

    elif hint == ChartHint.METRIC_CARDS:
        _render_metric_cards(df, metric_cols, dim_cols)

    else:
        st.dataframe(df, use_container_width=True)

    # Always show raw data below the chart
    with st.expander("Raw data", expanded=False):
        st.dataframe(df, use_container_width=True)


def _render_metric_cards(df: pd.DataFrame, metric_cols: list[str], dim_cols: list[str]) -> None:
    """Render aggregate metric value cards for summary-style results."""
    cols = st.columns(min(len(metric_cols), 4))
    for i, col_name in enumerate(metric_cols[:4]):
        try:
            val = df[col_name].sum()
            cols[i].metric(col_name.replace("_", " ").title(), f"{val:,.0f}")
        except Exception:
            cols[i].write(col_name)
    if len(df) > 1:
        with st.expander("Full breakdown", expanded=False):
            st.dataframe(df, use_container_width=True)


def _find_columns(df: pd.DataFrame, names: list[str]) -> list[str]:
    """Match requested metric names to actual DataFrame columns (case-insensitive)."""
    lower_map = {c.lower(): c for c in df.columns}
    result = []
    for name in names:
        if name in df.columns:
            result.append(name)
        elif name.lower() in lower_map:
            result.append(lower_map[name.lower()])
    return result or [c for c in df.columns if df[c].dtype in ("int64", "float64")][:1]


def _pick_time_col(df: pd.DataFrame, candidates: list[str]) -> Optional[str]:
    """Prefer columns whose name contains a time-related keyword."""
    for c in candidates:
        if any(k in c.lower() for k in ("date", "time", "month", "week", "year", "quarter", "day")):
            return c
    return candidates[0] if candidates else None


def _chart_title(plan: dict) -> str:
    metrics = ", ".join(plan.get("metrics", []))
    interpretation = plan.get("interpretation", "")
    return interpretation or metrics
