"""Built-in skills — the reusable, bounded analytical operations.

Each skill translates a structured plan dict into a dbt SL QueryPlan.
Time dimension handling follows dbt SL conventions:
  group_by: ["metric_time__month"]  (grain embedded in the name)
  where: ["{{ TimeDimension('metric_time', 'DAY') }} >= '2024-01-01'"]
"""
from __future__ import annotations

from app.semantic.executor import QueryPlan
from .base import ChartHint, Skill
from dbtsl.api.shared.query_params import OrderByGroupBy, OrderByMetric


def _time_grain_token(grain: str | None) -> str:
    return (grain or "month").lower()


def _time_group_by(grain: str | None) -> str:
    return f"metric_time__{_time_grain_token(grain)}"


def _time_where_filters(time_start: str | None, time_end: str | None, grain: str | None) -> list[str]:
    """Build dbt SL Jinja where expressions for a time window."""
    g = _time_grain_token(grain).upper()
    filters = []
    if time_start:
        filters.append(f"{{{{ TimeDimension('metric_time', '{g}') }}}} >= '{time_start}'")
    if time_end:
        filters.append(f"{{{{ TimeDimension('metric_time', '{g}') }}}} <= '{time_end}'")
    return filters


class TrendSkill(Skill):
    """Metric value over time, optionally split by a categorical dimension."""

    name = "trend"
    description = "Plot a metric over time with an optional dimension breakdown."
    examples = [
        "Show me monthly order counts",
        "Revenue trend by region over the last quarter",
        "Weekly active customers this year",
    ]

    @property
    def chart_hint(self) -> ChartHint:
        return ChartHint.LINE

    def build_query(self, plan: dict) -> QueryPlan:
        grain = plan.get("time_grain") or "month"
        time_gb = _time_group_by(grain)

        # Always include metric_time at the requested grain
        group_by = [time_gb]
        # Add any categorical split dimensions (exclude time dims to avoid duplication)
        for dim in plan.get("group_by", []):
            if "metric_time" not in dim and dim != time_gb:
                group_by.append(dim)

        where = list(plan.get("where", []))
        where.extend(_time_where_filters(plan.get("time_start"), plan.get("time_end"), grain))

        order_by = [OrderByGroupBy(name=time_gb, grain=grain, descending=False)]

        return QueryPlan(
            metrics=plan["metrics"],
            group_by=group_by,
            where=where,
            order_by=order_by,
            limit=plan.get("limit") or 5000,
        )


class CompareSkill(Skill):
    """Compare a metric across values of a single categorical dimension."""

    name = "compare"
    description = "Side-by-side comparison of a metric across dimension values."
    examples = [
        "Compare revenue by customer segment",
        "Order count by order status",
        "Revenue by region this year",
    ]

    @property
    def chart_hint(self) -> ChartHint:
        return ChartHint.BAR

    def build_query(self, plan: dict) -> QueryPlan:
        grain = plan.get("time_grain")
        group_by = list(plan.get("group_by", []))
        if grain and not any("metric_time" in g for g in group_by):
            group_by.append(_time_group_by(grain))

        where = list(plan.get("where", []))
        where.extend(_time_where_filters(plan.get("time_start"), plan.get("time_end"), grain))

        desc = plan.get("order_desc", True)
        order_by = [OrderByMetric(name=plan["metrics"][0], descending=desc)] if plan.get("metrics") else []

        return QueryPlan(
            metrics=plan["metrics"],
            group_by=group_by,
            where=where,
            order_by=order_by,
            limit=plan.get("limit") or 50,
        )


class BreakdownSkill(Skill):
    """Distribute a metric across dimension values (good for pie/bar share)."""

    name = "breakdown"
    description = "Show how a metric distributes across categories."
    examples = [
        "Break down orders by product category",
        "Revenue distribution by region",
        "What share of orders are from each facility type?",
    ]

    @property
    def chart_hint(self) -> ChartHint:
        return ChartHint.PIE

    def build_query(self, plan: dict) -> QueryPlan:
        grain = plan.get("time_grain")
        group_by = list(plan.get("group_by", []))
        where = list(plan.get("where", []))
        where.extend(_time_where_filters(plan.get("time_start"), plan.get("time_end"), grain))

        desc = not plan.get("order_desc", False)  # breakdown defaults to highest first
        order_by = [OrderByMetric(name=plan["metrics"][0], descending=desc)] if plan.get("metrics") else []

        return QueryPlan(
            metrics=plan["metrics"],
            group_by=group_by,
            where=where,
            order_by=order_by,
            limit=plan.get("limit") or 20,
        )


class RankSkill(Skill):
    """Top or bottom N entities by metric value."""

    name = "rank"
    description = "Rank entities by a metric, returning the top or bottom N."
    examples = [
        "Top 10 customers by revenue",
        "Bottom 5 products by order count last month",
        "Which regions have the lowest revenue this quarter?",
    ]

    @property
    def chart_hint(self) -> ChartHint:
        return ChartHint.BAR_HORIZONTAL

    def build_query(self, plan: dict) -> QueryPlan:
        grain = plan.get("time_grain")
        group_by = list(plan.get("group_by", []))
        where = list(plan.get("where", []))
        where.extend(_time_where_filters(plan.get("time_start"), plan.get("time_end"), grain))

        desc = not plan.get("order_desc", False)  # rank defaults to descending (top)
        order_by = [OrderByMetric(name=plan["metrics"][0], descending=desc)] if plan.get("metrics") else []

        return QueryPlan(
            metrics=plan["metrics"],
            group_by=group_by,
            where=where,
            order_by=order_by,
            limit=plan.get("limit") or 10,
        )


class SummarySkill(Skill):
    """Snapshot of one or more key metrics, no dimensional breakdown."""

    name = "summary"
    description = "High-level snapshot of key metrics for a time period."
    examples = [
        "How are we doing this month?",
        "Give me a summary of key metrics for Q1",
        "What was total revenue last year?",
    ]

    @property
    def chart_hint(self) -> ChartHint:
        return ChartHint.METRIC_CARDS

    def build_query(self, plan: dict) -> QueryPlan:
        grain = plan.get("time_grain")
        group_by = list(plan.get("group_by", []))
        # For summary, always add a time grain if specified so we get a period breakdown
        if grain and not any("metric_time" in g for g in group_by):
            group_by.append(_time_group_by(grain))

        where = list(plan.get("where", []))
        where.extend(_time_where_filters(plan.get("time_start"), plan.get("time_end"), grain))

        return QueryPlan(
            metrics=plan["metrics"],
            group_by=group_by,
            where=where,
            order_by=[],
            limit=plan.get("limit") or 1000,
        )


BUILTIN_SKILLS: list[Skill] = [
    TrendSkill(),
    CompareSkill(),
    BreakdownSkill(),
    RankSkill(),
    SummarySkill(),
]
