"""Snowflake Cortex COMPLETE wrapper for structured query planning.

The agent receives the user's natural language query, the full semantic catalog
context, and conversation history, then returns a validated JSON plan that the
orchestrator routes to the appropriate skill.
"""
from __future__ import annotations

import json
import re
from typing import Any


PLANNER_SYSTEM_PROMPT = """\
You are a senior data analyst assistant with access to a curated dbt Semantic Layer.
Your job is to translate user questions into structured metric queries.

## Rules
1. Only use metrics and dimensions listed in the catalog below. Never invent names.
2. Use `metric_time__<grain>` (e.g. `metric_time__month`) as the group_by value for time series.
3. For where filters, use the Jinja format:
   - Time: "{{{{ TimeDimension('metric_time', 'DAY') }}}} >= '2024-01-01'"
   - Dimension: "{{{{ Dimension('customer__region') }}}} = 'US'"
4. Set `clarification` if the query is genuinely ambiguous and you cannot make a reasonable assumption.
5. Set `out_of_scope: true` if the question cannot be answered with the available metrics.
6. Respond with ONLY valid JSON. No markdown, no extra text.

## Output schema
{{
  "skill": "trend" | "compare" | "breakdown" | "rank" | "summary",
  "confidence": 0.0-1.0,
  "interpretation": "one sentence describing what you are computing",
  "metrics": ["metric_name"],
  "group_by": ["qualified_dimension_name"],
  "time_grain": "day" | "week" | "month" | "quarter" | "year" | null,
  "time_start": "YYYY-MM-DD" | null,
  "time_end": "YYYY-MM-DD" | null,
  "where": ["jinja filter string"],
  "limit": integer | null,
  "order_desc": true | false,
  "clarification": null | "question to ask the user",
  "out_of_scope": false | true
}}

## Skill guidance
- trend: time series of a metric, requires time_grain and metric_time in group_by
- compare: metric side-by-side across values of one categorical dimension
- breakdown: how a metric distributes across a dimension (pie/bar)
- rank: top or bottom N by metric value, requires limit
- summary: snapshot of multiple key metrics at a point in time

{catalog_context}
"""


def _parse_json_from_llm(text: str) -> dict:
    """Robustly parse JSON from an LLM response that may have markdown fencing."""
    text = text.strip()
    # Strip markdown code fences
    text = re.sub(r"^```(?:json)?\s*", "", text)
    text = re.sub(r"\s*```$", "", text)
    return json.loads(text.strip())


class CortexAgent:
    """Calls SNOWFLAKE.CORTEX.COMPLETE via the Snowflake connector."""

    def __init__(self, sf_conn: Any, model: str = "claude-3-5-sonnet"):
        self._conn = sf_conn
        self.model = model

    def _complete(self, system: str, messages: list[dict]) -> str:
        """Execute CORTEX.COMPLETE and return the assistant text."""
        payload = [{"role": "system", "content": system}] + messages
        payload_json = json.dumps(payload)
        cur = self._conn.cursor()
        try:
            cur.execute(
                "SELECT SNOWFLAKE.CORTEX.COMPLETE(%s, PARSE_JSON(%s)):choices[0]:messages::VARCHAR",
                (self.model, payload_json),
            )
            row = cur.fetchone()
            return row[0] if row else ""
        finally:
            cur.close()

    def plan_query(
        self,
        user_query: str,
        catalog_context: str,
        history: list[dict] | None = None,
        today: str | None = None,
    ) -> dict:
        """Return a validated query plan dict from the user's natural language query."""
        system = PLANNER_SYSTEM_PROMPT.format(catalog_context=catalog_context)
        if today:
            system += f"\n\nToday's date: {today}"

        messages: list[dict] = []
        for turn in (history or []):
            if turn.get("role") in ("user", "assistant"):
                messages.append({"role": turn["role"], "content": turn["content"]})

        messages.append({"role": "user", "content": user_query})

        raw = self._complete(system, messages)
        plan = _parse_json_from_llm(raw)
        # Ensure required keys exist
        plan.setdefault("skill", "summary")
        plan.setdefault("confidence", 0.5)
        plan.setdefault("interpretation", user_query)
        plan.setdefault("metrics", [])
        plan.setdefault("group_by", [])
        plan.setdefault("time_grain", None)
        plan.setdefault("time_start", None)
        plan.setdefault("time_end", None)
        plan.setdefault("where", [])
        plan.setdefault("limit", None)
        plan.setdefault("order_desc", False)
        plan.setdefault("clarification", None)
        plan.setdefault("out_of_scope", False)
        return plan

    def refresh_connection(self, sf_conn: Any) -> None:
        self._conn = sf_conn
