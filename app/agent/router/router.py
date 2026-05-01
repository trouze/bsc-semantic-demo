"""SkillRouter — routes user intent to a registered Skill via Cortex Complete."""
from __future__ import annotations
import logging
import re
from typing import Any, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from agent.types import AgentTurn, ContextPack, SkillCall
    from snowflake.snowpark import Session

logger = logging.getLogger(__name__)

# Regex fast-path: exact order_id or PO prefix — skip LLM entirely
_EXACT_ORDER_RE = re.compile(r'\b(ORD-\d+|PO[-_]\w+)\b', re.IGNORECASE)

_ROUTER_PROMPT = """You are a routing agent for a Boston Scientific order management assistant.

Given a user message, you must select exactly ONE skill and extract its slots.

Available skills:
{skill_catalog}

User message: {user_input}

Recent conversation context:
{history_summary}

Respond with ONLY valid JSON in this exact format:
{{
  "skill": "<skill_name>",
  "slots": {{<slot_key>: <value>, ...}},
  "rationale": "<one sentence explaining the routing decision>"
}}

Rules:
- skill must be one of: {skill_names}
- If the intent is unclear or the query cannot be routed to a data skill, use "clarify"
- Never include fields outside the JSON object
- Do not hallucinate metric names or dimension names — use only what you know from context
"""


class SkillRouter:
    def __init__(
        self,
        session: "Session",
        registry: Any,
        model: Optional[str] = None,
    ) -> None:
        from agent.cortex.client import CortexClient
        from agent import config
        self._cortex = CortexClient(session, default_model=model or config.CORTEX_ROUTER_MODEL)
        self._registry = registry

    def route(self, turn: "AgentTurn", ctx: "ContextPack") -> "SkillCall":
        from agent.types import SkillCall

        m = _EXACT_ORDER_RE.search(turn.user_input)
        if m:
            logger.debug("Router fast-path: exact ID match '%s'", m.group())
            matched = m.group()
            matched_upper = matched.upper()
            return SkillCall(
                skill_name="order_lookup",
                slots={
                    "order_id": matched if matched_upper.startswith("ORD") else None,
                    "purchase_order_id": matched if matched_upper.startswith("PO") else None,
                },
                rationale="Exact order/PO ID detected — fast-path routing.",
            )

        history_summary = _format_history(ctx.history)
        prompt = _ROUTER_PROMPT.format(
            skill_catalog=self._registry.skill_catalog_text(),
            user_input=turn.user_input,
            history_summary=history_summary,
            skill_names=", ".join(self._registry.names()),
        )

        parsed = self._cortex.complete_json(prompt, label="router")

        if parsed is None:
            logger.warning("Router: JSON parse failed, routing to clarify")
            return SkillCall(
                skill_name="clarify",
                slots={"question": "I didn't understand that — could you rephrase?"},
                rationale="Router JSON parse failure.",
            )

        skill_name = parsed.get("skill", "clarify")
        if skill_name not in self._registry.names():
            logger.warning("Router: unknown skill '%s', routing to clarify", skill_name)
            skill_name = "clarify"

        return SkillCall(
            skill_name=skill_name,
            slots=parsed.get("slots", {}),
            rationale=parsed.get("rationale", ""),
            router_raw=str(parsed),
        )


def _format_history(history: list[dict]) -> str:
    if not history:
        return "(no prior turns)"
    lines = []
    for turn in history[-3:]:
        user_in = turn.get("user_input", "")
        skill = turn.get("routed_skill", "")
        lines.append(f"  User: {user_in[:100]}\n  Skill: {skill}")
    return "\n".join(lines)
