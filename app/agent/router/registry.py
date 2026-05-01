"""SkillRegistry — maps skill name strings to Skill instances."""
from __future__ import annotations
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from snowflake.snowpark import Session


class SkillRegistry:
    """Holds a name → Skill mapping. Built once per session."""

    def __init__(self) -> None:
        self._skills: dict[str, Any] = {}

    def register(self, skill: Any) -> None:
        self._skills[skill.name] = skill

    def get(self, name: str) -> Any | None:
        return self._skills.get(name)

    def names(self) -> list[str]:
        return list(self._skills.keys())

    def skill_catalog_text(self) -> str:
        """Returns a human-readable skill catalog for the router prompt."""
        lines = []
        for skill in self._skills.values():
            schema_fields = []
            if hasattr(skill, "slot_schema") and hasattr(skill.slot_schema, "model_fields"):
                schema_fields = list(skill.slot_schema.model_fields.keys())
            lines.append(
                f"- {skill.name} (v{skill.version}): {skill.description}\n"
                f"  slots: {', '.join(schema_fields)}"
            )
        return "\n".join(lines)

    @classmethod
    def build_default(cls, session: "Session") -> "SkillRegistry":
        """Build a registry pre-loaded with all default skills."""
        from agent.skills.order_lookup import OrderLookupSkill
        from agent.skills.metric_query import MetricQuerySkill
        from agent.skills.metric_compare import MetricCompareSkill
        from agent.skills.clarify import ClarifySkill

        registry = cls()
        registry.register(OrderLookupSkill(session))
        registry.register(MetricQuerySkill(session))
        registry.register(MetricCompareSkill(session))
        registry.register(ClarifySkill())
        return registry
