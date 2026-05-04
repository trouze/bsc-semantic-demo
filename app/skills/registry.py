"""Skill registry — maps plan intent strings to Skill instances."""
from __future__ import annotations

from .base import Skill


class SkillRegistry:
    def __init__(self, skills: list[Skill]):
        self._by_name: dict[str, Skill] = {s.name: s for s in skills}

    def get(self, name: str) -> Skill:
        if name not in self._by_name:
            # Default to summary when the LLM returns an unrecognized skill name
            return self._by_name.get("summary", next(iter(self._by_name.values())))
        return self._by_name[name]

    def list_for_llm(self) -> str:
        return "\n\n".join(s.llm_description() for s in self._by_name.values())

    def names(self) -> list[str]:
        return list(self._by_name.keys())
