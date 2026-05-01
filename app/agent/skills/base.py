"""Skill protocol — the interface every skill must implement."""
from __future__ import annotations
from typing import Any, Protocol, runtime_checkable

from pydantic import BaseModel


class SlotSpec(BaseModel):
    """Base class for all skill slot schemas. Subclass with Pydantic fields."""
    model_config = {"extra": "ignore"}  # tolerate extra keys from LLM output


@runtime_checkable
class Skill(Protocol):
    """Protocol that every skill must satisfy."""
    name: str
    version: str
    description: str
    slot_schema: type[SlotSpec]

    def validate(self, slots: SlotSpec, ctx: Any) -> list[str]:
        """Return list of validation error strings. Empty = valid."""
        ...

    def execute(self, slots: SlotSpec, ctx: Any) -> Any:
        """Execute the skill and return a SkillResult."""
        ...

    def present(self, result: Any, ctx: Any) -> None:
        """Render the result using st.* calls."""
        ...
