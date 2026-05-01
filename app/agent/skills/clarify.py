"""ClarifySkill — router fallback when intent is ambiguous or routing fails."""
from __future__ import annotations

from typing import TYPE_CHECKING

from pydantic import Field

if TYPE_CHECKING:
    from agent.types import ContextPack, SkillResult

try:
    from agent.skills.base import SlotSpec
except ImportError:
    from pydantic import BaseModel

    class SlotSpec(BaseModel):  # type: ignore[no-redef]
        model_config = {"extra": "ignore"}


class ClarifySlots(SlotSpec):  # noqa: F821
    question: str = Field(default="", description="The disambiguation question to ask")
    original_input: str = ""
    validation_errors: list[str] = Field(default_factory=list)


class ClarifySkill:
    """Always asks one clarifying question — never executes data queries."""

    name = "clarify"
    version = "v1"
    description = (
        "Ask a single clarifying question when the user's intent is ambiguous, "
        "routing fails, or slot validation cannot be resolved. Never queries data."
    )
    slot_schema = ClarifySlots

    def validate(self, slots: ClarifySlots, ctx: "ContextPack") -> list[str]:
        return []  # ClarifySkill always passes validation

    def execute(self, slots: ClarifySlots, ctx: "ContextPack") -> "SkillResult":
        from agent.types import SkillResult

        question = slots.question or (
            "I'm not sure I understood your request. Could you clarify what you're looking for?"
        )
        return SkillResult(
            skill_name=self.name,
            skill_version=self.version,
            data={"question": question, "validation_errors": slots.validation_errors},
            status="clarify",
        )

    def present(self, result: "SkillResult", ctx: "ContextPack") -> None:
        try:
            import streamlit as st
        except ImportError:
            return  # non-Streamlit context (tests, stored procs)

        st.warning(result.data["question"])
        if result.data.get("validation_errors"):
            with st.expander("Details"):
                for e in result.data["validation_errors"]:
                    st.write(f"• {e}")
