"""Skill base class — bounded, named operations with clear query contracts.

Each skill knows how to translate a Cortex plan dict into a dbt SL QueryPlan.
Skills are the guardrail-safe middle layer between intent and execution.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from enum import Enum
from typing import Optional


class ChartHint(str, Enum):
    LINE = "line"
    BAR = "bar"
    BAR_HORIZONTAL = "bar_horizontal"
    PIE = "pie"
    TABLE = "table"
    METRIC_CARDS = "metric_cards"


class Skill(ABC):
    name: str
    description: str
    examples: list[str]

    @abstractmethod
    def build_query(self, plan: dict) -> "QueryPlan":  # noqa: F821
        """Translate a Cortex plan dict into a dbt SL QueryPlan."""
        ...

    @property
    def chart_hint(self) -> ChartHint:
        return ChartHint.TABLE

    def llm_description(self) -> str:
        ex = "\n  - ".join(self.examples)
        return f"**{self.name}**: {self.description}\n  Examples:\n  - {ex}"
