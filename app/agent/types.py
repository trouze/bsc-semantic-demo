from __future__ import annotations
import time
import contextlib
from dataclasses import dataclass, field
from typing import Any, Literal, Optional


@dataclass
class AgentTurn:
    turn_id: str
    session_id: str
    user_input: str
    user_email: Optional[str] = None


@dataclass
class ContextPack:
    turn: AgentTurn
    history: list[dict[str, Any]]
    metric_names: list[str]
    dimension_names: list[str]
    entity_names: list[str]
    glossary_terms: dict[str, str]
    status_values: list[str]
    catalog_hash: str = ""


@dataclass
class SkillCall:
    skill_name: str
    slots: dict[str, Any]
    rationale: str
    router_raw: str = ""


@dataclass
class SkillResult:
    skill_name: str
    skill_version: str
    data: Any
    compiled_sql: Optional[str] = None
    executed_sql_hash: Optional[str] = None
    snowflake_qid: Optional[str] = None
    confidence: float = 1.0
    error: Optional[str] = None
    status: Literal["ok", "error", "clarify"] = "ok"
    timings: dict[str, float] = field(default_factory=dict)


class Timer:
    """Accumulates named timing segments in milliseconds."""

    def __init__(self) -> None:
        self._start = time.monotonic()
        self._segments: dict[str, float] = {}

    @contextlib.contextmanager
    def segment(self, name: str):
        t0 = time.monotonic()
        try:
            yield
        finally:
            self._segments[name] = round((time.monotonic() - t0) * 1000, 1)

    def elapsed_ms(self) -> float:
        return round((time.monotonic() - self._start) * 1000, 1)

    def get(self, name: str) -> float:
        return self._segments.get(name, 0.0)

    def total_ms(self) -> float:
        return self.elapsed_ms()

    def as_dict(self) -> dict[str, float]:
        return dict(self._segments)
