"""Main orchestration loop — connects catalog, Cortex, skills, guardrails, and feedback."""
from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import TYPE_CHECKING, Any, Optional

from app.async_utils import run_async
from app.semantic.executor import ExecutionResult, QueryPlan

if TYPE_CHECKING:
    import pandas as pd
    from app.agents.cortex import CortexAgent
    from app.feedback.collector import FeedbackCollector
    from app.guardrails.validator import GuardrailsValidator
    from app.semantic.catalog import SemanticCatalog
    from app.semantic.executor import SLExecutor
    from app.skills.registry import SkillRegistry


@dataclass
class ConversationTurn:
    role: str   # "user" | "assistant"
    content: str
    plan: Optional[dict] = None
    result: Optional[Any] = None        # ExecutionResult
    interaction_id: Optional[str] = None
    error: Optional[str] = None
    rating: Optional[str] = None        # "up" | "down"


@dataclass
class QueryResult:
    """Everything the UI needs to render one assistant turn."""
    interaction_id: str
    interpretation: str
    plan: dict
    execution: Optional[ExecutionResult]
    error: Optional[str] = None
    blocked: bool = False
    clarification_needed: Optional[str] = None
    out_of_scope: bool = False


class Orchestrator:
    def __init__(
        self,
        catalog: "SemanticCatalog",
        cortex: "CortexAgent",
        executor: "SLExecutor",
        skills: "SkillRegistry",
        guardrails: "GuardrailsValidator",
        feedback: "FeedbackCollector",
    ):
        self._catalog = catalog
        self._cortex = cortex
        self._executor = executor
        self._skills = skills
        self._guardrails = guardrails
        self._feedback = feedback

    def process(
        self,
        user_query: str,
        history: list[ConversationTurn],
        session_id: str,
        user_id: str = "anonymous",
    ) -> QueryResult:
        interaction_id = str(uuid.uuid4())
        today = date.today().isoformat()

        # Build compact history for the LLM (last N turns, alternating roles)
        llm_history = [
            {"role": t.role, "content": t.content}
            for t in history[-10:]
            if t.role in ("user", "assistant")
        ]

        # Retrieve top relevant metrics for the catalog context block
        catalog_ctx = self._catalog.format_for_llm(query=user_query, top_n=20)

        # --- 1. Plan ---
        try:
            plan = self._cortex.plan_query(
                user_query=user_query,
                catalog_context=catalog_ctx,
                history=llm_history,
                today=today,
            )
        except Exception as exc:
            self._log_interaction(interaction_id, session_id, user_id, user_query, {}, None, f"planner_error: {exc}")
            return QueryResult(
                interaction_id=interaction_id,
                interpretation=user_query,
                plan={},
                execution=None,
                error=f"Query planning failed: {exc}",
            )

        # --- 2. Early exits ---
        if plan.get("clarification"):
            self._log_interaction(interaction_id, session_id, user_id, user_query, plan, None, "clarification")
            return QueryResult(
                interaction_id=interaction_id,
                interpretation=plan["interpretation"],
                plan=plan,
                execution=None,
                clarification_needed=plan["clarification"],
            )

        if plan.get("out_of_scope"):
            self._log_interaction(interaction_id, session_id, user_id, user_query, plan, None, "out_of_scope")
            return QueryResult(
                interaction_id=interaction_id,
                interpretation=plan["interpretation"],
                plan=plan,
                execution=None,
                out_of_scope=True,
            )

        # --- 3. Guardrails ---
        validation = self._guardrails.validate_plan(plan, self._catalog)
        if not validation.ok:
            self._log_interaction(interaction_id, session_id, user_id, user_query, plan, None, f"blocked: {validation.reason}")
            return QueryResult(
                interaction_id=interaction_id,
                interpretation=plan["interpretation"],
                plan=plan,
                execution=None,
                blocked=True,
                error=validation.reason,
            )

        # --- 4. Skill → QueryPlan ---
        skill = self._skills.get(plan.get("skill", "summary"))
        query_plan: QueryPlan = skill.build_query(plan)

        # --- 5. Execute (compile SL → run on Snowflake) ---
        try:
            execution = run_async(self._executor.compile_and_run(query_plan))
        except Exception as exc:
            self._log_interaction(interaction_id, session_id, user_id, user_query, plan, None, f"execution_error: {exc}")
            return QueryResult(
                interaction_id=interaction_id,
                interpretation=plan["interpretation"],
                plan=plan,
                execution=None,
                error=f"Query execution failed: {exc}",
            )

        self._log_interaction(interaction_id, session_id, user_id, user_query, plan, execution, "success")

        return QueryResult(
            interaction_id=interaction_id,
            interpretation=plan["interpretation"],
            plan=plan,
            execution=execution,
        )

    def _log_interaction(
        self,
        interaction_id: str,
        session_id: str,
        user_id: str,
        query: str,
        plan: dict,
        execution: Optional[ExecutionResult],
        status: str,
    ) -> None:
        try:
            self._feedback.log_interaction(
                interaction_id=interaction_id,
                session_id=session_id,
                user_id=user_id,
                user_query=query,
                skill=plan.get("skill"),
                metrics=plan.get("metrics", []),
                dimensions=plan.get("group_by", []),
                confidence=plan.get("confidence"),
                sql=execution.sql if execution else None,
                execution_time_ms=execution.execution_time_ms if execution else None,
                row_count=execution.row_count if execution else None,
                status=status,
            )
        except Exception:
            pass  # feedback failures must never break the main flow
