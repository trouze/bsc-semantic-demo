"""EvalRunner — in-process evaluation against AGENT_GOLDEN golden set."""
from __future__ import annotations
import json
import logging
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from snowflake.snowpark import Session

logger = logging.getLogger(__name__)

_READ_GOLDEN_SQL = """
SELECT golden_id, user_input, expected_skill, expected_slots, expected_checks
FROM DEMO_BSC.AGENT_GOLDEN
WHERE active = TRUE
ORDER BY golden_id
"""

_INSERT_EVAL_RUN_SQL = """
INSERT INTO DEMO_BSC.AGENT_EVAL_RUNS (
  run_id, golden_id, prompt_version, model_version, skill_version,
  routed_skill, passed, check_details, latency_ms, error, trace_id
) VALUES (?, ?, ?, ?, ?, ?, ?, PARSE_JSON(?), ?, ?, ?)
"""


@dataclass
class CheckResult:
    check_type: str
    passed: bool
    expected: Any = None
    actual: Any = None
    note: str = ""


@dataclass
class EvalOutcome:
    golden_id: str
    passed: bool
    routed_skill: str = ""
    check_details: list[CheckResult] = field(default_factory=list)
    latency_ms: float = 0.0
    error: str = ""
    trace_id: str = ""


_TOKEN_FIELD: dict[str, tuple[str, str]] = {
    "facility_token": ("FACILITY_NAME", "facility_name"),
    "customer_token": ("CUSTOMER_NAME", "customer_name"),
}


def evaluate_checks(
    expected_checks: list[dict],
    result: Any,  # SkillResult
    skill_call: Any,  # SkillCall
) -> list[CheckResult]:
    """Evaluate a list of golden checks against a skill result.

    Check types: exact_order_id, order_id_suffix, po_suffix,
    facility_token, customer_token, field_not_null, skill_name.
    """
    checks = []
    rows = result.data if isinstance(result.data, list) else []

    for chk in expected_checks:
        chk_type = chk.get("type", "")
        expected_val = chk.get("expected", "")

        if chk_type == "skill_name":
            passed = skill_call.skill_name == expected_val
            checks.append(CheckResult(
                check_type=chk_type, passed=passed,
                expected=expected_val, actual=skill_call.skill_name,
            ))

        elif chk_type == "exact_order_id":
            actual_ids = [r.get("ORDER_ID", r.get("order_id", "")) for r in rows]
            passed = expected_val in actual_ids
            checks.append(CheckResult(
                check_type=chk_type, passed=passed,
                expected=expected_val, actual=actual_ids[:5],
            ))

        elif chk_type == "order_id_suffix":
            passed = any(
                str(r.get("ORDER_ID", r.get("order_id", ""))).endswith(str(expected_val))
                for r in rows
            )
            checks.append(CheckResult(check_type=chk_type, passed=passed, expected=expected_val))

        elif chk_type == "po_suffix":
            passed = any(
                str(r.get("PURCHASE_ORDER_ID", r.get("purchase_order_id", ""))).endswith(str(expected_val))
                for r in rows
            )
            checks.append(CheckResult(check_type=chk_type, passed=passed, expected=expected_val))

        elif chk_type in _TOKEN_FIELD:
            upper_key, lower_key = _TOKEN_FIELD[chk_type]
            token = str(expected_val).lower()
            passed = any(
                token in str(r.get(upper_key, r.get(lower_key, ""))).lower()
                for r in rows
            )
            checks.append(CheckResult(check_type=chk_type, passed=passed, expected=expected_val))

        elif chk_type == "field_not_null":
            field_name = chk.get("field", "")
            passed = bool(rows) and rows[0].get(field_name) is not None
            checks.append(CheckResult(
                check_type=chk_type, passed=passed,
                expected=f"{field_name} is not null",
            ))

        else:
            checks.append(CheckResult(
                check_type=chk_type, passed=False,
                note=f"Unknown check type: {chk_type}",
            ))

    return checks


def compute_summary(outcomes: list[EvalOutcome]) -> dict[str, Any]:
    """Compute accuracy, p50/p95 latency, and SLO pass/fail from a run."""
    if not outcomes:
        return {"total": 0, "passed": 0, "accuracy": 0.0}

    total = len(outcomes)
    passed = sum(1 for o in outcomes if o.passed)
    latencies = sorted(o.latency_ms for o in outcomes if o.latency_ms > 0)

    def percentile(data: list[float], p: float) -> float:
        if not data:
            return 0.0
        idx = int(len(data) * p / 100)
        return data[min(idx, len(data) - 1)]

    p95 = percentile(latencies, 95)
    return {
        "total": total,
        "passed": passed,
        "accuracy": round(passed / total, 3),
        "p50_ms": percentile(latencies, 50),
        "p95_ms": p95,
        "slo_pass": p95 < 5000,
    }


class EvalRunner:
    def __init__(self, session: "Session") -> None:
        self._session = session

    def run(self, golden_ids: Optional[list[str]] = None) -> dict[str, Any]:
        """Run eval against AGENT_GOLDEN, write results to AGENT_EVAL_RUNS.

        Returns summary dict from compute_summary().
        """
        from agent.context.builder import ContextBuilder
        from agent.feedback.trace_writer import TraceWriter
        from agent.router.registry import SkillRegistry
        from agent.router.router import SkillRouter

        run_id = str(uuid.uuid4())[:8]

        golden_rows = self._session.sql(_READ_GOLDEN_SQL).collect()
        if golden_ids:
            golden_rows = [r for r in golden_rows if r["GOLDEN_ID"] in golden_ids]

        logger.info("EvalRunner: running %d golden prompts (run_id=%s)", len(golden_rows), run_id)

        registry = SkillRegistry.build_default(self._session)
        ctx_builder = ContextBuilder(self._session)
        router = SkillRouter(self._session, registry)
        writer = TraceWriter(self._session)

        outcomes: list[EvalOutcome] = []
        for row in golden_rows:
            outcome = self._run_one(run_id, row, registry, ctx_builder, router, writer)
            outcomes.append(outcome)

        summary = compute_summary(outcomes)
        logger.info("EvalRunner done: %s", summary)
        return summary

    def _run_one(
        self,
        run_id: str,
        golden_row: Any,
        registry: Any,
        ctx_builder: Any,
        router: Any,
        writer: Any,
    ) -> EvalOutcome:
        golden_id = golden_row["GOLDEN_ID"]
        user_input = golden_row["USER_INPUT"]
        expected_checks_raw = golden_row["EXPECTED_CHECKS"] or "[]"

        t0 = time.monotonic()
        error = ""
        routed_skill = ""
        trace_id = ""
        check_results: list[CheckResult] = []

        try:
            from agent.types import AgentTurn

            turn = AgentTurn(
                turn_id=str(uuid.uuid4()),
                session_id=f"eval_{run_id}",
                user_input=user_input,
            )

            ctx = ctx_builder.build(turn, [])
            skill_call = router.route(turn, ctx)
            routed_skill = skill_call.skill_name

            skill = registry.get(skill_call.skill_name)
            slots_obj = skill.slot_schema(**skill_call.slots)
            validation_errors = skill.validate(slots_obj, ctx)

            if validation_errors:
                from agent.skills.clarify import ClarifySkill, ClarifySlots
                skill_call.skill_name = "clarify"
                routed_skill = "clarify"
                skill = ClarifySkill()
                slots_obj = ClarifySlots(validation_errors=validation_errors)

            result = skill.execute(slots_obj, ctx)
            trace_id = writer.write(turn, skill_call, result, ctx)

            expected_checks = json.loads(expected_checks_raw) if isinstance(expected_checks_raw, str) else expected_checks_raw
            check_results = evaluate_checks(expected_checks, result, skill_call)

        except Exception as exc:
            logger.exception("EvalRunner._run_one failed for golden_id=%s", golden_id)
            error = str(exc)

        latency_ms = round((time.monotonic() - t0) * 1000, 1)
        all_passed = bool(check_results) and all(c.passed for c in check_results) and not error

        outcome = EvalOutcome(
            golden_id=golden_id,
            passed=all_passed,
            routed_skill=routed_skill,
            check_details=check_results,
            latency_ms=latency_ms,
            error=error,
            trace_id=trace_id,
        )

        try:
            self._session.sql(_INSERT_EVAL_RUN_SQL, params=[
                run_id,
                golden_id,
                "",  # prompt_version
                "",  # model_version
                "",  # skill_version
                routed_skill,
                all_passed,
                json.dumps([
                    {
                        "type": c.check_type,
                        "passed": c.passed,
                        "expected": str(c.expected),
                        "actual": str(c.actual),
                    }
                    for c in check_results
                ]),
                latency_ms,
                error,
                trace_id,
            ]).collect()
        except Exception:
            logger.exception("Failed to write AGENT_EVAL_RUNS for golden_id=%s", golden_id)

        return outcome
