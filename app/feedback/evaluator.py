"""Eval framework — runs golden set queries and scores against expected outputs.

Intended to be run in CI or as a Snowflake Task before deploying prompt changes.
Compares compiled SQL fragments and skill routing against the golden set.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Optional


@dataclass
class EvalCase:
    golden_id: str
    query: str
    expected_skill: Optional[str]
    expected_metrics: list[str]
    expected_sql_fragment: Optional[str]


@dataclass
class EvalResult:
    golden_id: str
    query: str
    passed: bool
    skill_match: Optional[bool]
    metrics_match: Optional[bool]
    sql_fragment_match: Optional[bool]
    actual_skill: Optional[str]
    actual_metrics: list[str]
    actual_sql: Optional[str]
    error: Optional[str] = None


class EvalRunner:
    """Loads golden cases from Snowflake and runs them through the orchestrator."""

    def __init__(self, sf_conn: Any, db: str, schema: str):
        self._conn = sf_conn
        self._fq = f"{db}.{schema}"

    def load_golden_set(self) -> list[EvalCase]:
        cur = self._conn.cursor()
        try:
            cur.execute(
                f"""
                SELECT GOLDEN_ID, QUERY, EXPECTED_SKILL, EXPECTED_METRICS, EXPECTED_SQL_FRAGMENT
                FROM {self._fq}.GOLDEN_SET
                ORDER BY CREATED_AT
                """
            )
            cases = []
            for row in cur.fetchall():
                metrics_raw = row[3]
                metrics = json.loads(metrics_raw) if metrics_raw else []
                cases.append(EvalCase(
                    golden_id=row[0],
                    query=row[1],
                    expected_skill=row[2],
                    expected_metrics=metrics,
                    expected_sql_fragment=row[4],
                ))
            return cases
        finally:
            cur.close()

    def run(self, orchestrator: Any, session_id: str = "eval") -> list[EvalResult]:
        """Execute each golden case and score results."""
        cases = self.load_golden_set()
        results = []
        for case in cases:
            try:
                qr = orchestrator.process(
                    user_query=case.query,
                    history=[],
                    session_id=session_id,
                    user_id="eval_runner",
                )
                skill_match = (
                    qr.plan.get("skill") == case.expected_skill
                    if case.expected_skill else None
                )
                actual_metrics = qr.plan.get("metrics", [])
                metrics_match = (
                    set(actual_metrics) == set(case.expected_metrics)
                    if case.expected_metrics else None
                )
                actual_sql = qr.execution.sql if qr.execution else None
                sql_fragment_match = (
                    case.expected_sql_fragment.lower() in (actual_sql or "").lower()
                    if case.expected_sql_fragment else None
                )
                passed = all(
                    v is not False
                    for v in [skill_match, metrics_match, sql_fragment_match]
                    if v is not None
                )
                results.append(EvalResult(
                    golden_id=case.golden_id,
                    query=case.query,
                    passed=passed,
                    skill_match=skill_match,
                    metrics_match=metrics_match,
                    sql_fragment_match=sql_fragment_match,
                    actual_skill=qr.plan.get("skill"),
                    actual_metrics=actual_metrics,
                    actual_sql=actual_sql,
                ))
            except Exception as exc:
                results.append(EvalResult(
                    golden_id=case.golden_id,
                    query=case.query,
                    passed=False,
                    skill_match=None,
                    metrics_match=None,
                    sql_fragment_match=None,
                    actual_skill=None,
                    actual_metrics=[],
                    actual_sql=None,
                    error=str(exc),
                ))
        return results

    def score(self, results: list[EvalResult]) -> dict:
        total = len(results)
        if not total:
            return {"total": 0, "passed": 0, "pass_rate": 0.0}
        passed = sum(1 for r in results if r.passed)
        return {
            "total": total,
            "passed": passed,
            "pass_rate": round(passed / total, 3),
            "skill_accuracy": self._rate(results, "skill_match"),
            "metrics_accuracy": self._rate(results, "metrics_match"),
            "sql_accuracy": self._rate(results, "sql_fragment_match"),
        }

    @staticmethod
    def _rate(results: list[EvalResult], attr: str) -> Optional[float]:
        scored = [r for r in results if getattr(r, attr) is not None]
        if not scored:
            return None
        return round(sum(1 for r in scored if getattr(r, attr)) / len(scored), 3)
