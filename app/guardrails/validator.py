"""Guardrails — validate plans before execution.

Enforces the governance boundary: only catalog-defined metrics and dimensions
are permitted. Blocks PII dimensions, enforces result-size limits, and
requires minimum confidence before executing.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from app.semantic.catalog import SemanticCatalog


@dataclass
class ValidationResult:
    ok: bool
    reason: str = ""


class GuardrailsValidator:
    def __init__(
        self,
        blocked_dimensions: list[str] | None = None,
        max_metrics_per_query: int = 5,
        confidence_threshold: float = 0.45,
    ):
        self._blocked = set(blocked_dimensions or [])
        self._max_metrics = max_metrics_per_query
        self._min_confidence = confidence_threshold

    def validate_plan(self, plan: dict, catalog: "SemanticCatalog") -> ValidationResult:
        metrics = plan.get("metrics", [])
        group_by = plan.get("group_by", [])
        confidence = plan.get("confidence", 1.0)

        # Confidence floor
        if confidence < self._min_confidence:
            return ValidationResult(
                ok=False,
                reason=(
                    f"I'm not confident enough in my interpretation of your question "
                    f"(confidence: {confidence:.0%}). Could you rephrase it?"
                ),
            )

        # At least one metric required
        if not metrics:
            return ValidationResult(ok=False, reason="No metrics were identified in your question.")

        # Too many metrics
        if len(metrics) > self._max_metrics:
            return ValidationResult(
                ok=False,
                reason=f"Please limit queries to {self._max_metrics} metrics at a time.",
            )

        # All metrics must exist in the catalog
        catalog_names = set(catalog.metric_names())
        unknown_metrics = [m for m in metrics if m not in catalog_names]
        if unknown_metrics:
            return ValidationResult(
                ok=False,
                reason=f"Unknown metrics: {', '.join(unknown_metrics)}. Only catalog metrics are allowed.",
            )

        # All dimensions must be valid for at least one of the requested metrics
        valid_dims = catalog.dimensions_for(metrics)
        # Also allow metric_time grains (they're always valid)
        for dim in group_by:
            if dim.startswith("metric_time__"):
                continue
            if dim not in valid_dims:
                return ValidationResult(
                    ok=False,
                    reason=f"Dimension '{dim}' is not available for the requested metrics.",
                )

        # Blocked (PII) dimensions
        blocked_found = [d for d in group_by if d in self._blocked]
        if blocked_found:
            return ValidationResult(
                ok=False,
                reason=f"Dimension(s) {', '.join(blocked_found)} are restricted by data governance policy.",
            )

        return ValidationResult(ok=True)
