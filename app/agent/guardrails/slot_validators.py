"""Slot validators — checks slot values against catalog allowlists and business rules.

Used by each Skill.validate() method before execution.
"""
from __future__ import annotations
from datetime import date
from typing import Optional


def _validate_names(values: list[str], allowed: list[str], label: str) -> list[str]:
    """Return error messages for any value not in the allowlist."""
    preview = ", ".join(sorted(allowed)[:10])
    return [
        f"Unknown {label} '{v}'. Available: {preview}"
        for v in values
        if v not in allowed
    ]


def validate_metric_names(metrics: list[str], allowed: list[str]) -> list[str]:
    """Return list of error messages for any metric not in the catalog allowlist."""
    return _validate_names(metrics, allowed, "metric")


def validate_dimension_names(dimensions: list[str], allowed: list[str]) -> list[str]:
    """Return list of error messages for any dimension not in the catalog allowlist."""
    return _validate_names(dimensions, allowed, "dimension")


def validate_status_value(
    status: Optional[str],
    allowed_statuses: list[str],
) -> list[str]:
    """Validate a status slot value against the glossary allowlist."""
    if status is None:
        return []
    if status not in allowed_statuses:
        return [
            f"Unknown status '{status}'. "
            f"Valid values: {', '.join(sorted(allowed_statuses))}"
        ]
    return []


def validate_date_range(
    date_start: Optional[str],
    date_end: Optional[str],
) -> list[str]:
    """Validate that date_end >= date_start and both are valid ISO dates."""
    errors = []
    parsed_start: Optional[date] = None
    parsed_end: Optional[date] = None

    if date_start:
        try:
            parsed_start = date.fromisoformat(date_start)
        except ValueError:
            errors.append(f"Invalid date_start format '{date_start}'. Use YYYY-MM-DD.")

    if date_end:
        try:
            parsed_end = date.fromisoformat(date_end)
        except ValueError:
            errors.append(f"Invalid date_end format '{date_end}'. Use YYYY-MM-DD.")

    if parsed_start and parsed_end and parsed_end < parsed_start:
        errors.append(
            f"date_end ({date_end}) must be >= date_start ({date_start})."
        )

    return errors


def validate_top_n(top_n: Optional[int], max_allowed: int = 20) -> list[str]:
    """Validate top_n is a positive integer within the allowed maximum."""
    if top_n is None:
        return []
    if not isinstance(top_n, int) or top_n < 1:
        return [f"top_n must be a positive integer, got {top_n!r}"]
    if top_n > max_allowed:
        return [f"top_n {top_n} exceeds maximum of {max_allowed}"]
    return []
