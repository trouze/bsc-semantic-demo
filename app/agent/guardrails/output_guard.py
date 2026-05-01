"""Output guardrails — row caps, confidence escalation, PII redaction placeholder."""
from __future__ import annotations
import logging
from dataclasses import dataclass
from typing import Any, Optional

logger = logging.getLogger(__name__)

_DEFAULT_ROW_CAP = 500
_CONFIDENCE_THRESHOLD = 0.7


@dataclass
class OutputCheckResult:
    truncated: bool = False
    original_row_count: int = 0
    returned_row_count: int = 0
    low_confidence: bool = False
    confidence: float = 1.0
    pii_redacted: bool = False


def apply_row_cap(
    rows: list[dict[str, Any]],
    cap: int = _DEFAULT_ROW_CAP,
) -> tuple[list[dict[str, Any]], bool]:
    """Truncate rows to cap. Returns (truncated_rows, was_truncated)."""
    if len(rows) > cap:
        logger.warning("Row cap applied: %d → %d rows", len(rows), cap)
        return rows[:cap], True
    return rows, False


def check_confidence(confidence: float, threshold: float = _CONFIDENCE_THRESHOLD) -> bool:
    """Return True if confidence is below threshold (should escalate to ClarifySkill)."""
    return confidence < threshold


def redact_pii(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """PII redaction — no-op placeholder for v1. Returns rows unchanged."""
    # v2: implement field-level redaction based on AGENT_CONFIG sensitivity flags
    return rows


def apply_output_guards(
    rows: list[dict[str, Any]],
    confidence: float = 1.0,
    row_cap: int = _DEFAULT_ROW_CAP,
) -> tuple[list[dict[str, Any]], OutputCheckResult]:
    """Apply all output guards and return guarded rows + check result."""
    capped_rows, was_truncated = apply_row_cap(rows, cap=row_cap)
    redacted_rows = redact_pii(capped_rows)
    return redacted_rows, OutputCheckResult(
        truncated=was_truncated,
        original_row_count=len(rows),
        returned_row_count=len(redacted_rows),
        low_confidence=check_confidence(confidence),
        confidence=confidence,
        pii_redacted=False,
    )
