"""Unit tests for slot_validators."""
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from agent.guardrails.slot_validators import (
    validate_metric_names,
    validate_date_range,
    validate_status_value,
)


def test_validate_metric_names_valid():
    errors = validate_metric_names(["orders"], ["orders", "revenue"])
    assert errors == []


def test_validate_metric_names_unknown():
    errors = validate_metric_names(["unknown_metric"], ["orders"])
    assert len(errors) == 1
    assert "unknown_metric" in errors[0]


def test_validate_date_range_end_before_start():
    errors = validate_date_range("2024-01-01", "2023-12-31")
    assert len(errors) == 1
    assert "date_end" in errors[0]


def test_validate_date_range_valid():
    errors = validate_date_range("2024-01-01", "2024-12-31")
    assert errors == []


def test_validate_status_value_valid():
    errors = validate_status_value("shipped", ["shipped", "delivered"])
    assert errors == []


def test_validate_status_value_unknown():
    errors = validate_status_value("unknown", ["shipped"])
    assert len(errors) == 1
    assert "unknown" in errors[0]


def test_validate_date_range_both_none():
    errors = validate_date_range(None, None)
    assert errors == []
