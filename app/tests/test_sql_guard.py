"""Unit tests for agent.guardrails.sql_guard."""
from __future__ import annotations

import pytest

from agent.guardrails.sql_guard import assert_safe


# ---------------------------------------------------------------------------
# 1. Clean SELECT from DEMO_BSC passes
# ---------------------------------------------------------------------------

def test_clean_select_demo_bsc():
    """A plain SELECT against an allowed DEMO_BSC schema should not raise."""
    assert_safe(
        "SELECT id, status FROM DEMO_BSC.PUBLIC.ORDERS WHERE id = 1",
        allowed_schemas=["DEMO_BSC", "PUBLIC"],
    )


def test_two_part_allowed_ref():
    """A two-part DEMO_BSC.table reference should pass."""
    assert_safe(
        "SELECT * FROM DEMO_BSC.orders",
        allowed_schemas=["DEMO_BSC"],
    )


# ---------------------------------------------------------------------------
# 2. DROP TABLE raises ValueError
# ---------------------------------------------------------------------------

def test_drop_table_raises():
    """DROP is a forbidden DML/DDL token and must raise ValueError."""
    with pytest.raises(ValueError, match="DML/DDL not allowed: DROP"):
        assert_safe("DROP TABLE DEMO_BSC.orders", allowed_schemas=["DEMO_BSC"])


def test_delete_raises():
    with pytest.raises(ValueError, match="DML/DDL not allowed: DELETE"):
        assert_safe("DELETE FROM DEMO_BSC.orders WHERE 1=1", allowed_schemas=["DEMO_BSC"])


def test_insert_raises():
    with pytest.raises(ValueError, match="DML/DDL not allowed: INSERT"):
        assert_safe(
            "INSERT INTO DEMO_BSC.orders (id) VALUES (1)",
            allowed_schemas=["DEMO_BSC"],
        )


def test_truncate_raises():
    with pytest.raises(ValueError, match="DML/DDL not allowed: TRUNCATE"):
        assert_safe("TRUNCATE TABLE DEMO_BSC.orders", allowed_schemas=["DEMO_BSC"])


# ---------------------------------------------------------------------------
# 3. SELECT from an unknown schema raises ValueError
# ---------------------------------------------------------------------------

def test_unknown_schema_raises():
    """A reference to an unlisted schema must raise ValueError."""
    with pytest.raises(ValueError, match="SECRET_DB"):
        assert_safe(
            "SELECT * FROM SECRET_DB.PRIVATE.employees",
            allowed_schemas=["DEMO_BSC"],
        )


def test_unknown_two_part_raises():
    with pytest.raises(ValueError, match="BAD_SCHEMA"):
        assert_safe(
            "SELECT * FROM BAD_SCHEMA.table",
            allowed_schemas=["DEMO_BSC"],
        )


# ---------------------------------------------------------------------------
# 4. SELECT from ANALYTICS_DB (when configured) passes
# ---------------------------------------------------------------------------

def test_analytics_db_allowed_via_param():
    """ANALYTICS_DB schema should be accepted when in the explicit allowlist."""
    assert_safe(
        "SELECT order_id FROM ANALYTICS_DB.PUBLIC.fct_orders",
        allowed_schemas=["DEMO_BSC", "ANALYTICS_DB", "PUBLIC"],
    )


def test_analytics_db_two_part_allowed():
    assert_safe(
        "SELECT * FROM ANALYTICS_DB.fct_orders",
        allowed_schemas=["DEMO_BSC", "ANALYTICS_DB"],
    )


# ---------------------------------------------------------------------------
# 5. MetricFlow-style SQL with analytics DB table refs passes if ANALYTICS_DB set
# ---------------------------------------------------------------------------

def test_metricflow_style_sql():
    """Compiled MetricFlow SQL referencing an analytics DB schema should pass."""
    metricflow_sql = """
    SELECT
        subq_1.order__status,
        SUM(subq_1.order_count) AS order_count
    FROM (
        SELECT
            orders_src.status AS order__status,
            1 AS order_count
        FROM ANALYTICS_PROD.PUBLIC.fct_orders AS orders_src
    ) subq_1
    GROUP BY subq_1.order__status
    ORDER BY order_count DESC
    LIMIT 100
    """
    assert_safe(
        metricflow_sql,
        allowed_schemas=["DEMO_BSC", "ANALYTICS_PROD", "PUBLIC"],
    )


def test_metricflow_sql_fails_without_analytics_db():
    """MetricFlow SQL must fail when the analytics schema is not in the allowlist."""
    metricflow_sql = """
    SELECT order_count
    FROM ANALYTICS_PROD.PUBLIC.fct_orders
    """
    with pytest.raises(ValueError):
        assert_safe(metricflow_sql, allowed_schemas=["DEMO_BSC"])


# ---------------------------------------------------------------------------
# Exempt schemas (SNOWFLAKE, INFORMATION_SCHEMA, CORTEX) always pass
# ---------------------------------------------------------------------------

def test_snowflake_exempt_db():
    """References to the SNOWFLAKE DB (e.g. Cortex functions) must always pass."""
    assert_safe(
        "SELECT SNOWFLAKE.CORTEX.COMPLETE('mistral-7b', 'hello')",
        allowed_schemas=["DEMO_BSC"],
    )


def test_information_schema_exempt():
    assert_safe(
        "SELECT * FROM MYDB.INFORMATION_SCHEMA.TABLES",
        allowed_schemas=["DEMO_BSC"],
    )
