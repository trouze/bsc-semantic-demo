"""SQL safety guard — blocks DML/DDL and enforces schema allowlist.

Ported from api/services/snowflake_service._assert_schema_safe (lines 126-155)
with an extended allowlist for the dbt analytics database.
"""
from __future__ import annotations

import hashlib
import re

# DML/DDL tokens that are never allowed regardless of schema.
# Trailing space matches whole-word tokens to avoid false positives inside
# identifiers (e.g. a column named "created_at" won't match "CREATE ").
_FORBIDDEN_TOKENS = (
    "DROP ", "DELETE ", "INSERT ", "UPDATE ", "TRUNCATE ",
    "ALTER ", "CREATE ", "MERGE ", "REPLACE ", "GRANT ", "REVOKE ",
)

# Schema/DB identifiers that are always exempt from the allowlist check.
_EXEMPT_DBS = frozenset({"SNOWFLAKE"})
_EXEMPT_SCHEMAS = frozenset({"INFORMATION_SCHEMA", "CORTEX"})

# Regex: matches dot-separated identifier chains like DB.SCHEMA.TABLE or SCHEMA.TABLE.
_CHAIN_RE = re.compile(r"[A-Z_][A-Z0-9_]*(?:\.[A-Z_][A-Z0-9_]*)+")


def assert_safe(sql: str, allowed_schemas: list[str] | None = None) -> None:
    """Raise ValueError if sql contains DML/DDL or references non-allowed schemas.

    Port of _assert_schema_safe from api/services/snowflake_service.py:126-155.
    Extended to accept ANALYTICS_DB from config in addition to DEMO_BSC.

    Args:
        sql: The SQL string to validate.
        allowed_schemas: Override the allowlist from config. Useful in tests.

    Raises:
        ValueError: If the SQL contains a forbidden DML/DDL token or references
                    a schema outside the allowlist.
    """
    if allowed_schemas is None:
        from agent import config as _config
        allowed_schemas = _config.ALLOWED_SCHEMAS

    allowed = {s.upper() for s in allowed_schemas}
    sql_upper = sql.upper()

    # 1. Block DML/DDL tokens.
    for token in _FORBIDDEN_TOKENS:
        if token in sql_upper:
            raise ValueError(f"DML/DDL not allowed: {token.strip()}")

    # 2. Validate all dot-separated identifier chains against the schema allowlist.
    for chain in _CHAIN_RE.findall(sql_upper):
        parts = chain.split(".")
        if len(parts) >= 3:
            db, schema = parts[0], parts[1]
            if db in _EXEMPT_DBS or schema in _EXEMPT_SCHEMAS:
                continue
            if schema not in allowed:
                raise ValueError(
                    f"Schema '{schema}' is not in the allowed list {sorted(allowed)}. "
                    "Only pre-approved schemas may be queried."
                )
        elif len(parts) == 2:
            schema = parts[0]
            if schema in _EXEMPT_DBS or schema in _EXEMPT_SCHEMAS:
                continue
            if schema not in allowed:
                raise ValueError(
                    f"Schema '{schema}' is not in the allowed list {sorted(allowed)}. "
                    "Only pre-approved schemas may be queried."
                )


def sql_hash(sql: str) -> str:
    """Return a 16-char SHA-256 hex digest of the SQL string."""
    return hashlib.sha256(sql.encode()).hexdigest()[:16]
