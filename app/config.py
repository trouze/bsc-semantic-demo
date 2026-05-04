"""App-wide settings loaded from Streamlit secrets or environment variables."""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from functools import lru_cache
from typing import Optional


def _env(key: str, default: str = "") -> str:
    return os.environ.get(key, default)


@dataclass
class Settings:
    # dbt Semantic Layer
    dbt_sl_environment_id: int = field(default_factory=lambda: int(_env("DBT_SL_ENVIRONMENT_ID", "335860")))
    dbt_sl_auth_token: str = field(default_factory=lambda: _env("DBT_SL_AUTH_TOKEN"))
    dbt_sl_host: str = field(default_factory=lambda: _env("DBT_SL_HOST", "hy250.semantic-layer.us1.dbt.com"))

    # Snowflake
    sf_account: str = field(default_factory=lambda: _env("SNOWFLAKE_ACCOUNT"))
    sf_user: str = field(default_factory=lambda: _env("SNOWFLAKE_USER"))
    sf_password: str = field(default_factory=lambda: _env("SNOWFLAKE_PASSWORD"))
    sf_warehouse: str = field(default_factory=lambda: _env("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"))
    sf_database: str = field(default_factory=lambda: _env("SNOWFLAKE_DATABASE", "ANALYTICS"))
    sf_schema: str = field(default_factory=lambda: _env("SNOWFLAKE_SCHEMA", "PUBLIC"))
    sf_role: Optional[str] = field(default_factory=lambda: _env("SNOWFLAKE_ROLE") or None)
    sf_feedback_db: str = field(default_factory=lambda: _env("SNOWFLAKE_FEEDBACK_DB", "ANALYTICS"))
    sf_feedback_schema: str = field(default_factory=lambda: _env("SNOWFLAKE_FEEDBACK_SCHEMA", "SEMANTIC_DEMO_FEEDBACK"))

    # Agent
    cortex_model: str = field(default_factory=lambda: _env("CORTEX_MODEL", "claude-3-5-sonnet"))
    max_result_rows: int = field(default_factory=lambda: int(_env("MAX_RESULT_ROWS", "10000")))
    max_context_turns: int = field(default_factory=lambda: int(_env("MAX_CONTEXT_TURNS", "10")))

    # Guardrails
    blocked_dimensions: list[str] = field(default_factory=list)
    max_metrics_per_query: int = 5
    confidence_threshold: float = 0.45


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return settings, preferring Streamlit secrets over env vars."""
    try:
        import streamlit as st
        s = st.secrets
        return Settings(
            dbt_sl_environment_id=int(s.get("DBT_SL_ENVIRONMENT_ID", _env("DBT_SL_ENVIRONMENT_ID", "335860"))),
            dbt_sl_auth_token=s.get("DBT_SL_AUTH_TOKEN", _env("DBT_SL_AUTH_TOKEN")),
            dbt_sl_host=s.get("DBT_SL_HOST", _env("DBT_SL_HOST", "hy250.semantic-layer.us1.dbt.com")),
            sf_account=s.get("SNOWFLAKE_ACCOUNT", _env("SNOWFLAKE_ACCOUNT")),
            sf_user=s.get("SNOWFLAKE_USER", _env("SNOWFLAKE_USER")),
            sf_password=s.get("SNOWFLAKE_PASSWORD", _env("SNOWFLAKE_PASSWORD")),
            sf_warehouse=s.get("SNOWFLAKE_WAREHOUSE", _env("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")),
            sf_database=s.get("SNOWFLAKE_DATABASE", _env("SNOWFLAKE_DATABASE", "ANALYTICS")),
            sf_schema=s.get("SNOWFLAKE_SCHEMA", _env("SNOWFLAKE_SCHEMA", "PUBLIC")),
            sf_role=s.get("SNOWFLAKE_ROLE", _env("SNOWFLAKE_ROLE")) or None,
            sf_feedback_db=s.get("SNOWFLAKE_FEEDBACK_DB", _env("SNOWFLAKE_FEEDBACK_DB", "ANALYTICS")),
            sf_feedback_schema=s.get("SNOWFLAKE_FEEDBACK_SCHEMA", _env("SNOWFLAKE_FEEDBACK_SCHEMA", "SEMANTIC_DEMO_FEEDBACK")),
            cortex_model=s.get("CORTEX_MODEL", _env("CORTEX_MODEL", "claude-3-5-sonnet")),
        )
    except Exception:
        return Settings()
