from __future__ import annotations
from typing import TYPE_CHECKING

try:
    import streamlit as st
    _HAS_STREAMLIT = True
except ImportError:
    _HAS_STREAMLIT = False

if TYPE_CHECKING:
    from snowflake.snowpark import Session


def get_session() -> "Session":
    """Return a cached Snowpark session.

    In SPCS the container already has an ambient Snowflake context so
    snowflake.snowpark.context.get_active_session() works without credentials.
    Falls back to explicit connection params for local dev.
    """
    if _HAS_STREAMLIT:
        return _get_session_cached()
    return _create_session()


if _HAS_STREAMLIT:
    @st.cache_resource
    def _get_session_cached() -> "Session":
        return _create_session()


def _create_session() -> "Session":
    try:
        from snowflake.snowpark.context import get_active_session
        return get_active_session()
    except Exception:  # snowpark raises varied internal errors outside SPCS ambient context
        pass

    from snowflake.snowpark import Session as _Session
    from agent import config
    return _Session.builder.configs({
        "account": config.SNOWFLAKE_ACCOUNT,
        "user": config.SNOWFLAKE_USER,
        "role": config.SNOWFLAKE_ROLE,
        "warehouse": config.SNOWFLAKE_WAREHOUSE,
        "database": config.SNOWFLAKE_DATABASE,
        "schema": config.SNOWFLAKE_SCHEMA,
    }).create()
