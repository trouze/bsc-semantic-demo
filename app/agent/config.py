"""App-level configuration for the SPCS-hosted Streamlit agent.

Reads from environment variables with sensible defaults for the demo.
"""
from __future__ import annotations

import os

# Primary demo schema (always allowed)
_DEMO_BSC = "DEMO_BSC"

# Analytics DB / schema from dbt (allowed when set)
ANALYTICS_DB: str = os.environ.get("ANALYTICS_DB", "").upper()

# Allowed schemas for SQL guard.
# Always includes DEMO_BSC; also includes ANALYTICS_DB when configured.
# Evaluated once at import time — set ANALYTICS_DB before importing this module.
ALLOWED_SCHEMAS: list[str] = list(
    filter(None, [_DEMO_BSC, ANALYTICS_DB])
)
