"""Shared helpers for SPCS Streamlit pages."""
from __future__ import annotations
import sys
from pathlib import Path

import streamlit as st

# Ensure the app package root is on sys.path so `agent.*` is importable.
_APP_ROOT = Path(__file__).parent
if str(_APP_ROOT) not in sys.path:
    sys.path.insert(0, str(_APP_ROOT))


def get_session():
    """Return the Snowpark session, stopping the page on failure."""
    try:
        from agent.session import get_session as _get  # noqa: PLC0415
        return _get()
    except Exception as e:
        st.error(f"Session init failed: {e}")
        st.stop()
