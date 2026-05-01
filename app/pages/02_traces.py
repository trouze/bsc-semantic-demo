"""Traces page — browse AGENT_TRACE and replay from trace_id deep links."""
from __future__ import annotations
import logging
import sys
from pathlib import Path

import streamlit as st

sys.path.insert(0, str(Path(__file__).parents[1]))
from utils import get_session  # noqa: E402

logger = logging.getLogger(__name__)

_PAGE_SIZE = 20

_LIST_TRACES_SQL = """
SELECT trace_id, created_at, user_input, routed_skill, status, total_ms, user_email
FROM DEMO_BSC.AGENT_TRACE
ORDER BY created_at DESC
LIMIT ? OFFSET ?
"""

_COUNT_TRACES_SQL = "SELECT COUNT(*) AS n FROM DEMO_BSC.AGENT_TRACE"

_GET_TRACE_SQL = """
SELECT *
FROM DEMO_BSC.AGENT_TRACE
WHERE trace_id = ?
LIMIT 1
"""


def _render_trace_detail(session, trace_id: str):
    """Render full detail for a single trace."""
    rows = session.sql(_GET_TRACE_SQL, params=[trace_id]).collect()
    if not rows:
        st.warning(f"No trace found for ID: `{trace_id}`")
        return

    row = rows[0].as_dict()
    st.subheader(f"Trace: `{trace_id[:12]}…`")

    col1, col2, col3 = st.columns(3)
    col1.metric("Skill", row.get("ROUTED_SKILL", "—"))
    col2.metric("Status", row.get("STATUS", "—"))
    col3.metric("Latency (ms)", row.get("TOTAL_MS", "—"))

    st.write(f"**User input:** {row.get('USER_INPUT', '')}")
    st.write(f"**Timestamp:** {row.get('CREATED_AT', '')}")

    if row.get("COMPILED_SQL"):
        with st.expander("Compiled SQL"):
            st.code(row["COMPILED_SQL"], language="sql")

    if row.get("SLOTS"):
        with st.expander("Slots"):
            st.json(row["SLOTS"])

    if row.get("ROUTER_RAW"):
        with st.expander("Router response"):
            st.json(row["ROUTER_RAW"])

    if row.get("ERROR"):
        st.error(f"Error: {row['ERROR']}")

    if row.get("RESULT_SUMMARY"):
        with st.expander("Result summary"):
            st.json(row["RESULT_SUMMARY"])

    if row.get("TIMINGS_MS"):
        with st.expander("Timings"):
            st.json(row["TIMINGS_MS"])

    st.divider()
    if st.button("← Back to trace list"):
        st.query_params.clear()
        st.rerun()


def _render_trace_list(session):
    """Render paginated list of traces."""
    total_rows = session.sql(_COUNT_TRACES_SQL).collect()
    total = total_rows[0]["N"] if total_rows else 0

    page = st.number_input("Page", min_value=1, value=1, step=1)
    offset = (page - 1) * _PAGE_SIZE

    st.caption(f"Showing page {page} of {max(1, (total + _PAGE_SIZE - 1) // _PAGE_SIZE)} ({total} total traces)")

    rows = session.sql(_LIST_TRACES_SQL, params=[_PAGE_SIZE, offset]).collect()

    if not rows:
        st.info("No traces recorded yet.")
        return

    for row in rows:
        r = row.as_dict()
        trace_id = r.get("TRACE_ID", "")
        col1, col2, col3, col4 = st.columns([3, 2, 1, 1])
        col1.write(f"**{r.get('USER_INPUT', '')[:80]}**")
        col2.write(f"`{r.get('ROUTED_SKILL', '?')}`")
        col3.write(r.get("STATUS", "?"))
        if col4.button("View", key=f"view_{trace_id}"):
            st.query_params["trace_id"] = trace_id
            st.rerun()


def main():
    st.title("🔍 Trace Browser")
    session = get_session()

    trace_id = st.query_params.get("trace_id", "")

    if trace_id:
        _render_trace_detail(session, trace_id)
    else:
        _render_trace_list(session)


if __name__ == "__main__":
    main()
