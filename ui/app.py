"""Order Status Assistant — Streamlit UI.

Connects to the FastAPI backend.  Set API_BASE_URL in .env or the
environment to point at a non-local API server.
"""

import os
import json
import httpx
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

from components.search_form import render_search_form
from components.results_table import render_results
from components.metric_panel import render_metric_results
from components.sql_panel import render_explain_panel
from components.trace_panel import render_trace_panel

API_BASE = os.getenv("API_BASE_URL", "http://localhost:8000")

st.set_page_config(
    page_title="Order Status Assistant",
    page_icon="📦",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── Session state init ─────────────────────────────────────────────────────
if "last_response" not in st.session_state:
    st.session_state.last_response = None
if "last_explain" not in st.session_state:
    st.session_state.last_explain = None
if "trace_history" not in st.session_state:
    st.session_state.trace_history = []

# ── Header ─────────────────────────────────────────────────────────────────
st.markdown(
    """
    <div style="display:flex; align-items:flex-start; gap:18px;">
        <div style="flex:1; min-width:0;">
            <h1 style="margin:0; padding:0; line-height:1.2;">
                &#x1F4E6; Order Status Assistant
            </h1>
            <p style="margin:4px 0 0 0; color:#888; font-style:italic; font-size:0.95rem;">
                Medical Device Fulfillment · Powered by Snowflake + Cortex + dbt Semantic Layer
            </p>
        </div>
        <img src="https://upload.wikimedia.org/wikipedia/commons/0/02/Boston_Scientific_Logo.svg"
             alt="Boston Scientific" style="height:48px; margin-top:4px;" />
    </div>
    """,
    unsafe_allow_html=True,
)

# Health check badges
try:
    hc = httpx.get(f"{API_BASE}/health", timeout=10.0)
    hc_data = hc.json()

    col_sf, col_dbt, col_backend = st.columns(3)
    with col_sf:
        if hc_data.get("snowflake", False):
            st.success("\u2705 Connected to Snowflake")
        else:
            st.warning("Snowflake degraded")
    with col_dbt:
        if hc_data.get("dbt_cloud", False):
            st.success("\u2705 Connected to dbt Cloud (MCP)")
        elif hc_data.get("dbt_cloud_configured", False):
            st.warning("dbt Cloud configured but unreachable")
        else:
            st.info("dbt Cloud not configured")
    with col_backend:
        backend = hc_data.get("semantic_backend", "direct_sql")
        if backend == "dbt_mcp":
            st.success("\u2705 Semantic Layer: Active")
        else:
            st.info("Semantic Layer: Direct SQL")
except Exception:
    st.error("API unreachable — is the FastAPI server running?")

st.markdown("---")

# ── Main layout ─────────────────────────────────────────────────────────────
left_col, right_col = st.columns([1, 2])

with left_col:
    payload, run = render_search_form()

with right_col:
    if run:
        with st.spinner("Searching…"):
            try:
                resp = httpx.post(
                    f"{API_BASE}/search/orders",
                    json=payload,
                    timeout=30.0,
                )
                resp.raise_for_status()
                data = resp.json()
                st.session_state.last_response = data

                # Fetch explain data
                trace_id = data.get("trace_id", "")
                try:
                    ex_resp = httpx.get(
                        f"{API_BASE}/explain/{trace_id}", timeout=5.0
                    )
                    st.session_state.last_explain = ex_resp.json() if ex_resp.is_success else {}
                except Exception:
                    st.session_state.last_explain = {}

                # Append to history
                timings = data.get("timings_ms", {})
                st.session_state.trace_history.append(
                    {
                        "trace_id": trace_id[:8] + "…",
                        "type": data.get("response_type", "order_lookup"),
                        "total_ms": timings.get("total_ms", 0),
                        "cortex_parse_ms": timings.get("cortex_parse_ms", 0),
                        "mcp_query_ms": timings.get("mcp_query_ms", 0),
                        "sql_candidate_ms": timings.get("sql_candidate_ms", 0),
                        "cortex_rerank_ms": timings.get("cortex_rerank_ms", 0),
                        "sql_fetch_top_ms": timings.get("sql_fetch_top_ms", 0),
                    }
                )

            except httpx.HTTPStatusError as exc:
                st.error(f"API error {exc.response.status_code}: {exc.response.text}")
            except Exception as exc:
                st.error(f"Request failed: {exc}")

    if st.session_state.last_response:
        data = st.session_state.last_response
        response_type = data.get("response_type", "order_lookup")

        if response_type == "metric_query":
            metric_tab, explain_tab, perf_tab = st.tabs(
                ["📊 Metric Results", "🧠 Explain", "⏱️ Performance"]
            )

            with metric_tab:
                render_metric_results(
                    data.get("metric_result"), data.get("trace_id", "")
                )

            with explain_tab:
                explain_data = st.session_state.last_explain or {}
                if not explain_data.get("candidate_sql") and data.get("candidate_sql"):
                    explain_data["candidate_sql"] = data["candidate_sql"]
                explain_data["candidate_count"] = data.get("candidate_count", "—")
                explain_data["_response_type"] = "metric_query"
                explain_data["_metric_result"] = data.get("metric_result")
                render_explain_panel(explain_data)

            with perf_tab:
                render_trace_panel(
                    timings=data.get("timings_ms", {}),
                    trace_id=data.get("trace_id", ""),
                    history=st.session_state.trace_history,
                )
        else:
            results_tab, explain_tab, perf_tab = st.tabs(
                ["🔍 Results", "🧠 Explain", "⏱️ Performance"]
            )

            with results_tab:
                render_results(data.get("results", []), data.get("trace_id", ""))

            with explain_tab:
                explain_data = st.session_state.last_explain or {}
                if not explain_data.get("candidate_sql") and data.get("candidate_sql"):
                    explain_data["candidate_sql"] = data["candidate_sql"]
                if not explain_data.get("fetch_sql") and data.get("fetch_sql"):
                    explain_data["fetch_sql"] = data["fetch_sql"]
                explain_data["candidate_count"] = data.get("candidate_count", "—")
                explain_data["_response_type"] = "order_lookup"
                render_explain_panel(explain_data)

            with perf_tab:
                render_trace_panel(
                    timings=data.get("timings_ms", {}),
                    trace_id=data.get("trace_id", ""),
                    history=st.session_state.trace_history,
                )
    else:
        st.info("Enter search criteria and click **Search Orders** to begin.")
