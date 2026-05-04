"""BSci Semantic Demo — main Streamlit entry point.

Architecture:
  User query → CortexAgent (plan) → GuardrailsValidator → Skill → SLExecutor
    (compile_sql via dbt SL → execute on Snowflake connector) → UI render

All Snowflake and dbt SL credentials come from .streamlit/secrets.toml (local)
or environment variables (SPCS deployment).
"""
from __future__ import annotations

import sys
import uuid
from pathlib import Path

import streamlit as st

# Make the app/ directory importable when running `streamlit run app/streamlit_app.py`
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.agents.cortex import CortexAgent
from app.agents.orchestrator import ConversationTurn, Orchestrator
from app.config import get_settings
from app.feedback.collector import FeedbackCollector
from app.guardrails.validator import GuardrailsValidator
from app.semantic.catalog import SemanticCatalog, load_catalog
from app.semantic.executor import SLExecutor
from app.skills.builtin import BUILTIN_SKILLS
from app.skills.registry import SkillRegistry
from app.ui.chat import render_message
from app.ui.sidebar import render_sidebar

# ─── Page config ──────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="BSci Analytics",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ─── Cached resources (one per process) ───────────────────────────────────────

@st.cache_resource(show_spinner="Loading semantic catalog…")
def _get_catalog() -> SemanticCatalog:
    cfg = get_settings()
    return load_catalog(cfg.dbt_sl_environment_id, cfg.dbt_sl_auth_token, cfg.dbt_sl_host)


def _get_sf_conn():
    """Snowflake connection — one per user session, stored in session state."""
    if "sf_conn" not in st.session_state or st.session_state.sf_conn is None:
        cfg = get_settings()
        try:
            # Inside SiS / SPCS with mounted credentials
            from snowflake.snowpark.context import get_active_session
            st.session_state.sf_conn = get_active_session().connection
        except Exception:
            import snowflake.connector
            st.session_state.sf_conn = snowflake.connector.connect(
                account=cfg.sf_account,
                user=cfg.sf_user,
                password=cfg.sf_password,
                warehouse=cfg.sf_warehouse,
                database=cfg.sf_database,
                schema=cfg.sf_schema,
                role=cfg.sf_role,
                client_session_keep_alive=True,
            )
    return st.session_state.sf_conn


def _build_orchestrator(sf_conn) -> Orchestrator:
    """Assemble the orchestrator from its components."""
    cfg = get_settings()
    catalog = _get_catalog()
    return Orchestrator(
        catalog=catalog,
        cortex=CortexAgent(sf_conn, model=cfg.cortex_model),
        executor=SLExecutor(
            environment_id=cfg.dbt_sl_environment_id,
            auth_token=cfg.dbt_sl_auth_token,
            host=cfg.dbt_sl_host,
            sf_conn=sf_conn,
        ),
        skills=SkillRegistry(BUILTIN_SKILLS),
        guardrails=GuardrailsValidator(
            blocked_dimensions=cfg.blocked_dimensions,
            max_metrics_per_query=cfg.max_metrics_per_query,
            confidence_threshold=cfg.confidence_threshold,
        ),
        feedback=FeedbackCollector(
            sf_conn=sf_conn,
            db=cfg.sf_feedback_db,
            schema=cfg.sf_feedback_schema,
        ),
    )


# ─── Session state init ────────────────────────────────────────────────────────

def _init_session() -> None:
    if "session_id" not in st.session_state:
        st.session_state.session_id = str(uuid.uuid4())
    if "messages" not in st.session_state:
        st.session_state.messages = []   # list of ConversationTurn-like dicts
    if "sf_conn" not in st.session_state:
        st.session_state.sf_conn = None


# ─── Feedback handler ──────────────────────────────────────────────────────────

def _handle_rating(interaction_id: str, rating: str) -> None:
    """Record a thumbs up/down rating and store it in session state."""
    try:
        cfg = get_settings()
        conn = _get_sf_conn()
        collector = FeedbackCollector(conn, cfg.sf_feedback_db, cfg.sf_feedback_schema)
        collector.log_rating(interaction_id, rating)
    except Exception:
        pass
    # Mark in session state so the button disappears
    st.session_state[f"rating_{interaction_id}"] = rating
    # Also update the stored message so re-renders show the rating
    for msg in st.session_state.messages:
        if msg.get("interaction_id") == interaction_id:
            msg["rating"] = rating
            break
    st.rerun()


# ─── Main app ─────────────────────────────────────────────────────────────────

def main() -> None:
    _init_session()

    catalog = _get_catalog()
    render_sidebar(catalog, st.session_state.session_id)

    st.title("📊 BSci Analytics")
    st.caption("Ask questions about your data in plain English. Powered by dbt Semantic Layer + Snowflake Cortex.")

    # Render conversation history
    for msg in st.session_state.messages:
        render_message(msg, on_rate=_handle_rating)

    # Chat input
    user_input = st.chat_input("Ask a question about your data…")
    if not user_input:
        return

    # Append user turn and render it immediately
    user_turn = {"role": "user", "content": user_input}
    st.session_state.messages.append(user_turn)
    with st.chat_message("user"):
        st.markdown(user_input)

    # Build history for the orchestrator (exclude current turn)
    history = [
        ConversationTurn(role=m["role"], content=m["content"])
        for m in st.session_state.messages[:-1]
        if m.get("role") in ("user", "assistant")
    ]

    # Run the orchestration pipeline
    with st.chat_message("assistant"):
        with st.spinner("Thinking…"):
            try:
                conn = _get_sf_conn()
                orchestrator = _build_orchestrator(conn)
                qr = orchestrator.process(
                    user_query=user_input,
                    history=history,
                    session_id=st.session_state.session_id,
                )
            except Exception as exc:
                st.error(f"Unexpected error: {exc}")
                return

        st.markdown(f"**{qr.interpretation}**")

        from app.ui.results import render_result
        render_result(qr)

        from app.ui.chat import render_feedback_buttons
        render_feedback_buttons(qr.interaction_id, _handle_rating)

    # Store assistant turn in session state
    assistant_turn = {
        "role": "assistant",
        "content": qr.interpretation,
        "result": qr,
        "interaction_id": qr.interaction_id,
        "rating": None,
    }
    st.session_state.messages.append(assistant_turn)


if __name__ == "__main__":
    main()
