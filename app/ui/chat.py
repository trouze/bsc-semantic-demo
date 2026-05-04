"""Chat message rendering and feedback button components."""
from __future__ import annotations

from typing import TYPE_CHECKING, Callable

import streamlit as st

from app.ui.results import render_result

if TYPE_CHECKING:
    from app.agents.orchestrator import QueryResult


def render_message(turn: dict, on_rate: Callable[[str, str], None] | None = None) -> None:
    """Render a single conversation turn (user or assistant)."""
    role = turn.get("role", "user")

    if role == "user":
        with st.chat_message("user"):
            st.markdown(turn["content"])
        return

    with st.chat_message("assistant"):
        qr: QueryResult | None = turn.get("result")

        if qr is None:
            st.markdown(turn.get("content", ""))
            return

        # Interpretation headline
        st.markdown(f"**{qr.interpretation}**")

        # Render chart/table/cards/error
        render_result(qr)

        # Feedback buttons (only shown once — after rating, show the selection)
        interaction_id = qr.interaction_id
        rating_key = f"rating_{interaction_id}"
        current_rating = st.session_state.get(rating_key) or turn.get("rating")

        if on_rate and not current_rating:
            render_feedback_buttons(interaction_id, on_rate)
        elif current_rating:
            icon = "👍" if current_rating == "up" else "👎"
            st.caption(f"You rated this {icon}")


def render_feedback_buttons(
    interaction_id: str,
    on_rate: Callable[[str, str], None],
) -> None:
    """Inline thumbs up/down rating UI."""
    col1, col2, col3 = st.columns([1, 1, 8])
    with col1:
        if st.button("👍", key=f"up_{interaction_id}", help="Helpful"):
            on_rate(interaction_id, "up")
    with col2:
        if st.button("👎", key=f"down_{interaction_id}", help="Not helpful"):
            on_rate(interaction_id, "down")
