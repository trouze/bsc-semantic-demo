"""Sidebar: semantic context panel and skill reference guide."""
from __future__ import annotations

from typing import TYPE_CHECKING

import streamlit as st

if TYPE_CHECKING:
    from app.semantic.catalog import SemanticCatalog


def render_sidebar(catalog: "SemanticCatalog", session_id: str) -> None:
    with st.sidebar:
        st.markdown("## BSci Analytics")
        st.caption(f"Session `{session_id[:8]}…`")
        st.divider()

        # Catalog summary
        st.markdown("### Semantic Layer")
        st.caption(catalog.summary())

        with st.expander("Available metrics", expanded=False):
            for m in catalog.metrics:
                label = m.label or m.name
                desc = m.description[:80] + "…" if len(m.description) > 80 else m.description
                st.markdown(f"**{label}** (`{m.name}`)")
                if desc:
                    st.caption(desc)

        st.divider()

        # Skill reference
        st.markdown("### What can you ask?")
        skill_examples = {
            "📈 Trend": "Show me monthly revenue for the last 6 months",
            "📊 Compare": "Compare order count by customer segment",
            "🍕 Breakdown": "Break down revenue by product category",
            "🏆 Rank": "Top 10 customers by revenue this quarter",
            "📋 Summary": "How are we doing this month?",
        }
        for skill, example in skill_examples.items():
            st.markdown(f"**{skill}**")
            st.caption(f'"{example}"')

        st.divider()

        # Clear conversation
        if st.button("Clear conversation", use_container_width=True):
            st.session_state.messages = []
            st.rerun()
