"""Chat page — primary user-facing interface for order intelligence queries."""
from __future__ import annotations
import logging
import streamlit as st

logger = logging.getLogger(__name__)


def _get_agent_components():
    """Lazy-load agent components. Returns (session, registry, ctx_builder, router, trace_writer, feedback_writer) or calls st.stop()."""
    try:
        import sys
        from pathlib import Path
        sys.path.insert(0, str(Path(__file__).parents[1]))

        from agent.session import get_session
        from agent.router.registry import SkillRegistry
        from agent.context.builder import ContextBuilder
        from agent.router.router import SkillRouter
        from agent.feedback.trace_writer import TraceWriter
        from agent.feedback.feedback_writer import FeedbackWriter

        session = get_session()
        registry = SkillRegistry.build_default(session)
        ctx_builder = ContextBuilder(session)
        router = SkillRouter(session, registry)
        trace_writer = TraceWriter(session)
        feedback_writer = FeedbackWriter(session)
        return session, registry, ctx_builder, router, trace_writer, feedback_writer
    except Exception as e:
        st.error(f"Failed to initialize agent: {e}")
        st.stop()


def _run_turn(user_input: str, components) -> dict:
    """Execute one agent turn. Returns a dict for rendering."""
    session, registry, ctx_builder, router, trace_writer, feedback_writer = components

    try:
        from agent.types import AgentTurn
        import uuid
        turn = AgentTurn(
            turn_id=str(uuid.uuid4()),
            session_id=st.session_state.session_id,
            user_input=user_input,
            user_email=st.session_state.get("user_email", ""),
        )

        ctx = ctx_builder.build(turn, st.session_state.history)
        skill_call = router.route(turn, ctx)

        skill = registry.get(skill_call.skill_name)
        slots_obj = skill.slot_schema(**skill_call.slots)
        validation_errors = skill.validate(slots_obj, ctx)

        if validation_errors:
            from agent.skills.clarify import ClarifySkill, ClarifySlots
            skill = ClarifySkill()
            slots_obj = ClarifySlots(
                question="There was an issue with your request.",
                validation_errors=validation_errors,
            )
            skill_call.skill_name = "clarify"

        result = skill.execute(slots_obj, ctx)
        trace_id = trace_writer.write(turn, skill_call, result, ctx)

        st.session_state.history.append({
            "user_input": user_input,
            "routed_skill": result.skill_name,
            "slots": skill_call.slots,
            "result_summary": {"row_count": len(result.data) if isinstance(result.data, list) else 1},
            "trace_id": trace_id,
        })

        return {
            "skill": skill,
            "result": result,
            "skill_call": skill_call,
            "trace_id": trace_id,
            "feedback_writer": feedback_writer,
            "ctx": ctx,
        }
    except Exception as e:
        logger.exception("Turn execution failed")
        return {"error": str(e)}


def _render_feedback_widget(trace_id: str, feedback_writer, skill_call, ctx):
    """Render thumbs up/down + correction expander for a result."""
    col1, col2, _ = st.columns([1, 1, 8])

    key_up = f"up_{trace_id}"
    key_down = f"down_{trace_id}"

    with col1:
        if st.button("👍", key=key_up, help="This was helpful"):
            feedback_writer.write(trace_id=trace_id, rating="up",
                                  user_email=ctx.turn.user_email if hasattr(ctx, "turn") else "")
            st.toast("Thanks for the feedback!")
    with col2:
        if st.button("👎", key=key_down, help="This wasn't right"):
            st.session_state[f"show_correction_{trace_id}"] = True

    if st.session_state.get(f"show_correction_{trace_id}"):
        with st.expander("Was the interpretation right?", expanded=True):
            st.write(f"**Routed to:** `{skill_call.skill_name}`")
            st.json(skill_call.slots)
            correction = st.text_area("What should it have done?", key=f"corr_{trace_id}")
            if st.button("Submit correction", key=f"submit_{trace_id}"):
                feedback_writer.write(
                    trace_id=trace_id,
                    rating="down",
                    correction_text=correction,
                    user_email=ctx.turn.user_email if hasattr(ctx, "turn") else "",
                )
                st.session_state[f"show_correction_{trace_id}"] = False
                st.toast("Correction saved!")

    save_key = f"save_{trace_id}"
    if st.button("Save as eval example", key=save_key):
        feedback_writer.write(
            trace_id=trace_id,
            rating="up",
            notes="eval_candidate",
        )
        st.toast("Saved as eval candidate!")


def main():
    st.title("💬 Order Intelligence Chat")

    # Lazy-load components (cached via @st.cache_resource inside)
    components = _get_agent_components()

    for turn in st.session_state.get("history", []):
        with st.chat_message("user"):
            st.write(turn["user_input"])
        with st.chat_message("assistant"):
            st.caption(f"Skill: `{turn.get('routed_skill', '?')}`")

    user_input = st.chat_input("Ask about orders (e.g. 'show me ORD-12345' or 'orders by status this quarter')")

    if user_input:
        with st.chat_message("user"):
            st.write(user_input)

        with st.chat_message("assistant"):
            with st.spinner("Thinking..."):
                turn_result = _run_turn(user_input, components)

            if "error" in turn_result:
                st.error(f"Error: {turn_result['error']}")
            else:
                skill = turn_result["skill"]
                result = turn_result["result"]
                skill_call = turn_result["skill_call"]
                trace_id = turn_result["trace_id"]
                feedback_writer = turn_result["feedback_writer"]
                ctx = turn_result["ctx"]

                if result.status == "error":
                    st.error(result.error or "An error occurred.")
                else:
                    try:
                        skill.present(result, ctx)
                    except Exception as render_err:
                        st.warning(f"Could not render result: {render_err}")
                        if isinstance(result.data, list):
                            import pandas as pd
                            st.dataframe(pd.DataFrame(result.data))

                _render_feedback_widget(trace_id, feedback_writer, skill_call, ctx)

                if result.confidence < 0.7:
                    st.warning(
                        f"I wasn't fully confident in this answer (score: {result.confidence:.0%}). "
                        "Was this what you were looking for?"
                    )


if __name__ == "__main__":
    main()
