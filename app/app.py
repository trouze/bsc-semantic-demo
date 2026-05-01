"""SPCS Streamlit app entry point — page config and session bootstrap."""
import streamlit as st


def _init_session_state():
    if "history" not in st.session_state:
        st.session_state.history = []
    if "session_id" not in st.session_state:
        import uuid
        st.session_state.session_id = str(uuid.uuid4())


def main():
    st.set_page_config(
        page_title="BSC Order Intelligence",
        page_icon="🏥",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    _init_session_state()
    st.title("BSC Order Intelligence")
    st.caption("Ask questions about Boston Scientific orders using natural language.")
    st.info("Use the sidebar to navigate to Chat, Traces, or Eval.")


if __name__ == "__main__":
    main()
