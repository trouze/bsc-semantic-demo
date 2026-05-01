"""Eval dashboard — read-only AGENT_EVAL_SUMMARY view."""
from __future__ import annotations
import logging
import sys
from pathlib import Path

import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).parents[1]))
from utils import get_session  # noqa: E402

logger = logging.getLogger(__name__)

_SUMMARY_SQL = """
SELECT run_id, prompt_version, model_version, skill_version,
       total, passed,
       ROUND(passed::FLOAT / NULLIF(total, 0) * 100, 1) AS accuracy_pct,
       p50_ms, p95_ms
FROM DEMO_BSC.AGENT_EVAL_SUMMARY
ORDER BY run_id DESC
LIMIT 100
"""

_DETAIL_SQL = """
SELECT r.golden_id, g.user_input, r.routed_skill, r.passed,
       r.latency_ms, r.error, r.check_details
FROM DEMO_BSC.AGENT_EVAL_RUNS r
JOIN DEMO_BSC.AGENT_GOLDEN g USING (golden_id)
WHERE r.run_id = ?
ORDER BY r.passed ASC, r.latency_ms DESC
"""


def main():
    st.title("📊 Eval Dashboard")
    session = get_session()

    try:
        rows = session.sql(_SUMMARY_SQL).collect()
    except Exception as e:
        st.warning(f"Could not load eval summary: {e}")
        st.info("Run `CALL DEMO_BSC.RUN_NIGHTLY_EVAL()` to populate this dashboard.")
        return

    if not rows:
        st.info("No eval runs yet. Run `CALL DEMO_BSC.RUN_NIGHTLY_EVAL()` to start.")
        return

    df = pd.DataFrame([r.as_dict() for r in rows])

    latest = df.iloc[0]
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Latest accuracy", f"{latest.get('ACCURACY_PCT', 0):.1f}%")
    col2.metric("Total prompts", int(latest.get("TOTAL", 0)))
    col3.metric("p50 latency", f"{latest.get('P50_MS', 0):.0f} ms")
    col4.metric(
        "p95 latency",
        f"{latest.get('P95_MS', 0):.0f} ms",
        delta="SLO ✓" if (latest.get("P95_MS", 9999) < 5000) else "⚠ SLO miss",
        delta_color="normal",
    )

    st.divider()
    st.subheader("All eval runs")
    st.dataframe(
        df[["RUN_ID", "PROMPT_VERSION", "ACCURACY_PCT", "TOTAL", "PASSED", "P50_MS", "P95_MS"]],
        use_container_width=True,
    )

    selected_run = st.selectbox("Drill into run", df["RUN_ID"].tolist())

    if selected_run:
        try:
            detail_rows = session.sql(_DETAIL_SQL, params=[selected_run]).collect()
            detail_df = pd.DataFrame([r.as_dict() for r in detail_rows])
            if not detail_df.empty:
                st.subheader(f"Prompt-level results for run `{selected_run}`")
                st.dataframe(
                    detail_df[["GOLDEN_ID", "USER_INPUT", "ROUTED_SKILL", "PASSED", "LATENCY_MS", "ERROR"]],
                    use_container_width=True,
                )
        except Exception as e:
            st.warning(f"Could not load detail: {e}")


if __name__ == "__main__":
    main()
