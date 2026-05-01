import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from agent.types import AgentTurn, ContextPack, SkillCall, SkillResult, Timer


def test_agent_turn_defaults():
    turn = AgentTurn(turn_id="t1", session_id="s1", user_input="hello")
    assert turn.turn_id == "t1"
    assert turn.session_id == "s1"
    assert turn.user_input == "hello"
    assert turn.user_email is None


def test_agent_turn_with_email():
    turn = AgentTurn(turn_id="t2", session_id="s2", user_input="hi", user_email="a@b.com")
    assert turn.user_email == "a@b.com"


def test_context_pack():
    turn = AgentTurn(turn_id="t1", session_id="s1", user_input="q")
    cp = ContextPack(
        turn=turn,
        history=[],
        metric_names=["revenue"],
        dimension_names=["date"],
        entity_names=["order"],
        glossary_terms={"SLA": "Service Level Agreement"},
        status_values=["open", "closed"],
    )
    assert cp.catalog_hash == ""
    assert cp.glossary_terms["SLA"] == "Service Level Agreement"


def test_skill_call_defaults():
    sc = SkillCall(skill_name="lookup", slots={"order_id": "123"}, rationale="direct match")
    assert sc.router_raw == ""


def test_skill_result_defaults():
    sr = SkillResult(skill_name="lookup", skill_version="1.0", data={"rows": []})
    assert sr.status == "ok"
    assert sr.confidence == 1.0
    assert sr.error is None
    assert sr.timings == {}


def test_timer_total_ms_nonnegative():
    t = Timer()
    assert t.total_ms() >= 0.0


def test_timer_segment():
    t = Timer()
    with t.segment("step"):
        pass  # segment overhead is nonzero on any real system
    assert t.get("step") >= 0.0
    assert "step" in t.as_dict()


def test_timer_missing_segment_returns_zero():
    t = Timer()
    assert t.get("nonexistent") == 0.0


def test_timer_elapsed_increases():
    t = Timer()
    a = t.elapsed_ms()
    # Force nonzero work so monotonic clock advances
    _ = sum(range(10_000))
    b = t.elapsed_ms()
    assert b >= a


def test_timer_as_dict_is_copy():
    t = Timer()
    with t.segment("x"):
        pass
    d = t.as_dict()
    d["x"] = 9999.0
    assert t.get("x") != 9999.0
