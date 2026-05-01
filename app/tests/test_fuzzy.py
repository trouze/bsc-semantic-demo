"""Unit tests for app.agent.skills._fuzzy."""
import sys
import os

# Ensure 'app' package root is on the path when run from the repo root
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest

from agent.skills._fuzzy import (
    _normalize,
    _tokenize,
    _expand_tokens,
    FuzzyService,
    SearchFields,
    SearchRequest,
)


def test_normalize_punctuation_and_accents():
    # Apostrophe becomes a space, so "Mary's" → "mary s" (two tokens)
    result = _normalize("St. Mary's Hospital!")
    assert result == "st mary s hospital"


def test_tokenize_basic():
    assert _tokenize("boston scientific") == ["boston", "scientific"]


def test_tokenize_none():
    assert _tokenize(None) == []


def test_tokenize_short_tokens_dropped():
    # single-char tokens should be dropped (len < 2)
    assert "a" not in _tokenize("a big hospital")


def test_expand_tokens_st():
    result = _expand_tokens(["st", "hosp"])
    assert "saint" in result
    assert "hospital" in result
    # originals preserved
    assert "st" in result
    assert "hosp" in result


def test_expand_tokens_no_duplicates():
    result = _expand_tokens(["med"])
    assert result.count("med") == 1


def test_expand_tokens_unknown_token_unchanged():
    result = _expand_tokens(["acme"])
    assert result == ["acme"]


def test_normalize_inputs_exact_order_id():
    svc = FuzzyService()
    req = SearchRequest(fields=SearchFields(order_id="ORD-12345"))
    nq = svc.normalize_inputs(req)
    assert nq.order_id == "ORD-12345"


def test_normalize_inputs_exact_flag_via_plan():
    """is_exact is set on the CandidateQueryPlan, not on NormalizedQuery."""
    svc = FuzzyService()
    req = SearchRequest(fields=SearchFields(order_id="ORD-12345"))
    nq = svc.normalize_inputs(req)
    plan = svc.build_candidate_query(nq)
    assert plan.is_exact is True


def test_build_candidate_query_fuzzy_name_contains_like():
    svc = FuzzyService()
    req = SearchRequest(fields=SearchFields(facility_name="General Hospital"))
    nq = svc.normalize_inputs(req)
    plan = svc.build_candidate_query(nq)
    assert plan.is_exact is False
    assert "LIKE" in plan.sql


def test_build_candidate_query_exact_order_id():
    svc = FuzzyService()
    nq = svc.normalize_inputs(SearchRequest(fields=SearchFields(order_id="ABC-001")))
    plan = svc.build_candidate_query(nq)
    assert plan.is_exact is True
    assert "order_id" in plan.sql.lower()


def test_build_candidate_query_respects_max_candidates():
    svc = FuzzyService(max_candidates=5)
    req = SearchRequest(fields=SearchFields(customer_name="Acme Corp"))
    nq = svc.normalize_inputs(req)
    plan = svc.build_candidate_query(nq)
    assert plan.params["max_candidates"] == 5
