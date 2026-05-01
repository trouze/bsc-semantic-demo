"""Unit tests for agent.cortex.json_repair."""
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from agent.cortex.json_repair import extract_content, extract_json_block, repair_json


class TestRepairJson:
    def test_clean_json_parsed_correctly(self):
        assert repair_json('{"a": 1, "b": "hello"}') == {"a": 1, "b": "hello"}

    def test_json_in_markdown_fences_extracted_and_parsed(self):
        assert repair_json('```json\n{"key": "value"}\n```') == {"key": "value"}

    def test_json_in_plain_fences_extracted_and_parsed(self):
        assert repair_json('```\n{"x": 42}\n```') == {"x": 42}

    def test_slightly_malformed_trailing_comma(self):
        result = repair_json('{"a": 1,}')
        assert result is None or isinstance(result, dict)

    def test_completely_unparseable_returns_none(self):
        assert repair_json("this is not JSON at all") is None

    def test_truncated_json_repaired_with_closing_brace(self):
        result = repair_json('{"metrics": ["order_count"], "group_by": []')
        assert result is not None
        assert result.get("metrics") == ["order_count"]

    def test_empty_string_returns_none(self):
        assert repair_json("") is None

    def test_nested_object(self):
        assert repair_json('{"outer": {"inner": 99}}') == {"outer": {"inner": 99}}


class TestExtractJsonBlock:
    def test_strips_markdown_fences(self):
        assert extract_json_block('```json\n{"a": 1}\n```') == '{"a": 1}'

    def test_returns_first_json_object(self):
        assert extract_json_block('Some text before {"key": "val"} after') == '{"key": "val"}'

    def test_no_json_returns_cleaned_text(self):
        assert extract_json_block("no json here") == "no json here"

    def test_multiline_json_extracted(self):
        result = extract_json_block('```json\n{\n  "a": 1,\n  "b": 2\n}\n```')
        assert '"a": 1' in result
        assert '"b": 2' in result


class TestExtractContent:
    def test_plain_string_returned_as_is(self):
        assert extract_content("hello") == "hello"

    def test_cortex_envelope_unwrapped(self):
        envelope = json.dumps({"choices": [{"messages": "the actual text"}]})
        assert extract_content(envelope) == "the actual text"

    def test_non_envelope_json_returned_as_is(self):
        data = json.dumps({"foo": "bar"})
        assert extract_content(data) == data

    def test_invalid_json_returned_as_is(self):
        raw = "not json {{"
        assert extract_content(raw) == raw
