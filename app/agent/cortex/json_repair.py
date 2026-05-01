"""JSON extraction and repair utilities ported from cortex_service._repair_json."""
import json
import re
from typing import Any, Optional

_FENCE_RE = re.compile(r"```(?:json)?\s*", re.IGNORECASE)

_REPAIR_SUFFIXES = ["}", "]}", "null}", "null]}", '"}', '"]}']


def _strip_fences(text: str) -> str:
    return _FENCE_RE.sub("", text).replace("```", "").strip()


def extract_content(raw: Any) -> str:
    """Extract string content from a Cortex Complete response."""
    try:
        parsed = json.loads(raw)
        # Standard Cortex envelope: {"choices": [{"messages": "..."}]}
        if isinstance(parsed, dict) and "choices" in parsed:
            return parsed["choices"][0].get("messages", raw)
    except (json.JSONDecodeError, KeyError, IndexError):
        pass
    return raw


def extract_json_block(text: str) -> str:
    """Extract the first JSON object from a text blob, stripping markdown fences."""
    text = _strip_fences(text)
    match = re.search(r"\{.*\}", text, re.DOTALL)
    return match.group(0) if match else text


def repair_json(text: str) -> Optional[dict]:
    """Best-effort parse of a JSON string that may have LLM artifacts."""
    cleaned = _strip_fences(text)
    start = cleaned.find("{")
    if start < 0:
        return None
    fragment = cleaned[start:]
    # First try a straight parse of the extracted block
    try:
        return json.loads(fragment)
    except json.JSONDecodeError:
        pass
    # Fallback: try appending common truncation suffixes
    for suffix in _REPAIR_SUFFIXES:
        try:
            return json.loads(fragment + suffix)
        except json.JSONDecodeError:
            continue
    return None
