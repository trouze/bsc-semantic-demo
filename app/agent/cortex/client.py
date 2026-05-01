from __future__ import annotations
import logging
from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    from snowflake.snowpark import Session

from agent.cortex.json_repair import repair_json, extract_content

logger = logging.getLogger(__name__)

_DEFAULT_MODEL = "llama3.3-70b"


class CortexClient:
    def __init__(self, session: "Session", default_model: str = _DEFAULT_MODEL) -> None:
        self._session = session
        self._default_model = default_model

    def complete(
        self,
        prompt: str,
        model: Optional[str] = None,
        label: str = "",
    ) -> str:
        """Call SNOWFLAKE.CORTEX.COMPLETE and return the text response."""
        chosen_model = model or self._default_model
        try:
            rows = self._session.sql(
                "SELECT SNOWFLAKE.CORTEX.COMPLETE(?, ?) AS response",
                [chosen_model, prompt],
            ).collect()
            if not rows:
                raise RuntimeError(f"Empty response from Cortex [{label}]")
            raw = rows[0]["RESPONSE"] or ""
            return extract_content(raw)
        except Exception as exc:
            raise RuntimeError(str(exc)) from exc

    def complete_json(
        self,
        prompt: str,
        model: Optional[str] = None,
        label: str = "",
    ) -> Optional[dict[str, Any]]:
        """Call Cortex and parse the response as JSON, with repair fallback."""
        raw = self.complete(prompt, model=model, label=label)
        result = repair_json(raw)
        if result is None:
            logger.warning(
                "complete_json[%s]: JSON repair failed. Raw: %.200s", label, raw
            )
        return result
