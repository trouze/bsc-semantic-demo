"""Glossary — reads AGENT_GLOSSARY table for status values and business terms."""
from __future__ import annotations
import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from snowflake.snowpark import Session

logger = logging.getLogger(__name__)

_QUERY = """
SELECT term, category, definition
FROM DEMO_BSC.AGENT_GLOSSARY
WHERE active = TRUE
ORDER BY category, term
"""


@dataclass
class GlossaryData:
    status_values: list[str] = field(default_factory=list)
    business_terms: dict[str, str] = field(default_factory=dict)
    entity_relationships: dict[str, str] = field(default_factory=dict)


def load_glossary(session: "Session") -> GlossaryData:
    """Load glossary from AGENT_GLOSSARY. Cached by caller via @st.cache_resource."""
    try:
        rows = session.sql(_QUERY).collect()
        glossary = GlossaryData()
        for r in rows:
            term = r["TERM"]
            category = r["CATEGORY"]
            definition = r["DEFINITION"] or ""
            if category == "status_value":
                glossary.status_values.append(term)
            elif category == "business_term":
                glossary.business_terms[term] = definition
            elif category == "entity_rel":
                glossary.entity_relationships[term] = definition
        logger.info(
            "Loaded glossary: %d status values, %d business terms",
            len(glossary.status_values), len(glossary.business_terms),
        )
        return glossary
    except Exception:
        logger.exception("Failed to load glossary; returning empty")
        return GlossaryData()
