"""ContextBuilder — assembles a ContextPack for each agent turn."""
from __future__ import annotations
import logging
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from snowflake.snowpark import Session
    from agent.context.catalog import CatalogSnapshot
    from agent.context.glossary import GlossaryData

logger = logging.getLogger(__name__)

try:
    import streamlit as st
    _HAS_ST = True
except ImportError:
    _HAS_ST = False


def _get_catalog_cached(session: "Session"):
    from agent.context.catalog import load_catalog
    return load_catalog(session)


def _get_glossary_cached(session: "Session"):
    from agent.context.glossary import load_glossary
    return load_glossary(session)


if _HAS_ST:
    _get_catalog_cached = st.cache_resource(ttl=3600)(_get_catalog_cached)
    _get_glossary_cached = st.cache_resource(ttl=3600)(_get_glossary_cached)


class ContextBuilder:
    def __init__(self, session: "Session") -> None:
        self._session = session

    def build(self, turn: Any, history: list[dict[str, Any]]) -> Any:
        """Build a ContextPack for the current turn.

        Returns a ContextPack (imported lazily to avoid circular deps at test time).
        """
        catalog: CatalogSnapshot = _get_catalog_cached(self._session)
        glossary: GlossaryData = _get_glossary_cached(self._session)

        try:
            from agent.types import ContextPack
        except ImportError:
            # Stub for units where types.py may not yet be merged
            class ContextPack:  # type: ignore
                def __init__(self, **kw):
                    self.__dict__.update(kw)

        return ContextPack(
            turn=turn,
            history=history[-3:],  # last 3 turns
            metric_names=catalog.metric_names,
            dimension_names=catalog.dimension_names,
            entity_names=catalog.entity_names,
            glossary_terms=glossary.business_terms,
            status_values=glossary.status_values,
            catalog_hash=catalog.fingerprint,
        )
