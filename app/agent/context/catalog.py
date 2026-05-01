"""SemanticCatalog — reads SEMANTIC_CATALOG_CACHE table.

The table is populated nightly by the REFRESH_SEMANTIC_CATALOG stored proc.
In-process cache via @st.cache_resource(ttl=3600) prevents per-turn DB hits.
"""
from __future__ import annotations
import hashlib
import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from snowflake.snowpark import Session

logger = logging.getLogger(__name__)

_QUERY = """
SELECT object_name, object_type, description, expr
FROM DEMO_BSC.SEMANTIC_CATALOG_CACHE
ORDER BY object_type, object_name
"""


@dataclass
class CatalogEntry:
    name: str
    object_type: str  # 'metric' | 'dimension' | 'entity'
    description: str = ""
    expr: str = ""


@dataclass
class CatalogSnapshot:
    entries: list[CatalogEntry] = field(default_factory=list)

    @property
    def metric_names(self) -> list[str]:
        return [e.name for e in self.entries if e.object_type == "metric"]

    @property
    def dimension_names(self) -> list[str]:
        return [e.name for e in self.entries if e.object_type == "dimension"]

    @property
    def entity_names(self) -> list[str]:
        return [e.name for e in self.entries if e.object_type == "entity"]

    @property
    def fingerprint(self) -> str:
        key = ",".join(sorted(e.name for e in self.entries))
        return hashlib.md5(key.encode()).hexdigest()[:8]


def load_catalog(session: "Session") -> CatalogSnapshot:
    """Load catalog from SEMANTIC_CATALOG_CACHE. Cached by caller via @st.cache_resource."""
    try:
        rows = session.sql(_QUERY).collect()
        entries = [
            CatalogEntry(
                name=r["OBJECT_NAME"],
                object_type=r["OBJECT_TYPE"].lower(),
                description=r["DESCRIPTION"] or "",
                expr=r["EXPR"] or "",
            )
            for r in rows
        ]
        logger.info("Loaded %d catalog entries", len(entries))
        return CatalogSnapshot(entries=entries)
    except Exception:
        logger.exception("Failed to load catalog from SEMANTIC_CATALOG_CACHE; returning empty")
        return CatalogSnapshot()
