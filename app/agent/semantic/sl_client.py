"""dbt Semantic Layer client — wraps dbt-sl-sdk SemanticLayerClient.

Compile-only: compile_sql() returns MetricFlow SQL but does NOT execute it.
Execution is handled by SnowflakeExecutor in agent.semantic.executor.

NOTE on dbt-sl-sdk API versioning:
The API surface changed across versions. This file targets the synchronous
context-manager style: `with client.session(): client.metrics()`.
If the installed version is async-first (e.g., uses `async with` and `await`),
the `_get_client` and method bodies must be updated accordingly — replace
`with client.session():` with `async with client.session():` and add `await`
before each SDK call, then make the public methods async.
"""
from __future__ import annotations

import logging
import os
import pickle
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)

_CATALOG_CACHE_PATH = Path(os.getenv("CATALOG_CACHE_PATH", "/data/catalog_cache.pkl"))


@dataclass
class MetricInfo:
    name: str
    description: str = ""
    meta: dict[str, Any] = field(default_factory=dict)


@dataclass
class DimensionInfo:
    name: str
    dimension_type: str = ""
    description: str = ""


@dataclass
class EntityInfo:
    name: str
    entity_type: str = ""
    description: str = ""


@dataclass
class SemanticCatalogData:
    metrics: list[MetricInfo] = field(default_factory=list)
    dimensions: list[DimensionInfo] = field(default_factory=list)
    entities: list[EntityInfo] = field(default_factory=list)

    @property
    def metric_names(self) -> list[str]:
        return [m.name for m in self.metrics]

    @property
    def dimension_names(self) -> list[str]:
        return [d.name for d in self.dimensions]

    @property
    def entity_names(self) -> list[str]:
        return [e.name for e in self.entities]


class SLClient:
    """Wraps dbt-sl-sdk SemanticLayerClient for compile-only usage."""

    def __init__(self, host: str, environment_id: str, token: str) -> None:
        self._host = host
        self._environment_id = int(environment_id)
        self._token = token
        self._sdk_client = None

    def _get_client(self):
        if self._sdk_client is None:
            try:
                from dbtsl import SemanticLayerClient

                self._sdk_client = SemanticLayerClient(
                    environment_id=self._environment_id,
                    auth_token=self._token,
                    host=self._host,
                )
            except ImportError as e:
                raise RuntimeError(
                    "dbt-sl-sdk is not installed. Add dbt-sl-sdk to requirements.txt."
                ) from e
        return self._sdk_client

    def list_metrics(self) -> list[MetricInfo]:
        """List all available metrics from the Semantic Layer."""
        client = self._get_client()
        try:
            with client.session():
                raw = client.metrics()
            return [
                MetricInfo(
                    name=m.name,
                    description=getattr(m, "description", "") or "",
                    meta=getattr(m, "meta", {}) or {},
                )
                for m in raw
            ]
        except Exception:
            logger.exception("SLClient.list_metrics failed")
            raise

    def get_dimensions(self, metrics: Optional[list[str]] = None) -> list[DimensionInfo]:
        """List dimensions (optionally scoped to given metrics)."""
        client = self._get_client()
        try:
            with client.session():
                raw = client.dimensions(metrics=metrics or [])
            return [
                DimensionInfo(
                    name=d.name,
                    dimension_type=str(getattr(d, "type", "")),
                    description=getattr(d, "description", "") or "",
                )
                for d in raw
            ]
        except Exception:
            logger.exception("SLClient.get_dimensions failed")
            raise

    def get_entities(self, metrics: Optional[list[str]] = None) -> list[EntityInfo]:
        """List entities (optionally scoped to given metrics)."""
        client = self._get_client()
        try:
            with client.session():
                raw = client.entities(metrics=metrics or [])
            return [
                EntityInfo(
                    name=e.name,
                    entity_type=str(getattr(e, "type", "")),
                    description=getattr(e, "description", "") or "",
                )
                for e in raw
            ]
        except Exception:
            logger.exception("SLClient.get_entities failed")
            raise

    def compile_sql(
        self,
        metrics: list[str],
        group_by: Optional[list[str]] = None,
        where: Optional[list[str]] = None,
        order_by: Optional[list[str]] = None,
        limit: Optional[int] = None,
    ) -> str:
        """Compile MetricFlow SQL without executing it.

        Returns the SQL string that can be run directly against Snowflake.
        """
        client = self._get_client()
        try:
            with client.session():
                sql = client.compile_sql(
                    metrics=metrics,
                    group_by=group_by or [],
                    where=where or [],
                    order_by=order_by or [],
                    limit=limit,
                )
            return sql
        except Exception:
            logger.exception("SLClient.compile_sql failed for metrics=%s", metrics)
            raise

    def load_catalog(self) -> SemanticCatalogData:
        """Load full catalog — tries disk cache first, then live SL."""
        try:
            with open(_CATALOG_CACHE_PATH, "rb") as f:
                cached: SemanticCatalogData = pickle.load(f)
            logger.info("Loaded catalog from disk cache (%s metrics)", len(cached.metrics))
            return cached
        except FileNotFoundError:
            pass
        except Exception:
            logger.warning("Disk cache read failed; fetching live catalog")

        return self._fetch_and_cache_catalog()

    def _fetch_and_cache_catalog(self) -> SemanticCatalogData:
        metrics = self.list_metrics()
        all_metric_names = [m.name for m in metrics]
        dimensions = self.get_dimensions(all_metric_names)
        entities = self.get_entities(all_metric_names)
        catalog = SemanticCatalogData(metrics=metrics, dimensions=dimensions, entities=entities)
        try:
            _CATALOG_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
            with open(_CATALOG_CACHE_PATH, "wb") as f:
                pickle.dump(catalog, f)
            logger.info("Wrote catalog to disk cache")
        except Exception:
            logger.warning("Could not write catalog to disk cache")
        return catalog
