"""Eagerly-loaded semantic catalog — the curated context layer.

Loaded once at app startup and cached. All LLM prompts reference this catalog,
never raw schema. This is the governance boundary: only what's in the semantic
layer is visible to the agent.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any

from dbtsl.asyncio import AsyncSemanticLayerClient
from dbtsl.models import AsyncMetric, Dimension, DimensionType


@dataclass
class MetricMeta:
    name: str
    label: str
    description: str
    metric_type: str
    time_grains: list[str]
    dimensions: list[str]          # qualified names: "customer__region"
    categorical_dimensions: list[str]
    time_dimensions: list[str]
    entities: list[str]

    def to_llm_block(self) -> str:
        dims_display = ", ".join(self.categorical_dimensions[:10]) or "none"
        grains_display = ", ".join(self.time_grains) or "none"
        desc = self.description or "No description provided."
        return (
            f"### {self.name}\n"
            f"- Label: {self.label}\n"
            f"- Type: {self.metric_type}\n"
            f"- Description: {desc}\n"
            f"- Time grains: {grains_display}\n"
            f"- Dimensions: {dims_display}\n"
        )


@dataclass
class SemanticCatalog:
    metrics: list[MetricMeta] = field(default_factory=list)
    all_dimensions: set[str] = field(default_factory=set)  # union across all metrics

    # --- lookup helpers -------------------------------------------------------

    def metric(self, name: str) -> MetricMeta | None:
        for m in self.metrics:
            if m.name == name:
                return m
        return None

    def metric_names(self) -> list[str]:
        return [m.name for m in self.metrics]

    def dimensions_for(self, metric_names: list[str]) -> set[str]:
        result: set[str] = set()
        for name in metric_names:
            m = self.metric(name)
            if m:
                result.update(m.dimensions)
        return result

    def search_metrics(self, query: str, top_n: int = 15) -> list[MetricMeta]:
        """Keyword search — ranks metrics whose name/description overlap with query tokens."""
        tokens = set(query.lower().split())
        scored: list[tuple[int, MetricMeta]] = []
        for m in self.metrics:
            haystack = f"{m.name} {m.label} {m.description}".lower()
            score = sum(1 for t in tokens if t in haystack)
            scored.append((score, m))
        scored.sort(key=lambda x: x[0], reverse=True)
        return [m for _, m in scored[:top_n]]

    # --- LLM context ----------------------------------------------------------

    def format_for_llm(self, query: str | None = None, top_n: int = 20) -> str:
        """Return a compact markdown catalog block for injection into LLM prompts."""
        candidates = self.search_metrics(query, top_n) if query else self.metrics[:top_n]
        blocks = "\n".join(m.to_llm_block() for m in candidates)
        return f"## Available Metrics\n\n{blocks}"

    def all_valid_dimensions_for_llm(self) -> str:
        dims = sorted(self.all_dimensions)
        return "\n".join(f"- {d}" for d in dims)

    def summary(self) -> str:
        return f"{len(self.metrics)} metrics, {len(self.all_dimensions)} unique dimensions"


def _build_meta(m: AsyncMetric) -> MetricMeta:
    dims = getattr(m, "dimensions", []) or []
    entities = getattr(m, "entities", []) or []
    categorical = [d.qualified_name for d in dims if d.type == DimensionType.CATEGORICAL]
    time_dims = [d.qualified_name for d in dims if d.type == DimensionType.TIME]
    all_dim_names = [d.qualified_name for d in dims]
    return MetricMeta(
        name=m.name,
        label=getattr(m, "label", m.name),
        description=m.description or "",
        metric_type=m.type.value,
        time_grains=list(m.queryable_time_granularities),
        dimensions=all_dim_names,
        categorical_dimensions=categorical,
        time_dimensions=time_dims,
        entities=[e.name for e in entities],
    )


async def _fetch_catalog(environment_id: int, auth_token: str, host: str) -> SemanticCatalog:
    """Open one SL session, eagerly load everything, return a populated catalog."""
    client = AsyncSemanticLayerClient(
        environment_id=environment_id,
        auth_token=auth_token,
        host=host,
        lazy=False,  # eagerly resolve dimensions/entities per metric
    )
    async with client.session():
        metrics_raw = await client.metrics()

    metas = [_build_meta(m) for m in metrics_raw]
    all_dims: set[str] = set()
    for meta in metas:
        all_dims.update(meta.dimensions)

    return SemanticCatalog(metrics=metas, all_dimensions=all_dims)


def load_catalog(environment_id: int, auth_token: str, host: str) -> SemanticCatalog:
    """Sync entry point — runs the async fetch in a fresh event loop."""
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(_fetch_catalog(environment_id, auth_token, host))
    finally:
        loop.close()
