"""dbt Semantic Layer explorer — DIOH demo with three exploration narratives.

Layers: (1) dimensions / attributes, (2) entities as curated entry points, (3) metrics / KPIs.
Filter pick-lists are mocked; compilation uses real dbt Semantic Layer compileSql.
"""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Sequence

import streamlit as st

sys.path.insert(0, str(Path(__file__).parent.parent))

from app.config import Settings, get_settings
from app.semantic.catalog import MetricMeta, SemanticCatalog, load_catalog
from app.semantic.executor import compile_sql_sync, entities_sync
from dbtsl.api.shared.query_params import GroupByParam, GroupByType, OrderByGroupBy, OrderByMetric

st.set_page_config(
    page_title="dbt Semantic Layer — Explorer",
    page_icon="◆",
    layout="wide",
    initial_sidebar_state="expanded",
)


@st.cache_resource(show_spinner="Loading metrics from dbt Semantic Layer…")
def _get_catalog() -> SemanticCatalog:
    cfg = get_settings()
    if not (cfg.dbt_sl_auth_token or "").strip():
        raise ValueError("DBT_SL_AUTH_TOKEN is missing. Add it to .streamlit/secrets.toml or the environment.")
    return load_catalog(cfg.dbt_sl_environment_id, cfg.dbt_sl_auth_token, cfg.dbt_sl_host)


def _is_dioh_metric(metric_name: str) -> bool:
    return "dioh" in metric_name.lower()


def _dioh_metrics(catalog: SemanticCatalog) -> list[MetricMeta]:
    return [m for m in catalog.metrics if _is_dioh_metric(m.name)]


def _union_dimensions(dioh: list[MetricMeta]) -> list[str]:
    seen: set[str] = set()
    for m in dioh:
        seen.update(m.dimensions)
    return sorted(seen)


def _mock_dimension_values(dimension_name: str) -> list[str]:
    key = dimension_name.lower()
    if "region" in key or "geo" in key:
        return ["North America", "EMEA", "APAC", "LATAM", "Corporate"]
    if "channel" in key or "route" in key:
        return ["Direct", "Distributor", "Digital", "Field", "Other"]
    if "product" in key or "sku" in key or "family" in key:
        return ["Implantable", "Capital equipment", "Consumables", "Software", "Services"]
    if "site" in key or "facility" in key or "location" in key:
        return ["Boston", "Paris", "Singapore", "San Jose", "Remote"]
    if "country" in key:
        return ["United States", "Germany", "Japan", "Brazil", "Australia"]
    if "tier" in key or "segment" in key or "class" in key:
        return ["Tier 1", "Tier 2", "Tier 3", "Strategic", "Emerging"]
    if "year" in key and "month" not in key:
        return ["2023", "2024", "2025"]
    return ["Alpha", "Bravo", "Charlie", "Delta", "Echo"]


def _sql_string_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _where_dimension_membership(dimension_name: str, values: list[str]) -> str | None:
    if not values:
        return None
    lit = ", ".join(_sql_string_literal(v) for v in values)
    return f"{{{{ Dimension('{dimension_name}') }}}} IN ({lit})"


def _build_group_by(metric: MetricMeta, breakdown: list[str], time_grain: str | None) -> list[str | GroupByParam] | None:
    if not breakdown:
        return []
    out: list[str | GroupByParam] = []
    for dim in breakdown:
        if dim in metric.time_dimensions:
            grain = time_grain or (metric.time_grains[0] if metric.time_grains else None)
            if not grain:
                return None
            out.append(GroupByParam(name=dim, type=GroupByType.TIME_DIMENSION, grain=grain))
        else:
            out.append(dim)
    return out


def _dimensions_for_entity(entity_name: str, all_dims: Sequence[str]) -> list[str]:
    prefix = f"{entity_name}__"
    return sorted(d for d in all_dims if d.startswith(prefix))


def _render_filters_and_adv(
    categorical_dimensions: list[str],
    *,
    key_prefix: str,
) -> list[str]:
    where_clauses: list[str] = []
    filter_dim = st.selectbox(
        "Filter dimension",
        [""] + categorical_dimensions,
        format_func=lambda x: "(none)" if x == "" else x,
        key=f"{key_prefix}_fdim",
        help="Values are illustrative only for this demo.",
    )
    if filter_dim:
        mock_options = _mock_dimension_values(filter_dim)
        st.caption("_Demo: mock pick-list._")
        filter_values = st.multiselect(
            f"Values — `{filter_dim}`",
            options=mock_options,
            default=[],
            key=f"{key_prefix}_fvals",
        )
        clause = _where_dimension_membership(filter_dim, filter_values)
        if clause:
            where_clauses.append(clause)

    adv = st.text_area(
        "Additional `where` clauses (one per line)",
        height=72,
        key=f"{key_prefix}_adv",
        placeholder="Example:\n{{ Dimension('customer__tier') }} = 'Enterprise'",
    )
    for line in (adv or "").splitlines():
        line = line.strip()
        if line:
            where_clauses.append(line)
    return where_clauses


def _show_compile_result(payload: dict, sql: str) -> None:
    st.divider()
    st.subheader("Compiled warehouse SQL")
    st.code(sql, language="sql")
    with st.expander("Request payload (semantic layer)", expanded=False):
        st.json(payload)


def _compile_with_anchor_fallback(
    cfg: Settings,
    *,
    anchor_metric: str,
    metrics: list[str] | None,
    group_by: list[Any] | None,
    where: list[str] | None,
    order_by: list[Any] | None,
    limit: int,
) -> tuple[str, list[str], bool]:
    """Compile SL SQL; retry with ``metrics=[anchor_metric]`` when dimension-only compile fails.

    MetricFlow often cannot resolve **SCD / validity-window** joins without a metric_time spine;
    pure ``group_by`` queries then error until a metric is included.
    """
    try:
        sql = compile_sql_sync(
            cfg.dbt_sl_environment_id,
            cfg.dbt_sl_auth_token,
            cfg.dbt_sl_host,
            metrics=metrics if metrics else None,
            group_by=group_by,
            where=where or None,
            order_by=order_by,
            limit=limit,
        )
        return sql, list(metrics) if metrics else [], False
    except Exception as first:
        if metrics:
            raise first
        try:
            sql = compile_sql_sync(
                cfg.dbt_sl_environment_id,
                cfg.dbt_sl_auth_token,
                cfg.dbt_sl_host,
                metrics=[anchor_metric],
                group_by=group_by,
                where=where or None,
                order_by=[OrderByMetric(name=anchor_metric, descending=True)],
                limit=limit,
            )
            return sql, [anchor_metric], True
        except Exception:
            raise first


# --- Cached entity fetch (scoped to DIOH metric graph) ---------------------------------


@st.cache_data(ttl=1800, show_spinner="Loading entities from Semantic Layer…")
def _cached_entities(
    environment_id: int,
    host: str,
    _token_fragment: str,
    dioh_metric_names: tuple[str, ...],
) -> list[tuple[str, str]]:
    """Return (entity_name, description) pairs."""
    cfg_token = get_settings().dbt_sl_auth_token
    if not cfg_token:
        return []
    names = list(dioh_metric_names)
    if not names:
        return []
    try:
        raw = entities_sync(
            environment_id,
            cfg_token,
            host,
            anchor_metrics=names,
        )
    except Exception:
        raw = entities_sync(
            environment_id,
            cfg_token,
            host,
            anchor_metrics=names[:1],
        )
    out: list[tuple[str, str]] = []
    for e in raw:
        desc = (getattr(e, "description", None) or "").strip()
        out.append((e.name, desc))
    return sorted(out, key=lambda x: x[0])


def _render_why_dbt_sl() -> None:
    st.markdown(
        """
### Why define metrics in dbt (vs only in the warehouse)?

| | **Warehouse semantic views** | **dbt Semantic Layer** |
|---|------------------------------|---------------------------|
| **Definition** | Lives inside one warehouse | Lives in **dbt** — versioned with your analytics code |
| **Consumers** | Great for SQL users on that platform | **Metric definitions + compiled SQL** for BI tools, notebooks, **and LLMs** |
| **Portability** | Tied to that engine’s dialect | Same metric spec; dbt **compiles to native SQL** per platform |

The tabs in **Explore** walk three ways teams ask questions — attributes, entity-scoped exploration,
then KPIs — before you plug an LLM on top of the same compiled SQL.
        """.strip()
    )


def _layer_dimensions(
    cfg,
    catalog: SemanticCatalog,
    dioh_list: list[MetricMeta],
    dioh_names: list[str],
) -> None:
    st.markdown("#### Dimensions & attributes")
    st.caption(
        "Grain / list queries — **group_by only**, or add a metric when you need counts or KPIs. "
        "Models with **SCD validity windows** may auto-include the anchor metric so MetricFlow "
        "has **metric_time** for joins (see note after compile)."
    )
    anchor = st.selectbox(
        "Semantic context (anchor metric)",
        dioh_names,
        format_func=lambda n: f"{n} — {catalog.metric(n).label}" if catalog.metric(n) else n,
        key="dim_anchor",
        help="Defines which dimensions are valid for compile (same joined graph as this metric).",
    )
    metric = catalog.metric(anchor)
    assert metric is not None

    col_a, col_b = st.columns(2)
    with col_a:
        breakdown = st.multiselect(
            "Fields / breakdown",
            options=metric.dimensions,
            default=[],
            key="dim_breakdown",
        )
    with col_b:
        use_metric = st.checkbox(
            "Include metric aggregation",
            value=False,
            key="dim_use_metric",
            help='Examples: “count users by country”, “revenue by region”.',
        )
        agg_metric = None
        if use_metric:
            agg_metric = st.selectbox(
                "Metric",
                dioh_names,
                format_func=lambda n: f"{n} — {catalog.metric(n).label}" if catalog.metric(n) else n,
                key="dim_agg_metric",
            )

    time_grain: str | None = None
    if any(d in metric.time_dimensions for d in breakdown):
        grains = list(metric.time_grains) or ["day", "week", "month", "quarter", "year"]
        time_grain = st.selectbox("Time grain (for time dimensions)", grains, key="dim_tg")

    group_by = _build_group_by(metric, breakdown, time_grain)
    if group_by is None:
        st.error("Pick a queryable time grain for the selected time dimensions.")
        return

    where_clauses = _render_filters_and_adv(metric.categorical_dimensions, key_prefix="dim")

    limit = st.number_input("Row limit", min_value=1, max_value=100_000, value=500, key="dim_lim")

    if st.button("Compile SQL", type="primary", key="dim_compile"):
        if not breakdown and not (use_metric and agg_metric):
            st.warning("Select at least one breakdown dimension, or enable metric aggregation.")
            return

        gb_for_compile = group_by if breakdown else None
        order_by = None
        payload_metrics = None
        if use_metric and agg_metric:
            payload_metrics = [agg_metric]
            order_by = [OrderByMetric(name=agg_metric, descending=True)]
        elif breakdown:
            first = breakdown[0]
            order_by = [OrderByGroupBy(name=first, grain=None, descending=False)]

        try:
            sql, effective_metrics, used_fallback = _compile_with_anchor_fallback(
                cfg,
                anchor_metric=anchor,
                metrics=payload_metrics,
                group_by=gb_for_compile,
                where=where_clauses,
                order_by=order_by,
                limit=int(limit),
            )
        except Exception as exc:  # noqa: BLE001
            st.error(f"Compilation failed: {exc}")
            return

        if used_fallback:
            st.info(
                "**Compile used the anchor metric** because a dimension-only request failed "
                "(often **validity-window / SCD** joins need **metric_time**). "
                "The SQL now aggregates that anchor metric by your dimensions — same grain fix "
                "you’d use for LLM or BI tools on this graph."
            )

        payload = {
            "layer": "dimensions",
            "anchor_metric": anchor,
            "metrics": effective_metrics,
            "metrics_requested": payload_metrics,
            "compile_anchor_fallback": used_fallback,
            "group_by": [str(x) for x in group_by] if breakdown else [],
            "where": where_clauses,
            "limit": int(limit),
        }
        _show_compile_result(payload, sql)


def _layer_entities(
    cfg,
    catalog: SemanticCatalog,
    dioh_list: list[MetricMeta],
    dioh_names: list[str],
) -> None:
    st.markdown("#### Entities — explore a semantic model")
    st.caption(
        "Entities are declared in your semantic project; fields below are dimensions whose names "
        "start with `entity__…` across all DIOH metrics. "
        "Dimension-only compiles may retry with the anchor metric when **SCD validity** joins require metric_time."
    )

    all_dims = _union_dimensions(dioh_list)
    pairs = _cached_entities(
        cfg.dbt_sl_environment_id,
        cfg.dbt_sl_host,
        (cfg.dbt_sl_auth_token or "")[:24],
        tuple(sorted(dioh_names)),
    )
    if not pairs:
        st.info(
            "No entities returned for these metrics — check the semantic layer or try expanding "
            "which metrics anchor the entity graph."
        )

    entity_names = [p[0] for p in pairs]
    entity_labels = {p[0]: p[1] for p in pairs}
    pick_e = st.selectbox(
        "Entity",
        entity_names if entity_names else [""],
        format_func=lambda n: n if n else "(none)",
        disabled=not entity_names,
        key="ent_pick",
    )
    if pick_e and entity_labels.get(pick_e):
        st.caption(entity_labels[pick_e][:280])

    fields = _dimensions_for_entity(pick_e, all_dims) if pick_e else []
    if pick_e and not fields:
        st.warning(f"No dimensions prefixed `{pick_e}__` found in this environment’s DIOH metrics.")

    breakdown = st.multiselect(
        "Fields on this entity",
        options=fields,
        default=[],
        key="ent_fields",
    )

    use_anchor = st.selectbox(
        "Anchor metric (for time grains & validation)",
        dioh_names,
        format_func=lambda n: f"{n} — {catalog.metric(n).label}" if catalog.metric(n) else n,
        key="ent_anchor",
        help="Used to resolve time dimensions and grains the same way as elsewhere.",
    )
    anchor_meta = catalog.metric(use_anchor)
    assert anchor_meta is not None

    # Breakdown dims must be validated against anchor graph — filter to intersection
    valid_break = [d for d in breakdown if d in anchor_meta.dimensions]
    if len(valid_break) != len(breakdown):
        st.caption("_Some fields may be omitted at compile time if they are not joinable from the anchor metric._")

    use_metric = st.checkbox(
        "Include metric (aggregation)",
        value=False,
        key="ent_use_metric",
    )
    agg_metric = None
    if use_metric:
        agg_metric = st.selectbox(
            "Metric",
            dioh_names,
            format_func=lambda n: f"{n} — {catalog.metric(n).label}" if catalog.metric(n) else n,
            key="ent_metric",
        )

    time_grain: str | None = None
    if any(d in anchor_meta.time_dimensions for d in valid_break):
        grains = list(anchor_meta.time_grains) or ["day", "week", "month", "quarter", "year"]
        time_grain = st.selectbox("Time grain", grains, key="ent_tg")

    group_by = _build_group_by(anchor_meta, valid_break, time_grain)
    if group_by is None:
        st.error("Pick a queryable time grain for time dimensions.")
        return

    where_clauses = _render_filters_and_adv(anchor_meta.categorical_dimensions, key_prefix="ent")

    limit = st.number_input("Row limit", min_value=1, max_value=100_000, value=500, key="ent_lim")

    if st.button("Compile SQL", type="primary", key="ent_compile"):
        if not valid_break and not (use_metric and agg_metric):
            st.warning("Select fields on the entity, or enable a metric.")
            return

        payload_metrics = [agg_metric] if use_metric and agg_metric else None
        gb_final = group_by if valid_break else None
        order_by = None
        if payload_metrics:
            order_by = [OrderByMetric(name=payload_metrics[0], descending=True)]
        elif valid_break:
            order_by = [OrderByGroupBy(name=valid_break[0], grain=None, descending=False)]

        try:
            sql, effective_metrics, used_fallback = _compile_with_anchor_fallback(
                cfg,
                anchor_metric=use_anchor,
                metrics=payload_metrics,
                group_by=gb_final,
                where=where_clauses,
                order_by=order_by,
                limit=int(limit),
            )
        except Exception as exc:  # noqa: BLE001
            st.error(f"Compilation failed: {exc}")
            return

        if used_fallback:
            st.info(
                "**Compile used the anchor metric** after a dimension-only attempt failed "
                "(typical when the graph has **validity-window / SCD** semantics)."
            )

        payload = {
            "layer": "entities",
            "entity": pick_e,
            "anchor_metric": use_anchor,
            "metrics": effective_metrics,
            "metrics_requested": payload_metrics,
            "compile_anchor_fallback": used_fallback,
            "group_by": [str(x) for x in group_by] if valid_break else [],
            "where": where_clauses,
            "limit": int(limit),
        }
        _show_compile_result(payload, sql)


def _layer_metrics(
    cfg,
    catalog: SemanticCatalog,
    dioh_list: list[MetricMeta],
    dioh_names: list[str],
) -> None:
    st.markdown("#### Metrics & KPIs")
    st.caption(
        "Governed aggregations — the usual production path for dashboards and LLM-grounded answers."
    )

    pick = st.selectbox(
        "Metric",
        dioh_names,
        format_func=lambda n: f"{n} — {catalog.metric(n).label}" if catalog.metric(n) else n,
        key="m_pick",
    )
    metric = catalog.metric(pick)
    assert metric is not None

    col_left, col_right = st.columns((1, 1), gap="large")

    with col_left:
        st.markdown(f"**{metric.label}** (`{metric.name}`)")
        st.caption(metric.description or "_No description._")
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("**Type**")
            st.text(metric.metric_type)
        with c2:
            st.markdown("**Time grains**")
            st.text(", ".join(metric.time_grains) or "—")

        with st.expander("Dimensions for this metric", expanded=False):
            st.markdown(
                "**Categorical**\n\n"
                + "\n".join(f"- `{d}`" for d in metric.categorical_dimensions)
                or "_None._"
            )
            st.markdown(
                "**Time**\n\n" + "\n".join(f"- `{d}`" for d in metric.time_dimensions) or "_None._"
            )

    with col_right:
        breakdown = st.multiselect(
            "Breakdown by (group by)",
            options=metric.dimensions,
            default=[],
            key="m_gb",
        )

        time_grain: str | None = None
        if any(d in metric.time_dimensions for d in breakdown):
            grains = list(metric.time_grains) or ["day", "week", "month", "quarter", "year"]
            time_grain = st.selectbox("Time grain", grains, key="m_tg")

        group_by = _build_group_by(metric, breakdown, time_grain)
        if group_by is None:
            st.error("Queryable time grain required for selected time dimensions.")
            return

        where_clauses = _render_filters_and_adv(metric.categorical_dimensions, key_prefix="m")

        limit = st.number_input("Row limit", min_value=1, max_value=100_000, value=500, key="m_lim")
        order_by_metric = st.checkbox("Order by this metric (descending)", value=True, key="m_ob")

        if st.button("Compile SQL", type="primary", key="m_compile"):
            order_by = [OrderByMetric(name=pick, descending=True)] if order_by_metric else None
            try:
                sql = compile_sql_sync(
                    cfg.dbt_sl_environment_id,
                    cfg.dbt_sl_auth_token,
                    cfg.dbt_sl_host,
                    metrics=[pick],
                    group_by=group_by or None,
                    where=where_clauses or None,
                    order_by=order_by,
                    limit=int(limit),
                )
            except Exception as exc:  # noqa: BLE001
                st.error(f"Compilation failed: {exc}")
                return

            payload = {
                "layer": "metrics",
                "metrics": [pick],
                "group_by": [str(x) for x in group_by],
                "where": where_clauses,
                "order_by": [f"{pick} desc"] if order_by_metric else None,
                "limit": int(limit),
            }
            _show_compile_result(payload, sql)


def main() -> None:
    st.sidebar.title("Connection")
    try:
        catalog = _get_catalog()
    except Exception as exc:  # noqa: BLE001
        st.error(str(exc))
        st.stop()

    cfg = get_settings()
    st.sidebar.caption("dbt Semantic Layer — compile via GraphQL")
    dioh_list = _dioh_metrics(catalog)
    dioh_names = [m.name for m in dioh_list]
    st.sidebar.metric("DIOH metrics (demo scope)", len(dioh_list))
    st.sidebar.caption(f"{len(catalog.metrics)} metrics total in environment (filtered in UI).")
    st.sidebar.text(f"Environment ID: {cfg.dbt_sl_environment_id}")
    st.sidebar.text(f"Host: {cfg.dbt_sl_host}")

    st.title("Semantic Layer explorer")
    st.caption(
        "Three ways to explore — **dimensions**, **entities**, then **metrics**. "
        "DIOH scope · mock filter lists · real compileSql."
    )

    tab_explore, tab_story = st.tabs(["Explore", "Why dbt Semantic Layer?"])
    with tab_story:
        _render_why_dbt_sl()

    with tab_explore:
        if not dioh_names:
            st.warning(
                "No **DIOH** metrics found (names must contain `dioh`). "
                "Deploy semantic models or relax `_is_dioh_metric` in `streamlit_app.py`."
            )
            st.stop()

        MODE = {
            "dimensions": "1 · Dimensions & attributes",
            "entities": "2 · Entities / models",
            "metrics": "3 · Metrics & KPIs",
        }
        layer = st.radio(
            "Exploration layer",
            list(MODE.keys()),
            format_func=lambda k: MODE[k],
            horizontal=True,
            label_visibility="collapsed",
        )

        st.divider()

        if layer == "dimensions":
            _layer_dimensions(cfg, catalog, dioh_list, dioh_names)
        elif layer == "entities":
            _layer_entities(cfg, catalog, dioh_list, dioh_names)
        else:
            _layer_metrics(cfg, catalog, dioh_list, dioh_names)


if __name__ == "__main__":
    main()
