-- Uses raw requests (no dbt-sl-sdk) because stored procs can't install arbitrary packages.
-- DBT_SL_HOST and DBT_CLOUD_TOKEN arrive via the DBT_CLOUD_EAI external access integration.
CREATE OR REPLACE PROCEDURE DEMO_BSC.REFRESH_SEMANTIC_CATALOG()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python', 'requests')
EXTERNAL_ACCESS_INTEGRATIONS = (DBT_CLOUD_EAI)
SECRETS = ('dbt_token' = DEMO_BSC.DBT_CLOUD_TOKEN)
HANDLER = 'run'
AS $$
import os
import json
import requests
from datetime import datetime, timezone

# GraphQL query placeholders — fill in once dbt SL schema is confirmed.
METRICS_QUERY = """
query GetMetrics($environmentId: BigInt!) {
  environment(id: $environmentId) {
    metricByName {
      name
      description
      type
    }
  }
}
"""

DIMENSIONS_QUERY = """
query GetDimensions($environmentId: BigInt!, $metrics: [MetricInput!]!) {
  environment(id: $environmentId) {
    dimensions(metrics: $metrics) {
      name
      type
      description
    }
  }
}
"""

ENTITIES_QUERY = """
query GetEntities($environmentId: BigInt!, $metrics: [MetricInput!]!) {
  environment(id: $environmentId) {
    entities(metrics: $metrics) {
      name
      type
      description
    }
  }
}
"""


def _gql(host: str, token: str, query: str, variables: dict) -> dict:
    url = f"https://{host}/api/graphql"
    resp = requests.post(
        url,
        json={"query": query, "variables": variables},
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def run(session):
    dbt_host = os.environ.get("DBT_SL_HOST", "semantic-layer.cloud.getdbt.com")
    dbt_token = session.get_secret("dbt_token")
    env_id = int(os.environ.get("DBT_ENVIRONMENT_ID", "0"))

    rows = []
    now = datetime.now(tz=timezone.utc).isoformat()

    try:
        metrics_resp = _gql(dbt_host, dbt_token, METRICS_QUERY, {"environmentId": env_id})
        metrics = (
            metrics_resp.get("data", {})
            .get("environment", {})
            .get("metricByName", []) or []
        )
        for m in metrics:
            rows.append({
                "object_name": m.get("name", ""),
                "object_type": "metric",
                "description": m.get("description", ""),
                "expr": None,
                "meta": json.dumps(m),
                "refreshed_at": now,
            })
    except Exception as exc:
        return f"ERROR fetching metrics: {exc}"

    try:
        metric_inputs = [{"name": r["object_name"]} for r in rows]
        dims_resp = _gql(dbt_host, dbt_token, DIMENSIONS_QUERY,
                         {"environmentId": env_id, "metrics": metric_inputs})
        dims = (
            dims_resp.get("data", {})
            .get("environment", {})
            .get("dimensions", []) or []
        )
        for d in dims:
            rows.append({
                "object_name": d.get("name", ""),
                "object_type": "dimension",
                "description": d.get("description", ""),
                "expr": None,
                "meta": json.dumps(d),
                "refreshed_at": now,
            })

        ents_resp = _gql(dbt_host, dbt_token, ENTITIES_QUERY,
                         {"environmentId": env_id, "metrics": metric_inputs})
        ents = (
            ents_resp.get("data", {})
            .get("environment", {})
            .get("entities", []) or []
        )
        for e in ents:
            rows.append({
                "object_name": e.get("name", ""),
                "object_type": "entity",
                "description": e.get("description", ""),
                "expr": None,
                "meta": json.dumps(e),
                "refreshed_at": now,
            })
    except Exception as exc:
        return f"ERROR fetching dimensions/entities: {exc}"

    if not rows:
        return "WARNING: no objects returned from dbt SL API"

    df = session.create_dataframe(rows)
    df.write.mode("overwrite").save_as_table("DEMO_BSC.SEMANTIC_CATALOG_CACHE")
    return f"OK: wrote {len(rows)} objects into SEMANTIC_CATALOG_CACHE"
$$;


-- Stub: replace handler once the agent package is deployed into the SPCS container.
CREATE OR REPLACE PROCEDURE DEMO_BSC.RUN_NIGHTLY_EVAL()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
AS $$
def run(session):
    # TODO: once the agent package is installed in the stored proc environment,
    # replace this stub with:
    #   from agent.eval.runner import EvalRunner
    #   results = EvalRunner(session=session).run()
    #   return f"OK: {results.passed}/{results.total} passed"
    return "STUB: nightly eval not yet wired — deploy agent package first"
$$;
