#!/usr/bin/env bash
set -euo pipefail

: "${DBT_CLOUD_TOKEN:?must be set in env}"
: "${DBT_SL_HOST:=semantic-layer.cloud.getdbt.com}"
: "${DBT_ENVIRONMENT_ID:?must be set in env}"
: "${SF_ACCOUNT:?must be set in env}"
: "${SF_ROLE:?must be set in env}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SQL_DIR="${SCRIPT_DIR}/sql"
SPEC_DIR="${SCRIPT_DIR}/spec"
APP_DIR="${SCRIPT_DIR}/../app"

echo "==> [1/4] Provisioning Snowflake objects..."
snow sql -f "${SQL_DIR}/01_database_schema.sql"
snow sql -f "${SQL_DIR}/02_warehouse.sql"
snow sql -f "${SQL_DIR}/03_tables.sql"
envsubst < "${SQL_DIR}/04_secrets_eai.sql" | snow sql -i
snow sql -f "${SQL_DIR}/05_compute_pool.sql"
snow sql -f "${SQL_DIR}/06_image_repo.sql"

echo "==> [2/4] Building and pushing SPCS image..."
REGISTRY_URL=$(snow sql -q "SHOW IMAGE REPOSITORIES IN SCHEMA DEMO_BSC" \
  --format json | jq -r '.[] | select(.name=="AGENT_REPO") | .repository_url')

if [ -z "${REGISTRY_URL}" ]; then
  echo "ERROR: Could not retrieve image registry URL" >&2
  exit 1
fi

docker build -t "${REGISTRY_URL}/agent_app:latest" -f "${APP_DIR}/Dockerfile" "${APP_DIR}"
snow spcs image-registry login
docker push "${REGISTRY_URL}/agent_app:latest"

echo "==> [3/4] Running seeds and creating stored procs + tasks..."
snow sql -f "${SQL_DIR}/07_seed_glossary.sql"
snow sql -f "${SQL_DIR}/08_seed_golden.sql"
snow sql -f "${SQL_DIR}/09_stored_procs.sql"
snow sql -f "${SQL_DIR}/10_tasks.sql"

echo "==> [4/4] Creating SPCS service..."
envsubst < "${SPEC_DIR}/streamlit_service.yaml" > /tmp/service.yaml
snow spcs service create DEMO_BSC.AGENT_APP \
  --compute-pool DEMO_AGENT_POOL \
  --spec-path /tmp/service.yaml \
  --replace \
  --query-warehouse DEMO_WH \
  --external-access-integrations DBT_CLOUD_EAI

echo ""
echo "==> Service created. Public endpoint:"
snow sql -q "SHOW SERVICES IN SCHEMA DEMO_BSC" --format json \
  | jq -r '.[] | select(.name=="AGENT_APP") | .public_endpoints'
echo ""
echo "✅  Setup complete."
