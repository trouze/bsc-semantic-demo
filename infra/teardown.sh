#!/usr/bin/env bash
set -euo pipefail

: "${SF_ACCOUNT:?must be set in env}"

echo "==> Dropping SPCS service..."
snow sql -q "DROP SERVICE IF EXISTS DEMO_BSC.AGENT_APP"

echo "==> Suspending tasks..."
snow sql -q "ALTER TASK IF EXISTS DEMO_BSC.NIGHTLY_AGENT_EVAL SUSPEND"
snow sql -q "ALTER TASK IF EXISTS DEMO_BSC.REFRESH_CATALOG_DAILY SUSPEND"

echo "==> Dropping compute pool..."
snow sql -q "DROP COMPUTE POOL IF EXISTS DEMO_AGENT_POOL"

echo "==> Dropping image repository..."
snow sql -q "DROP IMAGE REPOSITORY IF EXISTS DEMO_BSC.AGENT_REPO"

echo "==> Dropping External Access Integration..."
snow sql -q "DROP EXTERNAL ACCESS INTEGRATION IF EXISTS DBT_CLOUD_EAI"
snow sql -q "DROP NETWORK RULE IF EXISTS DEMO_BSC.DBT_CLOUD_RULE"
snow sql -q "DROP SECRET IF EXISTS DEMO_BSC.DBT_CLOUD_TOKEN"

echo "==> Dropping warehouse..."
snow sql -q "DROP WAREHOUSE IF EXISTS DEMO_WH"

echo "==> Dropping database..."
snow sql -q "DROP DATABASE IF EXISTS DEMO_BSC"

echo "✅  Teardown complete."
