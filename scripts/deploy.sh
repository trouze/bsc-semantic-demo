#!/usr/bin/env bash
# =============================================================================
# BSci Semantic Demo — Full Snowflake Deployment
# =============================================================================
#
# Prerequisites (install before running):
#   1. Snowflake CLI v2+  https://docs.snowflake.com/developer-guide/snowflake-cli/installation
#      Configure a connection first:  snow connection add
#   2. Docker Desktop     https://docs.docker.com/get-docker/
#   3. A Snowflake role with ACCOUNTADMIN or these privileges:
#        CREATE DATABASE, CREATE COMPUTE POOL, CREATE INTEGRATION,
#        CREATE NETWORK RULE, BIND SERVICE ENDPOINT
#
# Quick start:
#   export SNOW_CONNECTION="my_connection"   # from `snow connection list`
#   export DBT_SL_ENVIRONMENT_ID="335860"
#   export DBT_SL_AUTH_TOKEN="dbtu_..."
#   chmod +x scripts/deploy.sh && ./scripts/deploy.sh
#
# To re-deploy after a code change (rebuild + push + upgrade service):
#   ./scripts/deploy.sh --upgrade
#
# To tear down all created Snowflake resources:
#   ./scripts/deploy.sh --teardown
# =============================================================================
set -euo pipefail
IFS=$'\n\t'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
SPEC_FILE="$(mktemp /tmp/spcs-spec-XXXXXX.yaml)"
FEEDBACK_SQL_TMP="$(mktemp /tmp/feedback-schema-XXXXXX.sql)"
trap 'rm -f "$SPEC_FILE" "$FEEDBACK_SQL_TMP"' EXIT

MODE="deploy"   # deploy | upgrade | teardown
for arg in "$@"; do
  case "$arg" in
    --upgrade)  MODE="upgrade"  ;;
    --teardown) MODE="teardown" ;;
  esac
done

# =============================================================================
# CONFIGURATION — edit or export as environment variables before running
# =============================================================================

# ── Snowflake CLI connection ─────────────────────────────────────────────────
# Run `snow connection list` to see available connections.
SNOW_CONNECTION="${SNOW_CONNECTION:-default}"
SNOW_ROLE="${SNOW_ROLE:-ACCOUNTADMIN}"       # needs CREATE COMPUTE POOL + CREATE INTEGRATION
SNOW_WAREHOUSE="${SNOW_WAREHOUSE:-COMPUTE_WH}"

# ── Snowflake objects ────────────────────────────────────────────────────────
SNOW_DATABASE="${SNOW_DATABASE:-ANALYTICS}"
SNOW_SCHEMA="${SNOW_SCHEMA:-PUBLIC}"
FEEDBACK_SCHEMA="${FEEDBACK_SCHEMA:-SEMANTIC_DEMO_FEEDBACK}"
IMAGE_REPO="${IMAGE_REPO:-SEMANTIC_DEMO_REGISTRY}"
COMPUTE_POOL="${COMPUTE_POOL:-SEMANTIC_DEMO_POOL}"
INSTANCE_FAMILY="${INSTANCE_FAMILY:-CPU_X64_XS}"   # cheapest CPU pool; use CPU_X64_S for more RAM
SERVICE_NAME="${SERVICE_NAME:-SEMANTIC_DEMO_APP}"

# ── dbt Semantic Layer (REQUIRED — get from dbt Cloud) ──────────────────────
DBT_SL_ENVIRONMENT_ID="${DBT_SL_ENVIRONMENT_ID:-}"
DBT_SL_AUTH_TOKEN="${DBT_SL_AUTH_TOKEN:-}"
DBT_SL_HOST="${DBT_SL_HOST:-hy250.semantic-layer.us1.dbt.com}"

# ── App tuning ───────────────────────────────────────────────────────────────
CORTEX_MODEL="${CORTEX_MODEL:-claude-3-5-sonnet}"
MAX_RESULT_ROWS="${MAX_RESULT_ROWS:-10000}"

# =============================================================================
# HELPERS
# =============================================================================

if [[ -t 1 ]]; then
  RED='\033[0;31m' GRN='\033[0;32m' YEL='\033[1;33m' BLU='\033[0;34m' BOLD='\033[1m' NC='\033[0m'
else
  RED='' GRN='' YEL='' BLU='' BOLD='' NC=''
fi

info()    { printf "${BLU}[info]${NC}  %s\n"    "$*"; }
ok()      { printf "${GRN}[ ok ]${NC}  %s\n"    "$*"; }
warn()    { printf "${YEL}[warn]${NC}  %s\n"    "$*"; }
die()     { printf "${RED}[fail]${NC}  %s\n"    "$*" >&2; exit 1; }
section() { printf "\n${BOLD}${BLU}▸ %s${NC}\n" "$*"; }
hr()      { printf "${BLU}%s${NC}\n" "─────────────────────────────────────────────────────"; }

# Run arbitrary SQL via Snow CLI
SQL() {
  snow sql \
    --connection  "$SNOW_CONNECTION" \
    --role        "$SNOW_ROLE"       \
    --warehouse   "$SNOW_WAREHOUSE"  \
    --format      plain              \
    -q "$1"
}

# Run snow spcs subcommands with consistent context flags
SPCS() {
  snow spcs "$@" \
    --connection "$SNOW_CONNECTION" \
    --role       "$SNOW_ROLE"       \
    --database   "$SNOW_DATABASE"   \
    --schema     "$SNOW_SCHEMA"
}

# =============================================================================
# TEARDOWN
# =============================================================================
if [[ "$MODE" == "teardown" ]]; then
  section "Teardown — removing all SPCS resources"
  warn "This drops: service, compute pool, image repo, secret, network rule, and EAI."
  warn "Feedback schema ${SNOW_DATABASE}.${FEEDBACK_SCHEMA} is preserved."
  read -rp "  Continue? [y/N] " confirm
  [[ "$confirm" == [yY] ]] || { info "Aborted."; exit 0; }

  SQL "DROP SERVICE           IF EXISTS ${SNOW_DATABASE}.${SNOW_SCHEMA}.${SERVICE_NAME};"       || true
  SQL "DROP COMPUTE POOL      IF EXISTS ${COMPUTE_POOL};"                                        || true
  SQL "DROP EXTERNAL ACCESS INTEGRATION IF EXISTS SEMANTIC_DEMO_EAI;"                           || true
  SQL "DROP NETWORK RULE      IF EXISTS ${SNOW_DATABASE}.${SNOW_SCHEMA}.SEMANTIC_DEMO_EGRESS_RULE;" || true
  SQL "DROP SECRET            IF EXISTS ${SNOW_DATABASE}.${SNOW_SCHEMA}.SEMANTIC_DEMO_DBT_TOKEN;"   || true
  SQL "DROP IMAGE REPOSITORY  IF EXISTS ${SNOW_DATABASE}.${SNOW_SCHEMA}.${IMAGE_REPO};"        || true

  ok "Resources dropped."
  info "To drop the feedback schema: DROP SCHEMA ${SNOW_DATABASE}.${FEEDBACK_SCHEMA};"
  exit 0
fi

# =============================================================================
# PHASE 0: PREFLIGHT
# =============================================================================
section "Preflight"

command -v snow    &>/dev/null || die "Snowflake CLI not found. Install: https://docs.snowflake.com/developer-guide/snowflake-cli/installation"
command -v docker  &>/dev/null || die "Docker not found. Install: https://docs.docker.com/get-docker/"
command -v python3 &>/dev/null || die "python3 required (used to parse CLI JSON output)."

[[ -n "$DBT_SL_AUTH_TOKEN" ]]     || die "DBT_SL_AUTH_TOKEN is required. Export it before running."
[[ -n "$DBT_SL_ENVIRONMENT_ID" ]] || die "DBT_SL_ENVIRONMENT_ID is required (e.g. 335860)."

snow connection test --connection "$SNOW_CONNECTION" --format plain &>/dev/null \
  || die "Cannot connect with Snow CLI connection '${SNOW_CONNECTION}'. Run: snow connection add"
ok "Snow CLI connection verified"

info "Mode:         ${MODE}"
info "Connection:   ${SNOW_CONNECTION}  (role: ${SNOW_ROLE})"
info "Database:     ${SNOW_DATABASE}.${SNOW_SCHEMA}"
info "Compute pool: ${COMPUTE_POOL} (${INSTANCE_FAMILY})"
info "Service:      ${SERVICE_NAME}"
info "dbt SL host:  ${DBT_SL_HOST}"

# =============================================================================
# PHASE 1: SNOWFLAKE INFRASTRUCTURE  (deploy only — skipped on --upgrade)
# =============================================================================
if [[ "$MODE" == "deploy" ]]; then
  section "Snowflake infrastructure"

  # ── Database + schemas ──────────────────────────────────────────────────────
  info "Creating database and schemas..."
  SQL "CREATE DATABASE IF NOT EXISTS ${SNOW_DATABASE};"
  SQL "CREATE SCHEMA  IF NOT EXISTS ${SNOW_DATABASE}.${SNOW_SCHEMA};"
  SQL "CREATE SCHEMA  IF NOT EXISTS ${SNOW_DATABASE}.${FEEDBACK_SCHEMA};"
  ok  "Database ${SNOW_DATABASE} ready"

  # ── Image repository ────────────────────────────────────────────────────────
  info "Creating image repository..."
  SQL "CREATE IMAGE REPOSITORY IF NOT EXISTS ${SNOW_DATABASE}.${SNOW_SCHEMA}.${IMAGE_REPO};"
  ok  "Image repository ${IMAGE_REPO} ready"

  # ── Compute pool ────────────────────────────────────────────────────────────
  info "Creating compute pool ${COMPUTE_POOL} (first-time provisioning can take 3-5 min)..."
  SQL "CREATE COMPUTE POOL IF NOT EXISTS ${COMPUTE_POOL}
         MIN_NODES         = 1
         MAX_NODES         = 1
         INSTANCE_FAMILY   = ${INSTANCE_FAMILY}
         AUTO_RESUME       = TRUE
         AUTO_SUSPEND_SECS = 3600
         COMMENT           = 'BSci Semantic Demo SPCS pool';"
  ok  "Compute pool ${COMPUTE_POOL} created"

  # ── Network egress rule + EAI (for dbt SL gRPC calls) ──────────────────────
  info "Creating network egress rule for ${DBT_SL_HOST}:443..."
  SQL "CREATE OR REPLACE NETWORK RULE ${SNOW_DATABASE}.${SNOW_SCHEMA}.SEMANTIC_DEMO_EGRESS_RULE
         TYPE       = HOST_PORT
         MODE       = EGRESS
         VALUE_LIST = ('${DBT_SL_HOST}:443')
         COMMENT    = 'Allow SPCS container to reach dbt Semantic Layer gRPC endpoint';"

  info "Creating external access integration..."
  SQL "CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION SEMANTIC_DEMO_EAI
         ALLOWED_NETWORK_RULES = (${SNOW_DATABASE}.${SNOW_SCHEMA}.SEMANTIC_DEMO_EGRESS_RULE)
         ENABLED               = TRUE
         COMMENT               = 'BSci Semantic Demo — dbt SL gRPC egress';"
  ok  "Network egress + EAI ready for ${DBT_SL_HOST}"

  # ── Secret: dbt SL auth token ───────────────────────────────────────────────
  info "Storing dbt SL auth token as Snowflake secret..."
  SQL "CREATE OR REPLACE SECRET ${SNOW_DATABASE}.${SNOW_SCHEMA}.SEMANTIC_DEMO_DBT_TOKEN
         TYPE          = GENERIC_STRING
         SECRET_STRING = '${DBT_SL_AUTH_TOKEN}'
         COMMENT       = 'dbt Semantic Layer service token for BSci Semantic Demo';"
  SQL "GRANT USAGE ON INTEGRATION SEMANTIC_DEMO_EAI
         TO ROLE ${SNOW_ROLE};"
  SQL "GRANT READ ON SECRET ${SNOW_DATABASE}.${SNOW_SCHEMA}.SEMANTIC_DEMO_DBT_TOKEN
         TO ROLE ${SNOW_ROLE};"
  ok  "Secret SEMANTIC_DEMO_DBT_TOKEN stored"
fi

# =============================================================================
# PHASE 2: BUILD & PUSH DOCKER IMAGE
# =============================================================================
section "Docker image"

# Resolve registry URL — try snow CLI first, fall back to SQL SHOW
info "Resolving image registry URL..."
REGISTRY_URL=$(
  snow spcs image-repository url "$IMAGE_REPO" \
    --connection "$SNOW_CONNECTION" \
    --database   "$SNOW_DATABASE"   \
    --schema     "$SNOW_SCHEMA"     \
    2>/dev/null | tr -d '[:space:]' || true
)

if [[ -z "$REGISTRY_URL" ]]; then
  REGISTRY_URL=$(
    SQL "SHOW IMAGE REPOSITORIES IN SCHEMA ${SNOW_DATABASE}.${SNOW_SCHEMA};" 2>/dev/null \
    | grep -i "$IMAGE_REPO" | awk '{print $NF}' | head -1 || true
  )
fi

[[ -n "$REGISTRY_URL" ]] \
  || die "Could not resolve image registry URL. Verify role permissions and that the image repo exists."

REGISTRY_HOST="${REGISTRY_URL%%/*}"
FULL_IMAGE_TAG="${REGISTRY_URL}/bsc-semantic-demo:latest"

info "Registry:  ${REGISTRY_URL}"
info "Image tag: ${FULL_IMAGE_TAG}"

# ── Docker login ─────────────────────────────────────────────────────────────
info "Logging Docker into Snowflake image registry..."
if snow spcs image-registry login \
     --connection "$SNOW_CONNECTION" \
     --role       "$SNOW_ROLE"       \
     2>/dev/null; then
  ok "Docker logged in via Snow CLI"
else
  warn "Automatic registry login failed. Attempting manual login to ${REGISTRY_HOST}..."
  warn "Use your Snowflake username and password/token when prompted."
  docker login "$REGISTRY_HOST" \
    || die "Docker login to ${REGISTRY_HOST} failed. Check credentials."
fi

# ── Build ────────────────────────────────────────────────────────────────────
info "Building Docker image for linux/amd64..."
docker build \
  --platform linux/amd64 \
  --tag      "$FULL_IMAGE_TAG" \
  --file     "$PROJECT_DIR/Dockerfile" \
  "$PROJECT_DIR"
ok "Image built"

# ── Push ─────────────────────────────────────────────────────────────────────
info "Pushing image to Snowflake registry (may take a few minutes on first push)..."
docker push "$FULL_IMAGE_TAG"
ok "Image pushed: ${FULL_IMAGE_TAG}"

# =============================================================================
# PHASE 3: SPCS SERVICE SPEC + DEPLOY
# =============================================================================
section "SPCS service"

info "Generating service spec..."
cat > "$SPEC_FILE" <<SPEC
spec:
  containers:
    - name: semantic-demo
      image: ${FULL_IMAGE_TAG}
      env:
        DBT_SL_ENVIRONMENT_ID:     "${DBT_SL_ENVIRONMENT_ID}"
        DBT_SL_HOST:               "${DBT_SL_HOST}"
        SNOWFLAKE_WAREHOUSE:       "${SNOW_WAREHOUSE}"
        SNOWFLAKE_DATABASE:        "${SNOW_DATABASE}"
        SNOWFLAKE_SCHEMA:          "${SNOW_SCHEMA}"
        SNOWFLAKE_FEEDBACK_DB:     "${SNOW_DATABASE}"
        SNOWFLAKE_FEEDBACK_SCHEMA: "${FEEDBACK_SCHEMA}"
        CORTEX_MODEL:              "${CORTEX_MODEL}"
        MAX_RESULT_ROWS:           "${MAX_RESULT_ROWS}"
      secrets:
        - snowflakeSecret:
            objectName: "${SNOW_DATABASE}.${SNOW_SCHEMA}.SEMANTIC_DEMO_DBT_TOKEN"
          envVarName: DBT_SL_AUTH_TOKEN
      readinessProbe:
        port: 8501
        path: /_stcore/health
  endpoints:
    - name: streamlit
      port: 8501
      public: true
  externalAccessIntegrations:
    - SEMANTIC_DEMO_EAI
SPEC

if [[ "$MODE" == "upgrade" ]]; then
  info "Upgrading service ${SERVICE_NAME} (zero-downtime spec + image update)..."
  SPCS service upgrade "$SERVICE_NAME" \
    --spec-path "$SPEC_FILE" \
    || die "Service upgrade failed. Verify service exists: snow spcs service status ${SERVICE_NAME}"
  ok "Service ${SERVICE_NAME} upgraded"
else
  info "Creating SPCS service ${SERVICE_NAME}..."
  SPCS service create "$SERVICE_NAME" \
    --spec-path     "$SPEC_FILE"    \
    --compute-pool  "$COMPUTE_POOL" \
    --min-instances 1               \
    --max-instances 1               \
    2>/dev/null || {
      warn "CREATE SERVICE returned non-zero (may already exist) — retrying as upgrade..."
      SPCS service upgrade "$SERVICE_NAME" --spec-path "$SPEC_FILE" \
        || die "Both create and upgrade failed. Check: snow spcs service status ${SERVICE_NAME} --connection ${SNOW_CONNECTION}"
    }
  ok "Service ${SERVICE_NAME} deployed"
fi

# =============================================================================
# PHASE 4: FEEDBACK SCHEMA BOOTSTRAP
# =============================================================================
section "Feedback schema"

info "Bootstrapping tables + views in ${SNOW_DATABASE}.${FEEDBACK_SCHEMA}..."
# Substitute actual DB/schema if the customer uses different names than the file defaults
sed \
  -e "s|ANALYTICS\.SEMANTIC_DEMO_FEEDBACK|${SNOW_DATABASE}.${FEEDBACK_SCHEMA}|g" \
  -e "s|DATABASE IF NOT EXISTS ANALYTICS|DATABASE IF NOT EXISTS ${SNOW_DATABASE}|g" \
  "$PROJECT_DIR/app/feedback/schema.sql" > "$FEEDBACK_SQL_TMP"

snow sql \
  --connection "$SNOW_CONNECTION" \
  --role       "$SNOW_ROLE"       \
  --warehouse  "$SNOW_WAREHOUSE"  \
  --format     plain              \
  -f           "$FEEDBACK_SQL_TMP"

ok "Feedback tables + views ready"

# =============================================================================
# PHASE 5: WAIT FOR SERVICE + PRINT ENDPOINT
# =============================================================================
section "Waiting for service to become active"

info "Polling (cold start typically takes 3–7 min)..."
STATUS="PENDING"
ATTEMPT=0
MAX_ATTEMPTS=42   # 7 minutes at 10s intervals

while [[ "$STATUS" != "READY" && $ATTEMPT -lt $MAX_ATTEMPTS ]]; do
  STATUS=$(
    SPCS service status "$SERVICE_NAME" --format json 2>/dev/null \
    | python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
    rows = d if isinstance(d, list) else [d]
    print(rows[0].get('status', 'UNKNOWN'))
except Exception:
    print('UNKNOWN')
" 2>/dev/null || echo "UNKNOWN"
  )
  ATTEMPT=$((ATTEMPT + 1))
  printf "\r  Status: %-22s  (attempt %d/%d)" "$STATUS" "$ATTEMPT" "$MAX_ATTEMPTS"
  [[ "$STATUS" == "READY" ]] || sleep 10
done
echo

if [[ "$STATUS" != "READY" ]]; then
  warn "Service has not reached READY state after ${MAX_ATTEMPTS} polls."
  warn "Continue monitoring with: snow spcs service status ${SERVICE_NAME} --connection ${SNOW_CONNECTION}"
fi

# Resolve public endpoint URL
ENDPOINT=$(
  SPCS service list-endpoints "$SERVICE_NAME" --format json 2>/dev/null \
  | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    rows = data if isinstance(data, list) else [data]
    for ep in rows:
        name = ep.get('name', ep.get('endpoint_name', '')).lower()
        url  = ep.get('ingress_url', ep.get('url', ''))
        if url:
            print(url)
            break
except Exception:
    pass
" 2>/dev/null || true
)

# =============================================================================
# DONE
# =============================================================================
hr
printf "\n${GRN}${BOLD}  Deployment complete!${NC}\n\n"

if [[ -n "$ENDPOINT" ]]; then
  printf "  ${BOLD}App URL:${NC}      https://%s\n" "$ENDPOINT"
else
  printf "  ${BOLD}App URL:${NC}      Snowflake console → Snowpark Container Services → %s → Endpoints\n" "$SERVICE_NAME"
fi

printf "  ${BOLD}Service:${NC}      %s.%s.%s\n"  "$SNOW_DATABASE" "$SNOW_SCHEMA"    "$SERVICE_NAME"
printf "  ${BOLD}Compute pool:${NC} %s\n"          "$COMPUTE_POOL"
printf "  ${BOLD}Feedback DB:${NC}  %s.%s\n\n"   "$SNOW_DATABASE" "$FEEDBACK_SCHEMA"

printf "  Useful commands:\n"
printf "    Logs:     snow spcs service logs %s --container-name semantic-demo --connection %s\n" "$SERVICE_NAME" "$SNOW_CONNECTION"
printf "    Status:   snow spcs service status %s --connection %s\n" "$SERVICE_NAME" "$SNOW_CONNECTION"
printf "    Upgrade:  %s/scripts/deploy.sh --upgrade\n" "$PROJECT_DIR"
printf "    Teardown: %s/scripts/deploy.sh --teardown\n" "$PROJECT_DIR"
hr
echo
