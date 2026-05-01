import os

# Snowflake
SNOWFLAKE_ACCOUNT: str = os.getenv("SNOWFLAKE_ACCOUNT", "")
SNOWFLAKE_USER: str = os.getenv("SNOWFLAKE_USER", "")
SNOWFLAKE_ROLE: str = os.getenv("SNOWFLAKE_ROLE", "")
SNOWFLAKE_WAREHOUSE: str = os.getenv("SNOWFLAKE_WAREHOUSE", "DEMO_WH")
SNOWFLAKE_DATABASE: str = os.getenv("SNOWFLAKE_DATABASE", "DEMO_BSC")
SNOWFLAKE_SCHEMA: str = os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC")

# Cortex
CORTEX_ROUTER_MODEL: str = os.getenv("CORTEX_ROUTER_MODEL", "llama3.3-70b")
CORTEX_SKILL_MODEL: str = os.getenv("CORTEX_SKILL_MODEL", "mistral-7b")
CORTEX_TIMEOUT_S: int = int(os.getenv("CORTEX_TIMEOUT_S", "30"))

# dbt Semantic Layer
DBT_SL_HOST: str = os.getenv("DBT_SL_HOST", "semantic-layer.cloud.getdbt.com")
DBT_ENVIRONMENT_ID: str = os.getenv("DBT_ENVIRONMENT_ID", "")

# Query limits
QUERY_MAX_ROWS: int = int(os.getenv("QUERY_MAX_ROWS", "500"))
QUERY_TIMEOUT_S: int = int(os.getenv("QUERY_TIMEOUT_S", "30"))
MAX_CANDIDATES: int = int(os.getenv("MAX_CANDIDATES", "20"))
DEFAULT_TOP_N: int = int(os.getenv("DEFAULT_TOP_N", "5"))
RERANK_CACHE_TTL_S: int = int(os.getenv("RERANK_CACHE_TTL_S", "300"))

# Schema allowlist (extend with ANALYTICS_DB for dbt-compiled MetricFlow SQL)
ALLOWED_SCHEMAS: list[str] = ["DEMO_BSC"]
ANALYTICS_DB: str = os.getenv("ANALYTICS_DB", "")
if ANALYTICS_DB:
    ALLOWED_SCHEMAS.append(ANALYTICS_DB)

# Prompt versions
ROUTER_PROMPT_VERSION: str = os.getenv("ROUTER_PROMPT_VERSION", "v1")
RERANK_PROMPT_VERSION: str = os.getenv("RERANK_PROMPT_VERSION", "v1")
